# MIT License
# 
# Copyright (c) 2025 NTT InfraNet
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Python標準ライブラリ
import pickle
import math
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
cv2 = import_module("cv2")

from raster_to_vector.common.base_raster_vector_logic import BaseRasterVectorLogic


class CalculateAngleDirectionSymbolLogic(BaseRasterVectorLogic):
    def __init__(self):
        pass
        
    def input_check(self, data_path, attribute):
        """
        入力データと属性の妥当性を確認します。

        Parameters
        ----------
        byte_data : bytes
            入力されるデータのバイト列です。
        attribute : dict
            画像の属性情報を含む辞書です。ColorSpace 属性が含まれている必要があります。

        Raises
        ------
        Exception
            byte_data が None の場合や ColorSpace が設定されていない場合に例外を発生させます。
        """
        if data_path is None:
            raise Exception('入力データが設定されていません')
        
        if not attribute or attribute.get('ColorSpace') is None:
            raise Exception('attributeにColorSpaceが設定されていません')

    def __call__(self, byte_data, attribute, properties):
        """
        輪郭検出された輪郭データに対してひし形(四角形)近似を行い、真北まで時計回りに回転させるための必要な角度を返します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の輪郭座標を持つFieldSetFileである必要があります。
        attribute : dict
            画像の属性情報を含む辞書です。'ColorSpace' キーが含まれます。
        properties : dict
            プロパティ設定です。このプロセッサでは使用しません。
        
        Returns
        -------
        tuple
            - new_byte_data : bytes
                真北まで時計回りに回転させるための必要な角度のバイト列です。
                - angle_out: 真北まで時計回りに回転させるための必要な角度（int型）。
            - attribute : dict
                更新された属性情報を含む辞書です。 ColorSpace は元の値を引き継ぎます。
        
        Raises
        ------
        Exception
            - 入力データが無効な場合や、交点や探索矩形が見つからない場合に発生します。
        """

        self.input_check(byte_data, attribute)

        try:
            contours = pickle.loads(byte_data)
        except:
            raise Exception('輪郭データを入力してください')

        # 輪郭データを大きい順にソート
        list_contours = list(contours)
        list_contours.sort(key=lambda x: cv2.contourArea(x), reverse=True)
        contours = tuple(list_contours)
        
        # 輪郭データから凸包を取得
        # contours[0]は画像全体の外枠になるので使わない
        approx_rate = 0.02
        hull = cv2.convexHull(contours[1])
        epsilon = approx_rate * cv2.arcLength(hull, True)
        approx = cv2.approxPolyDP(hull, epsilon, True)

        # 方位シンボルを囲うひし形から対角の座標を取得
        if len(approx) == 4:
            points = approx.reshape(4,2)
            d1_start =  points[0]
            d1_end = points[2]
            d2_start =  points[1]
            d2_end = points[3]

            # 対角の座標から長さを取得
            length_d1 = np.linalg.norm(d1_start - d1_end)
            length_d2 = np.linalg.norm(d2_start - d2_end)

            # 長いほうの対角線を方位シンボルの軸線とする
            if length_d1 > length_d2:
                axis_line_start = d1_start
                axis_line_end = d1_end
            else:
                axis_line_start = d2_start
                axis_line_end = d2_end
            
            # 対角線の交点を計算
            intersection_point = self.intersection(d1_start, d1_end, d2_start, d2_end)
            if intersection_point is None:
                raise Exception('交点が見つかりませんでした')

            # 対角線の交点と軸線の始点、終点との距離を取得
            distance_intersection1 = np.linalg.norm(axis_line_start - intersection_point)
            distance_intersection2 = np.linalg.norm(axis_line_end - intersection_point)

            # 輪郭データから三角形を近似し、検出した三角形をカウントする
            approx_rate = 0.06
            triangle = 0
            for cnt in contours:
                epsilon_tri = approx_rate * cv2.arcLength(cnt, True)
                approx_tri = cv2.approxPolyDP(cnt, epsilon_tri, True)
                if len(approx_tri) == 3: 
                    triangle += 1
                    approx_arrow = approx_tri

            # 方位シンボルで矢じりの三角形を1つだけ検出した場合はその三角形の重心から始点・終点で近いほうを北向きとする
            if triangle == 1:
                # 重心を計算
                m = cv2.moments(approx_arrow)
                mx, my = m['m10'] / m['m00'] , m['m01'] / m['m00']
                # 座標を四捨五入
                m_tri = round(mx), round(my)
                # 矢じりの三角形の重心と軸線の始点、終点との距離を取得
                distance_attention_point1 = np.linalg.norm(axis_line_start - m_tri)
                distance_attention_point2 = np.linalg.norm(axis_line_end - m_tri)
                # 
                if distance_attention_point1 < distance_attention_point2:
                    axis_line_end, axis_line_start = axis_line_start, axis_line_end
            # 矢じりの三角形を0または複数検出した場合はひし形の対角線の交点から始点・終点で遠いほうを北向きとする
            elif distance_intersection1 > distance_intersection2:
                axis_line_end, axis_line_start = axis_line_start, axis_line_end

            # 角度計算
            angle = math.degrees(math.atan2(axis_line_end[1] - axis_line_start[1], axis_line_end[0] - axis_line_start[0]))

            # -180°~180°を0°~360°に変換
            if angle < -90 and angle >= -180:
                angle2 = round(angle) + 450
            else:
                angle2 = round(angle) + 90

            # 時計周りに何度回転するか計算
            angle_out = 360 - angle2

            new_byte_data = pickle.dumps(int(angle_out))
        else:
            raise Exception('探索矩形が見つかりませんでした')

        return new_byte_data, attribute

    def intersection(self, p1, p2, p3, p4):
        # 分子の要素を計算
        A1 = p2[1] - p1[1]
        B1 = p1[0] - p2[0]
        C1 = A1 * p1[0] + B1 * p1[1]
        A2 = p4[1] - p3[1]
        B2 = p3[0] - p4[0]
        C2 = A2 * p3[0] + B2 * p3[1]

        # 分母を計算
        determinant = A1 * B2 - A2 * B1

        # 交わらない場合
        if determinant == 0:
            return None

        # 交点ベクトルのx, y
        x = (B2 * C1 - B1 * C2) / determinant
        y = (A1 * C2 - A2 * C1) / determinant

        return int(x), int(y)
