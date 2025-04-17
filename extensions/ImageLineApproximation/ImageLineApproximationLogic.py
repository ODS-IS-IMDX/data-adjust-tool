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
pandas = import_module("pandas")
cv2 = import_module("cv2")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic
from raster_to_vector.common.image_utils import *


class ImageLineApproximationLogic(FlexibleRasterVectorLogic):
    def __init__(self):
        pass
        
    def input_check(self, byte_data, attribute):
        """
        入力データと属性の妥当性を確認します。

        Parameters
        ----------
        byte_data : bytes
            入力される画像データのバイト列です。
        attribute : dict
            画像の属性情報を含む辞書です。ColorSpace 属性が含まれている必要があります。

        Raises
        ------
        Exception
            byte_data が None の場合や ColorSpace が設定されていない場合に例外を発生させます。
        """
        if byte_data is None:
            raise Exception('入力データが設定されていません')

        if not attribute or attribute.get('ColorSpace') is None:
            raise Exception('attributeにColorSpaceが設定されていません')

    def get_line_thickness(self, line_list, image, line_thickness_threshold=200):
        if image.ndim == 3:
            # RGB -> GrayScale化
            img_gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
        else:
            img_gray = image

        thickness_list = calc_line_thickness(line_list, img_gray, line_thickness_threshold)

        return thickness_list

    def __call__(self, byte_data, attribute, properties):
        """
        入力データの管路抽出結果から管路を分離し、分離した管路を直線近似します。
        直線近似の前のコーナー検出でコーナーが検出できなかった場合は空のリストのバイト列を返します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
             画像データに関連する属性情報の辞書です。ColorSpace が BINARY GRAYSCALE である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - max_corners : int
                検出されるコーナーの最大数が設定されます。

            - quality_level : float
                コーナーを判断するための最小の品質が設定されます。

            - min_distance : int
                コーナー間の最小の距離(ピクセル)が設定されます。

            - block_size : int
                コーナー検出に使うスムージングの近傍サイズが設定されます。

            - circle_size : int
                コーナー毎に円を描画して管路を分離する際の円のサイズが設定されます。

            - is_measure_thickness : bool
                線の検出と同時に線の太さも検出するフラグが設定されます。

            - line_thickness_threshold : int
                線の太さを検出する際に、この閾値以下のピクセルを線の内部の点として計算する値が設定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_line_list: string
                検出した線分を出力するFieldSetFileの項目名が指定されます。

            - fsf_image_org: string
                線の太さを検出する際に利用する元画像かGRAYSCALE画像が格納されているFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                管路を分離後に直線近似をした結果リスト([(始点X座標, 始点Y座標)]または[(始点X座標, 始点Y座標), 太さ])のバイト列です。

            dict
                更新された属性情報を含む辞書です。ColorSpace は 元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """

        self.input_check(byte_data, attribute)

         # 入出力項目取得
        fsf_image_src = properties.get('fsf_image_src', 'content')
        fsf_line_list = properties.get('fsf_line_list', 'content')
        fsf_image_org = properties.get('fsf_image_org', 'content')

        # 線の太さを計測するかどうかを指定するフラグ
        is_measure_thickness = properties.get('is_measure_thickness', 'False') == 'True'

        # 入力データ取得
        if type(byte_data) is pandas.core.series.Series:
            try:
                image = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            image = pickle.loads(byte_data)

        color_space = attribute['ColorSpace']
        if color_space != 'BINARY' and color_space != 'GRAYSCALE':
            raise Exception('BINARY, GRAYSCALE以外のColorSpaceが設定されています')
        
        # ノイズ除去
        cv2.medianBlur(image, ksize=3)

        # 画像端の管路が画像端の輪郭と同じにならないようにパディング
        pad = 10
        image_pad = cv2.copyMakeBorder(image, pad, pad, pad, pad, cv2.BORDER_CONSTANT, (255, 255, 255))
        
        # プロパティから値を取得
        try:
            max_corners = int(properties.get('max_corners', 50))
            quality_level = float(properties.get('quality_level', 0.3))
            min_distance = int(properties.get('min_distance', 5))
            block_size = int(properties.get('block_size', 5))
            circle_size = int(properties.get('circle_size', 4))
            line_thickness_threshold = int(properties.get('line_thickness_threshold', 200))
        except Exception as e:
            raise ValueError(f'プロパティの取得時にエラーが発生しました: {e}')

        # コーナー検出の最大数
        if not (0 <= max_corners):
            raise ValueError("max_cornersは0以上である必要があります")
        # コーナー検出の最小品質
        if not (0.0 < quality_level <= 1.0):
            raise ValueError("quality_levelは0.0より大きく1.0以下である必要があります")
        # コーナー間の最小距離
        if not (0 <= min_distance):
            raise ValueError("min_distanceは0以上である必要があります")
        # コーナー検出の際のスムージング近傍サイズ
        if not (0 < block_size):
            raise ValueError("block_sizeは0より大きい必要があります")
        # コーナーで分離する際の円のサイズ
        if not (0 < circle_size):
            raise ValueError("circle_sizeは0より大きい必要があります")

        if not (line_thickness_threshold in range(0, 256)):
            raise ValueError("line_thickness_thresholdは0 ~ 255の範囲で指定する必要があります")

        # コーナー検出
        corners = cv2.goodFeaturesToTrack(image_pad ,max_corners, quality_level,
                                        min_distance, blockSize=block_size, useHarrisDetector=False)
        if corners is not None:
            corners = np.int32(corners)
        else:
            # コーナーが見つからなければ空のリストを返す
            if type(byte_data) is pandas.core.series.Series:
                byte_data[fsf_line_list] = pickle.dumps([])
                new_byte_data = byte_data
            else:
                new_byte_data = pickle.dumps([])

            return new_byte_data, attribute
        
        # 検出したコーナー位置に円を描画
        for i in corners:
            corner_x, corner_y = i.ravel()
            cv2.circle(image_pad, (corner_x, corner_y), circle_size, (0), -1)

        # ラベリング
        _, labels, stats, _ = cv2.connectedComponentsWithStats(image_pad)

        points_data = np.array([], dtype=np.int32)
        area = 10
        # ラベルごとに処理
        for label_index in range(labels.max() + 1):
            # 背景と面積が10以下の領域は除く
            if label_index == 0 or stats[label_index][4] < area:
                continue
            
            # 分離した管路ごとに輪郭抽出を行う
            label_group_index = np.where(labels == label_index)
            image_pad_tmp = np.ones_like(image_pad) * 255
            image_pad_tmp[label_group_index] = (0)
            contours, _ = cv2.findContours(image_pad_tmp, cv2.RETR_CCOMP, cv2.CHAIN_APPROX_NONE)

            #面積の降順にソート
            list_contours = list(contours)
            list_contours.sort(key=lambda x: cv2.contourArea(x), reverse=True)
            contours = tuple(list_contours)

            # 輪郭から直線近似を行う
            [vx, vy, x, y] = cv2.fitLine(contours[1], cv2.DIST_L2, 0, 0.01, 0.01)
            rect = cv2.minAreaRect(contours[1])

            # 直線の始点と終点を計算
            rect_width = round(rect[1][0])
            rect_height = round(rect[1][1])
            length = math.sqrt(rect_width**2 + rect_height**2)
            start_point = (int(x[0] - vx[0] * length / 2), int(y[0] - vy[0] * length / 2))
            end_point = (int(x[0] + vx[0] * length / 2), int(y[0] + vy[0] * length / 2))
            points_data = np.append(points_data, (start_point, end_point))
        
        # 後フロー用に次元を変更
        transformed_data = points_data.reshape(int(points_data.shape[0] / 4), 4)
        transformed_no_pad_data = []
        for row in transformed_data:
            x1, y1, x2, y2 = row
            adj_point = [(x1 - pad, y1 - pad), (x2 - pad, y2 - pad)]
            transformed_no_pad_data.append(adj_point)

        thickness_list = []
        if is_measure_thickness:
            try:
                org_img = pickle.loads(byte_data[fsf_image_org])
            except Exception as e:
                raise ValueError(f'線の太さの検出には元画像か元画像のgray scale画像が必要ですが、取得できませんでした: {e}')

            try:
                thickness_list = self.get_line_thickness(transformed_no_pad_data, org_img, line_thickness_threshold)
            except Exception as e:
                raise ValueError(f'線の太さの計測に失敗しました: {e}')

            if len(thickness_list) != len(transformed_no_pad_data):
                raise ValueError("線分の数と太さのデータの整合性がとれませんでした")

        if 0 < len(thickness_list):
            pixel_lines = [
                [row[0], row[1], thickness] for row, thickness in zip(transformed_no_pad_data, thickness_list)
            ]

        else:
            pixel_lines = [
                [row[0], row[1]] for row in transformed_no_pad_data
            ]

        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_line_list] = pickle.dumps(pixel_lines)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(pixel_lines)

        return new_byte_data, attribute

