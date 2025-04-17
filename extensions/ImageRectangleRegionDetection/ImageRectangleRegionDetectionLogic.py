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
from importlib import import_module

# 外部ライブラリの動的インポート
cv2 = import_module("cv2")
np = import_module("numpy")
pandas = import_module("pandas")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ImageRectangleRegionDetectionLogic(FlexibleRasterVectorLogic):
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

    def __call__(self, byte_data, attribute, properties):
        """
        矩形を検出し、座標データを返します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が BINARY である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - return_type: string
                検出した矩形座標の形式が設定されます。

            - min_area: int
                検出する矩形の最小面積(縦pixel x 横pixel)が設定されます。

            - max_area: int
                検出する矩形の最大面積(縦pixel x 横pixel)が設定されます。

            - sort_largest: bool
                応答する矩形座標の順番を矩形面積の大きい順か小さい順かが設定されます。

            - limit_count: int
                応答する矩形座標の数が設定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_detection_data: string
                検出した矩形座標を出力するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                検出した矩形座標のバイト列です。

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
        fsf_detection_data = properties.get('fsf_detection_data', 'content')

        # 入出力情報データ取得
        if type(byte_data) is pandas.core.series.Series:
            try:
                image = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            image = pickle.loads(byte_data)

        # 画像の色空間チェック
        color_space = attribute['ColorSpace']
        if color_space != 'BINARY':
            raise Exception(f'BINARY以外のColorSpaceが設定されています: {color_space}')

        # プロパティ値取得
        # 検出した矩形座標を応答する形式
        self.return_type = properties.get('return_type', 'CONTOUR')
        # 検出する矩形の最小面積
        try:
            self.min_area = int(properties.get('min_area', 0))
        except:
            raise ValueError(f'無効なmin_areaが選択されています: {properties.get("min_area")}')
        if self.min_area < 0:
            raise Exception(f'min_areaは0以上の整数を設定する必要があります: {properties.get("min_area")}')
        # 検出する矩形の最大面積
        try:
            self.max_area = int(properties.get('max_area', -1))
        except:
            raise ValueError(f'無効なmax_areaが選択されています: {properties.get("max_area")}')
        if self.max_area < -1:
            raise Exception(f'max_areaは-1以上の整数を設定する必要があります: {properties.get("max_area")}')
        if self.max_area != -1:
            if self.max_area <= self.min_area:
                raise Exception(f'min_areaとmax_areaの大小関係はmin_area < max_areaである必要があります: min_area={properties.get("min_area")}, max_area={properties.get("max_area")}')
        # 応答する矩形座標の順番
        sort_largest = properties.get('sort_largest', 'true')  # リスト選択式ではあるがチェック処理を実装しておく
        if sort_largest.lower() == 'true':
            self.sort_largest = True
        elif sort_largest.lower() == 'false':
            self.sort_largest = False
        else:
            raise Exception(f'sort_largestはTrueまたはFalseである必要があります: {properties.get("sort_largest")}')
        # 応答する矩形座標の数
        try:
            self.limit_count = int(properties.get('limit_count', -1))
        except:
            raise ValueError(f'無効なlimit_countが選択されています: {properties.get("limit_count")}')
        if self.limit_count < -1:
            raise Exception(f'limit_countは-1以上の整数を設定する必要があります: {properties.get("limit_count")}')

        # 輪郭抽出
        contours, hierarchy = cv2.findContours(image, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        # 輪郭抽出結果のフィルタリング
        selected_contours = self.filter_contours(contours, hierarchy)

        # 指定されて応答形式に変換
        if self.return_type == 'CONTOUR':
            transformed_list = selected_contours
        elif self.return_type == 'RECT':
            transformed_list = []
            for contour in selected_contours:
                x, y, w, h = cv2.boundingRect(contour)
                top_left = (x, y)
                bottom_right = (x + w, y + h)
                transformed_list.append([top_left, bottom_right])
        else:
            raise Exception(f'無効なreturn_typeが選択されています: {properties.get("return_type")}')

        # 応答データ作成
        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_detection_data] = pickle.dumps(transformed_list)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(transformed_list)

        return new_byte_data, attribute


    def filter_contours(self, contours, hierarchy):
        '''
        検出した輪郭情報を以下の３つの観点でフィルタリングする
          ・矩形形状に近い
          ・面積が指定範囲内
          ・面積大きい順にＮ個 or 小さい順位にＮ個
        '''
        # 枠の候補を保存
        border_candidates = []
        if hierarchy is not None:
            hierarchy = hierarchy[0]
            for i, (contour, h) in enumerate(zip(contours, hierarchy)):
                # 矩形に近いかを判断
                if self.is_contour_rect(contour):
                    area = cv2.contourArea(contour)
                    # 面積が指定範囲内かを判断
                    if self.is_area_within_range(area):
                        border_candidates.append({
                            'contour': contour,
                            'area': area,
                            'parent': h[3],
                            'index': i
                            })

        # 絞り込み結果の上位/下位を決定
        selected_contours = []
        if 0 < len(border_candidates):
            # 面積でソート
            border_candidates.sort(key=lambda x: x['area'], reverse=self.sort_largest)
            
            # 最大N個の枠を選択用
            used_indices = set()
            for candidate in border_candidates:
                # 既に使用済みのインデックスをスキップ
                if candidate['index'] in used_indices:
                    continue
                # 親が使用済みをスキップ
                if candidate['parent'] != -1 and candidate['parent'] in used_indices:
                    continue
                # 条件を満たす輪郭を追加
                selected_contours.append(candidate['contour'])
                used_indices.add(candidate['index'])
                # 指定数（n個）に達したら終了
                if len(selected_contours) == self.limit_count:
                    break

        # cv2.findContoursの戻り値の型に合わせてtuple型にする
        return tuple(selected_contours)

    def is_area_within_range(self, area):
        '''
        検出した輪郭の面積が指定範囲内かを判定する
        '''
        if self.max_area == -1:
            # 上限なし
            if self.min_area <= area:
                return True
        else:
            # 範囲指定
            if self.min_area <= area <= self.max_area:
                return True

    def is_contour_rect(self, contour):
        '''
        検出した輪郭が矩形の形状に近いかを判定する
        '''
        epsilon = 0.02 * cv2.arcLength(contour, True)  # 周長の2%を精度として設定
        approx = cv2.approxPolyDP(contour, epsilon, True)  # 輪郭を近似
        # 頂点が4なら矩形と判断
        if len(approx) != 4:
            return False
    
        # 角度チェック（直角に近いかどうか）
        angles = []
        for i in range(4):
            pt1 = approx[i][0]
            pt2 = approx[(i+1)%4][0]
            pt3 = approx[(i+2)%4][0]

            # 3点から角度を計算
            vec1 = pt1 - pt2
            vec2 = pt3 - pt2
            cos_angle = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
            angle = np.degrees(np.arccos(cos_angle))
            angles.append(angle)

        # 全ての角度が90度に近い(85度-95度)かチェック
        return all(abs(angle - 90) < 5 for angle in angles)
