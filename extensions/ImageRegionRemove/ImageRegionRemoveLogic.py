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


class ImageRegionRemoveLogic(FlexibleRasterVectorLogic):
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
        指定された領域（矩形座標またた輪郭座標）を除去します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が BINARY である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - remove_outline_only: string
                領域の枠のみを除去するか、領域内部まで除去するかが設定されます。

            - outline_thickness: int
                削除する際の枠の太さが設定されます。

            - fsf_remove_data: int
                除去する領域情報を取得するFieldSetFileの項目名が指定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。
            
            - fsf_image_dst: string
                処理した画像を出力するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                指定した領域を除去した画像のバイト列です。

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

        # 画像の色空間チェック
        color_space = attribute['ColorSpace']
        if color_space != 'BINARY':
            raise Exception('BINARY以外のColorSpaceが設定されています')

        # 入出力項目取得
        fsf_remove_data = properties.get('fsf_remove_data', 'detection_data')
        fsf_image_src = properties.get('fsf_image_src', 'content')
        fsf_image_dst = properties.get('fsf_image_dst', 'image_removed')

        # 入出力情報データ取得
        if type(byte_data) is pandas.core.series.Series:
            # 削除領域
            try:
                remove_data_list = pickle.loads(byte_data[fsf_remove_data])
            except:
                raise ValueError(f'fsf_remove_dataに無効な値が設定されています: {properties.get("fsf_remove_data")}')
            # 入力画像
            try:
                image = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            raise Exception('入力データはFieldSetFile形式である必要があります')

        # プロパティ値取得
        # 領域の枠のみを除去するか領域内部まで除去するかの指定
        remove_outline_only = properties.get('remove_outline_only', 'true')  # リスト選択式ではあるがチェック処理を実装しておく
        if remove_outline_only.lower() == 'true':
            self.remove_outline_only = True
        elif remove_outline_only.lower() == 'false':
            self.remove_outline_only = False
        else:
            raise Exception(f'remove_outline_onlyはTrueまたはFalseである必要があります: {properties.get("remove_outline_only")}')
        # 削除する領域の枠の太さの指定
        if remove_outline_only:
            # remove_outline_onlyがTrueの場合のみ使用
            try:
                self.outline_thickness = int(properties.get('outline_thickness', 3))
            except:
                raise ValueError(f'無効なoutline_thicknessが選択されています: {properties.get("outline_thickness")}')
            if self.outline_thickness <= 0:
                raise Exception(f'outline_thicknessは1以上の整数を設定する必要があります: {properties.get("outline_thickness")}')

        # マスク画像作成
        mask = self.make_mask(image, remove_data_list)

        # 画素除去
        image_remove_region = cv2.bitwise_and(image, image, mask=mask)  # マスク処理

        # 応答データ作成
        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_image_dst] = pickle.dumps(image_remove_region)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(image_remove_region)

        return new_byte_data, attribute

    def make_mask(self, image, remove_data_list):
        '''
        除去するためのマスク画像を作成
        '''
        mask = np.zeros(image.shape[:2], dtype=np.uint8)
        # 矩形座標の除去
        if type(remove_data_list) is list:
            for (top_left, bottom_right) in remove_data_list:
                if self.remove_outline_only:
                    # 矩形枠のみ（線として描画）のマスク
                    cv2.rectangle(mask, top_left, bottom_right, 255, self.outline_thickness)
                else:
                    # 矩形全体（塗りつぶし）のマスク
                    cv2.rectangle(mask, top_left, bottom_right, 255, -1)

        # 輪郭座標の除去
        elif type(remove_data_list) is tuple:
            for contour in remove_data_list:
                if self.remove_outline_only:
                    # 輪郭のみ（線として描画）のマスク
                    cv2.drawContours(mask, [contour], -1, 255, self.outline_thickness)
                else:
                    # 輪郭内部（塗りつぶし）のマスク
                    cv2.drawContours(mask, [contour], -1, 255, -1)
        else:
            raise Exception(f'削除領域データの形式はlist形式かtuple形式である必要があります: {type(remove_data_list)}')

        return cv2.bitwise_not(mask)  # マスク画像反転(黒背景白画素作成)
