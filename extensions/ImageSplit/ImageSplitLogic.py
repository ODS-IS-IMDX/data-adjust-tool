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
Image = import_module('PIL.Image')

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic

class ImageSplitLogic(FlexibleRasterVectorLogic):
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
        画像を指定した分割数で分割します。
        分割した後の画像で円や直線の座標を元の大きい画像に適用できる座標を保存します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace の指定はありません。
        properties : dict
            プロパティ設定を含む辞書です。
            - width_division_num: int
                画像を横方向へ分割する数が設定されます。

            - height_division_num: int
                画像を縦方向へ分割する数が設定されます。

            - overlap_region: int
                画像を分割した際の縦横の重なり領域が設定されます。
            
            - offset_coordinate_x: string
                オフセットのX座標を格納するFieldSetFileの項目名が指定されます。

            - offset_coordinate_y: string
                オフセットのY座標を格納するFieldSetFileの項目名が指定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                分割された画像(リスト形式)のバイト列です。

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
        offset_coordinate_x = properties.get('offset_coordinate_x', 'offset_coordinate_x')
        offset_coordinate_y = properties.get('offset_coordinate_y', 'offset_coordinate_y')
        fsf_image_src = properties.get('fsf_image_src', 'content')

        # 入力データ取得
        if type(byte_data) is pandas.core.series.Series:
            try:
                image = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            raise Exception('入力データはFieldSetFile形式である必要があります')

        # プロパティ取得
        try:
            width_division_num = int(properties.get('width_division_num', 10))
            height_division_num = int(properties.get('height_division_num', 10))
            overlap_region = int(properties.get('overlap_region', 500))
        except Exception as e:
            raise ValueError(f'プロパティの取得時にエラーが発生しました: {e}')
        
        if width_division_num <= 0:
            raise ValueError("width_division_numは0より大きい数である必要があります")
        if height_division_num <= 0:
            raise ValueError("height_division_numは0より大きい数である必要があります")
        if overlap_region < 0:
            raise ValueError("overlap_regionは0以上である必要があります")

        height = image.shape[0]
        width = image.shape[1]
        increase_height = height // height_division_num
        increase_width = width // width_division_num

        split_image_df_list = []
        # 画像を分割
        for row in range(height_division_num):
            for column in range(width_division_num):
                y1 = row * increase_height
                y2 = (row + 1) * increase_height + overlap_region
                x1 = column * increase_width
                x2 = (column + 1) * increase_width + overlap_region
                if y2 > height:
                    y2 = height
                if x2 > width:
                    x2 = width

                offset_x = column * increase_width
                offset_y = row * increase_height

                # pandas.Seriesに変換
                content_series = pandas.Series({'content': pickle.dumps(image[y1 : y2, x1 : x2])})
                offset_x_series = pandas.Series({offset_coordinate_x: offset_x}, dtype='int64')
                offset_y_series = pandas.Series({offset_coordinate_y: offset_y}, dtype='int64')
                # それぞれの pandas.Series を一つの辞書形式で格納
                combined_series = pandas.Series({'content': content_series['content'],
                                                'color_space': byte_data['color_space'],
                                                'layer': byte_data['layer'],
                                                'Findex': byte_data['Findex'],
                                                'offset_coordinate_x': offset_x_series['offset_coordinate_x'],
                                                'offset_coordinate_y': offset_y_series['offset_coordinate_y']
                                                })
                split_image_df_list.append(combined_series)

        # ImageMergelogicで使用する値をattribute追加
        attribute['height_division_num'] = str(height_division_num)
        attribute['width_division_num'] = str(width_division_num)
        attribute['overlap_region'] = str(overlap_region)
        attribute['offset_coordinate_x'] = offset_coordinate_x
        attribute['offset_coordinate_y'] = offset_coordinate_y

        return split_image_df_list, attribute