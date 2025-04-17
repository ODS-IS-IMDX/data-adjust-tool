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
pandas = import_module("pandas")
np = import_module("numpy")
cv2 = import_module("cv2")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ImageColorExtractionLogic(FlexibleRasterVectorLogic):
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

    def __call__(self, byte_data, attribute, properties):
        """
        RGB画像データに対して指定された色情報の画素を抽出します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が RGB である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - color_range : string
                画素抽出に使用する色情報の範囲(RGB Value)の±が設定されます。

            - color_name : string
                抽出したい色の名前が設定されます。

            - rgb_value : string
                抽出したい色のRGB値が設定されます

            - fsf_image_src : string
                処理する画像を取得するFieldSetFileの項目名が指定されます。
            
            - other_color_image_output : bool
                Color Nameで指定した色以外の画素をother画像(RGB形式)として出力するかどうかのフラグです。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                画素抽出した結果画像のバイト列です。
                
            - attribute : dict
                更新された属性情報です。'ColorSpace' は'BINARY'を返します。

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
        # 入力データ取得
        if type(byte_data) is pandas.core.series.Series:
            try:
                image = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            image = pickle.loads(byte_data)

        color_space = attribute['ColorSpace']
        if color_space != 'RGB':
            raise Exception('RGB以外のColorSpaceが設定されています')

        # プロパティから値を取得
        try:
            color_range = int(properties.get('color_range', 20))
        except:
            raise ValueError(f'color_rangeに無効な値が設定されています: {properties.get("color_range")}')
        if color_range < 0 or color_range > 255:
            raise ValueError(f'color_rangeは0~255の値を指定してください: {color_range}')
        
        color_name = properties.get('color_name', '')
        rgb_value = properties.get('rgb_value', '')
        if color_name == '' or rgb_value == '':
            raise ValueError('color_nameまたはrgb_valueに抽出したい色情報が設定されていません')
        if 'other' in color_name:
            raise ValueError('color_nameにotherが入力されています')

        other_output = properties.get('other_color_image_output', 'false')  # リスト選択式ではあるがチェック処理を実装しておく
        if other_output.lower() == 'true':
            other_output = True
        elif other_output.lower() == 'false':
            other_output = False
        else:
            raise Exception(f'other_color_image_outputはTrueまたはFalseである必要があります: {properties.get("other_color_image_output")}')

        # 空白を削除し、カンマ後ろにスペースを追加
        color_name_replace = color_name.replace(' ', '').replace(',', ', ')
        rgb_value_replace = rgb_value.replace(' ', '').replace('],', '], ')
        # 先頭と末尾の[]を削除し、カンマとスペース部分で区切る
        color_name_split = [name for name in color_name_replace.split(', ')]
        rgb_value_split = [num for num in rgb_value_replace.split(', ')]
        if len(color_name_split) != len(rgb_value_split):
            raise ValueError(f'color_name:{len(color_name_split)}とrgb_value:{len(rgb_value_split)}の要素数が異なります')

        mask_list = []
        for index_name, rgb in zip(color_name_split, rgb_value_split):
            # color_nameの[]の有無が正しいか確認
            if color_name.count('[') >= 1 or color_name.count(']') >= 1:
                raise ValueError(f'color_nameの[]の数が不正です:{color_name}')
            # rgbの[]の有無が正しいか確認
            if not rgb.count('[') == 1 or not rgb.count(']') == 1:
                raise ValueError(f'rgb_valueの[]の数が不正です:{rgb}')
            
            # 先頭と末尾の[]を削除し、カンマで区切る
            rgb = rgb[1:-1].split(',')
            try:
                # str型をint型に変換
                rgb_int = [int(num) for num in rgb]
            except:
                raise ValueError(f'rgb_valueに数字以外が含まれています:{rgb}')
            if len(rgb_int) != 3:
                raise ValueError(f'rgb_valueはRGBの3つの要素が必要です:{rgb}')

            # rgb_value_splitにcolor_rangeを適用
            rgb_value_upper = np.array([min(255, x + color_range) for x in rgb_int])
            rgb_value_lower = np.array([max(0, x - color_range) for x in rgb_int])
            # 指定された色情報から画素を抽出(R,G,Bの順)
            mask = cv2.inRange(image, rgb_value_lower, rgb_value_upper)
            mask_list.append(mask)
        
        new_color_space = 'BINARY'
        if type(byte_data) is pandas.core.series.Series:
            for index_name, (index, mask) in zip(color_name_split, enumerate(mask_list)):
                if index == 0:
                    # 黒画素部分をTrueにする
                    other_mask = mask == 0
                    other = np.zeros_like(mask)
                else:
                    # 各maskで黒画素部分をAND演算でTrue部分を抽出
                    other_mask = other_mask & (mask == 0)

                # maskをfsfに書き込み
                byte_data[index_name] = pickle.dumps(mask)
                byte_data['color_space'] = new_color_space

            # other画像
            if other_output:
                # True(mask以外)部分を白画素で表示
                other[other_mask] = 255
                # 入力画像から抽出対象外の色を白画素として表示
                other_image = image.copy()
                other_image[other==0] = [255, 255, 255]

                # otherイメージをfsfに書き込み
                color_space_other = 'RGB'
                byte_data['other'] = pickle.dumps(other_image)
                byte_data['color_space_other'] = color_space_other

            new_byte_data = byte_data   
        else:
            new_byte_data = pickle.dumps(mask)            
            
        attribute['ColorSpace'] = new_color_space
        # other画像カラースペース
        if other_output:
            attribute['ColorSpaceOther'] = color_space_other

        return new_byte_data, attribute