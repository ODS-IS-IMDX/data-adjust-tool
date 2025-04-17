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
pandas = import_module("pandas")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ConvertImageToBinaryLogic(FlexibleRasterVectorLogic):
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
        入力データをチェックし、グレースケールの画像をしきい値に基づいてバイナリ画像に変換します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が GRAYSCALE である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - threshold: int
                2値化で使用するしきい値が設定されます。
            
            - threshold_type: string
                2値化の方法が設定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。
            
            - fsf_image_dst: string
                処理した画像を出力するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                変換されたバイナリ画像データのバイト列です。

            - attribute : dict
                更新された属性情報を含む辞書です。ColorSpace は BINARY に設定されます。

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
        fsf_image_dst = properties.get('fsf_image_dst', 'content')

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
        if color_space != 'GRAYSCALE':
            raise Exception('GRAYSCALE以外のColorSpaceが設定されています')

        # 2値化閾値
        try:
            threshold = int(properties.get('threshold', 127))
        except Exception as e:
            raise Exception('プロパティThresholdに数値以外の値が設定されています')
        # 2値化方法
        threshold_type_str = properties.get('threshold_type', 'THRESH_BINARY')
        threshold_type = getattr(cv2, threshold_type_str, None)
        if threshold_type is None:
            raise ValueError(f'無効なThreshold Typeが設定されています: {threshold_type_str}')

        # 2値化処理
        _, image_converted = cv2.threshold(image, threshold, 255, threshold_type)
        new_color_space = 'BINARY'
        
        # 応答データ作成
        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_image_dst] = pickle.dumps(image_converted)
            new_byte_data = byte_data
            byte_data['color_space'] = new_color_space
        else:
            new_byte_data = pickle.dumps(image_converted)
        attribute['ColorSpace'] = new_color_space

        return new_byte_data, attribute
