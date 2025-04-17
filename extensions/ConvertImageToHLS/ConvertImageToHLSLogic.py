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

from raster_to_vector.common.base_raster_vector_logic import BaseRasterVectorLogic


class ConvertImageToHLSLogic(BaseRasterVectorLogic):
    def __init__(self):
        """
        初期化メソッド。色空間の変換パラメータを保持する辞書を設定する。
        'RGB', 'HSV', 'BGR' の色空間に対して、対応するOpenCVの変換コードを設定する。
        'HSV' は特別なケースで、2段階の変換が必要なためリストを使用する。
        """
        self.convert_param_dict = {
            'RGB': cv2.COLOR_RGB2HLS,
            'HSV': [cv2.COLOR_HSV2BGR, cv2.COLOR_BGR2HLS],
            'BGR': cv2.COLOR_BGR2HLS,
        }

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
        指定されたカラースペースに基づいて、画像データをHLS色空間に変換します。
        特に、HSVからHLSに変換する場合は、まずHSVからBGRに変換し、その後BGRからHLSに変換します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が RGB、HSV、BGR である必要があります。
        properties : dict
            プロパティ設定です。このプロセッサでは使用しません。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                HLS色空間に変換された画像データのバイト列です。
                
            - attribute : dict
                更新された属性情報を含む辞書です。ColorSpace は HLS に設定されます。

        Raises
        ------
        Exception
            - ColorSpace が RGB、HSV、BGR でない場合に例外を発生させます。
        """
        self.input_check(byte_data, attribute)

        image = pickle.loads(byte_data)
        color_space = attribute['ColorSpace']
        convert_param = self.convert_param_dict.get(color_space, None)
        if convert_param is None:
            raise Exception('存在しないColorSpaceが設定されています')

        if color_space != 'HSV':
            image_converted = cv2.cvtColor(image, convert_param)
        else:
            image_bgr = cv2.cvtColor(image, convert_param[0])
            image_converted = cv2.cvtColor(image_bgr, convert_param[1])

        new_byte_data = pickle.dumps(image_converted)
        attribute['ColorSpace'] = 'HLS'

        return new_byte_data, attribute
