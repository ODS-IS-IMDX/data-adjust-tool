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


class ImageContourDetectionLogic(BaseRasterVectorLogic):
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
        画像データに対して輪郭検出を行います。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が BINARY である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - contour_retrieval_mode : string
                輪郭の抽出モードが設定されます。

            - contour_approximation_method : string
                輪郭の近似方法が設定されます。

            - min_contour_area : float
                最小輪郭面積が設定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                抽出した輪郭座標データのバイト列です。
                
            attribute : dict
                更新された属性情報を含む辞書です。 ColorSpace は元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        image = pickle.loads(byte_data)
        color_space = attribute['ColorSpace']

        # BINARY画像であることを確認
        if color_space != 'BINARY':
            raise Exception('BINARY以外のColorSpaceが設定されています')

        # 輪郭抽出のモードを取得
        retrieval_mode_str = properties.get('contour_retrieval_mode', 'RETR_EXTERNAL')
        retrieval_mode = getattr(cv2, retrieval_mode_str, None)
        if retrieval_mode is None:
            raise ValueError(f'無効な輪郭抽出モードが選択されています: {retrieval_mode_str}')

        # 輪郭の近似方法を取得
        approximation_method_str = properties.get('contour_approximation_method', 'CHAIN_APPROX_SIMPLE')
        approximation_method = getattr(cv2, approximation_method_str, None)
        if approximation_method is None:
            raise ValueError(f'無効な輪郭近似方法が選択されています: {approximation_method_str}')

        # 最小輪郭面積を取得
        try:
            min_contour_area = float(properties.get('min_contour_area', 0))
        except Exception as e:
            raise ValueError(f'min_contour_areaに無効な値が設定されています: {properties.get("min_contour_area")}')

        # 輪郭を抽出
        contours, hierarchy = cv2.findContours(image, retrieval_mode, approximation_method)

        # 最小面積のフィルタリングL
        if min_contour_area > 0:
            contours = [cnt for cnt in contours if cv2.contourArea(cnt) >= min_contour_area]

        new_byte_data = pickle.dumps(contours)

        return new_byte_data, attribute
