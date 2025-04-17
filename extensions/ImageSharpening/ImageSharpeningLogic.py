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
np = import_module("numpy")
cv2 = import_module("cv2")

from raster_to_vector.common.base_raster_vector_logic import BaseRasterVectorLogic


class ImageSharpeningLogic(BaseRasterVectorLogic):
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
        画像データにカスタムフィルタまたはアンシャープマスク法を適用してシャープ化します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が RGB である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - kernel_size: int
                ラプラシアンフィルタで使用するカーネルサイズが設定されます。

            - custom_filter: int
                ラプラシアンフィルタで使用するカスタムフィルタが設定されます。

            - sharpening_method: string
                シャープ化の方法が設定されます。

            - unsharp_strength: float
                アンシャープマスク法の強度が設定されます。

            - blur_sigma: float
                アンシャープマスク法のガウシアンブラーのシグマ値が設定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                シャープ化された画像のバイト列です。

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

        image = pickle.loads(byte_data)
        color_space = attribute['ColorSpace']
        if color_space != 'RGB':
            raise Exception('RGB以外のColorSpaceが設定されています')

        method = properties.get('sharpening_method', 'Laplacian')

        if method == 'Laplacian':
            try:
                # カーネルサイズを取得 (必ず形式 '3,3' の形式で指定)
                kernel_size_str = properties.get('kernel_size', '3,3')
                kernel_size_tuple = tuple(map(int, kernel_size_str.split(',')))

                # カーネルサイズがint,int形式でない場合や、要素が2つでない場合に例外を発生させる
                if len(kernel_size_tuple) != 2:
                    raise ValueError("カーネルサイズは2つの整数で指定する必要があります")

                # カーネルサイズの要素が偶数の場合は例外を発生させる
                if any(k % 2 == 0 for k in kernel_size_tuple):
                    raise ValueError("カーネルサイズの各要素は奇数である必要があります")

                # カスタムフィルタを取得 (デフォルトはラプラシアンフィルタ)
                custom_filter_str = properties.get('custom_filter', '0,-1,0,-1,5,-1,0,-1,0')
                custom_filter = np.array(list(map(int, custom_filter_str.split(',')))).reshape(kernel_size_tuple)
            except Exception as e:
                raise Exception(f'プロパティに無効な値が設定されています: {str(e)}')

            # カスタムフィルタを適用してシャープ化
            image_sharpened = cv2.filter2D(image, -1, custom_filter)

        elif method == 'UnsharpMask':
            try:
                # アンシャープマスク法の強度とシグマを取得
                strength = float(properties.get('unsharp_strength', 1.5))
                blur_sigma = float(properties.get('blur_sigma', 1.0))
            except Exception as e:
                raise Exception(f'アンシャープマスクプロパティに無効な値が設定されています: {str(e)}')

            # アンシャープマスク法を適用
            blurred = cv2.GaussianBlur(image, (0, 0), blur_sigma)
            image_sharpened = cv2.addWeighted(image, 1.0 + strength, blurred, -strength, 0)

        else:
            raise Exception('無効なシャープ化メソッドが選択されています')

        new_byte_data = pickle.dumps(image_sharpened)

        return new_byte_data, attribute
