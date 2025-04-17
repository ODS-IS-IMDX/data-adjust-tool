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


class ImageContrastAdjustmentLogic(BaseRasterVectorLogic):
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
        指定された方法に基づき、アルファ・ベータ補正、ガンマ補正、ヒストグラム均等化、またはCLAHEを行います。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が RGB GRAYSCALE である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - contrast_adjustment_method: string
                使用するコントラスト調整方法が設定されます。

            - alpha: float
                アルファ補正の値が設定されます。

            - beta: float
                ベータ補正の値が設定されます。

            - gamma: float
                ガンマ補正の値が設定されます。

            - clahe_clip_limit: float
                CLAHEで使用するコントラスト制限値を設定されます。

            - clahe_tile_grid_size: string
                CLAHEで分割処理サイズ（縦pixel×横pixel）を設定されます。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                変換された画像データのバイト列です。

            - attribute : dict
                更新された属性情報を含む辞書です。ColorSpace は元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        image = pickle.loads(byte_data)
        method = properties.get('contrast_adjustment_method', 'AlphaBeta')

        if method == 'AlphaBeta':
            try:
                alpha = float(properties.get('alpha', 1.0))
                beta = float(properties.get('beta', 0))
            except Exception as e:
                raise Exception(f'アルファ・ベータ補正プロパティに無効な値が設定されています: {str(e)}')

            adjusted = cv2.convertScaleAbs(image, alpha=alpha, beta=beta)

        elif method == 'Gamma':
            try:
                gamma = float(properties.get('gamma', 1.0))
            except Exception as e:
                raise Exception(f'ガンマ補正プロパティに無効な値が設定されています: {str(e)}')

            invGamma = 1.0 / gamma
            table = np.array([((i / 255.0) ** invGamma) * 255 for i in np.arange(0, 256)]).astype("uint8")
            adjusted = cv2.LUT(image, table)

        elif method == 'HistogramEqualization':
            if attribute['ColorSpace'] != 'GRAYSCALE':
                raise Exception('ヒストグラム均等化はグレースケール画像のみ対応しています。ColorSpaceにGRAYSCALEを指定してください。')

            adjusted = cv2.equalizeHist(image)

        elif method == 'CLAHE':
            if attribute['ColorSpace'] != 'GRAYSCALE':
                raise Exception('CLAHEはグレースケール画像のみ対応しています。ColorSpaceにGRAYSCALEを指定してください。')

            try:
                clip_limit = float(properties.get('clahe_clip_limit', 2.0))
                tile_grid_size_str = properties.get('clahe_tile_grid_size', '8,8')
                tile_grid_size = tuple(map(int, tile_grid_size_str.split(',')))
            except Exception as e:
                raise Exception(f'CLAHEプロパティに無効な値が設定されています: {str(e)}')

            clahe = cv2.createCLAHE(clipLimit=clip_limit, tileGridSize=tile_grid_size)
            adjusted = clahe.apply(image)

        else:
            raise Exception('無効なコントラスト調整メソッドが選択されています')

        new_byte_data = pickle.dumps(adjusted)

        return new_byte_data, attribute
