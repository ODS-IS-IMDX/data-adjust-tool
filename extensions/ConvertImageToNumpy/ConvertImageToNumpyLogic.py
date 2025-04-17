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

from raster_to_vector.common.base_raster_vector_logic import BaseRasterVectorLogic


class ConvertImageToNumpyLogic(BaseRasterVectorLogic):
    def __init__(self):
        pass

    def __call__(self, byte_data, attribute, properties):
        """
        バイナリ形式の画像データをNumPy配列として処理します。

        画像データを読み込み、NumPy形式に変換した後、指定されたカラースペース（RGB、BINARY、GRAYSCALE、HLS、HSV、BGR）をAttribute情報に設定します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。 ColorSpace の指定はありません。
        properties : dict
            プロパティ設定を含む辞書です。
            - color_space: string
                画像のカラースペースが設定されます。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                NumPy配列のバイト列形式です。
                
            - attribute : dict
                更新された属性情報を含む辞書です。ColorSpace は指定されたものに更新されます。

        Raises
        ------
        Exception
            - なし
        """
        image = pickle.loads(byte_data)

        # 画像データをそのままシリアライズする
        new_byte_data = pickle.dumps(image)
        attribute['ColorSpace'] = properties['color_space']

        return new_byte_data, attribute
