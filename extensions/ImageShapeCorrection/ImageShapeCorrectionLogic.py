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

from raster_to_vector.common.base_raster_vector_logic import BaseRasterVectorLogic


class ImageShapeCorrectionLogic(BaseRasterVectorLogic):
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
        画像データに対して指定された方法で形状補正し画像を返します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が RGB GRAYSCALE BINARY である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - transformation_method: string
                形状補正の方法が設定されます。

            - angle: float
                回転処理の回転角度(反時計回り)が設定されます。

            - scale: float
                回転処理の拡大縮小のスケールが設定されます。

            - center: float
                回転処理の回転中心座標が設定されます。

            - scale_x: float
                拡大縮小処理の横方向の拡大縮小スケールが設定されます。

            - scale_y: float
                拡大縮小処理の縦方向の拡大縮小スケールが設定されます。

            - interpolation: string
                拡大縮小処理の補間方法が設定されます。

            - tx: float
                移動処理の横方向の移動量（ピクセル）が設定されます。

            - ty: float
                移動処理の縦方向の移動量（ピクセル）が設定されます。

            - shear_x: float
                せん断処理の横方向の強さが設定されます。

            - shear_y: float
                せん断処理の縦方向の強さが設定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                形状補正を行った画像のバイト列です。

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
        method = properties.get('transformation_method', 'Rotation')

        # 回転
        if method == 'Rotation':
            try:
                angle = float(properties.get('angle', 0))
                scale = float(properties.get('scale', 1.0))
                center = tuple(map(float, properties.get('center', '0.0').split(',')))
            except Exception as e:
                raise Exception(f'回転パラメータに無効な値が設定されています: {str(e)}')

            height, width = image.shape[:2]
            M = cv2.getRotationMatrix2D(center, angle, scale)
            transformed = cv2.warpAffine(image, M, (width, height))

        # 拡大縮小
        elif method == 'Scaling':
            try:
                scale_x = float(properties.get('scale_x', 1.0))
                scale_y = float(properties.get('scale_y', 1.0))

                # 検証：補間方法を選択デフォルトは線形補間（INTER_LINEAR）
                interpolation_method = properties.get('interpolation', 'LINEAR')
            except Exception as e:
                raise Exception(f'拡大縮小パラメータに無効な値が設定されています: {str(e)}')

            interpolation_dict = {
                'NEAREST': cv2.INTER_NEAREST,
                'LINEAR': cv2.INTER_LINEAR,
                'CUBIC': cv2.INTER_CUBIC,
                'AREA': cv2.INTER_AREA,
                'LANCZOS4': cv2.INTER_LANCZOS4
            }
            interpolation = interpolation_dict.get(interpolation_method, cv2.INTER_LINEAR)
            transformed = cv2.resize(image, None, fx=scale_x, fy=scale_y, interpolation=interpolation)

        # 移動
        elif method == 'Translation':
            try:
                tx = float(properties.get('tx', 0)) # x方向の移動量
                ty = float(properties.get('ty', 0)) # y方向の移動量
            except Exception as e:
                raise Exception(f'移動パラメータに無効な値が設定されています: {str(e)}')

            height, width = image.shape[:2]
            M = np.float32([[1, 0, tx], [0, 1, ty]])
            transformed = cv2.warpAffine(image, M, (width, height))

        # せん断
        elif method == 'Shearing':
            try:
                shear_x = float(properties.get('shear_x', 0.0))
                shear_y = float(properties.get('shear_y', 0.0))
            except Exception as e:
                raise Exception(f'せん断パラメータに無効な値が設定されています: {str(e)}')

            height, width = image.shape[:2]
            M = np.float32([[1, shear_x, 0], [shear_y, 1, 0]])
            transformed = cv2.warpAffine(image, M, (width, height))

        else:
            raise Exception('無効な変換メソッドが選択されています')

        new_byte_data = pickle.dumps(transformed)

        return new_byte_data, attribute
