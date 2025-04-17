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


class ImageMorphologyLogic(FlexibleRasterVectorLogic):
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
        画像データに対して指定された演算方法でモルフォロジー変換を適用し画像を返します。
        色空間はRGB、GRAYSCALE、BINARYに対応しており、モルフォロジー変換後でも色空間に変更はありません。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が RGB BINARY GRAYSCALE である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - morphology_method : string
                モルフォロジー変換の演算方法が設定されます。

            - kernel_shape : string
                演算に使用するカーネルの形状が設定されます。

            - kernel_size : string
                演算に使用するカーネルサイズが設定されます。

            - anchor : string
                演算の基準点(カーネルのアンカー位置)が設定されます。

            - iterations : int
                演算の回数が設定されます。

            - border_type : str
                画像境界ピクセルを埋める処理が設定されます。

            - border_value : int
                border_typeがBORDER_CONSTANTの場合に使用する画像境界ピクセルを埋める値(0～255)が設定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_image_dst: string
                処理した画像を出力するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                モルフォロジー変換が行われた画像のバイト列です。

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
        if not (color_space == 'RGB' or color_space == 'GRAYSCALE' or color_space == 'BINARY'):
            raise Exception('BINARY/GRAYSCALE/RGB以外のColorSpaceが設定されています')

        # モルフォロジー演算の種類を取得
        method_str = properties.get('morphology_method', 'MORPH_OPEN')
        method = getattr(cv2, method_str, None)
        if method is None:
            raise ValueError(f'無効なMorphology Methodが設定されています: {method_str}')
        if not (method == cv2.MORPH_OPEN or method == cv2.MORPH_CLOSE):
            # OpningとClosingでなければエラー
            raise Exception(f'未サポートのmorphology_methodが設定されています: {method_str}')

        # 演算で使用するカーネル形状を取得
        kernel_shape_str = properties.get('kernel_shape', 'MORPH_RECT')
        kernel_shape = getattr(cv2, kernel_shape_str, None)
        if kernel_shape is None:
            raise Exception(f'無効なKernel Shapeが設定されています: {kernel_shape_str}')

        # 演算で使用するカーネルサイズを取得
        try:
            kernel_size_str = properties.get('kernel_size', '5,5')
            kernel_size = tuple(map(int, kernel_size_str.split(',')))
        except Exception as e:
            raise ValueError(f'Kernel Sizeに無効な値が設定されています: {str(e)}')

        # 演算の基準点を取得
        try:
            anchor_str = properties.get('anchor', '-1,-1')
            anchor = tuple(map(int, anchor_str.split(',')))
        except Exception as e:
            raise ValueError(f'Anchorに無効な値が設定されています: {str(e)}')
        if not (-1 <= anchor[0] < kernel_size[0] and -1 <= anchor[1] < kernel_size[1]):
            # -1以上 ~ Kernel Size未満の範囲外ならエラー
            raise ValueError(f'Anchorの値にKernel Size以上の値が設定されています: {anchor_str}')

        # 演算を繰り返す回数を取得
        try:
            iterations = int(properties.get('iterations', 1))
        except Exception as e:
            raise ValueError(f'Iterationsに無効な値が設定されています: {str(e)}')
        if not (1 <= iterations):
            # 1以上でなければエラー
            raise ValueError(f'Iterationsに有効範囲外の値が設定されています: {str(iterations)}')

        # 画像境界ピクセル種別を取得
        border_type_str = properties.get('border_type', 'BORDER_CONSTANT')
        border_type = getattr(cv2, border_type_str, None)
        if border_type is None:
            raise Exception(f'無効なBorder Typeが設定されています: {border_type_str}')

        # 画像境界ピクセルとして埋める値を取得(Border TypeがBORDER_CONSTANTの場合のみ有効)
        try:
            border_value = int(properties.get('border_value', 0))
        except Exception as e:
            raise ValueError(f'Border Valueに無効な値が設定されています: {str(e)}')
        if not (0 <= border_value <= 255):
            # 0以上 ~ 255以下の範囲外ならエラー
            raise ValueError(f'Border Valueに有効範囲外の値が設定されています: {str(border_value)}')

        # モルフォロジー変換を適用
        kernel = cv2.getStructuringElement(kernel_shape, kernel_size)
        img_morph = cv2.morphologyEx(
            src=image,
            op=method,
            kernel=kernel,
            anchor=anchor,
            iterations=iterations,
            borderType=border_type,
            borderValue=border_value
        )

        # 応答データ作成
        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_image_dst] = pickle.dumps(img_morph)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(img_morph)

        return new_byte_data, attribute
