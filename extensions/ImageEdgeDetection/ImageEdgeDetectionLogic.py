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
pandas = import_module("pandas")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ImageEdgeDetectionLogic(FlexibleRasterVectorLogic):
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
        画像データに対してエッジ検出を行い、その結果を返します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace の指定はありません。
        properties : dict
            プロパティ設定を含む辞書です。
            - edge_detection_method: string
                エッジ検出の方法（CannyまたはSobel）が設定されます。

            - canny_threshold1: float
                Cannyエッジ検出の第1閾値が設定されます。

            - canny_threshold2: float
                Cannyエッジ検出の第2閾値が設定されます。

            - sobel_kernel_size: int
                Sobelフィルタに使用するカーネルサイズが設定されます。
                
            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_output: string
                エッジ検出した結果を出力するFieldSetFileの項目名が指定されます。

            - fsf_image_org: string
                次のフローで使う場合に入力画像を格納するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                エッジ検出後の画像データのバイト列です。
                
            - attribute : dict
                更新された属性情報を含む辞書です。ColorSpace は 元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        fsf_image_src = properties.get('fsf_image_src', 'content')
        fsf_image_org = properties.get('fsf_image_org', 'original')
        fsf_output = properties.get('fsf_output', 'content')

        # 入力データ取得
        if type(byte_data) is pandas.core.series.Series:
            try:
                image = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            image = pickle.loads(byte_data)

        color_space = attribute['ColorSpace']

        method = properties.get('edge_detection_method', 'Canny')

        if method == 'Canny':
            try:
                # Cannyエッジ検出の閾値を取得
                threshold1 = float(properties.get('canny_threshold1', 100))
                threshold2 = float(properties.get('canny_threshold2', 200))
            except Exception as e:
                raise Exception(f'Cannyプロパティに無効な値が設定されています: {str(e)}')

            # Cannyエッジ検出を適用
            image_edges = cv2.Canny(image, threshold1, threshold2)

        elif method == 'Sobel':
            try:
                # Sobelフィルタのカーネルサイズを取得
                kernel_size = int(properties.get('sobel_kernel_size', 3))
            except Exception as e:
                raise Exception(f'Sobelプロパティに無効な値が設定されています: {str(e)}')

            if kernel_size % 2 == 0 or kernel_size <= 0:
                raise ValueError("Sobelカーネルサイズは奇数の正整数である必要があります")

            # Sobelエッジ検出を適用
            grad_x = cv2.Sobel(image, cv2.CV_64F, 1, 0, ksize=kernel_size)
            grad_y = cv2.Sobel(image, cv2.CV_64F, 0, 1, ksize=kernel_size)
            image_edges = cv2.magnitude(grad_x, grad_y)

            # 正規化して0-255の範囲にする
            image_edges = cv2.normalize(image_edges, None, 0, 255, cv2.NORM_MINMAX)
            image_edges = image_edges.astype(np.uint8)

        else:
            raise Exception('無効なエッジ検出メソッドが選択されています')

        if type(byte_data) is pandas.core.series.Series:
            # 元画像を入力した項目名でを残すようにする
            if fsf_image_src != fsf_image_org:
                byte_data[fsf_image_org] = byte_data[fsf_image_src]

            byte_data[fsf_output] = pickle.dumps(image_edges)

            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(image_edges)

        return new_byte_data, attribute

