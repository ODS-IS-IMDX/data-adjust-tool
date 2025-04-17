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
pandas = import_module("pandas")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ImageCircleDetectionLogic(FlexibleRasterVectorLogic):
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
        HoughCirclesを使用して円を検出し、座標データを返します。

        Parameters
        ----------
        byte_data : bytes 
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpaceが BINARY または GRAYSCALE である必要があります
        properties : dict
            プロパティ設定を含む辞書です。
            - circle_detection_algorithm: string
                円検出のアルゴリズムが設定されます。現状はHOUGHのみとなっています

            - min_dist: int
                円検出時に使用する円の中心間の最小距離が設定されます。

            - param1: int
                円検出のエッジ検出に使用するCannyエッジ検出器の上限閾値が設定されます。

            - param2: int
                円検出における中心検出の閾値が設定されます。

            - min_circle_radius: int
                円検出時に検出する最小半径が設定されます。

            - max_circle_radius: int
                円検出時に検出する最大半径が設定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_circle_list: string
                検出した円を出力するFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            - new_byte_data : bytes
                検出された円の座標データ[(x, y), radius]のバイト列です。
                
            - attribute : dict
                属性情報を含む辞書です。元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        fsf_image_src = properties.get('fsf_image_src', 'content')
        fsf_circle_list = properties.get('fsf_circle_list', 'content')

        if type(byte_data) is pandas.core.series.Series:
            try:
                image = pickle.loads(byte_data[fsf_image_src])
            except:
                raise ValueError(f'fsf_image_srcに無効な値が設定されています: {properties.get("fsf_image_src")}')
        else:
            image = pickle.loads(byte_data)

        color_space = attribute['ColorSpace']
        if color_space != 'BINARY' and color_space != 'GRAYSCALE':
            raise Exception('BINARY, GRAYSCALE以外のColorSpaceが設定されています')

        circle_detection_algorithm = properties.get('circle_detection_algorithm', 'HOUGH')

        if circle_detection_algorithm == 'HOUGH':
            # 円検出
            try:
                min_dist = int(properties.get('min_dist', 50))
                param1 = int(properties.get('param1', 100))
                param2 = int(properties.get('param2', 30))
                min_circle_radius = int(properties.get('min_circle_radius', 20))
                max_circle_radius = int(properties.get('max_circle_radius', 100))
            except Exception as e:
                raise ValueError(f'プロパティの取得時にエラーが発生しました: {e}')

            detection_data = cv2.HoughCircles(
                image,
                cv2.HOUGH_GRADIENT,
                dp=1,
                minDist=min_dist,
                param1=param1,
                param2=param2,
                minRadius=min_circle_radius,
                maxRadius=max_circle_radius)

            # detection_dataがNoneの場合、空のリストを返す
            if detection_data is None:
                transformed_list = []
            else:
                detection_data = np.int32(np.around(detection_data))
                detection_data = detection_data.reshape(-1, 3)
                transformed_list = [
                    [(row[0], row[1]), row[2]] for row in detection_data
                ]

        else:
            raise Exception('無効な円検出のアルゴリズムが選択されています')

        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_circle_list] = pickle.dumps(transformed_list)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(transformed_list)

        return new_byte_data, attribute

