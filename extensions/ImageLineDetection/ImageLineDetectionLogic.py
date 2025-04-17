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
from raster_to_vector.common.image_utils import *


class ImageLineDetectionLogic(FlexibleRasterVectorLogic):
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

    def get_line_thickness(self, line_list, image, line_thickness_threshold=200):
        if image.ndim == 3:
            # RGB -> GrayScale化
            img_gray = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
        else:
            img_gray = image

        thickness_list = calc_line_thickness(line_list, img_gray, line_thickness_threshold)

        return thickness_list

    def __call__(self, byte_data, attribute, properties):
        """
        線分検出し、座標データを返します。
        Measure line thicknessがTrueの場合、線分検出座標と線分の太さを返します。

        線分検出の場合、HoughLinesPを使用して線分を検出します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace が BINARY GRAYSCALE である必要があります。
        properties : dict
            プロパティ設定を含む辞書です。
            - line_detection_algorithm: string
                線分検出のアルゴリズムが設定されます。

            - threshold: int
                線分検出の閾値が設定されます。

            - min_line_length: int
                線分検出時の最小線長（ピクセル単位）が設定されます。

            - is_measure_thickness: bool
                線の検出と同時に線の太さも検出するフラグが設定されます。

            - line_thickness_threshold: int
                線の太さを検出する際に、この閾値以下のピクセルを線の内部の点として計算する値が設定されます。

            - max_line_gap: int
                線分間の最大ギャップ（ピクセル単位）が設定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_line_list: string
                検出した線分を出力するFieldSetFileの項目名が指定されます。
                
            - fsf_image_org: string
                線の太さを検出する際に利用する元画像かGRAYSCALE画像が格納されているFieldSetFileの項目名が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                線分検出結果リスト([(始点X座標, 始点Y座標)]または[(始点X座標, 始点Y座標), 太さ])のバイト列です。

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

        fsf_image_src = properties.get('fsf_image_src', 'content')
        fsf_line_list = properties.get('fsf_line_list', 'content')
        fsf_image_org = properties.get('fsf_image_org', 'content')

        # 線の太さを計測するかどうかを指定するフラグ
        is_measure_thickness = properties.get('is_measure_thickness', 'False') == 'True'

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

        line_detection_algorithm = properties.get('line_detection_algorithm', 'HOUGH')

        if line_detection_algorithm == 'HOUGH':
            # 線分検出
            try:
                min_line_length = int(properties.get('min_line_length', 100))
                max_line_gap = int(properties.get('max_line_gap', 10))
                threshold = int(properties.get('threshold', 50))
                line_thickness_threshold = int(properties.get('line_thickness_threshold', 200))
            except Exception as e:
                raise ValueError(f'プロパティの取得時にエラーが発生しました: {e}')

            detection_data = cv2.HoughLinesP(
                image,
                1,
                np.pi / 180,
                threshold=threshold,
                minLineLength=min_line_length,
                maxLineGap=max_line_gap)

            transformed_list = []
            if detection_data is not None:
                detection_data = detection_data.reshape(-1, 4)
                transformed_list = [
                    [(row[0], row[1]), (row[2], row[3])] for row in detection_data
                ]

                if is_measure_thickness:
                    try:
                        org_img = pickle.loads(byte_data[fsf_image_org])
                    except Exception as e:
                        raise ValueError(f'線の太さの検出には元画像か元画像のgray scale画像が必要ですが、取得できませんでした: {e}')

                    try:
                        thickness_list = self.get_line_thickness(transformed_list, org_img, line_thickness_threshold)
                    except Exception as e:
                        raise ValueError(f'線の太さの計測に失敗しました: {e}')
                    
                    # 数が変わることはないが念のため
                    if len(thickness_list) != len(detection_data):
                        raise ValueError("線分の数と太さのデータの整合性がとれませんでした")

                    transformed_list = [
                        [row[0], row[1], thickness] for row, thickness in zip(transformed_list, thickness_list)
                    ]

        else:
            raise Exception('無効な線分検出のアルゴリズムが選択されています')

        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_line_list] = pickle.dumps(transformed_list)
            new_byte_data = byte_data
        else:
            new_byte_data = pickle.dumps(transformed_list)

        return new_byte_data, attribute

