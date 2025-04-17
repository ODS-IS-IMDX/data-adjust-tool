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

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from raster_to_vector.common.base_processor import BaseProcessor


class ConvertImageToNumpy(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ConvertImageToNumpy

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像データにColorSpaceのAttributeを付与するプロセッサクラスです。

    **処理概要:**

    - 入力画像データを読み込みます。
    - 指定されたカラースペース（RGB、BINARY、GRAYSCALE、HLS、HSV、BGR）をAttribute情報に設定します。
    - NumPy形式の画像データをバイト列形式で返します。

    **注意事項:**

    - カラースペースを指定しない場合、デフォルトでRGBが使用されます。

    タグ
    --------------------------------
    * image processing
    * numpy conversion
    * color space attribution

    プロパティ
    --------------------------------
    **Color Space**

    **必須入力  リスト入力**

    - 画像のカラースペースを指定します。
    - RGB、BINARY、GRAYSCALE、HLS、HSV、BGRを設定できます。
    - 初期値はRGBです。

    リレーションシップ
    --------------------------------
    * success: NumPy変換処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * なし

    Writes Attributes
    --------------------------------
    * ColorSpace: 処理が正常に完了した場合に指定されたカラースペース値に設定されます。

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(色空間の指定なし)である必要があります。

    System Resource Considerations
    --------------------------------
    * 画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ConvertImageToNumpy'

    color_space = PropertyDescriptor(
        name="ColorSpace",
        description="画像のカラースペースを設定します。\
                    'RGB'、'BINARY'、'GRAYSCALE'、'HLS'、'HSV'、'BGR'から選択可能です。",
        allowable_values=["RGB", "BINARY", "GRAYSCALE", "HLS", "HSV", "BGR"],
        required=True,
        default_value='RGB',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [color_space]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertImageToNumpyLogic.ConvertImageToNumpyLogic')
    def __init__(self, **kwargs):
        pass
