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


class ImageSplit(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageSplit

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像を指定した分割数で分割するプロセッサクラスです。

    このプロセッサは入力された画像を指定された分割数で分割します。
    分割した後の画像で円や直線の座標を元の大きい画像に適用できる座標を保存します。

    **処理概要:**

    - 入力された画像をプロパティで指定した値に分割します。
    - 分割した画像はプロパティで指定した重なり部分も含めて分割されます。
    - 分割した画像はFieldSetFileのcontent項目に出力します。

    **注意事項:**
    
    - 画像の大きさにより、処理時間が長くなります。
    - Java heap memory エラーが出た場合はconf/bootstrap.confよりjava.arg.3=-Xmx1gを調整してください。

    タグ
    --------------------------------
    * image split

    プロパティ
    --------------------------------
    **width_division_num**

    **必須入力  直接入力**

    - 画像を横方向へ分割する数を指定します。分割後のpxで小数以下は切り捨てられます。
    - 初期値は10です。

    **Height Division Num**

    **必須入力  直接入力**

    - 画像を縦方向へ分割する数を指定します。分割後のpxで小数以下は切り捨てられます。
    - 初期値は10です。

    **Overlap Region**

    **必須入力  直接入力**

    - 画像を分割した際の縦横の重なり領域を指定します。500と指定した場合、縦横500px分大きく分割されます。
    - 画像の高さ、幅の最大に達した場合は自動でOverlap Regionが調整されます。
    - 0も指定できますが、画像端のオブジェクトを検出できない可能性があります。
    - 初期値は500です。

    **Offset Coordinate X**

    **必須入力  直接入力**

    - オフセット座標を格納するFieldSetFileの項目名を指定します。
    - 画像を分割した後に円検出や直線検出したX座標を分割前のX座標に適用するオフセット座標です。
    - オフセット座標は自動で計算されます。
    - 初期値はoffset_coordinate_xです。

    **Offset Coordinate Y**

    **必須入力  直接入力**

    - オフセット座標を格納するFieldSetFileの項目名を指定します。
    - 画像を分割した後に円検出や直線検出したY座標を分割前のY座標に適用するオフセット座標です。
    - オフセット座標は自動で計算されます。
    - 初期値はoffset_coordinate_yです。

    **FSF Image Src**

    **必須入力  直接入力**

    - 分割する画像を取得するFieldSetFileの項目名を指定します。
    - 分割された画像はFSF Image Srcの入力にかかわらずcontentに格納されます。
    - 初期値はcontentです。

    リレーションシップ
    --------------------------------
    * success: 画像分割が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * なし

    Writes Attributes
    --------------------------------
    * height_division_num: 処理が正常に完了した場合に画像高さの分割数が設定されます。
    * width_division_num: 処理が正常に完了した場合に画像幅の分割数が定されます。
    * overlap_region: 処理が正常に完了した場合に縦横の重なり領域の大きさが設定されます。
    * offset_coordinate_x: 処理が正常に完了した場合にオフセットX座標が設定されます。
    * offset_coordinate_y: 処理が正常に完了した場合にオフセットY座標が設定されます。

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ImageSplit'

    width_division_num = PropertyDescriptor(
        name="Width Division Num",
        description="画像を横方向へ分割する数を指定します。分割後のpxで小数以下は切り捨てられます。\
                    初期値は10です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=10,
        sensitive=False
    )

    height_division_num = PropertyDescriptor(
        name="Height Division Num",
        description="画像を縦方向へ分割する数を指定します。分割後のpxで小数以下は切り捨てられます。\
                    初期値は10です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=10,
        sensitive=False
    )

    overlap_region = PropertyDescriptor(
        name="Overlap Region",
        description="画像を分割した際の縦横の重なり領域を指定します。500と指定した場合、縦横500px分大きく分割されます。\
                    画像の高さ、幅の最大に達した場合は自動でOverlap Regionが調整されます。\
                    0も指定できますが、画像端のオブジェクトを検出できない可能性があります。\
                    初期値は500です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=500,
        sensitive=False
    )

    offset_coordinate_x = PropertyDescriptor(
        name="Offset Coordinate X",
        description="オフセット座標を格納するFieldSetFileの項目名を指定します。\
                    画像を分割した後に円検出や直線検出したX座標を分割前のX座標に適用するオフセット座標です。\
                    オフセット座標は自動で計算されます。\
                    初期値はoffset_coordinate_xです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="offset_coordinate_x",
        sensitive=False
    )

    offset_coordinate_y = PropertyDescriptor(
        name="Offset Coordinate Y",
        description="オフセット座標を格納するFieldSetFileの項目名を指定します。\
                    画像を分割した後に円検出や直線検出したY座標を分割前のY座標に適用するオフセット座標です。\
                    オフセット座標は自動で計算されます。\
                    初期値はoffset_coordinate_yです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="offset_coordinate_y",
        sensitive=False
    )

    fsf_image_src = PropertyDescriptor(
        name="FSF Image Src",
        description="分割する画像を取得するFieldSetFileの項目名を指定します。\
                    分割された画像はFSF Image Srcの入力にかかわらずcontentに格納されます。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    property_descriptors = [
        width_division_num,
        height_division_num,
        overlap_region,
        offset_coordinate_x,
        offset_coordinate_y,
        fsf_image_src,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageSplitLogic.ImageSplitLogic')
    def __init__(self, **kwargs):
        pass
