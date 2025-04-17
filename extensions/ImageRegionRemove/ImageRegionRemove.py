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


class ImageRegionRemove(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageRegionRemove

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    領域を除去するプロセッサクラスです。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 入力された画像データに対し、入力された領域座標の画素を除去します。
    - 除去後の画像データをバイト列形式で返します。

    **注意事項:**

    - カラースペースはBINARYである必要があります。異なる場合はエラーとなります。
    - カラースペースがBINARYの場合、黒背景(0)/白画素(255)である必要があります。

    タグ
    --------------------------------
    * image processing
    * region remove
    * contour remove
    * rectangle remove
    * mask
    * opencv

    プロパティ
    --------------------------------
    **Remove Outline Only**

    **必須入力  リスト入力**

    - 領域の枠のみを除去するか、領域内部まで除去するかを指定します。
    - Trueを指定した場合は領域の枠のみを除去します。Falseを指定した場合は領域内部まで除去します。
    - 初期値はFalseです。

    **Outline Thickness**

    **任意入力  直接入力**

    - 除去する際の枠の太さを指定します。
    - Remove Outline OnlyがTrue場合のみ有効。
    - 初期値は3です。

    **FSF Remove Data**

    **必須入力  直接入力**

    - 除去する領域情報を取得するFieldSetFileの項目名を指定します。
    - FieldSetFileの項目名にはImageRectangleRegionDetectionまたはImageRectangleRegionDetectionの検出結果（FSF Detection Data項目）を指定できます。
    - 初期値はdetection_dataです。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。

    **FSF Image Dst**

    **必須入力  直接入力**

    - 処理した画像を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はimage_removedです。

    リレーションシップ
    --------------------------------
    * success: 領域除去処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。BINARYである必要があります。

    Writes Attributes
    --------------------------------
    * なし

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(BINARY形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Remove Region Processor'

    remove_outline_only = PropertyDescriptor(
        name="Remove Outline Only",
        description="領域の枠のみを除去するか、領域内部まで除去するかを指定します。\
                        True: 領域の枠のみを除去します。\
                        False: 領域内部まで除去します。\
                    初期値はFalseです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=["True","False"],
        required=True,
        default_value=False,
        sensitive=False
    )

    outline_thickness = PropertyDescriptor(
        name="Outline Thickness",
        description="除去する際の枠の太さを指定します。\
                    Remove Outline OnlyがTrue場合のみ有効。\
                    初期値は3です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value=3,
        sensitive=False
    )

    fsf_remove_data = PropertyDescriptor(
        name="FSF Remove Data",
        description="除去する領域情報を取得するFieldSetFileの項目名を指定します。\
                    FieldSetFileの項目名にはImageRectangleRegionDetectionまたはImageRectangleRegionDetectionの検出結果（FSF Detection Data項目）を指定できます。\
                    初期値はdetection_dataです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="detection_data",
        sensitive=False
    )

    fsf_image_src = PropertyDescriptor(
        name="FSF Image Src",
        description="処理する画像を取得するFieldSetFileの項目名を指定します。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    fsf_image_dst = PropertyDescriptor(
        name="FSF Image Dst",
        description="処理した画像を出力するFieldSetFileの項目名を指定します。\
                    既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。\
                    初期値はimage_removedです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="image_removed",
        sensitive=False
    )

    # すべてのプロパティをproperty_descriptorsリストに追加
    property_descriptors = [
        remove_outline_only,
        outline_thickness,
        fsf_remove_data,
        fsf_image_src,
        fsf_image_dst
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageRegionRemoveLogic.ImageRegionRemoveLogic')
    def __init__(self, **kwargs):
        pass
