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


class ImageLineDetection(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageLineDetection

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    線分を検出するプロセッサクラスです。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 線分検出には cv2.HoughLinesPを使用して、画像内の形状を検出します。
    - Measure line thicknessがTrueの場合、線分検出座標と線分の太さを返します。
    - 検出座標データをバイト列形式で返します。

    **注意事項:**

    - カラースペースはBINARYまたはGRAYSCALEである必要があります。異なる場合はエラーとなります。

    タグ
    --------------------------------
    * image processing
    * hough transform
    * line detection
    * opencv

    プロパティ
    --------------------------------
    **Line Detection Algorithm**

    **必須入力  直接入力**

    - 線分検出を行うアルゴリズムを指定します。現状ではHoughのみとなっています

    **LineThreshold**

    **任意入力  直接入力**

    - 線分検出時の最小投票数を指定します。
    - 投票数が多いほど、検出された線分が信頼性の高いものになりますが、ノイズも除去されます。
    - 投票数とは直線とみなせるピクセルがどれだけ多いかということです。
    - 投票数が少ない場合
        多くの直線が検出できますが、ノイズの影響を受けやすいです。
        同じ直線を何度も検出する可能性があります。
    - 投票数が多い場合
        明らかな直線しか検出しません。
        部分的にしか直線を検出できない場合もあります。
    - 初期値は50です。

    **Minimum Line Length**

    **任意入力  直接入力**

    - 線分検出時の最小線長を指定します（ピクセル単位）。
    - 線分がこの長さに満たない場合は無視されます。短い線分も検出したい場合は、小さい値に設定してください。
    - 可能ならばペイント等で検出したくない直線の長さを測ると効果的です。
    - 初期値は100ピクセルです。

    **Measure Line Thickness**

    **必須入力  リスト入力**

    - 線検出時に線の太さも算出するかどうかを指定するフラグです。
    - 初期値はFalseです。

    **Line Thickness Threshold**

    **任意入力  直接入力**

    - 線の太さを検出する際に、この閾値以下のピクセルを線の内部の点として計算に利用します。
    - 0 ~ 255の範囲で設定する必要があります。
    - 初期値は200です。

    **Maximum Line Gap**

    **任意入力  直接入力**

    - 線分検出時の線分同士の最大ギャップを指定します（ピクセル単位）。
    - この値以内であれば、離れている線分も一つの線分として扱われます。
    - デフォルト値の10だと他の直線の始点、終点が10ピクセル以内にあった場合に同じ直線とみなされ、大きな直線となります。
    - Maximum Line Gapを大きくすると直線が意図しない所で結合され、正確な直線を検出することが難しくなります。
    - 初期値は10ピクセルです。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。

    **FSF Line List**

    **必須入力  直接入力**

    - 検出した線分を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcontentです。

    **FSF Image Org**

    **必須入力  直接入力**

    - 線の太さを検出する際に利用する元画像かGRAYSCALE画像を指定します。
    - 初期値はcontentです。

    リレーションシップ
    --------------------------------
    * success: 線分検出処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。BINARYまたはGRAYSCALEである必要があります。

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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(BINARY、GRAYSCALE形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Line Detection Processor'

    line_detection_algorithm = PropertyDescriptor(
        name="Line Detection Algorithm",
        description="線分検出を行うアルゴリズムを指定します。現状ではHoughのみとなっています。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="HOUGH",
        sensitive=False
    )

    threshold = PropertyDescriptor(
        name="LineThreshold",
        description="線分検出時の最小投票数を指定します。\
                    投票数とは直線とみなせるピクセルがどれだけ多いかということです。\
                    投票数が少ないと多くの直線が検出されますがノイズの影響を受けやすく、同じ直線が何度も検出される可能性があります\
                    投票数が多いと:明確な直線のみが検出されますが、部分的な直線しか検出できないこともあります。\
                    初期値は50です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="50",
        sensitive=False
    )

    min_line_length = PropertyDescriptor(
        name="Minimum Line Length",
        description="線分検出時の最小線長を指定します（ピクセル単位）。\
                    線分がこの長さに満たない場合は無視されます。短い線分も検出したい場合は、小さい値に設定してください。\
                    初期値は100ピクセルです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="100",
        sensitive=False
    )

    is_measure_thickness = PropertyDescriptor(
        name="Measure Line Thickness",
        description="線検出時に線の太さも算出するかどうかを指定するフラグです。",
        required=True,
        allowable_values=["True", "False"],
        default_value="False",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    line_thickness_threshold = PropertyDescriptor(
        name="Line Thickness Threshold",
        description="線の太さを検出する際に、この閾値以下のピクセルを線の内部の点として計算に利用します。0 ~ 255の範囲で設定する必要があります。",
        required=False,
        default_value="200",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    max_line_gap = PropertyDescriptor(
        name="Maximum Line Gap",
        description="線分検出時の線分同士の最大ギャップを指定します（ピクセル単位）。\
                    この値以内であれば、離れている線分も一つの線分として扱われます。\
                    初期値は10ピクセルです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="10",
        sensitive=False
    )

    fsf_image_src = PropertyDescriptor(
        name="FSF Image Src",
        description="処理する画像を取得するFieldSetFileの項目名を指定します。\
                    初期値はcontentです。",
        required=True,
        default_value="content",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    fsf_line_list = PropertyDescriptor(
        name="FSF Line List",
        description="検出した線分を出力するFieldSetFileの項目名を指定します。\
                    初期値はcontentです。",
        required=True,
        default_value="content",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    fsf_image_org = PropertyDescriptor(
        name="FSF Image Org",
        description="線の太さを検出する際に利用する元画像かGRAYSCALE画像を指定します。\
                    既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。\
                    初期値はcontentです。",
        required=True,
        default_value="content",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # すべてのプロパティをproperty_descriptorsリストに追加
    property_descriptors = [
        line_detection_algorithm,
        threshold,
        min_line_length,
        max_line_gap,
        fsf_image_src,
        fsf_line_list,
        fsf_image_org,
        is_measure_thickness,
        line_thickness_threshold
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageLineDetectionLogic.ImageLineDetectionLogic')
    def __init__(self, **kwargs):
        pass
