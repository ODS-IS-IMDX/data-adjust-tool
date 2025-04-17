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


class ImageCircleDetection(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageCircleDetection

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    円を検出するプロセッサクラスです。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 円検出には cv2.HoughCirclesを使用して、画像内の形状（円の中心座標と半径）を検出します。
    - 若干歪んだ円でも検出可能ですが、重なった円の検出には不向きです。
    - 検出座標データ[(x, y), radius]をバイト列形式で返します。

    **注意事項:**

    - カラースペースはBINARYまたはGRAYSCALEである必要があります。異なる場合はエラーとなる。

    タグ
    --------------------------------
    * image processing
    * hough transform
    * circle detection
    * opencv

    プロパティ
    --------------------------------
    **Circle Detection Algorithm**

    **必須入力  直接入力**

    - 円検出を行うアルゴリズムを指定します。現状ではHoughのみとなっています

    **Minimum Distance Between Circles**

    **任意入力  直接入力**

    - 円検出時の円の中心点間の最小距離を指定します（ピクセル単位）。
    - 近接する円が重複して検出されることを防ぎます。
    - 検出対象の円の直径程度に設定することを推奨します。
    - 直径程度を指定しても近接する円が重複して検出される場合は、徐々に値を大きく調整する必要があります。
    - 初期値は50ピクセルです。

    **Param1 (Edge Detection Threshold)**

    **任意入力  直接入力**

    - 円検出時のCannyエッジ処理の上限閾値を指定します。
    - 低めの値を設定するとエッジが不明確な円や、コントラストの低い円も検出可能ですが、誤検出が増加する傾向があります。
    - 高めの値を設定すると明確なエッジを持つ円のみを検出可能ですが、高めに設定するにつれて未検出が増加する傾向があります。
    - 初期値は100です。

    **Param2 (Accumulator Threshold)**

    **任意入力  直接入力**
    
    - 円周上の点がどれだけ「円らしい」かを判定する閾値を指定します。
    - 低めの値を設定すると不完全な円も検出可能ですが、誤検出が増加する傾向があります。
    - 高めの値を設定すると円に近い形状のみを検出可能ですが、高めに設定するにつれて未検出が増加する傾向があります。
    - 初期値は30です。

    **Minimum Circle Radius**

    **任意入力  直接入力**

    - 円検出時の最小半径を指定します（ピクセル単位）。
    - 小さな円も検出したい場合は、この値を小さく設定します。
    - 初期値は20ピクセルです。

    **Maximum Circle Radius**

    **任意入力  直接入力**

    - 円検出時の最大半径を指定します（ピクセル単位）。
    - 大きな円が対象の場合は、この値を大きく設定してください。
    - 初期値は100ピクセルです。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。

    **FSF Circle List**

    **必須入力  直接入力**

    - 検出した円を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcontentです。

    リレーションシップ
    --------------------------------
    * success: 円検出処理が正常に完了した場合
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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(BINARY、GRAYSCALE)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Circle Detection Processor'

    circle_detection_algorithm = PropertyDescriptor(
        name="Circle Detection Algorithm",
        description="円検出を行うアルゴリズムを指定します。現状ではHoughのみとなっています。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="HOUGH",
        sensitive=False
    )

    min_dist = PropertyDescriptor(
        name="Minimum Distance Between Circles",
        description="円検出時の円の中心点間の最小距離を指定します（ピクセル単位）。\
                    近接する円が重複して検出されることを防ぎます。\
                    検出対象の円の直径程度に設定することを推奨します。\
                    初期値は50ピクセルです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="50",
        sensitive=False
    )

    param1 = PropertyDescriptor(
        name="Param1 (Edge Detection Threshold)",
        description="円検出時のCannyエッジ処理の上限閾値を指定します。\
                    低い値でエッジが不明確な円も検出できますが誤検出が増え、高い値で明確な円のみ検出できますが未検出が増えます。\
                    初期値は100です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="100",  # OpenCVの初期値
        sensitive=False
    )

    param2 = PropertyDescriptor(
        name="Param2 (Accumulator Threshold)",
        description="円検出時の累積された閾値（中心点検出用）を指定します。\
                    低い値で不完全な円を検出できますが誤検出が増え、高い値で円に近い形状のみ検出できますが未検出が増えます。\
                    初期値は30です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="30",  # OpenCVの初期値
        sensitive=False
    )

    min_circle_radius = PropertyDescriptor(
        name="Minimum Circle Radius",
        description="円検出時の最小半径を指定します（ピクセル単位）。\
                    小さな円も検出したい場合は、この値を小さく設定します。\
                    初期値は20ピクセルです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="20",
        sensitive=False
    )

    max_circle_radius = PropertyDescriptor(
        name="Maximum Circle Radius",
        description="円検出時の最大半径を指定します（ピクセル単位）。\
                    大きな円が対象の場合は、この値を大きく設定してください。\
                    初期値は100ピクセルです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="100",
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

    fsf_circle_list = PropertyDescriptor(
        name="FSF Circle List",
        description="検出した円を出力するFieldSetFileの項目名を指定します。\
                    既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。\
                    初期値はcontentです。",
        required=True,
        default_value="content",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # すべてのプロパティをproperty_descriptorsリストに追加
    property_descriptors = [
        circle_detection_algorithm,
        min_circle_radius,
        max_circle_radius,
        min_dist,
        param1,
        param2,
        fsf_image_src,
        fsf_circle_list
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageCircleDetectionLogic.ImageCircleDetectionLogic')
    def __init__(self, **kwargs):
        pass
