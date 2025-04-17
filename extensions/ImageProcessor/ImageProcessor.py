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


class ImageProcessor(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageProcessor

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像の各種処理（マスキング、透過処理、ブレンディング）を行うプロセッサクラスです。

    このプロセッサは、入力データに対して指定された処理を適用し、画像を加工します。

    **処理概要:**

    - マスキング：画像に対してマスク処理を適用します。前景と背景を分離するためのマスクを指定します。
    - 透過処理：前景画像を透過させ、背景画像と合成します。アルファチャンネルを使用して透過を制御します。
    - ブレンディング：前景と背景画像を指定したモードでブレンドします。加重平均や最大値・最小値を使用したブレンドが可能です。

    **注意事項:**
    
    - FieldSetFileのそれぞれの項目に前景または背景画像が格納されています。
    - プロパティから前景画像のFieldSetFileの項目名、背景画像のFieldSetFileの項目名を指定します。
    - foreground, background はともに NumPy 配列形式で、カラースペースはRGB または RGBA である必要があります。異なる場合はエラーとなります。
    - 前景と背景の画像サイズが異なる場合、自動的にリサイズされます。

    タグ
    --------------------------------
    * image processing
    * masking
    * transparency
    * blending

    プロパティ
    --------------------------------
    **Processing Method**

    **任意入力  リスト入力**

    - 画像処理方法を指定します。
      #. Masking: マスキングを行います。
      #. Transparency: 透過処理を行います。
      #. Blending: ブレンディング（合成）を行います

    - 初期値はMaskingです。

    **Mask Threshold**

    **任意入力  直接入力**

    - マスキング処理に使用するしきい値を指定します。
    - 指定された値以上の画素値を持つ部分がマスク(二値化)として使用されます。
    - 有効範囲は 0～255 で、初期値は127です。

    **Alpha Threshold**

    **任意入力  直接入力**

    - 透過処理に使用するアルファしきい値を指定します。
    - 透過処理の際にアルファチャンネルの値を調整するために使用されます。
    - 指定された値以上のアルファ値を持つ部分が不透明として処理されます。
    - 有効範囲は 0～255 で、初期値は127です。

    **Blend Mode**

    **任意入力  リスト入力**

    - ブレンディング処理に使用するブレンドモードを指定します。

    - 前景画像と背景画像をどのように合成するかを決定します。
      #. weighted_average: 各ピクセルの値を重み付けして平均化する手法です（加重平均）。
      #. maximum: 各ピクセルの最大値を選択する手法です。
      #. minimum: 各ピクセルの最小値を選択する手法です。

    - 初期値は weighted_averageです。

    **Alpha**

    **任意入力  直接入力**

    - ブレンディング処理に使用するアルファ値を指定します。
    - 前景画像と背景画像の合成比率を決定します。
    - 有効な値は0.0から1.0の浮動小数点数で、0.0は完全に背景画像、1.0は完全に前景画像を使用することを意味します。
    - 初期値は0.5で、前景画像と背景画像が同じ比率で合成されます。

    **FSF Foreground**

    **任意入力  直接入力**

    - 処理する前景画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はforegroundです。

    **FSF Background**

    **任意入力  直接入力**

    - 処理する後景画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はbackgroundです。

    **FSF Image Dst**

    **任意入力  直接入力**

    - 処理した画像を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcontentです。

    リレーションシップ
    --------------------------------
    * success: 画像処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。RGBまたはRGBA形式で設定されている必要があります。

    Writes Attributes
    --------------------------------
    * ColorSpace: 処理後の画像のカラースペース。処理結果に応じてGRAYSCALE、RGB、RGBAのいずれかに設定されます。

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(RGB、RGBA形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Image Processor: Masking, Transparency and Blending'

    processing_method = PropertyDescriptor(
        name="Processing Method",
        description="画像処理方法を指定します。\
                        Masking: マスキングを行います。\
                        Transparency: 透過処理を行います。\
                        Blending: ブレンディング（合成）を行います。\
                    初期値はMaskingです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=["Masking","Transparency","Blending"],
        required=True,
        default_value="Masking",
        sensitive=False
    )

    mask_threshold = PropertyDescriptor(
        name="Mask Threshold",
        description="マスキング処理に使用するしきい値を指定します。\
                    指定された値以上の画素値を持つ部分がマスク(二値化)として使用されます。\
                    有効範囲は 0～255 で、初期値は127です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="127",
        sensitive=False
    )

    alpha_threshold = PropertyDescriptor(
        name="Alpha Threshold",
        description="透過処理に使用するアルファしきい値を指定します。\
                    透過処理の際にアルファチャンネルの値を調整するために使用されます。\
                    指定された値以上のアルファ値を持つ部分が不透明として処理されます。\
                    有効範囲は 0～255 で、初期値は127です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="127",
        sensitive=False
    )

    blend_mode = PropertyDescriptor(
        name="Blend Mode",
        description="ブレンディング処理に使用するブレンドモードを指定します。\
                    前景画像と背景画像をどのように合成するかを決定します。\
                        weighted_average: 各ピクセルの値を重み付けして平均化する手法です（加重平均）。\
                        maximum: 各ピクセルの最大値を選択する手法です。\
                        minimum: 各ピクセルの最小値を選択する手法です。\
                    初期値は weighted_averageです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=["weighted_average","maximum","minimum"],
        required=False,
        default_value="weighted_average",
        sensitive=False
    )

    alpha = PropertyDescriptor(
        name="Alpha",
        description="ブレンディング処理に使用するアルファ値を指定します。\
                    前景画像と背景画像の合成比率を決定します。\
                    有効な値は0.0から1.0の浮動小数点数で、0.0は完全に背景画像、1.0は完全に前景画像を使用することを意味します。\
                    初期値は0.5で、前景画像と背景画像が同じ比率で合成されます。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0.5",
        sensitive=False
    )

    fsf_foreground = PropertyDescriptor(
        name="FSF Foreground",
        description="処理する前景画像を取得するFieldSetFileの項目名を指定します。\
                    初期値はforegroundです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="foreground",
        sensitive=False
    )

    fsf_background = PropertyDescriptor(
        name="FSF Background",
        description="処理する後景画像を取得するFieldSetFileの項目名を指定します。\
                    初期値はbackgroundです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="background",
        sensitive=False
    )

    fsf_image_dst = PropertyDescriptor(
        name="FSF Image Dst",
        description="処理した画像を出力するFieldSetFileの項目名を指定します。\
                    既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    property_descriptors = [
        processing_method,
        mask_threshold,
        alpha_threshold,
        blend_mode, alpha,
        fsf_foreground,
        fsf_background,
        fsf_image_dst
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageProcessorLogic.ImageProcessorLogic')
    def __init__(self, **kwargs):
        pass
