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


class ConvertImageToBinary(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ConvertImageToBinary

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    GRAYSCALE形式の画像を指定されたしきい値を使ってバイナリ画像に変換するプロセッサクラスです。

    **処理概要:**

    - 入力データのカラースペースがグレースケール画像であることを確認します。
    - しきい値を基に、画像データをバイナリ画像に変換します。
    - 変換後の画像データをバイト列形式で返し、カラースペースをBINARYに設定します。

    **注意事項:**

    - カラースペースはGRAYSCALEである必要があります。異なる場合はエラーとなります。

    タグ
    --------------------------------
    * image processing
    * binary conversion
    * thresholding

    プロパティ
    --------------------------------
    **Threshold**

    **必須入力  直接入力**

    - グレースケール画像をバイナリ画像に変換する際のしきい値を設定します。
    - この値を基に、画像データはしきい値処理を受け、2値化を行います。
    - 初期値は 127です。
    - 動作例は「Threshold Type」を参照してください。
    
    **Threshold Type**

    **必須入力  リスト入力**

    - 2値化の方法(背景を255(白)とするか0(黒)とするか)を設定します。
    - THRESH_BINARYを設定した場合、Thresholdの値以下の値を 0、それ以外の値を 255 にします。
    - THRESH_BINARY_INVを設定した場合、Thresholdの値以下の値を 255、それ以外の値を 0 にします。
    - 初期値は THRESH_BINARY です。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。

    **FSF Image Dst**

    **必須入力  直接入力**

    - 処理した画像を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcontentです。

    リレーションシップ
    --------------------------------
    * success: バイナリ変換処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。GRAYSCALEである必要があります。

    Writes Attributes
    --------------------------------
    * ColorSpace: 処理が正常に完了した場合にBINARYに設定されます。

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(GRAYSCALE形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ConvertImageToBinary'

    threshold = PropertyDescriptor(
        name="Threshold",
        description="グレースケール画像をバイナリ画像に変換する際のしきい値を設定します。\
                    この値を基に、画像データはしきい値処理を受け、2値化を行います。\
                    Threshold Typeで「THRESH_BINARY」を設定した場合、この値以下を 0、それ以外を 255 にします。\
                    Threshold Typeで「THRESH_BINARY_INV」を設定した場合、この値以下を 255、それ以外を 0 にします。\
                    初期値は 127です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=127,
        sensitive=False
    )

    threshold_type = PropertyDescriptor(
        name="Threshold Type",
        description="2値化の方法(背景を255(白)とするか0(黒)とするか)を設定します。\
                        THRESH_BINARY: Thresholdの値以下の値を 0、それ以外の値を 255 にします。\
                        THRESH_BINARY_INV: Thresholdの値以下の値を 255、それ以外の値を 0 にします。\
                    初期値は THRESH_BINARY です。",
        allowable_values=["THRESH_BINARY", "THRESH_BINARY_INV"],
        required=True,
        default_value="THRESH_BINARY",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
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
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    property_descriptors = [
        threshold,
        threshold_type,
        fsf_image_src,
        fsf_image_dst
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertImageToBinaryLogic.ConvertImageToBinaryLogic')
    def __init__(self, **kwargs):
        pass
