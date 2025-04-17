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


class ImageEdgeDetection(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageEdgeDetection

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像のエッジ検出を行うプロセッサクラスです。

    **処理概要:**

    - 入力データを受け取り、指定された方法でエッジを検出します。
    - エッジ抽出にはCannyメソッドまたはSobelメソッドが選択できます。
    - 検出結果はバイト列形式で返されます。

    **注意事項:**

    - Cannyエッジ検出の場合、閾値1と閾値2が指定されていない場合は、それぞれ100と200が使用されます。
    - Sobelエッジ検出ではカーネルサイズが奇数である必要があります。

    タグ
    --------------------------------
    * image processing
    * edge detection
    * opencv

    プロパティ
    --------------------------------
    **Edge Detection Method**

    **必須入力  リスト入力**

    - エッジ検出方法を選択します。

    #. Canny: エッジの検出精度が高い手法です。
    #. Sobel: Cannyと比べて高速ですがノイズの影響を受けやすい手法です。

    - 初期値はCannyです。

    **Canny Threshold 1**

    **任意入力  直接入力**

    - Cannyエッジ検出のための閾値1(小さいエッジを検出するための基準値)を指定します。
    - プロパティ（Edge Detection Method）でCannyを指定した場合のみ有効です。
    - 実用的な範囲は50～150程度であり、この値より小さい勾配は無視されます。
    - Canny Threshold 2より小さい値を推奨します。
    - 初期値は100です。

    **Canny Threshold 2**

    **任意入力  直接入力**

    - Cannyエッジ検出のための閾値2(確実なエッジを判定するための基準値)を指定します。
    - プロパティ（Edge Detection Method）でCannyを指定した場合のみ有効です。
    - 実用的な範囲は100～300程度であり、この値より大きい勾配はエッジとして検出されます。
    - Canny Threshold 1より大きい値を推奨します。
    - 初期値は200です。

    **Sobel Kernel Size**

    **任意入力  直接入力**

    - Sobelフィルタのカーネルサイズを指定します。
    - プロパティ（Edge Detection Method）でSobelを指定した場合のみ有効です。
    - 勾配計算の基準となる中心ピクセルが必要なため、明確な中心を持つ奇数サイズのカーネルを使用する必要があります。
    - 実用的な範囲3～7程度の奇数であり、以下の効果があります。

        + 3：細かいエッジを鮮明に検出（薄い線も含めて全てのエッジを検出）
        + 5：やや広い範囲の輝度変化を検出（やや薄い線のエッジは検出されにくい）
        + 7：より広い範囲の輝度変化を検出（はっきりした線のエッジのみ検出）

    - 初期値は3です。
    
    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。"

    **FSF Output**

    **必須入力  直接入力**

    - エッジ検出した結果を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcontentです。

    **FSF Image Org**

    **必須入力  直接入力**

    - 次のフローで使う場合に入力画像を格納するFieldSetFileの項目名を指定します。
    - FSF Outputと同じ項目名にすると入力画像は格納されずエッジ画像のみを返します。
    - 初期値はcontentです。

    リレーションシップ
    --------------------------------
    * success: エッジ検出処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。

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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(色空間の指定なし)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ImageEdgeDetection'

    edge_detection_method = PropertyDescriptor(
        name="Edge Detection Method",
        description="エッジ検出方法を選択します。\
                        Canny: エッジの検出精度が高い手法です。\
                        Sobel: Cannyと比べて高速ですがノイズの影響を受けやすい手法です。\
                    初期値はCannyです。",
        allowable_values=["Canny", "Sobel"],
        required=True,
        default_value="Canny",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    canny_threshold1 = PropertyDescriptor(
        name="Canny Threshold 1",
        description="Cannyエッジ検出のための閾値1(小さいエッジを検出するための基準値)を指定します。\
                    プロパティ（Edge Detection Method）でCannyを指定した場合のみ有効です。\
                    実用的な範囲は50～150程度であり、この値より小さい勾配は無視されます。\
                    Canny Threshold 2より小さい値を推奨します。\
                    初期値は100です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="100",
        sensitive=False
    )

    canny_threshold2 = PropertyDescriptor(
        name="Canny Threshold 2",
        description="Cannyエッジ検出のための閾値2(確実なエッジを判定するための基準値)を指定します。\
                    プロパティ（Edge Detection Method）でCannyを指定した場合のみ有効です。\
                    実用的な範囲は100～300程度であり、この値より大きい勾配はエッジとして検出されます。\
                    Canny Threshold 1より大きい値を推奨します。\
                    初期値は200です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="200",
        sensitive=False
    )

    sobel_kernel_size = PropertyDescriptor(
        name="Sobel Kernel Size",
        description="Sobelフィルタのカーネルサイズを指定します。\
                    プロパティ（Edge Detection Method）でSobelを指定した場合のみ有効です。\
                    勾配計算の基準となる中心ピクセルが必要なため、明確な中心を持つ奇数サイズのカーネルを使用する必要があります。\
                    実用的な範囲3～7程度の奇数であり、以下の効果があります。\
                    3：細かいエッジを鮮明に検出（薄い線も含めて全てのエッジを検出）\
                    5：やや広い範囲の輝度変化を検出（やや薄い線のエッジは検出されにくい）\
                    7：より広い範囲の輝度変化を検出（はっきりした線のエッジのみ検出）\
                    初期値は3です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="3",
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

    fsf_output = PropertyDescriptor(
        name="FSF Output",
        description="エッジ検出した結果を出力するFieldSetFileの項目名を指定します。\
                    既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    fsf_image_org = PropertyDescriptor(
        name="FSF Image Org",
        description="次のフローで使う場合に入力画像を格納するFieldSetFileの項目名を指定します。\
                    FSF Outputと同じ項目名にすると入力画像は格納されず、エッジ画像のみを返します。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    property_descriptors = [edge_detection_method,
                            canny_threshold1,
                            canny_threshold2,
                            sobel_kernel_size,
                            fsf_image_src,
                            fsf_output,
                            fsf_image_org,
                            ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageEdgeDetectionLogic.ImageEdgeDetectionLogic')
    def __init__(self, **kwargs):
        pass
