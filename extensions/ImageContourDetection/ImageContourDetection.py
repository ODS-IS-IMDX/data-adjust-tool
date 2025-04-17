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


class ImageContourDetection(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageContourDetection

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像の輪郭検出を行うプロセッサクラスです。

    **処理概要:**

    入力された画像データに対して輪郭抽出処理を行い、検出された輪郭座標をバイト列形式を返します。

    - 入力データと属性の妥当性を確認します。
    - 指定されたモードと近似方法に基づいて輪郭を検出します。
    - 検出された輪郭座標をバイト列形式で返します。

    **注意事項:**

    - カラースペースはBINARYである必要があります。異なる場合はエラーとなります。
    - 入力画像のフォーマットと品質によって、検出結果が異なる場合があります。
    - 大きな画像を処理する場合、処理時間が長くなる可能性があります。

    タグ
    --------------------------------
    * image processing
    * contour detection
    * opencv

    プロパティ
    --------------------------------
    **Contour Retrieval Mode**

    **必須入力  リスト入力**

    - 輪郭をどのように抽出するかを指定します。

    #. RETR_EXTERNAL: 外側の輪郭のみを抽出します。
    #. RETR_LIST: すべての輪郭をリスト化します。
    #. RETR_CCOMP: 二重階層の輪郭を抽出します。
    #. RETR_TREE: 輪郭の階層情報を保持して抽出します。

    - 初期値はRETR_EXTERNALです。

    **Contour Approximation Method**

    **必須入力  リスト入力**

    - 輪郭の近似方法を指定します。

    #. CHAIN_APPROX_SIMPLE: 直線を多く含む輪郭を簡略化します。
    #. CHAIN_APPROX_NONE: すべての輪郭点を保持します。

    - 初期値はCHAIN_APPROX_SIMPLEです。

    **Minimum Contour Area**

    **任意入力  直接入力**

    - 最小輪郭面積を指定します。
    - この値未満の輪郭は無視されます。輪郭のフィルタリングを行わない場合は0を指定します。
    - 初期値は0です。

    リレーションシップ
    --------------------------------
    * success: 輪郭検出処理が正常に完了した場合
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
        description = 'ImageContourDetection'

    contour_retrieval_mode = PropertyDescriptor(
        name="Contour Retrieval Mode",
        description="輪郭をどのように抽出するかを指定します。\
                        RETR_EXTERNAL: 外側の輪郭のみを抽出します。\
                        RETR_LIST: すべての輪郭をリスト化します。\
                        RETR_CCOMP: 二重階層の輪郭を抽出します。\
                        RETR_TREE: 輪郭の階層情報を保持して抽出します。\
                    初期値はRETR_EXTERNALです。",
        allowable_values=["RETR_EXTERNAL","RETR_LIST","RETR_CCOMP","RETR_TREE"],
        required=True,
        default_value="RETR_EXTERNAL",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    contour_approximation_method = PropertyDescriptor(
        name="Contour Approximation Method",
        description="輪郭の近似方法を指定します。\
                        CHAIN_APPROX_SIMPLE: 直線を多く含む輪郭を簡略化します。\
                        CHAIN_APPROX_NONE: すべての輪郭点を保持します。\
                    初期値はCHAIN_APPROX_SIMPLEです。",
        allowable_values=["CHAIN_APPROX_SIMPLE","CHAIN_APPROX_NONE"],
        required=True,
        default_value="CHAIN_APPROX_SIMPLE",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    min_contour_area = PropertyDescriptor(
        name="Minimum Contour Area",
        description="最小輪郭面積を指定します。\
                    この値未満の輪郭は無視されます。輪郭のフィルタリングを行わない場合は0を指定します。\
                    初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0",
        sensitive=False
    )

    property_descriptors = [contour_retrieval_mode, contour_approximation_method, min_contour_area]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageContourDetectionLogic.ImageContourDetectionLogic')
    def __init__(self, **kwargs):
        pass
