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


class ImageOCR(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageOCR

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像から文字列の認識を行い、文字列の位置と認識した文字列を返すプロセッサです。
    複数のOCR Engineの中から1つを選択することができます。
    EasyOCRは一番精度が良いですが、実行に時間がかかります。
    旗上げ線の座標(p1, p2)が渡された場合は、旗上げ線を水平にする回転を行ってからOCRを実行します。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 選択されたOCR Engineを使用して画像に存在する文字列の領域と認識結果を返します。
    - EasyOCR, TesseractOCRは認識した文字列を囲む矩形領域の座標情報も返します。
    - 返却する値の形式は次のようになります。
    - [(top_left, top_right, bottom_right, bottom_left), 認識した文字列, 確信度]
    - 旗上げ線の座標(p1, p2)と文字の関係： p1に文字列の開始箇所が来ることを前提としています。回転は文字が旗上げ線の上になる方向に回転します。

    **注意事項:**

    タグ
    --------------------------------
    * image processing
    * OCR

    プロパティ
    --------------------------------
    **OCR Engine**

    **必須入力  リスト入力**

    - OCR Engineを下記から選択します

    #. EasyOCR：精度は良いが、実行時間は遅いです。
    #. TesseractOCR：精度はEasyOCRほど良くないが、実行時間は早いです。

    - 初期値は EasyOCR です。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像が設定されたFieldSetFileの項目名を指定します。
    - 初期値はcontentです。

    **FSF Output**

    **必須入力  直接入力**

    - 管旗上げ線座標が設定されたFieldSetFileの項目名を指定します。
    - 初期値はcontentです。

    **line_coords_suffix**

    **任意入力  直接入力**

    - 旗上げ線座標の識別名(入力データ)を指定します。
    - 初期値はNoneです。

    リレーションシップ
    --------------------------------
    * success: 線分検出処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * なし

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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。

    System Resource Considerations
    --------------------------------
    * なし
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Line Detection Processor'

    ocr_engine = PropertyDescriptor(
        name="OCR Engine",
        description="OCR Engineを下記から選択します。\
                    EasyOCR：精度は良いが、実行時間は遅いです。\
                    TesseractOCR：精度はEasyOCRほど良くないが、実行時間は早いです。\
                    初期値は EasyOCRです。",
        required=True,
        allowable_values=["EasyOCR", "TesseractOCR"],
        default_value="EasyOCR",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    fsf_image_src = PropertyDescriptor(
        name="FSF Image Src",
        description="処理する画像が設定されたFieldSetFileの項目名を指定します。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    fsf_output = PropertyDescriptor(
        name="FSF Output",
        description="管旗上げ線座標が設定されたFieldSetFileの項目名を指定します。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    line_coords_suffix = PropertyDescriptor(
        name="Line Coords Suffix",
        description="旗上げ線座標の識別名(入力データ)を指定します。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value=None,
        sensitive=False
    )

    # すべてのプロパティをproperty_descriptorsリストに追加
    property_descriptors = [
        ocr_engine,
        fsf_image_src,
        fsf_output,
        line_coords_suffix,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageOCRLogic.ImageOCRLogic')
    def __init__(self, **kwargs):
        pass
