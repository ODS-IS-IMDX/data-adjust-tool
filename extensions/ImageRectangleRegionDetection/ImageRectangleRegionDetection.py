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


class ImageRectangleRegionDetection(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageRectangleRegionDetection

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    矩形を検出するプロセッサクラスです。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 矩形検出には cv2.findContoursを使用して、矩形を検出します。
    - 検出座標データをバイト列形式で返します。

    **注意事項:**

    - カラースペースはBINARYである必要があります。異なる場合はエラーとなります。
    - 正しく検出するには、黒背景(255)/白画素(0)である必要があります。
    - 白背景(255)/黒画素(0)でも動作はしますが検出精度が低下する可能性があります。

    タグ
    --------------------------------
    * image processing
    * rectangle region detection
    * findContours
    * opencv

    プロパティ
    --------------------------------
    **Return Type**

    **必須入力  リスト入力**

    - 検出した矩形座標を応答する形式を指定します。
    - CONTOURを指定すると検出した矩形の輪郭座標をtuple型で返します。
    - RECTを指定すると検出した矩形の左上座標と右下座標をlist型で返します。
    - 紙データの図面で枠を検出したい場合は歪んでる可能性があるため、CONTOURを指定することを推奨します。
    - 初期値はCONTOURです。

    **Min Area**

    **必須入力  直接入力**

    - 検出する矩形の最小面積(縦pixel x 横pixel)を指定します。
    - 初期値は0です。

    **Max Area**

    **必須入力  直接入力**

    - 検出する矩形の最大面積(縦pixel x 横pixel)を指定します。
    - -1を指定すると最大面積をチェックしません。
    - 初期値は-1です。

    **Sort Largest**

    **必須入力  リスト入力**

    - 応答する矩形座標の順番を矩形面積の大きい順か小さい順かを指定します。
    - Trueは大きい順(降順)、Falseは小さい順(昇順)で応答します。
    - 初期値はTrueです。

    **Limit Count**

    **必須入力  直接入力**

    - 応答する矩形座標の数を指定します。
    - -1を指定すると応答する矩形の数を制限しません。
    - 初期値は-1です。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。

    **FSF Detection Data**

    **必須入力  直接入力**

    - 検出した矩形座標を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcotentです。

    リレーションシップ
    --------------------------------
    * success: 矩形検出処理が正常に完了した場合
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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(BINARY形式、黒背景(255) / 白画素(0))である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Rectangle Region Detection Processor'

    return_type = PropertyDescriptor(
        name="Return Type",
        description="検出した矩形座標を応答する形式を指定します。\
                        CONTOUR: 検出した矩形の輪郭座標をtuple型で返します。\
                        RECT: 検出した矩形の左上座標と右下座標をlist型で返します。\
                    紙データの図面で枠を検出したい場合は歪んでる可能性があるため、CONTOURを指定することを推奨します。\
                    初期値はCONTOURです。",
        allowable_values=["CONTOUR","RECT"],
        required=True,
        default_value="CONTOUR",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    min_area = PropertyDescriptor(
        name="Min Area",
        description="検出する矩形の最小面積(縦pixel x 横pixel)を指定します。\
                    初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=0,
        sensitive=False
    )

    max_area = PropertyDescriptor(
        name="Max Area",
        description="検出する矩形の最大面積(縦pixel x 横pixel)を指定します。\
                    -1を指定すると最大面積をチェックしません。\
                    初期値は-1です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=-1,
        sensitive=False
    )

    sort_largest = PropertyDescriptor(
        name="Sort Largest",
        description="応答する矩形の順番を矩形面積の大きい順か小さい順かを指定します。\
                    Trueは大きい順(降順)、Falseは小さい順(昇順)で応答します。\
                    初期値はTrueです。",
        allowable_values=["True","False"],
        required=True,
        default_value="True",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    limit_count = PropertyDescriptor(
        name="Limit Count",
        description="応答する矩形座標の数を指定します。\
                    -1を指定すると応答する矩形座標の数を制限しません。\
                    初期値は-1です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=-1,
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

    fsf_detection_data = PropertyDescriptor(
        name="FSF Detection Data",
        description="検出した矩形座標を出力するFieldSetFileの項目名を指定します。\
                    既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    # すべてのプロパティをproperty_descriptorsリストに追加
    property_descriptors = [
        return_type,
        min_area,
        max_area,
        sort_largest,
        limit_count,
        fsf_image_src,
        fsf_detection_data
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageRectangleRegionDetectionLogic.ImageRectangleRegionDetectionLogic')
    def __init__(self, **kwargs):
        pass
