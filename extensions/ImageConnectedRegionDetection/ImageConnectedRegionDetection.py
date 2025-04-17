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


class ImageConnectedRegionDetection(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageConnectedRegionDetection

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    連結領域を検出するプロセッサクラスです。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 連結領域検出には cv2.connectedComponentsWithStatsを使用して、検出した連結領域(上下左右斜めの画素が隣接している領域)を検出します。
    - 検出座標データをバイト列形式で返します。

    **注意事項:**

    - カラースペースはBINARYである必要があります。異なる場合はエラーとなります。
    - 正しく検出するには、黒背景(255)/白画素(0)である必要があります。
    - 白背景(255)/黒画素(0)でも動作はしますが、検出精度が低下する可能性があります。

    **プロパティ設定値の目安**

    - 図面内に存在する文字列をノイズとして検出したい場合の例:
        ※検出したノイズの座標は後続処理としてImageRegionRemoveプロセッサを使用することで除去することができます。
        Min Area: 0
        Max Area: 500
        Limit Count: -1
        ⇒連結領域の面積が0～500であるものをすべて出力するという意味のプロパティ設定値です。
    
    - 図面内に３つ存在する平面図の座標を特定したい場合の例（図面の構成によりカスタマイズは必要）：
        ※検出した平面図の座標は後続処理としてImageCropプロセッサを使用することで平面図部分の画像を切り出すことができます。
        Min Area: 10000
        Max Area: -1
        Sort Largest: True
        Limit Count: 3
        ⇒平面図は比較的大きいため、連結領域の面積が10000pixel以上の連結領域のうち、面積が大きい方から3領域を出力するという意味のプロパティ設定値です。

    タグ
    --------------------------------
    * image processing
    * connected region detection
    * connectedComponentsWithStats
    * opencv

    プロパティ
    --------------------------------
    **Min Area**

    **必須入力  直接入力**

    - 検出する連結領域の最小面積(縦pixel x 横pixel)を指定します。
    - 0を指定した場合は、連結領域が1pixel x 1pixelの極小サイズの領域も出力の対象となります。
    - 初期値は0です。

    **Max Area**

    **必須入力  直接入力**

    - 検出する連結領域の最大面積(縦pixel x 横pixel)を指定します。
    - -1を指定すると最大面積をチェックしません。
    - 初期値は-1です。

    **Sort Largest**

    **必須入力  リスト入力**

    - 応答する連結領域の順番を連結領域面積の大きい順か小さい順かを指定します。
    - Trueは大きい順(降順)、Falseは小さい順(昇順)で応答します。
    - 初期値はTrueです。

    **Limit Count**

    **必須入力  直接入力**

    - 応答する連結領域座標の数を指定します。
    - -1を指定すると出力する上限数を制限しません。
    - 初期値は-1です。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。

    **FSF Detection Data**

    **必須入力  直接入力**

    - 検出した連結領域座標を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcotentです。

    リレーションシップ
    --------------------------------
    * success: 連結領域検出処理が正常に完了した場合
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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(バイナリ形式、黒背景(255) / 白画素(0))である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Connected Region Detection Processor'

    min_area = PropertyDescriptor(
        name="Min Area",
        description="検出する連結領域の最小面積(縦pixel x 横pixel)を指定します。\
                    0を指定した場合は、連結領域が1pixel x 1pixelの極小サイズの領域も出力の対象となります。\
                    初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=0,
        sensitive=False
    )

    max_area = PropertyDescriptor(
        name="Max Area",
        description="検出する連結領域の最大面積(縦pixel x 横pixel)を指定します。\
                    -1を指定すると最大面積をチェックしません。\
                    初期値は-1です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=-1,
        sensitive=False
    )

    sort_largest = PropertyDescriptor(
        name="Sort Largest",
        description="応答する連結領域の順番を連結領域面積の大きい順か小さい順かを指定します。\
                    Trueは大きい順(降順)、Falseは小さい順(昇順)で応答します。\
                    初期値はTrueです。",
        allowable_values=["True","False"],
        required=True,
        default_value=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    limit_count = PropertyDescriptor(
        name="Limit Count",
        description="応答する連結領域座標の数を指定します。\
                    -1を指定すると出力する上限数を制限しません。\
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
        description="検出した連結領域座標を出力するFieldSetFileの項目名を指定します。\
                    既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    # すべてのプロパティをproperty_descriptorsリストに追加
    property_descriptors = [
        min_area,
        max_area,
        sort_largest,
        limit_count,
        fsf_image_src,
        fsf_detection_data
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageConnectedRegionDetectionLogic.ImageConnectedRegionDetectionLogic')
    def __init__(self, **kwargs):
        pass
