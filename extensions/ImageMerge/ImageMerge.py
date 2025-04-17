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


class ImageMerge(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageMerge

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    分割した画像を結合するプロセッサクラスです。

    このプロセッサは分割された画像を結合します。
    分割された画像に円検出や線分検出をしている場合はその座標を結合後の画像に適用させます。
    同じ円や線分をクラスタリングもできます。

    **処理概要:**

    - 入力された分割画像を結合します。
    - 円や線分の座標も元の分割前の画像に適用させます。
    - プロパティの設定によっては円や線分の重複を削除できます。
    - 結合された画像はFieldSetFileのcontent項目に出力します。

    **注意事項:**
    
    - 分割数により、処理時間が長くなります。
    - Java heap memory エラーが出た場合はconf/bootstrap.confよりjava.arg.3=-Xmx1gを調整してください。

    タグ
    --------------------------------
    * image merge

    プロパティ
    --------------------------------
    **Color Space**

    **必須入力  リスト入力**

    - 結合後の画像のカラースペースを設定します。
    - RGB、BINARY、GRAYSCALE、HLS、HSV、BGR から選択可能です。

    **Offset Coordinate X**

    **必須入力  直接入力**

    - 使用するオフセットX座標のFieldSetFileの項目名を指定します。
    - 円や直線の座標を結合後の座標に適用します。
    - 初期値はoffset_coordinate_xです。

    **Offset Coordinate Y**

    **必須入力  直接入力**

    - 使用するオフセットY座標のFieldSetFileの項目名を指定します。
    - 円や直線の座標を結合後の座標に適用します。
    - 初期値はoffset_coordinate_yです。

    **Circle Cluster Epsilon**

    **必須入力  直接入力**

    - 分割時の重なり領域で複数検出した円の中心座標から同一の円とみなす距離(px)を指定します。
    - 小数点以下は切り捨てられます。
    - 小さすぎると本来同一の円であるにもかかわらず、別の円とみなされる傾向が強くなります。
    - 大きすぎると本来別の円であるにもかかわらず、同一の円とみなされる傾向が強くなります。
    - 0はクラスタリングは行わず、検出されたすべての円を残します。
    - 初期値は0です。

    **Line Cluster Epsilon**

    **必須入力  直接入力**

    - 分割時の重なり領域で複数検出した線分の始点、終点、中点から同一の線分とみなす距離(px)を指定します。
    - 小数点以下は切り捨てられます。
    - 小さすぎると本来同一の線分であるにもかかわらず、別の線分とみなされる傾向が強くなります。
    - 大きすぎると本来別の線分であるにもかかわらず、同一の線分とみなされる傾向が強くなります。
    - 0はクラスタリングは行わず、検出されたすべての線分を残します。
    - 初期値は0です。

    **FSF Image Src**

    **必須入力  直接入力**

    - 結合する画像を取得するFieldSetFileの項目名を指定します。
    - 結合された画像はFSF Image Srcの入力にかかわらずcontentに格納されます。
    - 初期値はcontentです。

    **FSF Circle Coordinate**

    **任意入力  直接入力**

    - 円の座標が格納されているFieldSetFileの項目名を指定します。
    - 使用しない場合は空白のままにしてください。

    **FSF Line Coordinate**

    **任意入力  直接入力**

    - 線分の座標が格納されているFieldSetFileの項目名を指定します。
    - 線分の太さ情報は使われず、出力にも使用されません。
    - 使用しない場合は空白のままにしてください。

    リレーションシップ
    --------------------------------
    * success: 画像分割が正常に完了した場合
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
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ImageMerge'

    color_space = PropertyDescriptor(
        name="Color Space",
        description="結合後の画像のカラースペースを設定します。\
                    'RGB'、'BINARY'、'GRAYSCALE'、'HLS'、'HSV'、'BGR'から選択可能です。",
        allowable_values=["RGB", "BINARY", "GRAYSCALE", "HLS", "HSV", "BGR"],
        required=True,
        default_value='RGB',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    offset_coordinate_x = PropertyDescriptor(
        name="Offset Coordinate X",
        description="使用するオフセットX座標のFieldSetFileの項目名を指定します。\
                    円や直線の座標を結合後の座標に適用します。\
                    初期値はoffset_coordinate_xです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="offset_coordinate_x",
        sensitive=False
    )

    offset_coordinate_y = PropertyDescriptor(
        name="Offset Coordinate Y",
        description="使用するオフセットY座標のFieldSetFileの項目名を指定します。\
                    円や直線の座標を結合後の座標に適用します。\
                    初期値はoffset_coordinate_yです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="offset_coordinate_y",
        sensitive=False
    )

    circle_cluster_epsilon = PropertyDescriptor(
        name="Circle Cluster Epsilon",
        description="分割時の重なり領域で複数検出した円の中心座標から同一の円とみなす距離(px)を指定します。\
                    小数点以下は切り捨てられます。\
                    小さすぎると本来同一の円であるにもかかわらず、別の円とみなされる傾向が強くなります。\
                    大きすぎると本来別の円であるにもかかわらず、同一の円とみなされる傾向が強くなります。\
                    0はクラスタリングは行わず、検出されたすべての円を残します。\
                    初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=0,
        sensitive=False
    )

    line_cluster_epsilon = PropertyDescriptor(
        name="Line Cluster Epsilon",
        description="分割時の重なり領域で複数検出した線分の始点、終点、中点から同一の線分とみなす距離(px)を指定します。\
                    小数点以下は切り捨てられます。\
                    小さすぎると本来同一の線分であるにもかかわらず、別の線分とみなされる傾向が強くなります。\
                    大きすぎると本来別の線分であるにもかかわらず、同一の線分とみなされる傾向が強くなります。\
                    0はクラスタリングは行わず、検出されたすべての線分を残します。\
                    初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=0,
        sensitive=False
    )

    fsf_image_src = PropertyDescriptor(
        name="FSF Image Src",
        description="結合する画像を取得するFieldSetFileの項目名を指定します。\
                    結合された画像はFSF Image Srcの入力にかかわらずcontentに格納されます。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    fsf_circle_coordinate = PropertyDescriptor(
        name="FSF Circle Coordinate",
        description="円の座標が格納されているFieldSetFileの項目名を指定します。\
                    使用しない場合は空白のままにしてください。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="",
        sensitive=False
    )

    fsf_line_coordinate = PropertyDescriptor(
        name="FSF Line Coordinate",
        description="線分の座標が格納されているFieldSetFileの項目名を指定します。\
                    線分の太さ情報は使われず、出力にも使用されません。\
                    使用しない場合は空白のままにしてください。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="",
        sensitive=False
    )

    property_descriptors = [
        color_space,
        offset_coordinate_x,
        offset_coordinate_y,
        circle_cluster_epsilon,
        line_cluster_epsilon,
        fsf_image_src,
        fsf_circle_coordinate,
        fsf_line_coordinate
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageMergeLogic.ImageMergeLogic')
    def __init__(self, **kwargs):
        pass
