# MIT License
#
# Copyright (c) 2026 NTT InfraNet
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


class ConvertLineFromImageToVector(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ConvertLineFromImageToVector

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    推論結果の白線画像から中心線を抽出し、最終的に line の配列として出力する
    プロセッサクラスです。

    - 入力は、pickle化画像ndarray(bytes) または FieldSetFile相当(pandas.Series)です。
    - 入力画像のColorSpaceは BINARY または GRAYSCALE である必要があります。
    - 出力は、検出した line を
        [[(x1,y1),(x2,y2)], ...]
      のリスト形式として pickle化して返します。

    処理概要
    --------------------------------
    - 二値化
    - open / close による前処理
    - skeletonize による 1px 中心線化
    - junction 統合
    - セグメント抽出
    - spur 除去
    - endpoint 再接続
    - 平滑化 / RDP簡略化
    - polyline を line 配列へ展開

    タグ
    --------------------------------
    * image processing
    * centerline
    * skeletonize
    * opencv

    リレーションシップ
    --------------------------------
    * success: 中心線抽出と line 出力が正常終了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。BINARYまたはGRAYSCALEである必要があります。

    Writes Attributes
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはその画像データを content 列に持つ FieldSetFile である必要があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = "vectorize white lines by skeleton graph and output line list"

    threshold = PropertyDescriptor(
        name="Threshold",
        description="二値化のしきい値 [pixel value: 0-255] を指定します。この値以上を白線として扱います。初期値は 200 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="200",
        sensitive=False,
    )

    open_kernel_size = PropertyDescriptor(
        name="Open Kernel Size (px)",
        description="オープニング処理のカーネルサイズ [px] を指定します。小さい白ノイズを除去します。初期値は 3 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="3",
        sensitive=False,
    )

    close_kernel_size = PropertyDescriptor(
        name="Close Kernel Size (px)",
        description="クロージング処理のカーネルサイズ [px] を指定します。小さな切れ目を埋めます。初期値は 3 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="3",
        sensitive=False,
    )

    min_component_area = PropertyDescriptor(
        name="Min Component Area (px^2)",
        description="残す連結成分の最小面積 [px^2] を指定します。これ未満の白領域はノイズとして除去します。初期値は 8 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="8",
        sensitive=False,
    )

    junction_dilate_radius = PropertyDescriptor(
        name="Junction Dilate Radius (px)",
        description="junction をまとめるために膨張させる半径 [px] です。交点周辺の複数分岐点を 1 つに寄せます。初期値は 2 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="2",
        sensitive=False,
    )

    min_spur_length_px = PropertyDescriptor(
        name="Min Spur Length (px)",
        description="spur とみなす最小長さの下限 [px] です。短いヒゲ線の削除に使います。初期値は 6.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="6.0",
        sensitive=False,
    )

    spur_length_width_factor = PropertyDescriptor(
        name="Spur Length Width Factor",
        description="spur 判定時に使う線幅倍率 [ratio] です。実際のしきい値は max(Min Spur Length, この値 × 平均線幅) で決まります。初期値は 1.2 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.2",
        sensitive=False,
    )

    max_gap_px = PropertyDescriptor(
        name="Max Gap (px)",
        description="endpoint 同士を再接続するときの最大距離 [px] です。初期値は 10.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="10.0",
        sensitive=False,
    )

    max_endpoint_angle_deg = PropertyDescriptor(
        name="Max Endpoint Angle (deg)",
        description="endpoint 再接続時に許容する最大角度差 [deg] です。小さいほど直線的な接続だけを許可します。初期値は 25.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="25.0",
        sensitive=False,
    )

    max_lateral_offset_px = PropertyDescriptor(
        name="Max Lateral Offset (px)",
        description="endpoint 再接続時に許容する最大横ずれ量 [px] です。初期値は 3.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="3.0",
        sensitive=False,
    )

    max_width_ratio = PropertyDescriptor(
        name="Max Width Ratio",
        description="endpoint 再接続時に許容する線幅比の最大値 [ratio] です。太さ差が大きい線の誤接続を抑えます。初期値は 1.8 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.8",
        sensitive=False,
    )

    smooth_window = PropertyDescriptor(
        name="Smooth Window (points)",
        description="ポリライン平滑化の移動平均窓サイズ [points] です。大きいほど滑らかになります。初期値は 5 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="5",
        sensitive=False,
    )

    simplify_epsilon = PropertyDescriptor(
        name="Simplify Epsilon (px)",
        description="Ramer-Douglas-Peucker 法の許容誤差 [px] です。大きいほど点数を減らします。初期値は 1.5 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.5",
        sensitive=False,
    )

    min_polyline_length_px = PropertyDescriptor(
        name="Min Polyline Length (px)",
        description="polyline 全体としての最小長さ [px] です。この値以下の短い polyline は出力対象から除外します。初期値は 30.0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="30.0",
        sensitive=False,
    )

    property_descriptors = [
        threshold,
        open_kernel_size,
        close_kernel_size,
        min_component_area,
        junction_dilate_radius,
        min_spur_length_px,
        min_polyline_length_px,
        spur_length_width_factor,
        max_gap_px,
        max_endpoint_angle_deg,
        max_lateral_offset_px,
        max_width_ratio,
        smooth_window,
        simplify_epsilon,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("ConvertLineFromImageToVectorLogic.ConvertLineFromImageToVectorLogic")
    def __init__(self, **kwargs):
        pass