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


class ImageLeaderDetection(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageLeaderDetection

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    管路の条数と段数をカウントするプロセッサクラスです。

    **処理概要:**

    - 円検出結果(管路候補)と旗上げ線の座標を受け取り、旗上げ線に対応する管路を検出し、管路の条数と段数を返します。

    **注意事項:**

    - 旗上げ線の座標(line_coords)は、バイト列形式を文字列化した形式である必要があります。


    タグ
    --------------------------------
    * image processing
    * leader

    プロパティ
    --------------------------------
    **Search Range Factor**

    **任意入力  直接入力**

    - 指定された旗上げ線に対応する管路の集合を探索する範囲の調整値を指定します。
    - 0を指定した場合は、画像全体を探索範囲とします。処理時間に問題がある場合は0より大きい値(1.5以上を推奨)を設定してください。
    - 旗上げ線と直交する方向に、旗上げ線の長さに本指定値を乗じた範囲に絞って管路を探索します。
    - 初期値は 0 です。
    
    **Cluster Epsilon**

    **任意入力  直接入力**

    - 近接する管路を同一クラスタとして判定する際の距離閾値(pixel単位)を指定します。
    - 小さすぎると本来同一の管路の集合であるにもかかわらず別の管路集合と見なされる傾向が強くなります。
    - 大きすぎると本来別の管路の集合であるにもかかわらず別の管路集合と見なされる傾向が強くなります。
    - 有効値は0以上です。0はクラスタリングは行わず検出されたすべての円を管理の集合と見なします。
    - 管路の集合をクラスタリングに使用するため、管路図形(円形)の半径の1.5倍～2倍程度を推奨します。
    - 初期値は 40 です。

    **Count Columns Threshold**

    **任意入力  直接入力**

    - 管路の条数算出処理の端数を切り上げる閾値を指定します。
    - 管路が交互に埋設(各段の管路が下の管路の間に配置)されている状態などを考慮するための閾値です。
    - Count Columns Thresholdが0.5の時、管路の条数が5.4と算出された場合は切り捨てて5、管路の条数が5.5と算出された場合は切り上げて6として条数をカウントします。
    - 初期値は 0.5 です。

    - 利用例：
    - 管路を表現する円図形間において横方向に僅かでも隙間がある場合に条数を切り上げるには0.1を指定します。
    - 管路の条数が5.0と算出された場合は5、管路の条数が5.1と算出された場合は切り上げて6として条数をカウントする。
    - 本プロセッサの結果を基に地面を掘り下げる際に、条数が5とカウントされてしまうと管路5本分の幅で地面を掘ってしまい管路を損傷させてしまう可能性があるため、端数を切り上げる閾値を設けています。

    **Coords Suffix**

    **任意入力  直接入力**

    - 管旗上げ線座標が設定されたFieldSetFileの項目名を指定します。
    - 初期値は line_coords です。

    **Pipe Columns Suffix**

    **任意入力  直接入力**

    - 管路の条数を出力するFieldSetFileの項目名を指定します。
    - 初期値は 条数 です。

    **Pipe Rows Suffix**

    **任意入力  直接入力**

    - 管路の段数を出力するFieldSetFileの項目名を指定します。
    - 初期値は 段数 です。

    リレーションシップ
    --------------------------------
    * success: 管路の条数と段数のカウント処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * line_coords: 旗上げ線の両端の座標[(始点x、始点y), (終点x,終点y)]。バイト列を文字列化した形式である必要があります。

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
    * バイトデータ形式の円検出結果と旗上げ線の座標を持つFieldSetFile

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ImageLeaderDetection'

    search_range_factor = PropertyDescriptor(
        name="Search Range Factor",
        description="指定された旗上げ線に対応する管路の集合を探索する範囲の調整値を指定します。\
            0を指定した場合は画像全体を対象とします。処理時間に問題がある場合は0より大きい値(1.5以上を推奨)を設定してください。\
            旗上げ線と直交する方向に、旗上げ線の長さに本指定値を乗じた範囲に絞って管路を探索します。初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value=0,
        sensitive=False
    )
    
    cluster_epsilon = PropertyDescriptor(
        name="Cluster Epsilon",
        description="指定された旗上げ線に対応する管路の集合をクラスタリングする調整値を指定します。\
            0はクラスタリングは行わず検出されたすべての円を管理の集合と見なし条数と段数をカウントします。\
            1以上を指定すると管路の集合をクラスタリングし、旗上げ線に最も近い管路の集合の条数と段数をカウントします。\
            小さすぎると本来同一の管路の集合であるにもかかわらず別の管路集合と見なされる傾向が強くなります。\
            大きすぎると本来別の管路の集合であるにもかかわらず同一の管路集合に含まれると見なされる傾向が強くなります。\
            初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value=0,
        sensitive=False
    )

    count_columns_threshold = PropertyDescriptor(
        name="Count Columns Threshold",
        description="管路の条数算出処理の端数を切り上げる閾値を指定します。\
            管路が交互に埋設(各段の管路が下の管路の間に配置)されている状態などを考慮するための閾値です。\
            e.g. count_columns_thresholdが0.5の時、管路の条数が5.4と算出された場合は切り捨てては5、管路の条数が5.5と算出された場合は切り上げて6となる。\
            初期値は0.5です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value=0.5,
        sensitive=False
    )

    line_coords_suffix = PropertyDescriptor(
        name="Line Coords Suffix",
        description="旗上げ線座標の識別名(入力データ)を指定します。\
            初期値は'line_coords'です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value='line_coords',
        sensitive=False
    )

    pipe_columns_suffix = PropertyDescriptor(
        name="Pipe Columns Suffix",
        description="管路の条数の識別名(出力データ)を指定します。\
            初期値は'条数'です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value='条数',
        sensitive=False
    )

    pipe_rows_suffix = PropertyDescriptor(
        name="Pipe Rows Suffix",
        description="管路の段数の識別名(出力データ)を指定します。\
            初期値は'段数'です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value='段数',
        sensitive=False
    )

    property_descriptors = [
        search_range_factor,
        cluster_epsilon,
        count_columns_threshold,
        line_coords_suffix,
        pipe_columns_suffix,
        pipe_rows_suffix
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageLeaderDetectionLogic.ImageLeaderDetectionLogic')
    def __init__(self, **kwargs):
        pass
