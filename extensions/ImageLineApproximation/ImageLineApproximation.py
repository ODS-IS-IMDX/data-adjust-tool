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


class ImageLineApproximation(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageLineApproximation

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    管路の入力画像に対して管路を分離し、輪郭から直線近似を行うプロセッサクラスです。
    Measure line thicknessにTrueを指定すると、線の太さも検出します。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - 管路抽出結果からコーナーを検出し、管路をラベリングで分離します。
    - 分離したそれぞれの管路の輪郭から直線近似を行います。
    - 検出できた場合は[(始点X座標, 始点Y座標), (終点X座標, 終点Y座標)]のようなリスト形式のバイト列が返されます。
    - Measure line thicknessにTrueを指定した場合。線の太さも検出し[(始点X座標, 始点Y座標), (終点X座標, 終点Y座標), 線の太さ]のようなリスト形式のバイト列が返されます。
    - 太さ検出例：[[(757, 180), (758, 211), 3.032258064516129], [(244, 194), (280, 196), 3.277777777777778]]

    **注意事項:**

    - カラースペースはBINARYまたはGRAYSCALEである必要があります。異なる場合はエラーとなります。


    タグ
    --------------------------------
    * image processing
    * line detection
    * opencv

    プロパティ
    --------------------------------
    **Max Corners**

    **任意入力  直接入力**

    - 検出されるコーナーの最大数を指定します。(すべてのコーナーを検出したい場合は0を指定してください)
    - 入力画像に対して人間の目で見たおおよその数のコーナー数を入力します。
    - 出力結果を見てMax Cornersを調整していくことを推奨します。
    - 初期値は 50 です。
    
    **quality_level**

    **任意入力  直接入力**

    - コーナーを判断するための最小の品質を指定します。
    - 1.0に近いほど厳密な基準でコーナーを選ぶので検出数は減ります。

        + 0.1:

        画像に多くのコーナーや細かいディテールが含まれており、それら全てを検出したい場合。
        ノイズが多い画像でも、重要なコーナーを見逃さないようにしたい場合。

        + 0.5:

        ノイズの少ない画像でコーナー検出を行いたい場合。
        浅い角度のコーナーを検出したくない場合。

        + 1.0:
        
        人が見て判断したような、明らかなコーナーを検出したい場合。

    - 初期値は 0.3 です。

    **min_distance**

    **任意入力  直接入力**

    - コーナー間の最小の距離(ピクセル)を指定します。
    - この距離よりも近いコーナーは別のコーナーとして検出されません。
    - 初期値は 5 です。

    **block_size**

    **任意入力  直接入力**

    - コーナー検出に使うスムージングの近傍サイズです。
    - 値が小さいほどノイズに敏感になり、値が大きいほど小さなコーナーが検出しにくくなります。
    - 初期値は 5 です。

    **circle_size**

    **任意入力  直接入力**

    - コーナー毎に円を描画して管路を分離します。
    - 円のサイズが小さいと管路がうまく分離できず、大きいと管路がうまく取得できない場合があります。
    - 円のサイズが小さいとL字のコーナーの結果が\のようになります。(コーナーで分離できず始点と終点を直線でつなぐため。)
    - 円のサイズが大きいとL字のコーナーが分離できても分離後の直線が実際よりも短くなる場合があります。(分離するための円によって元の線が消されるため。)
    - 初期値は 4 です。

    **Measure line thickness**

    **必須入力  リスト入力**

    - Trueの場合、線の検出と同時に線の太さも検出します。
    - 線の太さの検出には元画像か元画像をgray scale化した画像のどちらかが必要になります。
    - 元画像の取得先はFSF Image Orgで指定します。
    - 初期値は False です。

    **line thickness threshold**

    **任意入力  直接入力**

    - 線の太さを検出する際に、この閾値以下のピクセルを線の内部の点として計算に利用します。
    - 0 ~ 255の範囲で設定する必要があります。
    - 初期値は 200 です。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。"

    **FSF Line List**

    **必須入力  直接入力**

    - 検出した線分を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcontentです。

    **FSF Image Org**

    **必須入力  直接入力**

    - 線の太さを検出する際に利用する元画像かGRAYSCALE画像を指定します。
    - 初期値はcontentです。

    リレーションシップ
    --------------------------------
    * success: 管路の分離と管路を直線に近似した際、始点と終点を正常に取得した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。BINARYまたはGRAYSCALEである必要があります。

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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(BINARY、GRAYSCALE形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'separates lines and performs linear approximation'

    max_corners = PropertyDescriptor(
        name="Max Corners",
        description="検出されるコーナーの最大数を指定します。\
                    (すべてのコーナーを検出したい場合は0を指定してください)\
                    入力画像に対して人間の目で見たおおよその数のコーナー数を入力します。\
                    出力結果を見てMax Cornersを調整していくことを推奨します。\
                    初期値は 50 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="50",
        sensitive=False
    )

    quality_level = PropertyDescriptor(
        name="Quality Level",
        description="コーナーを判断するための最小の品質を指定します。\
                    1.0に近いほど厳密な基準でコーナーを選ぶので検出数は減ります。\
                    0.1: ノイズが多くても、細かいコーナーや重要なディテールを検出したい場合。\
                    0.5: ノイズの少ない画像で、浅い角度のコーナーを避けて検出したい場合。\
                    1.0: 人が見て明確に認識できるコーナーのみを検出したい場合。\
                    初期値は 0.3 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0.3",
        sensitive=False
    )

    min_distance = PropertyDescriptor(
        name="Min Distance",
        description="コーナー間の最小の距離(ピクセル)を指定します。\
                    この距離よりも近いコーナーは別のコーナーとして検出されません。\
                    初期値は 5 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="5",
        sensitive=False
    )

    block_size = PropertyDescriptor(
        name="Block Size",
        description="コーナー検出に使うスムージングの近傍サイズです。\
                    値が小さいほどノイズに敏感になり、値が大きいほど小さなコーナーが検出しにくくなります。\
                    初期値は 5 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="5",
        sensitive=False
    )

    circle_size = PropertyDescriptor(
        name="Circle Size",
        description="コーナー毎に円を描画して管路を分離します。\
                    円のサイズが小さいと管路がうまく分離できず、大きいと管路がうまく取得できない場合があります。\
                    初期値は 4 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="4",
        sensitive=False
    )

    is_measure_thickness = PropertyDescriptor(
        name="Measure line thickness",
        description="Trueの場合、線の検出と同時に線の太さも検出します。\
                    線の太さの検出には元画像か元画像をgray scale化した画像のどちらかが必要になります。\
                    元画像の取得先はFSF Image Orgで指定します。\
                    初期値は False です。",
        required=True,
        allowable_values=["True", "False"],
        default_value="False",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    line_thickness_threshold = PropertyDescriptor(
        name="line thickness threshold",
        description="線の太さを検出する際に、この閾値以下のピクセルを線の内部の点として計算に利用します。\
                    0 ~ 255の範囲で設定する必要があります。\
                    初期値は 200 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="200",
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

    fsf_line_list = PropertyDescriptor(
        name="FSF Line List",
        description="検出した線分を出力するFieldSetFileの項目名を指定します。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    fsf_image_org = PropertyDescriptor(
        name="FSF Image Org",
        description="線の太さを検出する際に利用する元画像かgray scale画像を指定します。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    property_descriptors = [max_corners,
                            quality_level,
                            min_distance,
                            block_size,
                            circle_size,
                            is_measure_thickness,
                            line_thickness_threshold,
                            fsf_image_src,
                            fsf_line_list,
                            fsf_image_org]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageLineApproximationLogic.ImageLineApproximationLogic')
    def __init__(self, **kwargs):
        pass
