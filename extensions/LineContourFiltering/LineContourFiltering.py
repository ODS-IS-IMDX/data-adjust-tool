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


class LineContourFiltering(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    LineContourFiltering

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    ハフ変換などで検出した線分やその線分の太さ、輪郭抽出で抽出した輪郭をフィルタリングするプロセッサクラスです。

    **処理概要:**

    - 入力データと属性の妥当性をチェックします。
    - ハフ変換などで検出した線分の座標から長さを計算します。その長さから条件に合うフィルタリングを行います。
    - 入力された線分データに太さのデータがある場合、太さから条件に合うフィルタリングを行います。
    - 輪郭の場合は輪郭の面積から条件に合うフィルタリングを行います。
    - フィルタリング結果は入力されたデータと同じ形式で返されます。
    - フィルタリング結果によっては空のリスト[]のバイト列が返されることがあります。

    **注意事項:**

    - 線分のフィルタリングの場合はリスト形式で[(始点x座標, 始点y座標), (終点x座標, 終点y座標)]または、
    - [(始点x座標, 始点y座標), (終点x座標, 終点y座標), 太さ]の構造である必要があります。

    タグ
    --------------------------------
    * image processing
    * hough transform
    * line detection
    * line thickness
    * contour detection
    * opencv

    プロパティ
    --------------------------------
    **Conditional Params**

    **必須入力  リスト入力**

    - ドロップダウン形式にてフィルタリングの条件を指定します。
    - 線分の場合は長さを、輪郭の場合は面積を用いてフィルタリングします。
    - '<>'は入力した値と異なるものを残すようにフィルタリングします。
    - '='は入力した値と同じものを残すようにフィルタリングします。
    - '>'は入力した値より大きいものを残すようにフィルタリングします。
    - '<'は入力した値より小さいものを残すようにフィルタリングします。
    - 'max'は入力した値に関係なく、最大のものを残すようにフィルタリングします。(最大が複数個の場合はすべて残します。)
    - 'min'は入力した値に関係なく、最小のものを残すようにフィルタリングします。(最小が複数個の場合はすべて残します。)
    - 'average'は入力した値に関係なく、平均と同じ長さを残すようにフィルタリングします。
    - 'between'は入力した2つの値の間のものを残すようフィルタリングします。(入力した2つの値も残されます。)
    - 初期値は'<>'です。

    **Conditional Values**

    **必須入力  直接入力**

    - フィルタリングの条件値を指定します。
    - Filtering ObjectがThicknessの場合、小数第2位まで入力値が反映されます。
    - それ以下は四捨五入されます。
    - Conditional Paramsが'between'の場合、Conditional ValuesとConditional Values 2の大小は不問です。
    - Conditional Paramsが'max'、'min'、'average'の場合、Conditional Valuesは使用されません。
    - 初期値は100です。

    **Conditional Values 2**

    **任意入力  直接入力**

    - フィルタリングの条件値を指定します。
    - Filtering ObjectがThicknessの場合、小数第2位まで入力値が反映されます。
    - それ以下は四捨五入されます。
    - Conditional Values 2はConditional Paramsが'between'の場合のみ使用されます。
    - 初期値は0です。
    
    **Filtering Object**

    **必須入力  リスト入力**

    - 線分をフィルタリングする際の対象を指定します。
    - 初期値はLengthです。

    **Output Line Thickness**

    **必須入力  リスト入力**

    - 線分フィルタリング後の出力に太さ情報を含めるかを指定します。
    - 初期値はFalseです。

    **Field Set File Filtering Target**

    **任意入力  直接入力**

    - フィルタリングする線分や輪郭が格納されているField Set Fileの項目名を入力します。
    - フィルタリング条件はConditional Params、Conditional Values、Conditional Values 2の入力値が使用されます。
    - 初期値は'content'です

    リレーションシップ
    --------------------------------
    * success: フィルタリング処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。指定はありません。

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
    * バイトデータ形式の座標データを持つFieldSetFile

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Line Filtering Processor'

    conditional_params = PropertyDescriptor(
        name="Conditional Params",
        description="ドロップダウン形式にてフィルタリングの条件を指定します。\
                    線分の場合は長さを、輪郭の場合は面積を用いてフィルタリングします。\
                        '<>': 入力した値と異なるものを残すようにフィルタリングします。\
                        '=': 入力した値と同じものを残すようにフィルタリングします。\
                        '>': 入力した値より大きいものを残すようにフィルタリングします。\
                        '<': 入力した値より小さいものを残すようにフィルタリングします。\
                        'max': 入力した値に関係なく、最大のものを残すようにフィルタリングします。(最大が複数個の場合はすべて残します。)\
                        'min': 入力した値に関係なく、最小のものを残すようにフィルタリングします。(最小が複数個の場合はすべて残します。)\
                        'average': 入力した値に関係なく、平均と同じ長さを残すようにフィルタリングします。\
                        'between': 入力した2つの値の間のものを残すようフィルタリングします。(入力した2つの値も残されます。)\
                    初期値は'<>'です。",
        allowable_values=["<>","=",">","<","max","min","average","between"],
        required=True,
        default_value="<>",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    conditional_values = PropertyDescriptor(
        name="Conditional Values",
        description="フィルタリングの条件値を指定します。\
                    Conditional Paramsが'between'の場合、Conditional ValuesとConditional Values 2の大小は不問です。\
                    Conditional Paramsが'max'、'min'、'average'の場合、Conditional Valuesは使用されません。\
                    初期値は100です。)",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=100,
        sensitive=False
    )

    conditional_values_2 = PropertyDescriptor(
        name="Conditional Values 2",
        description="フィルタリングの条件値を指定します。\
                    Conditional Values 2はConditional Paramsが'between'の場合のみ使用されます。\
                    初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value=0,
        sensitive=False
    )

    filtering_object = PropertyDescriptor(
        name="Filtering Object",
        description="線分をフィルタリングする際の対象を指定します。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=["Length","Thickness"],
        required=True,
        default_value='Length',
        sensitive=False
    )

    output_line_thickness = PropertyDescriptor(
        name="Output Line Thickness",
        description="線分フィルタリング後の出力に太さ情報を含めるか指定します。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=["True","False"],
        required=True,
        default_value='False',
        sensitive=False
    )

    fsf_filtering_target = PropertyDescriptor(
        name="Field Set File Filtering Target",
        description="フィルタリングする線分や輪郭が格納されているField Set Fileの項目名を入力します。\
                    フィルタリング条件はConditional Params、Conditional Values、Conditional Values 2の入力値が使用されます。\
                    初期値は'content'です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value='content',
        sensitive=False
    )

    # すべてのプロパティをproperty_descriptorsリストに追加
    property_descriptors = [
        conditional_params,
        conditional_values,
        conditional_values_2,
        filtering_object,
        output_line_thickness,
        fsf_filtering_target
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('LineContourFilteringLogic.LineContourFilteringLogic')
    def __init__(self, **kwargs):
        pass
