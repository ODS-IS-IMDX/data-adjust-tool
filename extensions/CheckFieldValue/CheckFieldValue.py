# MIT License
#
# Copyright (c) 2026 NTT InfraNet
# Copyright (c) 2026 NTT DATA Japan Co., Ltd.
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


class CheckFieldValue(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = "FlowFileのコンテンツに格納されているFieldSetFile形式に含まれるDWH(Geometry を除く)について、含まれる値をチェックする。"
        tags = ["FieldSetFile", "python", "validate", "regex"]

    # === プロパティ定義 ===

    DWH = PropertyDescriptor(
        name="DWH",
        description="対象属性名/対象ジオメトリ名。Dwh=<GeometryName>/<AttributeName>",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # ※ 演算子に ~ / !~（正規表現）を追加
    COMPARISON_OPERATOR = PropertyDescriptor(
        name="ComparisonOperator",
        description=(
            "比較に使用する演算子。"
            "正規表現は ~（マッチ）/ !~（非マッチ）。"
            "それ以外 (=, !=, <, >, <=, >=) は自動型判定（数値→真偽→文字列）により比較する。"
        ),
        default_value="=",
        allowable_values=["=", "!=", "<", ">", "<=", ">=", "~", "!~"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    CHECK_REG_EX_PATTERN = PropertyDescriptor(
        name="CheckRegExPattern",
        description="比較に使用する値を指定する。値は数値でも文字列でもよい。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    SUFFIX = PropertyDescriptor(
        name="Suffix",
        description="評価結果（bool配列）を書き出す Dwh グループ名（例：Validate）。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    OUTPUT_ATTRIBUTE_PREFIX = PropertyDescriptor(
        name="OutputAttributePrefix",
        description="チェック結果を格納するAttribute名に付与する接頭語",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="CheckResult",
        required=True,
    )

    OUTPUT_ROW_LIST = PropertyDescriptor(
        name="OutputRowList",
        description="条件に合致した行番号を出力する",
        sensitive=False,
        default_value="false",
        allowable_values=["true", "false"],
        required=True
    )

    OUTPUT_ROW_LIST_SPLIT_CHAR = PropertyDescriptor(
        name="OutputRowListSplitChar",
        description="詳細出力時の区切り文字を指定する",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value=",",
        required=True
    )

    EXPECT_TRUE = PropertyDescriptor(
        name="ExpectTrue",
        description="各レコードの比較結果 True をOKとみなすか（true=OK / false=NG）。falseを指定すると判定方向を反転する。",
        default_value="true",
        allowable_values=["true", "false"],
        required=True,
    )

    NULL_HANDLING = PropertyDescriptor(
        name="NullHandling",
        description="値にNull/Non/Nanが含まれていた場合の処理内容",
        default_value="check",
        allowable_values=["skip", "check", "failure"],
        required=True,
    )

    property_descriptors = [
        DWH,
        COMPARISON_OPERATOR,
        SUFFIX,
        CHECK_REG_EX_PATTERN,
        OUTPUT_ROW_LIST,
        OUTPUT_ROW_LIST_SPLIT_CHAR,
        OUTPUT_ATTRIBUTE_PREFIX,
        EXPECT_TRUE,
        NULL_HANDLING
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("CheckFieldValueLogic.CheckFieldValueLogic")
    def __init__(self, **kwargs):
        pass
