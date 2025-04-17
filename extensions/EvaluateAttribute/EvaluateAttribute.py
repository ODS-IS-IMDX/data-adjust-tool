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


class EvaluateAttribute(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """指定されたジオメトリデータの属性を比較し、その結果をFieldSetFile形式のbool型リストとして出力結果に追加する。"""
        tags = ["FieldSetFile", "python", "evaluate"]

    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name="GeometryName",
        description="対象のジオメトリデータ名。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    ATTRIBUTE_NAME = PropertyDescriptor(
        name="AttributeName",
        description="対象の属性データ名。Dwhが<GeometryName>/<AttributeName>のValueはタプルのリストの場合にのみ処理が行われる。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    COMPARISON_OPERATOR = PropertyDescriptor(
        name="ComparisonOperator",
        description="比較に使用する演算子を指定する。比較対象となる値が数値の場合は数値として、それ以外の場合は文字列として比較する。Resultsがfalseの場合にのみ適用される。",
        default_value="=",
        allowable_values=["=", "!=", "<", ">", "<=", ">="],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    COMPARISON_VALUE = PropertyDescriptor(
        name="ComparisonValue",
        description="比較に使用する値を指定する。値は数値でも文字列でもよい。Resultsがfalseの場合にのみ適用される。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUFFIX = PropertyDescriptor(
        name="Suffix",
        description="比較結果のbool型リストが格納されるグループのDwh名",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    RESULTS = PropertyDescriptor(
        name="Results",
        description="対象となる属性がresults型かどうかを指定する。trueの場合はresults型の左辺の値を属性値として比較対象とする。Dwhが<GeometryName>/FIDの値が存在する場合にのみ処理が行われる。",
        default_value="false",
        allowable_values=["true", "false"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=False,
    )

    property_descriptors = [
        GEOMETRY_NAME,
        ATTRIBUTE_NAME,
        COMPARISON_OPERATOR,
        COMPARISON_VALUE,
        SUFFIX,
        RESULTS,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("EvaluateAttributeLogic.EvaluateAttributeLogic")
    def __init__(self, **kwargs):
        pass
