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

from raster_to_vector.common.base_processor import BaseProcessor
from nifiapi.properties import (
    PropertyDescriptor,
    ExpressionLanguageScope,
    StandardValidators,
)


PARAM_TRUE = "true"
PARAM_FALSE = "false"


class IntegrateLineString(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """FieldSetFileから線分情報(LineString)を受け取り、分岐しない範囲で１つのポリラインに成形した線分(LineString)に変換するプロセッサ。\
        プロパティの設定により、交差での分割、重複した線分の削除を行う。\
        処理結果として、変換後の線分情報、FIDデータ、変換前と変換後の線分のFIndexを紐づけたペアリングデータの3点を持つFieldSetFileを出力する。"""
        tags = [
            "numpy",
            "dataframe",
            "fieldsetfile",
            "python",
        ]

    #:
    SPLIT = PropertyDescriptor(
        name="Split Lines",
        description="線分(LineString)同士の交差で分割を行う場合はtrue、行わない場合はfalseを指定。",
        default_value=PARAM_FALSE,
        allowable_values=[PARAM_TRUE, PARAM_FALSE],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    #:
    DELETE_DUPLICATES = PropertyDescriptor(
        name="Delete Duplicates",
        description="重複する線分(LineString)の削除を行う場合はtrue、行わない場合はfalseを指定。",
        default_value=PARAM_TRUE,
        allowable_values=[PARAM_TRUE, PARAM_FALSE],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    #:
    INPUT_GEOMETRY_NAME = PropertyDescriptor(
        name="Input Geometry Name",
        description="操作対象のジオメトリデータ名を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    OUTPUT_GEOMETRY_NAME = PropertyDescriptor(
        name="Output Geometry Name",
        description="出力結果のジオメトリデータ名を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        SPLIT,
        DELETE_DUPLICATES,
        INPUT_GEOMETRY_NAME,
        OUTPUT_GEOMETRY_NAME,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("IntegrateLineStringLogic.IntegrateLineStringLogic")
    def __init__(self, **kwargs):
        pass
