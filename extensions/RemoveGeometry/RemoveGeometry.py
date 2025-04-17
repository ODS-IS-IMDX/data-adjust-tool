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

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
from raster_to_vector.common.base_processor import BaseProcessor


class RemoveGeometry(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """FieldSetFileを受け取り、ジオメトリデータを条件(Condition)に基づいて削除した後、\
        処理結果としてFieldSetFileを出力するプロセッサ。"""
        tags = [
            "pandas",
            "pickle",
            "fieldsetfile",
            "dataframe",
        ]

    # 処理対象のジオメトリデータ名
    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name="Geometry Name",
        description="操作対象のジオメトリデータ名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )
    # 削除条件データ名の属性名
    #:
    CONDITION = PropertyDescriptor(
        name="Condition",
        description="削除するデータ名の属性名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )
    # 削除後のレコードセットのデータ名
    #:
    OUTPUT_NAME = PropertyDescriptor(
        name="Output Name",
        description="削除後のFieldSetFileのジオメトリデータ名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        GEOMETRY_NAME,
        CONDITION,
        OUTPUT_NAME,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('RemoveGeometryLogic.RemoveGeometryLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)