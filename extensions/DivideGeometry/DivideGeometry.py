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

from nifiapi.properties import (
    PropertyDescriptor,
    ExpressionLanguageScope,
    StandardValidators,
)

from raster_to_vector.common.base_processor import BaseProcessor


class DivideGeometry(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """FieldSetFileを受け取り、操作対象ジオメトリデータを分割条件データ（Condition）に\
        基づいて２つのジオメトリデータに分割し、処理結果としてFieldSetFileを出力するプロセッサ。"""
        tags = [
            "pandas",
            "pickle",
            "fieldsetfile",
            "dataframe",
        ]

    # 分割元ジオメトリデータ名
    #:
    SRC_GEOMETRY_NAME = PropertyDescriptor(
        name="Src Geometry Name",
        description="操作対象のジオメトリデータ名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )
    # 分割条件データの属性名
    #:
    CONDITION = PropertyDescriptor(
        name="Condition",
        description="分割条件データの属性名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )
    # 検出されたサークルジオメトリデータ名
    #:
    DST_GEOMETRY_NAME = PropertyDescriptor(
        name="Dst Geometry Name",
        description="出力する条件適合データのジオメトリデータ名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )
    # 検出されたサークル以外のジオメトリデータ名
    #:
    OTHER_GEOMETRY_NAME = PropertyDescriptor(
        name="Other Geometry Name",
        description="出力する条件不適合データのジオメトリデータ名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        SRC_GEOMETRY_NAME,
        CONDITION,
        DST_GEOMETRY_NAME,
        OTHER_GEOMETRY_NAME,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("DivideGeometryLogic.DivideGeometryLogic")
    def __init__(self, **kwargs):
        pass
