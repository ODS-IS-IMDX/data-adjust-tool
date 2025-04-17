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

class DividePipeLine(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """複数の旗上げ線が紐づく管路を、旗上げ線と１対１になるよう分割する。"""
        tags = [
            "pandas",
            "pickle",
            "fieldsetfile",
            "dataframe",
        ]

    #:
    MAIN_GEOMETRY_NAME = PropertyDescriptor(
        name="Main Geometry Name",
        description="演算対象メインジオメトリのデータ名を指定。",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    MAIN_ATTRIBUTE_NAME = PropertyDescriptor(
        name="Main Attribute Name",
        description="メインジオメトリのフィルタリング属性名を指定。",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUB_GEOMETRY_NAME = PropertyDescriptor(
        name="Sub Geometry Name",
        description="演算対象サブジオメトリのデータ名を指定。",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUB_ATTRIBUTE_NAME = PropertyDescriptor(
        name="Sub Attribute Name",
        description="サブジオメトリのフィルタリング属性名を指定。",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        MAIN_GEOMETRY_NAME,
        MAIN_ATTRIBUTE_NAME,
        SUB_GEOMETRY_NAME,
        SUB_ATTRIBUTE_NAME,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('DividePipeLineLogic.DividePipeLineLogic')
    def __init__(self, **kwargs):
        pass