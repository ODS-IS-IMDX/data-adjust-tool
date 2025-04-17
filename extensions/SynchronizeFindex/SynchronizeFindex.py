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


class SynchronizeFindex(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """FieldSetFileを受け取り、指定された2つのデータのFindexを同期させるプロセッサ。"""
        tags = ["pandas", "dataframe", "Synchronize", "python"]

    #:
    SRC_DATA_NAME = PropertyDescriptor(
        name="Src Data Name",
        description="基準となるデータの名前",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    BASE_ATTRIBUTE = PropertyDescriptor(
        name="Base Attribute",
        description="同期に使用する基準の属性名",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    DST_DATA_NAME = PropertyDescriptor(
        name="Dst Data Name",
        description="同期対象となるデータの名前",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    OUTPUT_DATA_NAME = PropertyDescriptor(
        name="OutPut Data Name",
        description="同期後に生成されるデータの出力名",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        SRC_DATA_NAME,
        BASE_ATTRIBUTE,
        DST_DATA_NAME,
        OUTPUT_DATA_NAME
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('SynchronizeFindexLogic.SynchronizeFindexLogic')
    def __init__(self, **kwargs):
        pass
