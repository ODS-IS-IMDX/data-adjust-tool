# MIT License
# 
# Copyright (c) 2026 NTT InfraNet
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
    StandardValidators,
    ExpressionLanguageScope,
)
from raster_to_vector.common.base_processor import BaseProcessor


class CreateIDFromDWH(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """指定DWHのFindex配列を雛形として、UUID/連番を生成するプロセッサ。"""
        tags = ["ID", "UUID", "Sequence", "FieldSetFile", "Python"]

    # 雛形DWH名
    #:
    BASE_DWH_NAME = PropertyDescriptor(
        name="Base DWH Name",
        description="雛形となるDWH名。入力行のDWHを雛形とする",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # 出力DWH名
    #:
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名（FlowFile属性DWHも更新）",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # UUID/連番方式
    #:
    GENERATOR_TYPE = PropertyDescriptor(
        name="Generator Type",
        description="UUID/SEQUENCEの方式",
        default_value="UUID",
        allowable_values=[
            "UUID",
            "SEQ_ASC_ONE", "SEQ_ASC_ZERO", "SEQ_DES_ONE", "SEQ_DES_ZERO"
        ],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [BASE_DWH_NAME, OUTPUT_DWH_NAME, GENERATOR_TYPE]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class(
        "CreateIDFromDWHLogic.CreateIDFromDWHLogic"
    )
    def __init__(self, **kwargs):
        pass
