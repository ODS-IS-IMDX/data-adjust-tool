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

# Python標準ライブラリ
import base64
import io
import pickle
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")

import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
from raster_to_vector.common.base_processor import BaseProcessor


class ConcatenateAttribute(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """FieldSetFileを受け取り、操作対象レコードのデータ間の関連性に基づいて連結処理を行い、\
                         処理結果としてFieldSetFileを出力するプロセッサ。"""
        tags = ["pandas", "dataframe", "concatenate", "attribute", "python"]

    #:
    MAIN_GEOMETRY_NAME = PropertyDescriptor(
        name="Main Geometry Name",
        description="メインのジオメトリデータ名",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    MAIN_ATTRIBUTE_NAME = PropertyDescriptor(
        name="Main Attribute Name",
        description="メインのジオメトリデータの属性名",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUB_GEOMETRY_NAME = PropertyDescriptor(
        name="Sub Geometry Name",
        description="サブのジオメトリデータ名",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUB_ATTRIBUTE_NAME = PropertyDescriptor(
        name="Sub Attribute Name",
        description="サブのジオメトリデータの属性名\
                    複数の値を指定する場合はカンマ区切りで指定する。(例: 属性名①,属性名②,...)",
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUB_GROUP_CONDITION = PropertyDescriptor(
        name="Sub Group Condition",
        description="サブグループの連結する条件データの属性名",
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    property_descriptors = [
        MAIN_GEOMETRY_NAME,
        MAIN_ATTRIBUTE_NAME,
        SUB_GEOMETRY_NAME,
        SUB_ATTRIBUTE_NAME,
        SUB_GROUP_CONDITION,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConcatenateAttributeLogic.ConcatenateAttributeLogic')
    def __init__(self, **kwargs):
        pass
