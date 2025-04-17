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


class AnalyzeDataFrame(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """DataFrame（またはGeoDataFrame）のデータ基本情報を解析し、\
        指定した列のデータ型判定、内容の統計、重複チェック、データ定義の出力を行うプロセッサ。"""
        tags = [
            "analyze"
            "pandas",
            "geopandas",
        ]

    #:
    COLUMN_NAME = PropertyDescriptor(
        name="Column Name",
        description="指定した列のデータ型推定と内容解析を行う。",
        validators=[StandardValidators.ALWAYS_VALID],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    KEY_NAME = PropertyDescriptor(
        name="Key Name",
        description="指定した列をキーとして重複データチェックを行う。",
        validators=[StandardValidators.ALWAYS_VALID],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    DATA_NAME = PropertyDescriptor(
        name="Data Name",
        description="データ定義出力時に使用する名前。",
        default_value="${filename:substringBefore('.')}",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        COLUMN_NAME,
        KEY_NAME,
        DATA_NAME
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("AnalyzeDataFrameLogic.AnalyzeDataFrameLogic")
    def __init__(self, **kwargs):
        pass
