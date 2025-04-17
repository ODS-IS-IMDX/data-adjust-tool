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
    ExpressionLanguageScope,
    PropertyDescriptor,
    StandardValidators,
)


class ClipFlagLineRanges(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """旗上げグループの外接矩形の情報を取得する"""
        tags = [
            "geopandas",
            "geodataframe",
            "dataframe",
            "fieldsetfile",
            "python",
        ]

    #:
    FLAG_LINE_NAME = PropertyDescriptor(
        name="Flag Line Name",
        description="外接矩形取得で使用する旗上げ情報",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    CIRCLE_NAME = PropertyDescriptor(
        name="Circle Name",
        description="外接矩形取得で使用するサークル情報",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    PAIRING_NAME = PropertyDescriptor(
        name="Pairing Name",
        description="旗上げ情報に対するサークル情報IDリスト",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    FLAG_LINE_GROUP_NAME = PropertyDescriptor(
        name="Flag Line Group Name",
        description="連結している旗上げ情報のIDリスト",
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    DST_INFO_NAME = PropertyDescriptor(
        name="Dst Info Name",
        description="出力名",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    CRS = PropertyDescriptor(
        name="CRS",
        description="座標参照系（CRS）",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=6677,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    property_descriptors = [
        FLAG_LINE_NAME,
        CIRCLE_NAME,
        PAIRING_NAME,
        FLAG_LINE_GROUP_NAME,
        DST_INFO_NAME,
        CRS,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("ClipFlagLineRangesLogic.ClipFlagLineRangesLogic")
    def __init__(self, **kwargs):
        pass
