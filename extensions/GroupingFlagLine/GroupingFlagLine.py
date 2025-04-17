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


class GroupingFlagLine(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """FieldSetFileのDwhが<Geometry Name>/<Group Results Name>の値に、メインの端部を延長し、サブの端部と最も直線的に繋がる組み合わせを追加する。"""
        tags = [
            "shapely",
            "scipy",
            "dataframe",
            "fieldsetfile",
            "python",
        ]

    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name="Geometry Name",
        description="FieldSetFileのDwh列のジオメトリ名",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    MAIN_RESULTS_NAME = PropertyDescriptor(
        name="Main Results Name",
        description="メインのFindexのFieldSetFileのDwh列の接尾語。<Geometry Name>/<Main Results Name>というDwhの値を取得。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUB_RESULTS_NAME = PropertyDescriptor(
        name="Sub Results Name",
        description="サブのFindexのFieldSetFileのDwh列の接尾語。<Geometry Name>/<Sub Results Name>というDwhの値を取得。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    GROUP_RESULTS_NAME = PropertyDescriptor(
        name="Group Results Name",
        description="FindexのグループのFieldSetFileのDwh列の接尾語。<Geometry Name>/<Group Results Name>というDwhの値を取得。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUFFIX = PropertyDescriptor(
        name="Suffix",
        description="出力用の接尾語",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    TOLERANCE = PropertyDescriptor(
        name="Tolerance",
        description="メインとサブをグループ化する距離の閾値",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        GEOMETRY_NAME,
        MAIN_RESULTS_NAME,
        SUB_RESULTS_NAME,
        GROUP_RESULTS_NAME,
        SUFFIX,
        TOLERANCE,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('GroupingFlagLineLogic.GroupingFlagLineLogic')
    def __init__(self, **kwargs):
        pass


