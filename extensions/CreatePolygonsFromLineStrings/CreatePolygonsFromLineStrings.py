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


class CreatePolygonsFromLineStrings(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """LineStringオブジェクトからPolygonオブジェクトを生成するプロセッサ。"""
        tags = ["numpy", "shapely", "geometry", "pandas", "polygonize", "linestring", "polygon", "scipy"]

    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name="Geometry Name",
        description="処理対象データのジオメトリデータ名を指定",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    CRS = PropertyDescriptor(
        name="Crs",
        description="座標参照系(CRS)を指定",
        default_value=6677,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    SUFFIX = PropertyDescriptor(
        name="Suffix",
        description="出力するFieldSetFileのサフィックス",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="polygon",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [GEOMETRY_NAME, CRS, SUFFIX]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class(
        "CreatePolygonsFromLineStringsLogic.CreatePolygonsFromLineStringsLogic"
    )
    def __init__(self, **kwargs):
        pass
