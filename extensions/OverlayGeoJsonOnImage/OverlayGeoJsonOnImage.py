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
    ExpressionLanguageScope,
    StandardValidators,
)
from raster_to_vector.common.base_processor import BaseProcessor


class OverlayGeoJsonOnImage(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """GeoJSON(ピクセル座標)の地物を画像に重ねる"""
        tags = [
            "geojson",
            "image",
            "overlay",
            "GeoNdArray",
        ]

    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name="Geometry Dwh Name",
        description="対象のGeoJsonを持つDwh名",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    IMAGE_NAME = PropertyDescriptor(
        name="Image Dwh Name",
        description="対象の画像(Numpy配列)を持つDwh名",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    GEOMETRY_COLOR = PropertyDescriptor(
        name="Geometry Color",
        description="描画する地物の色(R, G, B)",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="255,0,0",
        required=True,
    )

    #:
    POLYGON_TRANSMITTANCE = PropertyDescriptor(
        name="Polygon Transmittance",
        description="ポリゴン塗りつぶし透過率",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="0.3",
        required=True,
    )

    #:
    THICKNESS = PropertyDescriptor(
        name="thickness",
        description="描画する線の太さ",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="2",
        required=True,
    )

    #:
    OUTPUTDIR = PropertyDescriptor(
        name="Output Directry",
        description="出力先フォルダ名",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    OUTPUTFILENAME = PropertyDescriptor(
        name="Output File Name",
        description="出力先ファイル名",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    EXTENSION = PropertyDescriptor(
        name="Extension",
        description="出力拡張子",
        allowable_values=["jpg", "png"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        GEOMETRY_NAME,
        IMAGE_NAME,
        GEOMETRY_COLOR,
        POLYGON_TRANSMITTANCE,
        THICKNESS,
        OUTPUTDIR,
        OUTPUTFILENAME,
        EXTENSION
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("OverlayGeoJsonOnImageLogic.OverlayGeoJsonOnImageLogic")
    def __init__(self, **kwargs):
        pass
