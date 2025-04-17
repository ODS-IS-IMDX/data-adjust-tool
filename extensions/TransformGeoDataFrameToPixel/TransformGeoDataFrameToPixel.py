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

from nifiapi.properties import (ExpressionLanguageScope, PropertyDescriptor,
                                StandardValidators)
from raster_to_vector.common.base_processor import BaseProcessor


class TransformGeoDataFrameToPixel(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """指定されたGeoDataFrame内のジオメトリの座標をピクセル系座標に変換し、再度GeoDataFrame形式で出力する。"""
        tags = [
            "geopandas",
            "geodataframe",
            "dataframe",
            "fieldsetfile",
            "python",
        ]

    #:
    WIDTH = PropertyDescriptor(
        name="Width",
        description="画面の横幅（ピクセル単位）。縦横比は保持し、指定された横幅に合わせてスケーリングを行う。Meters per Pixelsを指定した場合は適用されない。",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    LAYER_NAME = PropertyDescriptor(
        name='Layer Name',
        description='対象とするレイヤー名を指定。カンマ区切りで複数指定可能。',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    #:
    SCALE_FACTOR = PropertyDescriptor(
        name='Scale Factor',
        description='GIS座標からピクセル座標に変換する解像度。1メートルあたりのピクセル数を指定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    #:
    OUT_OF_BOUNDS = PropertyDescriptor(
        name='Geometry Out of Bounds',
        description='ジオメトリが少しでも切り取り範囲外にある場合、そのジオメトリを削除するかどうかのフラグ。',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value="False",
        allowable_values=["True", "False"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [WIDTH, LAYER_NAME, SCALE_FACTOR, OUT_OF_BOUNDS]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("TransformGeoDataFrameToPixelLogic.TransformGeoDataFrameToPixelLogic")
    def __init__(self, **kwargs):
        pass
