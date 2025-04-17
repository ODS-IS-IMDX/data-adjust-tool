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


class UpdateSpatialDefinition(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """指定された緯度・経度の地理座標に基づいて、GeoDataFrame内の座標を移動およびスケール調整する。"""
        tags = ["georeference", "geodataframe", "gis", "python"]

    # Origin Position
    CENTER = 'center'
    TOP_LEFT = 'top-left'
    TOP_RIGHT = 'top-right'
    BOTTOM_LEFT = 'bottom-left'
    BOTTOM_RIGHT = 'bottom-right'

    # プロパティ設定
    #:
    ORIGIN_LATITUDE = PropertyDescriptor(
        name='Origin Latitude',
        description='GeoDataFrameの基準となる緯度オフセットを指定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='0.0',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    #:
    ORIGIN_LONGITUDE = PropertyDescriptor(
        name='Origin Longitude',
        description='GeoDataFrameの基準となる経度オフセットを指定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='0.0',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    #:
    SHIFT_X = PropertyDescriptor(
        name='Shift X',
        description='X軸方向への移動距離を指定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='0.0',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    #:
    SHIFT_Y = PropertyDescriptor(
        name='Shift Y',
        description='Y軸方向への移動距離を指定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='0.0',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    #:
    COORDINATE_SCALE = PropertyDescriptor(
        name='Coordinate Scale (meters per unit)',
        description='GeoDataFrameの座標値に適用するスケールを指定する。実際の物理的な単位(メートル、度など)は座標参照系に依存する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='1.0',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    #:
    ORIGIN_POSITION = PropertyDescriptor(
        name='Origin Position',
        description='座標調整時に基準とする原点の位置を指定する。\
                    (center, top-left, top-right, bottom-left, bottom-right の中から指定)',
        default_value=CENTER,
        allowable_values=[CENTER, TOP_LEFT, TOP_RIGHT, BOTTOM_LEFT, BOTTOM_RIGHT],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=False
    )

    #:
    GEO_DATA_FRAME_CRS = PropertyDescriptor(
        name='CRS of GeoDataFrame',
        description='使用する座標参照系(CRS)を指定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='6677',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    property_descriptors = [
        ORIGIN_LATITUDE, ORIGIN_LONGITUDE, SHIFT_X, SHIFT_Y,
        COORDINATE_SCALE, ORIGIN_POSITION, GEO_DATA_FRAME_CRS
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('UpdateSpatialDefinitionLogic.UpdateSpatialDefinitionLogic')
    def __init__(self, **kwargs):
        pass
