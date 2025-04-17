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

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope

from raster_to_vector.common.base_processor import BaseProcessor

class ConvertFromDxfToGeoDataFrame(BaseProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''DxfファイルをGeoDataFrameに変換する。'''
        tags = ['convert', 'geopandas', 'geodataframe', 'dxf', 'python']

    GEO_DATA_FRAME_CRS = PropertyDescriptor(
        name = 'Crs of GeoDataFrame',
        description = '使用する座標参照系(CRS)を指定',
        validators = [StandardValidators.NUMBER_VALIDATOR],
        default_value = 6677,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required = True
    )

    # CADの原点(0,0)に対応する平面直角座標系でのX座標を指定するプロパティ
    #:
    ORIGIN_X = PropertyDescriptor(
        name = 'Origin X',
        description = '平面座標系におけるCAD原点(0,0)点のX座標を指定する。',
        validators = [StandardValidators.NUMBER_VALIDATOR],
        default_value = 0.0,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required = True
    )

    # CADの原点(0,0)に対応する平面直角座標系でのY座標を指定するプロパティ
    #:
    ORIGIN_Y = PropertyDescriptor(
        name = 'Origin Y',
        description = '平面座標系におけるCAD原点(0,0)点のY座標を指定する。',
        validators = [StandardValidators.NUMBER_VALIDATOR],
        default_value = 0.0,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required = True
    )

    # CADの1座標単位が現実世界で何メートルに相当するかを指定するプロパティ
    #:
    COORDINATE_SCALE = PropertyDescriptor(
        name = 'Coordinate Scale (meters per unit)',
        description = 'CADの1座標単位が現実世界で何メートルに相当するかを指定する。',
        validators = [StandardValidators.NUMBER_VALIDATOR],
        default_value = 1.0,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required = True
    )

    # 座標単位
    #:
    COORDINATE_UNIT = PropertyDescriptor(
        name = 'Coordinate Unit',
        description = '指定したCADの座標単位を追加する。',
        allowable_values=['Meter', 'Millimeter', 'Degree', 'Second', 'Pixel', 'Dot', 'Inch'],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required = True
    )

    property_descriptors = [GEO_DATA_FRAME_CRS, ORIGIN_X, ORIGIN_Y, COORDINATE_SCALE, COORDINATE_UNIT]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertFromDxfToGeoDataFrameLogic.ConvertFromDxfToGeoDataFrameLogic')
    def __init__(self, **kwargs):
        pass
