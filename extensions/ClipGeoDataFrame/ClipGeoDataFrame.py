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


class ClipGeoDataFrame(BaseProcessor):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """入力されたGeoDataFrameを指定した左下および右上の座標でクリッピングし、GeoDataFrameで出力する。\
                         クリッピングは矩形範囲によって行われ、出力時にはGeoDataFrameをシリアライズする。"""
        tags = ["clip", "geodataframe", "bounding box"]

    #:
    LOWER_LEFT_X = PropertyDescriptor(
        name='Lower Left X Coordinate',
        description='クリップ範囲の左下のX座標。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    LOWER_LEFT_Y = PropertyDescriptor(
        name='Lower Left Y Coordinate',
        description='クリップ範囲の左下のY座標',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    UPPER_RIGHT_X = PropertyDescriptor(
        name='Upper Right X Coordinate',
        description='クリップ範囲の右上のX座標',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    UPPER_RIGHT_Y = PropertyDescriptor(
        name='Upper Right Y Coordinate',
        description='クリップ範囲の右上のY座標。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [LOWER_LEFT_X, LOWER_LEFT_Y, UPPER_RIGHT_X, UPPER_RIGHT_Y]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ClipGeoDataFrameLogic.ClipGeoDataFrameLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

