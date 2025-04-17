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

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
from raster_to_vector.common.base_processor import BaseProcessor

class RotateGeoDataFrame(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """DxfファイルのGeoDataFrameを読み込み、ジオメトリ全体の中心(外接矩形の重心または原点)から回転させる。"""
        tags = ["cad", "roll", "python"]

    # 回転方向を設定する。
    #:
    ROLL_DIRECTION = PropertyDescriptor(
        name='Direction of Rotation',
        description='回転方向を右か左で指定する。',
        default_value='right',
        allowable_values=['right', 'left'],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )
    # 回転させる角度を設定する。
    #:
    ROLL_ANGLE = PropertyDescriptor(
        name='Angle to Rotate',
        description='回転させる角度を設定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )
    # 回転の中心をどちらにするか設定する。
    #:
    CENTER_MODE = PropertyDescriptor(
        name='Center of Rotation',
        description='回転の中心を外接矩形の重心か原点のどちらにするか指定する。',
        default_value='centroid',
        allowable_values=['centroid', 'origin'],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    property_descriptors = [ROLL_DIRECTION, ROLL_ANGLE, CENTER_MODE]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('RotateGeoDataFrameLogic.RotateGeoDataFrameLogic')
    def __init__(self, **kwargs):
        pass
