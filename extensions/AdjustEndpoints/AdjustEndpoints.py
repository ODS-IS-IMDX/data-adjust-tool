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


class AdjustEndpoints(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """以下の3つの機能から選択された機能を実行するプロセッサ。\
        機能1:LineStringの端点が他のLineStringに一定の距離以内にある場合、LineStringと交差させるような座標を追加する。\
        機能2:LineString同士が交差している場合に、交点に端点を移動させる。\
        機能3:複数のLineStringの端点が一定の距離以内にある場合、端点間を結ぶLinestringを新たに発生させる。"""
        tags = [
            "geometry",
            "spatial",
            "adjust",
            "line",
        ]

    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name="Geometry Name",
        description="対象のジオメトリデータ名(レイヤ名)",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    EXTEND_ENDPOINT_TO_INTERSECT = PropertyDescriptor(
        name="Extend Endpoint to Intersect",
        description="端点を伸ばすかどうかを決定するフラグ",
        allowable_values=["true", "false"],
        default_value="false",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    INTERSECT_DISTANCE = PropertyDescriptor(
        name="Intersect Distance",
        description="対象とするLinestringを伸ばす距離の閾値",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    MOVE_ENDPOINT_TO_INTERSECT = PropertyDescriptor(
        name="Move Endpoint to Intersect",
        description="端点を最も近い交点に移動させるかどうかを決定するフラグ",
        allowable_values=["true", "false"],
        default_value="false",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    CLOSE_THRESHOLD = PropertyDescriptor(
        name="Close Threshold",
        description="対象とするLineStringが交差からはみ出している比率(0.5未満の値を設定してください)",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    CONNECT_NEARDY_ENDPOINTS = PropertyDescriptor(
        name="Connect Nearby Endpoints",
        description="端点同士が近い場合、繋げるかどうかを決定するフラグ",
        allowable_values=["true", "false"],
        default_value="false",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    DISTANCE_THRESHOLD = PropertyDescriptor(
        name="Distance Threshold",
        description="端点が他の端点に近いかどうかを判断するための閾値",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    CRS = PropertyDescriptor(
        name="crs",
        description="使用する座標参照系(CRS)を指定する。",
        default_value="6677",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    property_descriptors = [
        GEOMETRY_NAME,
        EXTEND_ENDPOINT_TO_INTERSECT,
        INTERSECT_DISTANCE,
        MOVE_ENDPOINT_TO_INTERSECT,
        CLOSE_THRESHOLD,
        CONNECT_NEARDY_ENDPOINTS,
        DISTANCE_THRESHOLD,
        CRS,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("AdjustEndpointsLogic.AdjustEndpointsLogic")
    def __init__(self, **kwargs):
        pass
