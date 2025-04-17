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

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

from raster_to_vector.common.base_processor import BaseProcessor


class CheckGeometryShapes(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """ポリゴン、ライン、ポイントのジオメトリデータが適切かチェックを行う"""
        tags = ["Polygon", "LineString", "Point", "shape", "shapefile", "python"]

    # LineStringの頂点間距離を求める際の許容距離
    #:
    PERMISSIONDISTANCE = PropertyDescriptor(
        name="Permission Distance",
        description="LineStringの頂点間距離を求める際の許容距離",
        default_value=0,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # 凸型ポリゴンを検出する際にポリゴンに対しsimplifyを行う数値
    #:
    TOLERANCE = PropertyDescriptor(
        name="Tolerance",
        description="凸型ポリゴンを検出する際にポリゴンに対し座標の簡略化を行う数値",
        default_value=0,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [PERMISSIONDISTANCE, TOLERANCE]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('CheckGeometryShapesLogic.CheckGeometryShapesLogic')
    def __init__(self, **kwargs):
        pass
