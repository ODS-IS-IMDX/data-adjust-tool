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


class ExtractGeometryFromGeoDataFrame(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """GeoDataFrameを受け取り、指定したレイヤとジオメトリタイプに応じて\
        対象のGeoDataFrameを抽出し、処理結果として出力するプロセッサ。"""
        tags = ["geodataframe", "geometry", "flowfile", "geopandas", "python"]

    # レイヤの指定
    #:
    LAYER_NAME = PropertyDescriptor(
        name='Layer Name',
        description='取得したいGeoDataFrameのレイヤ名を指定する。カンマ（,）区切りで複数指定が可能であり、空の場合は全レイヤが指定される。',
        default_value='',
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    property_descriptors = [LAYER_NAME]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ExtractGeometryFromGeoDataFrameLogic.ExtractGeometryFromGeoDataFrameLogic')
    def __init__(self, **kwargs):
        pass
