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
    StandardValidators,
    ExpressionLanguageScope,
)


DELIMITER_COMMA = "Comma"
DELIMITER_TAB = "Tab"


class ConvertGeoDataFrameToFieldSetFileMulti(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """GeoDataFrameをFieldSetFileに変換するプロセッサ。\
                         GeoDataFrameをレイヤ情報、ジオメトリタイプごとに分割し、\
                         処理結果として複数のジオメトリデータを持つFieldSetFileを出力する。"""
        tags = [
            "numpy",
            "shapely",
            "geometry",
            "pickle",
            "fieldsetfile",
            "geodataframe",
        ]

    #:
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="FieldSetFile生成用データ定義の区切り文字",
        default_value=DELIMITER_COMMA,
        allowable_values=[DELIMITER_COMMA, DELIMITER_TAB],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertGeoDataFrameToFieldSetFileMultiLogic'
                                   '.ConvertGeoDataFrameToFieldSetFileMultiLogic')
    def __init__(self, **kwargs):
        pass
