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


class ExtractLineCoordinates(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = '''指定されたジオメトリデータ内のLineString形式の座標を抽出し\
                        各LineStringの座標情報を文字列形式に変換して、対応するFIDとともにリストとして出力する。'''
        tags = ['python', 'linestring']

    # 演算対象メインジオメトリのデータ名
    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name='Geometry Name',
        description='ジオメトリデータ名',
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUFFIX = PropertyDescriptor(
        name='Suffix',
        description='出力結果の座標リストを格納するデータ名',
        validators=[StandardValidators.NON_EMPTY_EL_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # プロセッサに入ってくる形を選択
    #:
    INPUT_DATA_TYPE = PropertyDescriptor(
        name="Input Data Type",
        description="プロセッサに入ってくる形を選択",
        allowable_values=["Field Set File", "Geo Data Frame"],
        default_value="Geo Data Frame",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    property_descriptors = [
        GEOMETRY_NAME,
        SUFFIX,
        INPUT_DATA_TYPE,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ExtractLineCoordinatesLogic.ExtractLineCoordinatesLogic')
    def __init__(self, **kwargs):
        pass
