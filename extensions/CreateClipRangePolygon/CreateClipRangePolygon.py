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


class CreateClipRangePolygon(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """FieldSetFileからLineStringの線分データを抽出して各線分に対応する外接矩形ポリゴンを生成し、\
        処理結果としてFieldSetFileを出力するプロセッサ。"""
        tags = [
            "pandas",
            "fieldsetfile",
            "dataframe",
            "boundingbox",
            "python",
        ]

    # 操作対象データ名
    #:
    DATA_NAME = PropertyDescriptor(
        name="Data Name",
        description="ジオメトリデータ(LineString)を持つ行のデータ名を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # 操作対象属性名
    #:
    SUFFIX = PropertyDescriptor(
        name="Suffix",
        description="ジオメトリデータ(LineString)を持つ行の属性名を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # 生成されるポリゴンのジオメトリデータ名
    #:
    POLYGON_NAME = PropertyDescriptor(
        name="Polygon Name",
        description="出力される外接矩形ポリゴンを持つ行のデータ名を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        DATA_NAME,
        SUFFIX,
        POLYGON_NAME,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class(
        "CreateClipRangePolygonLogic.CreateClipRangePolygonLogic"
    )
    def __init__(self, **kwargs):
        pass
