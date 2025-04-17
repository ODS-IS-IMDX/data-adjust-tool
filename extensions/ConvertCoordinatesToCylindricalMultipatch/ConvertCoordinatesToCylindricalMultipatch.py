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


class ConvertCoordinatesToCylindricalMultipatch(BaseProcessor):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """座標配列を円筒形のマルチパッチに変換する。\
                        ①input: 座標配列と、円筒の半径を持った2行のFieldSetFile。\
                        ②output: マルチパッチの座標配列を持った1行のFieldSetFile"""
        tags = ['Coordinates', 'Multipatch', 'Python']

    CREATE = "作成する"
    NOT_CREATE = "作成しない"

    #:
    START_MULTIPATCH_FLAG = PropertyDescriptor(
        name="Start Multipatch Flag",
        description="円筒の入口にマルチパッチを作成するフラグ",
        required=True,
        allowable_values=[CREATE, NOT_CREATE],
        default_value=CREATE,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    #:
    END_MULTIPATCH_FLAG = PropertyDescriptor(
        name="End Multipatch Flag",
        description="円筒の出口にマルチパッチを作成するフラグ",
        required=True,
        allowable_values=[CREATE, NOT_CREATE],
        default_value=CREATE,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    #:
    CIRCLE_RADIUS = PropertyDescriptor(
        name="Circle Radius DWH Name",
        description="入力データの円筒の半径のDwh名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )

    #:
    CIRCLE_DIVISIONS = PropertyDescriptor(
        name="Circle Divisions",
        description="円柱の円周の分割数(3未満の値を設定した場合は12として処理を行う)",
        required=True,
        sensitive=False,
        default_value="12",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
    )

    #:
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDwh名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False
    )

    property_descriptors = [START_MULTIPATCH_FLAG,
                            END_MULTIPATCH_FLAG,
                            CIRCLE_RADIUS,
                            CIRCLE_DIVISIONS,
                            OUTPUT_DWH_NAME,
                            ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertCoordinatesToCylindricalMultipatchLogic.ConvertCoordinatesToCylindricalMultipatchLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

