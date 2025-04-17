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


class AlignMapGrid(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """ジオメトリの原点座標の移動や、スケールの調整を行うプロセッサ。"""
        tags = [
            "pandas",
            "geopandas",
            "pickle",
            "fieldsetfile",
            "geodataframe",
            "dataframe",
        ]

    # Source X-Axis Orientation
    LEFT_TO_RIGHT = "Left to Right"
    RIGHT_TO_LEFT = "Right to Left"

    # Source Y-Axis Orientation
    BOTTOM_TO_TOP = "Bottom to Top"
    TOP_TO_BOTTOM = "Top to Bottom"

    # Reference Corner
    TOP_LEFT = "top-left"
    TOP_RIGHT = "top-right"
    BOTTOM_LEFT = "bottom-left"
    BOTTOM_RIGHT = "bottom-right"

    #:
    DATA_NAME = PropertyDescriptor(
        name="Data Name",
        description="処理対象となるFieldSetFileのデータ名",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SCALE = PropertyDescriptor(
        name="Scale",
        description="CADの座標を何メートル単位で表すかを指定する。(デフォルトは1メートル)",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value=1,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    ORIGIN_X = PropertyDescriptor(
        name="Origin X",
        description="指定した値に基づきCADデータの原点X座標の値を調整する。",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value=0,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    ORIGIN_Y = PropertyDescriptor(
        name="Origin Y",
        description="指定した値に基づきCADデータの原点Y座標の値を調整する。",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value=0,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    SOURCE_X_AXIS_ORIENTATION = PropertyDescriptor(
        name="Source X-Axis Orientation",
        description="入力データのX軸の向きを指定する。\
                    ① Left to Right : 右方向が正\
                    ② Right to Left : 左方向が正",
        default_value=LEFT_TO_RIGHT,
        allowable_values=[LEFT_TO_RIGHT, RIGHT_TO_LEFT],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    #:
    SOURCE_Y_AXIS_ORIENTATION = PropertyDescriptor(
        name="Source Y-Axis Orientation",
        description="入力データのY軸の向きを指定する。\
                    ① Bottom to Top : 上方向が正\
                    ② Top to Bottom : 下方向が正",
        default_value=BOTTOM_TO_TOP,
        allowable_values=[BOTTOM_TO_TOP, TOP_TO_BOTTOM],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    #:
    REFERENCE_CORNER = PropertyDescriptor(
        name="Reference Corner",
        description="座標調整時に基準とする外接矩形の原点の位置を指定する。\
                    (top-left, top-right, bottom-left, bottom-right の中から指定)",
        default_value=TOP_LEFT,
        allowable_values=[TOP_LEFT, TOP_RIGHT, BOTTOM_LEFT, BOTTOM_RIGHT],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    #:
    CRS = PropertyDescriptor(
        name="CRS",
        description="使用する座標参照系(CRS)を指定する。",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value=6677,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    property_descriptors = [
        DATA_NAME,
        SCALE,
        ORIGIN_X,
        ORIGIN_Y,
        SOURCE_X_AXIS_ORIENTATION,
        SOURCE_Y_AXIS_ORIENTATION,
        REFERENCE_CORNER,
        CRS,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('AlignMapGridLogic.AlignMapGridLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)