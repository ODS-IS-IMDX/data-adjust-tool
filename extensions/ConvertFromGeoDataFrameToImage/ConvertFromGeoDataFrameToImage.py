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
    StandardValidators,
    ExpressionLanguageScope,
)
from raster_to_vector.common.base_processor import BaseProcessor


class ConvertFromGeoDataFrameToImage(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """GeoDataFrameを画像に変換し、FieldSetFileとして出力する。"""
        tags = ["cad", "GeoDataFrame", "python", "image", "clip"]

    # レイヤー範囲設定
    #:
    LAYER_RANGE = PropertyDescriptor(
        name="Layer Name",
        description="対象とするレイヤー名を指定。カンマ区切りで複数指定可能。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    # 横px
    #:
    IMAGE_WIDTH = PropertyDescriptor(
        name="Image Width",
        description="画像の横幅指定。Meters per Pixelsを指定した場合は適用されない。",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    # TEXT描画フラグ
    #:
    DRAW_TEXT_FLAG = PropertyDescriptor(
        name="Draw Text",
        description="テキストを画像に描画するかどうかのフラグ",
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value="False",
        allowable_values=["True", "False"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # フォントパス
    #:
    FONT_PATH = PropertyDescriptor(
        name="Font Path",
        description="テキストを描画する際のフォントファイルパス",
        validators=[StandardValidators.FILE_EXISTS_VALIDATOR],
        default_value="C:/Windows/Fonts/msgothic.ttc",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # Dhw名設定
    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name="Geometry Name",
        description="FieldSetFile、Dwh列の/以前の文字列。(Geometry Name/属性名)",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # 画像範囲設定
    #:
    WHOLE_IMAGE_FLAG = PropertyDescriptor(
        name="Whole Image Flag",
        description="画像を切り取る際に、指定したレイヤーのみの外接をとるか、全体の外接をとるかを決定するフラグ。",
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value="True",
        allowable_values=["True", "False"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SCALE_FACTOR = PropertyDescriptor(
        name="Scale Factor",
        description="GIS座標からピクセル座標に変換する解像度。1メートルあたりのピクセル数を指定する。",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    OUT_OF_BOUNDS = PropertyDescriptor(
        name='Geometry Out of Bounds',
        description='ジオメトリが少しでも切り取り範囲外にある場合、そのジオメトリを削除するかどうかのフラグ。',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value="False",
        allowable_values=["True", "False"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [
        LAYER_RANGE,
        IMAGE_WIDTH,
        DRAW_TEXT_FLAG,
        FONT_PATH,
        GEOMETRY_NAME,
        WHOLE_IMAGE_FLAG,
        SCALE_FACTOR,
        OUT_OF_BOUNDS
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class(
        "ConvertFromGeoDataFrameToImageLogic.ConvertFromGeoDataFrameToImageLogic"
    )
    def __init__(self, **kwargs):
        pass
