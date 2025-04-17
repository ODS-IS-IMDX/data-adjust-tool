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

class TransformPixelsToMultiPoint(BaseProcessor):
    """
    NiFiプロセッサ。ピクセル座標のリストを受け取り、重複チェック後にジオメトリ座標に変換し、GeoDataFrame形式の
    MultiPointオブジェクトとして出力する。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """ピクセル座標をジオメトリ座標に変換する。"""
        tags = ["pixels", "cad", "multipoint", "gis"]

    # プロパティ設定
    #:
    PIXEL_SIZE_X = PropertyDescriptor(
        name='X Pixel Size',
        description='X方向の１ピクセルのサイズ',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    PIXEL_SIZE_Y = PropertyDescriptor(
        name='Y Pixel Size',
        description='Y方向の1ピクセルのサイズ',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    UPPER_LEFT_X = PropertyDescriptor(
        name='Upper Left X Coordinate',
        description='ラスターの左上隅のX座標',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    UPPER_LEFT_Y = PropertyDescriptor(
        name='Upper Left Y Coordinate',
        description='ラスターの左上隅のY座標',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    PIXEL_TO_COORDINATE_X = PropertyDescriptor(
        name='Pixel to Coordinate X',
        description='ピクセルからジオメトリ X座標への変換係数',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    PIXEL_TO_COORDINATE_Y = PropertyDescriptor(
        name='Pixel to Coordinate Y',
        description='ピクセルからジオメトリ Y座標への変換係数',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    CRS = PropertyDescriptor(
        name='Crs',
        description='使用する座標参照系(CRS)',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value='6677',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    LAYER_NAME = PropertyDescriptor(
        name='Layer Name',
        description='設定するレイヤー名',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    TARGET_DWH_ATTRIBUTE = PropertyDescriptor(
        name='Target Dwh Attribute',
        description='処理対象とするDwhの"/"以降の属性名',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [PIXEL_SIZE_X, PIXEL_SIZE_Y, UPPER_LEFT_X, UPPER_LEFT_Y, PIXEL_TO_COORDINATE_X,
                            PIXEL_TO_COORDINATE_Y, CRS, LAYER_NAME, TARGET_DWH_ATTRIBUTE]

    def getPropertyDescriptors(self):
        """
        プロパティを返す関数
        """
        return self.property_descriptors

    @BaseProcessor.set_logic_class('TransformPixelsToMultiPointLogic.TransformPixelsToMultiPointLogic')
    def __init__(self, **kwargs):
        pass
