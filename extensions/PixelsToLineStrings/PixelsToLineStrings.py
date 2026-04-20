# PixelsToLineStrings.py
# MIT License
#
# Copyright (c) 2026 NTT InfraNet
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


class PixelsToLineStrings(BaseProcessor):
    """
    NiFiプロセッサ。ピクセル座標の線分リスト(2点)を受け取り、
    ピクセル座標を変更せずに LineString の GeoDataFrame として出力する。

    重要:
      - CRSは付与しない(=地図座標への変換はしない)
      - 位置合わせ(upper_left/pixel_to_coordinate/crs等)が必要なら別プロセッサで実施する
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """ピクセル座標をそのままLineString(GeoDataFrame)にする。CRS/座標変換は行わない。"""
        tags = ["pixels", "linestring", "geodataframe", "geojson"]

    #:
    USE_PIXEL_CENTER = PropertyDescriptor(
        name='Use Pixel Center',
        description='Trueの場合、(x+0.5,y+0.5)のピクセル中心座標を使う(ピクセル座標系のまま)',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value='false',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    EXTEND_ENDPOINTS = PropertyDescriptor(
        name='Extend Endpoints',
        description='Trueの場合、共有端点以外の端点のみ0.5ピクセル外へ延長する(ピクセル座標系のまま)',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value='false',
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

    property_descriptors = [
        USE_PIXEL_CENTER,
        EXTEND_ENDPOINTS,
        LAYER_NAME,
        TARGET_DWH_ATTRIBUTE
    ]

    def getPropertyDescriptors(self):
        """
        プロパティを返す関数
        """
        return self.property_descriptors

    @BaseProcessor.set_logic_class('PixelsToLineStringsLogic.PixelsToLineStringsLogic')
    def __init__(self, **kwargs):
        pass
