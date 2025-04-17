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

class SetCalculatedClipRange(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """基準位置を指定し、プロパティで設定した値に基づいて切り取り範囲のバウンディングボックスをフローファイルの属性に設定する。"""
        tags = ["cad", "clip", "python"]

    # 基準とする四隅の設定
    #:
    CORNER_SETTINGS = PropertyDescriptor(
        name='Corner Settings',
        description='外接矩形の四隅のどこを基準に切り取るか指定する。',
        default_value='top-left',
        allowable_values=['top-left', 'top-right', 'bottom-left', 'bottom-right'],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    # X方向比率の設定
    #:
    XDR = PropertyDescriptor(
        name='X Ratio',
        description='バウンディングボックスをX軸方向に調整する比率を0～1の範囲で指定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value=1
    )

    # Y方向比率の設定
    #:
    YDR = PropertyDescriptor(
        name='Y Ratio',
        description='バウンディングボックスをY軸方向に調整する比率を0～1の範囲で指定する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value=1
    )

    property_descriptors = [CORNER_SETTINGS, XDR, YDR]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('SetCalculatedClipRangeLogic.SetCalculatedClipRangeLogic')
    def __init__(self, **kwargs):
        pass
