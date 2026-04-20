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

# -*- coding: utf-8 -*-
"""

目的:
    FetchFile から来た FlowFile content(画像ファイルbytes)を入力として、
    指定RGB(許容幅あり)の画素だけ白(255)、それ以外黒(0)の2値画像を生成し、
    2値画像(ndarray)をpickle化したbytesをFlowFile contentとして返す。

後段:
    ImageLinkProcessor が pickle.loads(content) して PNG/HTML を出力する想定。
"""

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from raster_to_vector.common.base_processor import BaseProcessor


class CreateBinaryImage(BaseProcessor):
    """
    CreateBinaryImage
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "0.1.0"
        description = "Create binary mask (pickled ndarray) by extracting pixels in target RGB range."
        tags = ["image", "binary", "mask", "color", "pickle", "ndarray", "pillow", "numpy"]

    # =========================================================
    # プロパティ定義
    # =========================================================

    TARGET_RGB = PropertyDescriptor(
        name="Target RGB",
        description="Target RGB in decimal CSV format: 'R,G,B' (example: '0,0,255').",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="0,0,255",
        required=True,
    )

    TOLERANCE = PropertyDescriptor(
        name="Tolerance",
        description="Tolerance for each channel (+/-). Decimal integer 0-255.",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="20",
        required=True,
    )

    property_descriptors = [
        TARGET_RGB,
        TOLERANCE,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("CreateBinaryImageLogic.CreateBinaryImageLogic")
    def __init__(self, **kwargs):
        pass
