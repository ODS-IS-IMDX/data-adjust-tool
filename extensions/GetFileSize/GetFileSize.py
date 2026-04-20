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

# GetFileSize.py
from nifiapi.properties import PropertyDescriptor, StandardValidators
from raster_to_vector.common.base_processor import BaseProcessor


class GetFileSize(BaseProcessor):
    """
    FlowFileの属性(absolute.path + filename)をもとに、ファイルサイズと(必要なら)画像寸法を取得し、
    固定名のFlowFile属性に格納するProcessor。

    ■仕様(混乱しない版)
    - ファイルサイズは "file_size" の1項目だけを出力する
    - 単位は必ず "file_size_unit" に出力する
    - 画像寸法は GET_IMAGE_DIMENSIONS でON/OFFできる
    - 非画像/壊れ画像でも落とさず、画像寸法は 0 を出す
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = (
            'FlowFile属性(absolute.path + filename)からファイルサイズを取得し、'
            '選択した単位で換算した値を "file_size"、単位文字列を "file_size_unit" に格納する。'
            '必要に応じて画像の幅・高さ(px)も取得する。'
        )
        tags = ['file', 'size', 'image', 'dimension', 'attribute']

    # ============================================================
    # プロパティ定義
    # ============================================================

    FILE_SIZE_UNIT = PropertyDescriptor(
        name='File Size Unit',
        description=(
            'ファイルサイズの出力単位を選択する。'
            '"file_size" に換算後の値、"file_size_unit" に単位文字列が格納される。'
        ),
        allowable_values=['byte', 'KB', 'MB'],
        default_value='byte',
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )

    GET_IMAGE_DIMENSIONS = PropertyDescriptor(
        name='Get Image Dimensions',
        description=(
            '"true" の場合、画像の幅・高さ(px)を取得し、'
            'FlowFile属性 "image_width_px" と "image_height_px" に格納する。'
            '非画像/壊れ画像の場合は落とさず、0を格納する。'
        ),
        allowable_values=['true', 'false'],
        default_value='true',
        required=True,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
    )

    property_descriptors = [
        FILE_SIZE_UNIT,
        GET_IMAGE_DIMENSIONS,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('GetFileSizeLogic.GetFileSizeLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
