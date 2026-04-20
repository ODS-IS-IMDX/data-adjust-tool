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

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from raster_to_vector.common.base_processor import BaseProcessor


class MergeLasData(BaseProcessor):
    """
    タイルフォルダ内の .part.las を1本に統合するプロセッサである。
    ヘッダ整合（PF/Version/Scale/Offset）を検証し、不一致はエラーとする。
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = (
            '生成されたタイル配下の .part.las を1ファイルに統合する。統合にあたり '
            'Point Format（PDRF）/ Version / Scale / Offset の完全一致を検証し、'
            '不一致があればエラー停止とする。'
        )
        tags = ['las', 'merge', 'tile', 'pointcloud', 'concat']

    INPUT_DIR = PropertyDescriptor(
        name='InputDirectory',
        description='統合対象の .part.las が入っているタイルフォルダの親ディレクトリ',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    OUTPUT_DIR = PropertyDescriptor(
        name='OutputDirectory',
        description='統合後のLASファイルを書き出すディレクトリ（入力とは別フォルダを推奨）',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    HDR_CHECK = PropertyDescriptor(
        name='HeaderCheck',
        description='PF/Version/Scale/Offset を比較',
        allowable_values=['true', 'false'],
        default_value='true',
        required=False,
    )

    DEL_PARTS = PropertyDescriptor(
        name='DeletePartsAfterMerge',
        description='統合成功後に .part.las を削除',
        allowable_values=['true', 'false'],
        default_value='false',
        required=False,
    )

    CHUNK_WRITE = PropertyDescriptor(
        name='ChunkWritePoints',
        description='書き出しチャンク点数',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value='2000000',
        required=False,
    )

    property_descriptors = [
        INPUT_DIR,
        OUTPUT_DIR,
        HDR_CHECK,
        DEL_PARTS,
        CHUNK_WRITE,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('MergeLasDataLogic.MergeLasDataLogic')
    def __init__(self, **kwargs):
        pass
