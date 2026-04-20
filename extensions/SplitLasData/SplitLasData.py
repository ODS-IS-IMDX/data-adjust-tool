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
from nifiapi.relationship import Relationship
from raster_to_vector.common.base_processor import BaseProcessor


class SplitLasData(BaseProcessor):
    """LAS のチャンク分割プロセッサ。"""

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = 'Split LAS into chunks by points or estimated size.'
        tags = ['las', 'split', 'chunk', 'pointcloud']

    # === プロパティ定義 ===
    INPUT_PATH = PropertyDescriptor(
        name='InputPath',
        description='入力LASのファイルパス（.las）',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    SPLIT_BY = PropertyDescriptor(
        name='SplitBy',
        description='分割基準（PointsPerChunk または MaxSizeMB）',
        allowable_values=['PointsPerChunk', 'MaxSizeMB'],
        default_value='PointsPerChunk',
        required=True
    )

    CHUNK_SZ = PropertyDescriptor(
        name='ChunkSize',
        description='1チャンクの点数（SplitBy=PointsPerChunk時）',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value='5000000',
        required=True
    )

    MAX_MB = PropertyDescriptor(
        name='MaxSizeMB',
        description='1チャンクの概算最大サイズ（MB）',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value='100',
        required=True
    )

    OUTPUT_DIR = PropertyDescriptor(
        name='OutputDirectory',
        description='出力先ディレクトリ',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [INPUT_PATH, SPLIT_BY, CHUNK_SZ, MAX_MB, OUTPUT_DIR]

    # === リレーションシップ定義 ===
    REL_SUCCESS = Relationship(name='success', description='正常にLASを分割したFlowFile')
    REL_FAILURE = Relationship(name='failure', description='分割処理に失敗したFlowFile')

    relationships = [REL_SUCCESS, REL_FAILURE]

    # === 必須メソッド ===
    def getPropertyDescriptors(self):
        return self.property_descriptors

    def getRelationships(self):
        return self.relationships

    # === 実処理ロジックとの接続 ===
    @BaseProcessor.set_logic_class('SplitLasDataLogic.SplitLasDataLogic')
    def __init__(self, **kwargs):
        pass
