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


class SplitLasDataByArea(BaseProcessor):
    """
    LAS をグリッドに割り当て、tile_key 付きの .part.las を出力するプロセッサ。
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = (
            'LAS ポイントをグリッドに割り当て、tile_key ごとに .part.las を書き出す。'
        )
        tags = ['las', 'grid', 'tile', 'area', 'split', 'pointcloud']

    # === プロパティ定義 ===

    # 入力 LAS/LAZ が格納されているディレクトリ
    # 例: /data/input_las や ${las.input.dir}
    INPUT_DIR = PropertyDescriptor(
        name='InputDirectory',
        description='入力 LAS/LAZ ファイルが格納されているディレクトリ（例: /data/input_las）。',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # tile_key ごとの .part.las を出力するルートディレクトリ
    # 例: /data/area_tiles や ${las.tiles.dir}
    OUTPUT_DIR = PropertyDescriptor(
        name='OutputDirectory',
        description='tile_key ごとの .part.las を出力するルートディレクトリ（例: /data/area_tiles）。',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # グリッド原点（X 座標[m]）
    ORIGIN_X = PropertyDescriptor(
        name='OriginX(m)',
        description='グリッド原点の X 座標[m]。未指定時は 0.0（座標系の原点基準）。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='0.0',
        required=False,
    )

    # グリッド原点（Y 座標[m]）
    ORIGIN_Y = PropertyDescriptor(
        name='OriginY(m)',
        description='グリッド原点の Y 座標[m]。未指定時は 0.0（座標系の原点基準）。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='0.0',
        required=False,
    )

    # セルサイズ X（mm）
    CELL_X_MM = PropertyDescriptor(
        name='CellSizeX(mm)',
        description='X 方向のグリッドセル幅[mm]（例: 5000 = 5m）。',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        required=True,
    )

    # セルサイズ Y（mm）
    CELL_Y_MM = PropertyDescriptor(
        name='CellSizeY(mm)',
        description='Y 方向のグリッドセル幅[mm]（例: 5000 = 5m）。',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        required=True,
    )

    # LAS 読み込みチャンクサイズ（点数）
    READ_CHUNK = PropertyDescriptor(
        name='ReadChunkPoints',
        description='1回の読み出しで処理する最大点数（チャンクサイズ）。大きいほど高速だがメモリ使用量が増える。',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value='2000000',
        required=False,
    )

    # 開始時に出力先ディレクトリを掃除するかどうか
    CLEAN_OUT = PropertyDescriptor(
        name='CleanOutputOnStart',
        description='処理開始前に OutputDirectory 配下を全削除してから実行するかどうか（true/false）。',
        allowable_values=['true', 'false'],
        default_value='true',
        required=False,
    )

    # 並びは UI 表示順のため任意。必須→任意の順に配置。
    property_descriptors = [
        INPUT_DIR,
        OUTPUT_DIR,
        ORIGIN_X,
        ORIGIN_Y,
        CELL_X_MM,
        CELL_Y_MM,
        READ_CHUNK,
        CLEAN_OUT,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('SplitLasDataByAreaLogic.SplitLasDataByAreaLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
