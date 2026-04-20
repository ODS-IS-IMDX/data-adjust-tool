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

class ConvertXyzToLas(BaseProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = 'Convert XYZ/CSV point cloud to LAS/LAZ with optional EPSG reprojection and PDRF selection.'
        tags = ['xyz', 'csv', 'las', 'convert', 'gis']

    # === プロパティ定義 ===
    INPUT_PATH = PropertyDescriptor(
        name='Input Path',
        description='入力する .xyz / .csv ファイルのフルパス。',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    OUTPUT_PATH = PropertyDescriptor(
        name='Output Path',
        description='出力ファイル（.las）または出力フォルダ。フォルダ指定時は入力ファイル名を踏襲して拡張子のみ変換。',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # EPSG
    INPUT_EPSG = PropertyDescriptor(
        name='Input EPSG',
        description='入力点群の EPSG。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    OUTPUT_EPSG = PropertyDescriptor(
        name='Output EPSG',
        description='出力点群の EPSG。未設定の場合はInput EPSGを継承する。',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    # チャンク読み取り
    CHUNK_LINES = PropertyDescriptor(
        name='Chunk Lines',
        description='1回の読み取りで処理する行数（XYZテキストの行単位）。メモリ状況に応じて調整。',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value='1000000',
        required=True
    )

    # 入力列と区切り
    XYZ_COLUMNS = PropertyDescriptor(
        name='XYZ Columns',
        description='入力列の構成を指定。XYZ / XYZI / XYZRGB / XYZIRGB など。',
        required=True
    )
    
    DELIMITER = PropertyDescriptor(
        name='XYZ Delimiter',
        description='XYZ テキストの区切り文字（space または comma）。',
        allowable_values=['space', 'comma'],
        default_value='space',
        required=True
    )

    # 列数チェックの厳密さ
    STRICT_MODE = PropertyDescriptor(
        name='Strict Mode',
        description='true: 入力列数が指定と完全一致しない場合に failure。false: 余剰列は切り捨て、不足はエラー。',
        allowable_values=['true', 'false'],
        default_value='true',
        required=True
    )

    # 値レンジ（正規化）
    RGB_INPUT_RANGE = PropertyDescriptor(
        name='RGB Input Range',
        description='RGB 入力値のレンジ想定（16bitに正規化する際の基準）。0-1 / 0-255 / 0-65535 から選択。',
        allowable_values=['0-1', '0-255', '0-65535'],
        default_value='0-255',
        required=True
    )

    INTENSITY_INPUT_RANGE = PropertyDescriptor(
        name='Intensity Input Range',
        description='Intensity 入力値のレンジ想定（16bitに正規化する際の基準）。0-1 / 0-255 / 0-65535 から選択。',
        allowable_values=['0-1', '0-255', '0-65535'],
        default_value='0-255',
        required=True
    )

    # フラッシュ制御（メモリ上限に基づくサブチャンク分割を有効化）
    FLUSH_BYTES = PropertyDescriptor(
        name='Flush Bytes (Hint)',
        description=('サブチャンク分割の目安とするメモリ上限（MB単位）'
            '0 を指定するとメモリ上限制御を無効化し、行数（Chunk Lines）のみで処理する。'
            '本値は内部でバイト換算され、1回の読み取りチャンクが上限を超える場合にのみ分割を行う。'
            '大きい値ほど分割頻度が下がり高速になりやすいが、使用メモリが増える点に留意すること。'),
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR], default_value='512', required=True)

    # LAS 明示指定
    OUTPUT_LAS_PDRF = PropertyDescriptor(name='Output LAS PDRF',
        description=('LAS Point Data Record Format 0/1/2/3など。'),
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value='0',
        required=True)

    OUTPUT_LAS_VERSION = PropertyDescriptor(name='Output LAS Version',
        description=('出力するLASのバージョン 1.2 / 1.4など。'),
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value='1.2',
        required=True)

    property_descriptors = [
        INPUT_PATH, OUTPUT_PATH,
        INPUT_EPSG, OUTPUT_EPSG,
        CHUNK_LINES, XYZ_COLUMNS, DELIMITER,
        RGB_INPUT_RANGE, INTENSITY_INPUT_RANGE,
        STRICT_MODE, FLUSH_BYTES,
        OUTPUT_LAS_PDRF, OUTPUT_LAS_VERSION,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertXyzToLasLogic.ConvertXyzToLasLogic')
    def __init__(self, **kwargs):
        # BaseProcessor 側で FlowFileTransform 実装・プロパティ解決・属性設定を行う前提
        pass
