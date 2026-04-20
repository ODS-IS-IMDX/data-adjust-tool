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


class ConvertLasToXyz(BaseProcessor):
    """
    LAS/LAZ 点群を XYZ/CSV 形式に変換する NiFi プロセッサ。
    - チャンク逐次処理（大容量対応）
    - RGB/Intensity/GPS Time の選択出力（RGBは既定で16bit→8bit縮約）
    - XYZ / XYZI / XYZRGB / XYZRGBI / XYZRGBIT などの出力列構成を選択可能
    - メモリ制御：CHUNK_POINTS + FLUSH_BYTES(目安MB)
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = ('Convert LAS/LAZ point cloud to XYZ/CSV text with optional EPSG reprojection and memory-aware '
                       'chunking.')
        tags = ['pointcloud', 'las', 'laz', 'xyz', 'csv', 'reproject', 'gis']

    # === プロパティ定義　===

    INPUT_PATH = PropertyDescriptor(
        name='InputPath',
        description='入力 .las / .laz ファイルのフルパス（単一ファイル）',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    OUTPUT_PATH = PropertyDescriptor(
        name='OutputPath',
        description='フルパス（ファイルパス）。拡張子は無視して OutputPathで指定されたものが優先される。',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    OUTPUT_FORMAT = PropertyDescriptor(
        name='OutputFormat',
        description='出力形式（XYZ/CSV）。既定値はXYZ(拡張子は.xyz)',
        allowable_values=['XYZ', 'CSV'],
        default_value='XYZ',
        required=True
    )

    # 実際に書き出す列順（XYZIRGBTの任意並び）
    OUTPUT_COLUMNS = PropertyDescriptor(
        name='OutputColumns',
        description='実際にXYZ/CSVへ書き出す列順。XYZIRGBTの任意並び（例: XYZIRGBT, XYZRGB, XYZT など）。',
        required=True
    )

    DELIMITER = PropertyDescriptor(
        name='Delimiter',
        description='出力テキストの区切り文字（space / comma）',
        allowable_values=['space', 'comma'],
        default_value='space',
        required=True
    )

    DECIMALS = PropertyDescriptor(
        name='Decimals',
        description='XYZおよびTの小数点桁数（0～13）',
        allowable_values=[str(i) for i in range(14)],
        default_value='3',
        required=True
    )

    CHUNK_POINTS = PropertyDescriptor(
        name='ChunkPoints',
        description='一次チャンクとして読み込む点群数（メモリに余裕があれば増やすと高速化しやすい）',
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value='1000000',
        required=True
    )

    INCLUDE_HEADER = PropertyDescriptor(
        name='IncludeHeader',
        description='true: 1行目に列名ヘッダを出力。false: データ行のみ',
        allowable_values=['true', 'false'],
        default_value='false',
        required=True
    )

    FLUSH_BYTES = PropertyDescriptor(
        name='FlushBytes',
        description='メモリ上限 (MB)。0で無効。上限を超えそうな場合は行数でサブスプリットして逐次書出しする',
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        default_value='0',
        required=True
    )

    # 追加挙動
    STRICT_MODE = PropertyDescriptor(
        name='StrictMode',
        description='trueでXYZの有限値チェックを有効化（NaN/Inf検知で失敗）。既定=false',
        allowable_values=['true', 'false'],
        default_value='false',
        required=True
    )

    RGB_DOWNCAST = PropertyDescriptor(
        name='RGBDowncast',
        description='trueでRGB(16bit)を8bitへ縮約（右シフト8）。falseで16bit整数のまま出力',
        allowable_values=['true', 'false'],
        default_value='true',
        required=True
    )

    # === PropertyDescriptor 配列 ===
    property_descriptors = [
        INPUT_PATH, OUTPUT_PATH, OUTPUT_FORMAT,
        OUTPUT_COLUMNS, DELIMITER, DECIMALS,
        CHUNK_POINTS, INCLUDE_HEADER,
        FLUSH_BYTES, STRICT_MODE, RGB_DOWNCAST,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    # === Logicクラス定義 ===
    @BaseProcessor.set_logic_class('ConvertLasToXyzLogic.ConvertLasToXyzLogic')
    def __init__(self, **kwargs):
        pass
