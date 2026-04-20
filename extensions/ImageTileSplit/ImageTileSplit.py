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


class ImageTileSplit(BaseProcessor):
    """
    ImageTileSplit

    ■ 目的
    - 入力画像(ファイルパス)をタイル分割し、ディスクへPNGとして出力する。
    - Z(zoom level)は画像ピクセル数から自動決定し、単一Zのみ出力する。
    - stride指定でスライディングウィンドウ(重なりあり)にも対応する。
    - 回転(0/90/180/270)や左右反転(LR)の追加出力にも対応する。
    - FlowFile本文(byte_data)は使わない(ファイルパスで画像を読む)。
    - manifest.jsonは出さない(出力フォルダ名+ファイル名で判別できる)。
    - 出力画像は常にpng固定である(拡張子/品質設定は持たない)。

    ■ 出力形式(重要: フラット出力)
    - XYZ Tilesのキー(z,x,y)は維持するが、フォルダ階層(z/x/y)は作らない。
      つまり out_root/{z}/{x}/{y}.png ではなく、out_root直下にまとめて保存する。
    - ファイル名は out_root_name を接頭辞として含め、衝突しない形式にする。
      例:
        {out_root}/{out_root_name}_{z}_{x}_{y}.png

    ■ スライディング(stride < tile_size)時のファイル名
    - オフセット情報(ox/oy)を必ず付与する(0でも付与)。
      例:
        {out_root}/{out_root_name}_{z}_{x}_{y}_ox{ox}_oy{oy}.png

    ■ 回転/左右反転のファイル名
    - 回転は0度を除きサフィックスを付ける。
        _r90 / _r180 / _r270
    - 左右反転は、回転後の画像に対して適用し、追加で _lr を付けて出力する。
      例:
        {out_root}/{out_root_name}_{z}_{x}_{y}_ox{ox}_oy{oy}_r90_lr.png

    ■ 追加: XYZ Tiles Mode(出力範囲のモード)
    - xyz:
        tile_size * 2^Z の正方形キャンバスへ右端/下端をpadしてからタイルを切る(従来通り)
        ※1辺のタイル枚数が2^Zになりやすく、XYZ Tilesの考え方に寄せた出力になる
    - tight:
        元画像の範囲を必要最小のpadでカバーするだけの領域でタイルを切る
        ※出力枚数が減りやすく、高速化・容量削減の効果が大きい

    ■ 追加: PNG圧縮レベル
    - PNG圧縮レベル(0-9)をプロパティで指定できる。
      0が最速(容量大きめ)、9が最小(遅め)。

    ■ 追加: OUTPUT_RESIZED_256 の仕様(置き換え出力)
    - trueの場合、tile_sizeで切り出したタイルを256x256へリサイズして「通常の出力先にそのまま出力」する。
      ※別フォルダ(resized256等)には出さない。
      ※元のタイル(リサイズ前)の出力は行わない(置き換え)。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "画像をタイル分割し、PNGでフラット出力する。"
            "スライディング(重なり)、回転/左右反転、PNG圧縮、xyz/tightの出力範囲モードに対応。"
        )
        tags = ["image", "tile", "xyz", "split", "sliding-window", "opencv", "png"]

    # =========================================================
    # プロパティ定義
    # =========================================================

    INPUT_PATH = PropertyDescriptor(
        name="Input Path",
        description="入力画像ファイルの絶対パス(FlowFile属性参照可)",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    OUTPUT_BASE_DIR = PropertyDescriptor(
        name="Output Base Directory",
        description="出力ベースフォルダ(配下に設定フォルダを作り、その中にタイルPNGをフラット出力する)",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    TILE_SIZE = PropertyDescriptor(
        name="Tile Size (px)",
        description="タイル切り出し時の1辺(px)。正方形で切り出す。初期値は256。",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="256",
        required=False,
    )

    STRIDE = PropertyDescriptor(
        name="Stride (px)",
        description="ずらし幅(px)。初期値は256。TILE_SIZEと同じ値にすることで重なりなしになる。",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="256",
        required=False,
    )

    PADDING_MODE = PropertyDescriptor(
        name="Padding Mode",
        description="右端/下端のpadding方式",
        allowable_values=["zero", "edge", "reflect"],
        default_value="zero",
        required=True,
    )

    XYZ_TILES_MODE = PropertyDescriptor(
        name="XYZ Tiles Mode",
        description=(
            "出力範囲のモード。"
            "xyz=tile_size*2^Zの正方形キャンバスへpadしてから切る(従来通り)。"
            "tight=元画像範囲を必要最小padでカバーする領域だけで切る(出力枚数が減る)。"
        ),
        allowable_values=["xyz", "tight"],
        default_value="xyz",
        required=True,
    )

    PNG_COMPRESSION = PropertyDescriptor(
        name="PNG Compression (0-9)",
        description=(
            "PNG圧縮レベル(0-9)。"
            "0が最速(容量大きめ)、9が最小(遅め)。"
            "学習用中間データなら0-3あたりが実用的。"
        ),
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="3",
        required=True,
    )

    ROTATE_0 = PropertyDescriptor(
        name="Rotation 0",
        description="0度回転(元の向き)のタイルを出力するか(true/false)",
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
    )

    ROTATE_90 = PropertyDescriptor(
        name="Rotation 90",
        description="90度回転したタイルを出力するか(true/false)",
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
    )

    ROTATE_180 = PropertyDescriptor(
        name="Rotation 180",
        description="180度回転したタイルを出力するか(true/false)",
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
    )

    ROTATE_270 = PropertyDescriptor(
        name="Rotation 270",
        description="270度回転したタイルを出力するか(true/false)",
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
    )

    ROTATION_ANGLES = PropertyDescriptor(
        name="Rotation Angles",
        description=(
            "追加で出力する回転角度のリスト(カンマ区切り)。"
            "単位は0.01度(1度=100)で、範囲は1..35999。"
            "例: 3000,6050 -> 30.00度,60.50度(右方向=時計回り)"
        ),
        validators=[StandardValidators.ALWAYS_VALID],
        required=False,
    )

    MIRROR_LR = PropertyDescriptor(
        name="Mirror LR",
        description="左右反転(Left-Right)したタイルを追加で出力するか(true/false)",
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
    )

    OUTPUT_RESIZED_256 = PropertyDescriptor(
        name="Output Resized 256px Tile",
        description=(
            "trueの場合、タイルを256x256へリサイズして通常の出力先にそのまま保存する(置き換え)。"
            "元のタイル(リサイズ前)は出力しない。"
        ),
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
    )

    IF_OUTPUT_EXISTS = PropertyDescriptor(
        name="If Output Exists",
        description="出力先フォルダが存在する場合の挙動",
        allowable_values=["create_unique", "overwrite", "fail"],
        default_value="create_unique",
        required=True,
    )

    property_descriptors = [
        INPUT_PATH,
        OUTPUT_BASE_DIR,
        TILE_SIZE,
        STRIDE,
        PADDING_MODE,
        XYZ_TILES_MODE,
        PNG_COMPRESSION,
        ROTATE_0,
        ROTATE_90,
        ROTATE_180,
        ROTATE_270,
        ROTATION_ANGLES,
        MIRROR_LR,
        OUTPUT_RESIZED_256,
        IF_OUTPUT_EXISTS,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("ImageTileSplitLogic.ImageTileSplitLogic")
    def __init__(self, **kwargs):
        # BaseProcessor側の仕組みでロジッククラスが紐づく前提のため、ここでは何もしない。
        pass
