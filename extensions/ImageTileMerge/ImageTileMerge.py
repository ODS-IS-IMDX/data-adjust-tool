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

class ImageTileMerge(BaseProcessor):
    """
    ImageTileMerge

    ■目的
    - XYZ Tiles形式(単一Z)のタイル画像群を1枚のpngへ結合して保存する。
    - 結合後サイズは「元画像サイズ(W,H)」と完全一致させる。
      -> 貼り付け後に (0,0,W,H) でクロップする。

    ■前提
    - 入力タイルは {Z}_{x}_{y}.png (タイルサイズは任意の正方形)
    - スライディング由来(_ox/_oy)が混ざっていたら対象外なのでエラー
    - FlowFile本文(byte_data)は使用しない
    - 出力はpng固定
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = "Merge XYZ tiles (single Z) into one PNG. Crops padded area to original size. Sliding tiles are rejected."
        tags = ["image", "tile", "merge", "xyz", "png", "path"]

    # =========================================================
    # プロパティ定義
    # =========================================================

    INPUT_BASE_DIR = PropertyDescriptor(
        name="Input Base Directory",
        description="入力ベースフォルダ。配下にZフォルダ(数値)が存在する想定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    ORIGINAL_WIDTH = PropertyDescriptor(
        name="Original Image Width (px)",
        description="元画像の幅(px)。未指定ならFlowFile属性 tile.image_width を優先する。-1 は未指定扱い（空と同じ意味）",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="-1",
        required=False,
    )
    
    MERGE_KEY = PropertyDescriptor(
        name="Merge Key",
        description="マージさせるファイル名のキー",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="",
        required=True,
    )

    AUTO_DETECT_TILE_SIZE = PropertyDescriptor(
        name="Auto Detect Tile Size",
        description="タイルサイズが決定できない場合に、入力タイルpngの実サイズから自動判定する。Trueの場合、tile.tile_size属性やTile Size (px)が未指定でも、対象pngの最初の1枚から(tile_width==tile_height)のサイズを取得してtile_sizeとする。Falseの場合、tile.tile_size属性とTile Size (px)の両方が未指定なら、tile_sizeは256(px)として扱う。",
        allowable_values=["True", "False"],
        default_value="True",
        required=True,
    )

    TILE_SIZE = PropertyDescriptor(
        name="Tile Size (px)",
        description="タイル1枚の一辺のピクセル数(正方形)を明示指定する。優先順位は tile.tile_size属性 > 本プロパティ > Auto Detect Tile Size による自動判定 > 256(px)。本プロパティが-1(未指定)で、Auto Detect Tile Size=False、かつtile.tile_size属性も存在しない場合は、256(px)として扱う。",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="-1",
        required=False,
    )

    ORIGINAL_HEIGHT = PropertyDescriptor(
        name="Original Image Height (px)",
        description="元画像の高さ(px)。未指定ならFlowFile属性 tile.image_height を優先する。-1 は未指定扱い（空と同じ意味）",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="-1",
        required=False,
    )

    OUTPUT_PATH = PropertyDescriptor(
        name="Output Path",
        description="結合後pngの出力ファイルパス(絶対パス推奨)。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    OVERWRITE = PropertyDescriptor(
        name="Overwrite",
        description="出力ファイルが存在する場合に上書きするか。",
        allowable_values=["True", "False"],
        default_value="True",
        required=True,
    )

    MISSING_TILE_POLICY = PropertyDescriptor(
        name="Missing Tile Policy",
        description="タイル欠損がある場合の挙動。fail=エラー, blank=空で埋めて続行。",
        allowable_values=["fail", "blank"],
        default_value="fail",
        required=True,
    )

    OUTPUT_COLOR_MODE = PropertyDescriptor(
        name="Output Color Mode",
        description="出力画像の色モード。keep=最初のタイルに合わせる, RGB, L(グレースケール)。",
        allowable_values=["keep", "RGB", "L"],
        default_value="keep",
        required=True,
    )

    property_descriptors = [
        INPUT_BASE_DIR,
        MERGE_KEY,
        AUTO_DETECT_TILE_SIZE,
        TILE_SIZE,
        ORIGINAL_WIDTH,
        ORIGINAL_HEIGHT,
        OUTPUT_PATH,
        OVERWRITE,
        MISSING_TILE_POLICY,
        OUTPUT_COLOR_MODE,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class("ImageTileMergeLogic.ImageTileMergeLogic")
    def __init__(self, **kwargs):
        pass
