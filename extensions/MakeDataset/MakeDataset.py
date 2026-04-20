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


class MakeDataset(BaseProcessor):
    """
    MakeDataset

    images/masks のペアをフォルダ単位で train/val に分割し、
    フラット化 + 連番ファイル名でコピーする NiFi カスタムプロセッサ。

    追加仕様(今回の対応):
      - 分割モードを選べる
        - PERCENT: TRAIN_PERCENT で割合分割
        - COUNT  : TRAIN_GROUP_COUNT / VAL_GROUP_COUNT でグループ数分割(V=0許容)
      - 出力ファイル名モードを選べる
        - SEQ_AND_ORIGINAL: img_00001_original.png
        - SEQ_ONLY        : img_00001.png
      - COUNTモードで余ったグループの扱いを選べる(IGNORE/TRAIN/VAL/UNUSED)
      - グループの並び順を選べる(SORT/SHUFFLE)
      - DRY_RUN を追加(実ファイル出力せず予定のみログ出力)

    注意:
      - AUTO_ORDER は不要のため、プロパティとしても持たない
      - TRANSFER_MODE=AUTO の試行順は Logic 側で固定(hardlink -> symlink -> copy)
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = "Split image/mask dataset into train/val by folder grouping. Supports percent/count modes and filename modes."
        tags = ["dataset", "tensorflow", "segmentation", "train", "val", "split"]

    # =========================================================
    # プロパティ定義
    # =========================================================

    INPUT_IMAGES_DIR = PropertyDescriptor(
        name="Input Images Directory",
        description="images のルートフォルダ",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    INPUT_MASKS_DIR = PropertyDescriptor(
        name="Input Masks Directory",
        description="masks のルートフォルダ",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    OUTPUT_ROOT_DIR = PropertyDescriptor(
        name="Output Root Directory",
        description="出力ルートフォルダ(dataset/ と split_list.csv が作られる)",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    # -------------------------------
    # 分割方式
    # -------------------------------
    SPLIT_MODE = PropertyDescriptor(
        name="Split Mode",
        description="分割モード(PERCENT:割合分割 / COUNT:グループ数指定分割)",
        allowable_values=["PERCENT", "COUNT"],
        default_value="PERCENT",
        required=True,
    )

    TRAIN_PERCENT = PropertyDescriptor(
        name="Train Percent",
        description="PERCENTモード時のtrain割合(0-100)。0や100も許容(V=0を実現可能)",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="80",
        required=True,
    )

    TRAIN_GROUP_COUNT = PropertyDescriptor(
        name="Train Group Count",
        description="COUNTモード時のtrainグループ数(T)。SPLIT_MODE=COUNTのときのみ意味がある",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="0",
        required=True,
    )

    VAL_GROUP_COUNT = PropertyDescriptor(
        name="Val Group Count",
        description="COUNTモード時のvalグループ数(V)。0を許容する",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="0",
        required=True,
    )

    REMAINDER_MODE = PropertyDescriptor(
        name="Remainder Mode",
        description="COUNTモードで(T+V)<全グループ数のとき、余りグループをどう扱うか(IGNORE/TRAIN/VAL/UNUSED)",
        allowable_values=["IGNORE", "TRAIN", "VAL", "UNUSED"],
        default_value="IGNORE",
        required=True,
    )

    GROUP_ORDER = PropertyDescriptor(
        name="Group Order",
        description="グループキーの並び順(SORT:文字列順固定 / SHUFFLE:毎回ランダム)",
        allowable_values=["SORT", "SHUFFLE"],
        default_value="SORT",
        required=True,
    )

    GROUP_BY_PARTS = PropertyDescriptor(
        name="Group By Parts",
        description="フォルダ単位の分割レベル(例: 1なら最上位フォルダ単位、2なら2階層目まで含める)",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="1",
        required=True,
    )

    # -------------------------------
    # 連番ファイル名
    # -------------------------------
    SEQ_PREFIX = PropertyDescriptor(
        name="Sequence Prefix",
        description="連番ファイル名のプレフィックス",
        default_value="img_",
        required=True,
    )

    SEQ_DIGITS = PropertyDescriptor(
        name="Sequence Digits",
        description="連番の桁数",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="5",
        required=True,
    )

    SEQ_SEPARATOR = PropertyDescriptor(
        name="Sequence Separator",
        description="連番と元ファイル名の区切り文字(SEQ_ONLYモードでは使用されない)",
        default_value="_",
        required=True,
    )

    OUTPUT_FILENAME_MODE = PropertyDescriptor(
        name="Output Filename Mode",
        description="出力ファイル名モード(SEQ_AND_ORIGINAL:連番+元名 / SEQ_ONLY:連番のみ+拡張子)",
        allowable_values=["SEQ_AND_ORIGINAL", "SEQ_ONLY"],
        default_value="SEQ_AND_ORIGINAL",
        required=True,
    )

    # -------------------------------
    # 転送方式
    # -------------------------------
    TRANSFER_MODE = PropertyDescriptor(
        name="Transfer Mode",
        description="AUTO / HARDLINK / SYMLINK / COPY (AUTOは hardlink->symlink->copy を固定順で試す)",
        allowable_values=["AUTO", "HARDLINK", "SYMLINK", "COPY"],
        default_value="AUTO",
        required=True,
    )

    # -------------------------------
    # 動作モード
    # -------------------------------
    DRY_RUN = PropertyDescriptor(
        name="Dry Run",
        description="trueの場合、実ファイル出力せず、処理予定のみログ出力する",
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
    )

    # -------------------------------
    # GTあり/なし 混在制御
    # -------------------------------
    GT_MIX_MODE = PropertyDescriptor(
        name="GT Mix Mode",
        description="GTあり/なしの扱い(ALL:現状どおり / POS_ONLY:GTありのみ / MIX:GTあり+GTなしを混在)",
        allowable_values=["ALL", "POS_ONLY", "MIX"],
        default_value="ALL",
        required=True,
    )

    NEG_PER_POS = PropertyDescriptor(
        name="Negatives Per Positive",
        description="GTあり1に対するGTなしの混在数(例:0.0なら混在なし、0.25ならGTあり4にGTなし1)。GT Mix Mode=MIXのときのみ使用",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="0.0",
        required=True,
    )

    MASK_BLACK_MAX_VALUE = PropertyDescriptor(
        name="Mask Black Max Value",
        description="maskのGT判定で黒扱いする上限値(0なら完全な0のみ黒、2なら0-2を黒扱い)",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="2",
        required=True,
    )

    MIN_NONBLACK_PIXELS = PropertyDescriptor(
        name="Min Non-Black Pixels",
        description="maskのGT判定で、黒以外画素がこの数以上ならGTありとみなす",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        default_value="1",
        required=True,
    )

    property_descriptors = [
        INPUT_IMAGES_DIR,
        INPUT_MASKS_DIR,
        OUTPUT_ROOT_DIR,

        SPLIT_MODE,
        TRAIN_PERCENT,
        TRAIN_GROUP_COUNT,
        VAL_GROUP_COUNT,
        REMAINDER_MODE,
        GROUP_ORDER,

        GROUP_BY_PARTS,

        SEQ_PREFIX,
        SEQ_DIGITS,
        SEQ_SEPARATOR,
        OUTPUT_FILENAME_MODE,

        TRANSFER_MODE,
        DRY_RUN,
        GT_MIX_MODE,
        NEG_PER_POS,
        MASK_BLACK_MAX_VALUE,
        MIN_NONBLACK_PIXELS,
    ]

    def getPropertyDescriptors(self):
        """
        NiFiに公開するプロパティ一覧を返す。

        :return: PropertyDescriptorのリスト
        :rtype: list

        :raises Exception: 想定外のエラーが発生した場合
        """
        return self.property_descriptors

    @BaseProcessor.set_logic_class("MakeDatasetLogic.MakeDatasetLogic")
    def __init__(self, **kwargs):
        """
        プロセッサ初期化処理。

        注意:
          - 実処理はLogicクラス側に寄せているため、ここでは特別な初期化は行わない
          - BaseProcessor.set_logic_class で紐づいたLogicがtransform時に呼ばれる

        :param kwargs: NiFi側から渡される初期化引数
        :type kwargs: dict

        :return: なし
        :rtype: None
        """
        pass