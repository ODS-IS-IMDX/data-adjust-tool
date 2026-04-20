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

from __future__ import annotations

# Python標準ライブラリ
import os
import csv
import math
import shutil
import errno
import random
from pathlib import Path
from typing import Dict, List, Tuple
from importlib import import_module
import logging

# 外部ライブラリの動的インポート
Image = import_module("PIL.Image")

logger = logging.getLogger("nifi.python.MakeDataset")


class MakeDatasetLogic:
    """
    MakeDatasetLogic

    images と masks のフォルダを走査し、対応ペアを作成し、
    フォルダ単位で train/val に分割し、フラット化 + 連番ファイル名で出力する。
    FlowFile 本文(byte_data)は使わない(=「フォルダ→フォルダ」のバッチ処理).

    重要ポイント:
      1) images と masks は「同一フォルダ構成 & 同一ファイル名」を前提とし、相対パス一致でペアを作る
      2) 分割は「フォルダ単位(グループ単位)」で行う(フォルダ丸ごとtrain/valを保証)
      3) 出力は「フラット化 + 連番」(サブフォルダは出力に含めない)
      4) TRANSFER_MODE=AUTO の場合は hardlink -> symlink -> copy の順で固定試行
         ※今回AUTO_ORDERプロパティは不要と判断し、Logic側から削除している
    """

    IMAGE_EXT = {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"}

    # =========================================================
    # NiFi FlowFileTransform entry point
    # =========================================================
    def __call__(self, byte_data: bytes, attribute: dict, properties: dict):
        """
        メインの処理の入り口部分。
        各プロパティに基づいて、(1)ペア収集 (2)分割 (3)コピー (4)CSV出力 を実行する。

        注意:
          - FlowFileのcontent(byte_data)は使わない
          - 代わりに「指定された入力フォルダ」を走査して dataset を生成する
          - 生成結果(件数や出力先など)は attribute に書き戻す

        :param byte_data: 入力データとしてバイト型のFieldSetFileを受け取る(本ロジックでは未使用)
        :type byte_data: bytes
        :param attribute: 属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 空バイト列と、更新された属性情報を含むタプル
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # -----------------------------------------------------
            # 1) プロパティ取得
            # -----------------------------------------------------
            # 入力フォルダ(画像 / マスク)
            # - NiFi側で値が空だと事故るので、この段階でstripしておく
            images_root = Path(properties.get("INPUT_IMAGES_DIR", "").strip())
            masks_root = Path(properties.get("INPUT_MASKS_DIR", "").strip())

            # 出力ルート(この配下に dataset/ や split_list.csv を作る)
            output_root = Path(properties.get("OUTPUT_ROOT_DIR", "").strip())

            # 分割単位(=フォルダ単位の粒度)
            # - 1: 最上位フォルダ単位
            # - 2: 2階層目まで含めてグループ化
            group_by_parts = int(properties.get("GROUP_BY_PARTS", "1"))

            # 分割モード
            # - PERCENT: TRAIN_PERCENT を使う(従来互換)
            # - COUNT  : TRAIN_GROUP_COUNT / VAL_GROUP_COUNT を使う(今回追加)
            split_mode = properties.get("SPLIT_MODE", "PERCENT").strip().upper()

            # PERCENTモード用
            # - 0～100 を許容する
            # - 100なら val=0、0なら train=0 も許容する(要件: V=0許容)
            train_percent = int(properties.get("TRAIN_PERCENT", "80"))

            # COUNTモード用
            # - 例: T=32, V=8 のように「グループ数」を指定する
            # - V=0 も許容する(要件)
            train_group_count = int(properties.get("TRAIN_GROUP_COUNT", "0"))
            val_group_count = int(properties.get("VAL_GROUP_COUNT", "0"))

            # COUNTモードで「T+V < 全グループ数」のとき、余りをどうするか
            # - IGNORE: 余りグループは出力しない
            # - TRAIN : 余りグループをtrainへ寄せる
            # - VAL   : 余りグループをvalへ寄せる
            # - UNUSED: unused/ に出力して残す(検証用)
            remainder_mode = properties.get("REMAINDER_MODE", "IGNORE").strip().upper()

            # グループの順序
            # - SORT   : 文字列順で固定(再現性が高い)
            # - SHUFFLE: ランダム(毎回変化、seedなし)
            group_order = properties.get("GROUP_ORDER", "SORT").strip().upper()

            # 連番ファイル名設定
            seq_prefix = properties.get("SEQ_PREFIX", "img_")
            seq_digits = int(properties.get("SEQ_DIGITS", "5"))
            seq_sep = properties.get("SEQ_SEPARATOR", "_")

            # 出力ファイル名モード(今回追加)
            # - SEQ_AND_ORIGINAL: img_00001_xxx.png (連番 + 元ファイル名)
            # - SEQ_ONLY        : img_00001.png     (連番 + 拡張子のみ)
            output_filename_mode = properties.get("OUTPUT_FILENAME_MODE", "SEQ_AND_ORIGINAL").strip().upper()

            # 転送方式
            # - AUTO: hardlink -> symlink -> copy の順で固定試行(=AUTO_ORDERは廃止)
            # - HARDLINK / SYMLINK / COPY も指定可
            transfer_mode = properties.get("TRANSFER_MODE", "AUTO").strip().upper()

            # DRY_RUN(true/false)
            # - true なら実ファイルは作らず、ログに「何をする予定か」を出す
            dry_run = (properties.get("DRY_RUN", "false").strip().lower() == "true")

            # GTあり/なし 混在制御
            # - ALL     : 現状どおり(判定もしない)
            # - POS_ONLY: GTありのみ
            # - MIX     : GTあり+GTなしを比率で混在
            gt_mix_mode = properties.get("GT_MIX_MODE", "ALL").strip().upper()
            neg_per_pos_str = properties.get("NEG_PER_POS", "0.0").strip()
            mask_black_max_value_str = properties.get("MASK_BLACK_MAX_VALUE", "2").strip()
            min_nonblack_pixels_str = properties.get("MIN_NONBLACK_PIXELS", "1").strip()

            # -----------------------------------------------------
            # 2) 入力チェック
            # -----------------------------------------------------
            # 入力フォルダが存在しない場合、処理の前提が崩れているので即エラー
            if not images_root.exists():
                raise FileNotFoundError(f"Images root not found: {images_root}")
            if not masks_root.exists():
                raise FileNotFoundError(f"Masks root not found: {masks_root}")

            # OUTPUT_ROOT_DIR が空だと Path("")=カレント扱いになり、事故りやすいので明示的に弾く
            if str(output_root).strip() == "":
                raise ValueError("OUTPUT_ROOT_DIR is empty.")

            # 出力先
            dataset_dir = output_root / "dataset"
            csv_path = output_root / "split_list.csv"

            # 既存のdataset/があれば一旦削除して、常にクリーンな状態で作る
            # - ファイル衝突(同名)を避けるための安全策
            if dataset_dir.exists():
                if dry_run:
                    logger.info(f"[DRY_RUN] clean output: {dataset_dir}")
                else:
                    shutil.rmtree(dataset_dir)

            # -----------------------------------------------------
            # 3) ペア収集（欠損/孤児チェック）
            # -----------------------------------------------------
            # images_root と masks_root をそれぞれ走査し、
            # 相対パス一致で(画像,マスク)のペア一覧を作る
            # 欠損/孤児があれば pair_check_errors.txt を出力して停止する
            pairs = self.collect_pairs(images_root, masks_root, output_root, dry_run)

            # -----------------------------------------------------
            # 4) グループ単位で train/val 分割
            # -----------------------------------------------------
            # rel_pathからグループキーを作り、グループ単位で割り当てる
            # (フォルダ丸ごとtrain/valを保証するのが狙い)
            train_pairs, val_pairs, unused_pairs = self.split_by_group(
                pairs=pairs,
                train_percent=train_percent,
                group_by_parts=group_by_parts,
                split_mode=split_mode,
                train_group_count=train_group_count,
                val_group_count=val_group_count,
                remainder_mode=remainder_mode,
                group_order=group_order,
            )

            # -----------------------------------------------------
            # 4.5) GTあり/なし 混在制御
            # -----------------------------------------------------
            train_gt_stats = None
            val_gt_stats = None

            if gt_mix_mode not in ("ALL", "POS_ONLY", "MIX"):
                raise ValueError(f"GT_MIX_MODE must be ALL / POS_ONLY / MIX. got={gt_mix_mode}")

            if gt_mix_mode in ("POS_ONLY", "MIX"):
                try:
                    neg_per_pos = float(neg_per_pos_str) if gt_mix_mode == "MIX" else 0.0
                except ValueError:
                    raise ValueError(f"NEG_PER_POS must be float. got={neg_per_pos_str}")

                try:
                    mask_black_max_value = int(mask_black_max_value_str)
                except ValueError:
                    raise ValueError(f"MASK_BLACK_MAX_VALUE must be int. got={mask_black_max_value_str}")

                try:
                    min_nonblack_pixels = int(min_nonblack_pixels_str)
                except ValueError:
                    raise ValueError(f"MIN_NONBLACK_PIXELS must be int. got={min_nonblack_pixels_str}")

                if neg_per_pos < 0.0:
                    raise ValueError(f"NEG_PER_POS must be >= 0.0. got={neg_per_pos}")

                train_pairs, train_gt_stats = self.apply_gt_mix(
                    pairs=train_pairs,
                    gt_mix_mode=gt_mix_mode,
                    neg_per_pos=neg_per_pos,
                    mask_black_max_value=mask_black_max_value,
                    min_nonblack_pixels=min_nonblack_pixels,
                )

                val_pairs, val_gt_stats = self.apply_gt_mix(
                    pairs=val_pairs,
                    gt_mix_mode=gt_mix_mode,
                    neg_per_pos=neg_per_pos,
                    mask_black_max_value=mask_black_max_value,
                    min_nonblack_pixels=min_nonblack_pixels,
                )

            # -----------------------------------------------------
            # 5) コピー（フラット化 + 連番）
            # -----------------------------------------------------
            # dataset/train/images と dataset/train/masks に、
            # フラットな構造で「同名ファイル」として転送する
            self.copy_pairs(
                pairs=train_pairs,
                out_dir=dataset_dir / "train",
                seq_prefix=seq_prefix,
                seq_digits=seq_digits,
                seq_sep=seq_sep,
                output_filename_mode=output_filename_mode,
                transfer_mode=transfer_mode,
                dry_run=dry_run,
            )

            self.copy_pairs(
                pairs=val_pairs,
                out_dir=dataset_dir / "val",
                seq_prefix=seq_prefix,
                seq_digits=seq_digits,
                seq_sep=seq_sep,
                output_filename_mode=output_filename_mode,
                transfer_mode=transfer_mode,
                dry_run=dry_run,
            )

            # UNUSEDモードを使っている場合のみ、unused/ を出力する
            if unused_pairs:
                self.copy_pairs(
                    pairs=unused_pairs,
                    out_dir=dataset_dir / "unused",
                    seq_prefix=seq_prefix,
                    seq_digits=seq_digits,
                    seq_sep=seq_sep,
                    output_filename_mode=output_filename_mode,
                    transfer_mode=transfer_mode,
                    dry_run=dry_run,
                )

            # -----------------------------------------------------
            # 6) CSV 出力
            # -----------------------------------------------------
            # split_list.csv を output_root 直下に出す
            # - 出力されたファイル名(連番付き)と元の相対パスの対応を追えるようにする
            self.write_csv(
                csv_path=csv_path,
                train_pairs=train_pairs,
                val_pairs=val_pairs,
                unused_pairs=unused_pairs,
                seq_prefix=seq_prefix,
                seq_digits=seq_digits,
                seq_sep=seq_sep,
                output_filename_mode=output_filename_mode,
                group_by_parts=group_by_parts,
                dry_run=dry_run,
            )

            # -----------------------------------------------------
            # 7) FlowFile 属性更新
            # -----------------------------------------------------
            # NiFi上で「何件作られたか」「どこへ出したか」が見えるように属性へ格納する
            attribute.update({
                "dataset.total_pairs": str(len(pairs)),
                "dataset.train_pairs": str(len(train_pairs)),
                "dataset.val_pairs": str(len(val_pairs)),
                "dataset.unused_pairs": str(len(unused_pairs)),
                "dataset.output_root": str(dataset_dir),
                "dataset.csv": str(csv_path),

                "dataset.split_mode": split_mode,
                "dataset.group_by_parts": str(group_by_parts),
                "dataset.group_order": group_order,
                "dataset.remainder_mode": remainder_mode,

                "dataset.output_filename_mode": output_filename_mode,
                "dataset.transfer_mode": transfer_mode,
                "dataset.dry_run": str(dry_run).lower(),
                "dataset.gt_mix_mode": gt_mix_mode,
                "dataset.neg_per_pos": neg_per_pos_str,
                "dataset.mask_black_max_value": mask_black_max_value_str,
                "dataset.min_nonblack_pixels": min_nonblack_pixels_str,
            })

            # FlowFile本文は使わないため、空bytesを返す
            return b"", attribute

        except Exception as e:
            # エラー内容を属性にも残す(後段でログや通知に使える)
            attribute["dataset.error"] = str(e)
            raise

    # =========================================================
    # 収集：images/masks の相対パス対応
    # =========================================================
    def collect_pairs(self, images_root: Path, masks_root: Path, output_root: Path, dry_run: bool):
        """
        images_root と masks_root を走査し、相対パス一致で(画像,マスク)のペア一覧を作る。
        欠損マスク / 孤児マスクが存在する場合は、pair_check_errors.txt を出力して例外停止する。

        :param images_root: imagesフォルダのルート
        :type images_root: Path
        :param masks_root: masksフォルダのルート
        :type masks_root: Path
        :param output_root: エラー詳細(pair_check_errors.txt)の出力先ルート
        :type output_root: Path
        :param dry_run: DRY_RUNフラグ
        :type dry_run: bool

        :return: (image_abs, mask_abs, rel_path) のリスト
        :rtype: list(tuple(Path, Path, Path))

        :raises RuntimeError: ペア不成立(欠損/孤児)がある場合に例外をスローする
        """
        # それぞれ「相対パス -> 絶対パス」の辞書にして、
        # 相対パスをキーとして突合する
        image_map = self.collect_relpath_map(images_root)
        mask_map = self.collect_relpath_map(masks_root)

        missing_masks: List[Path] = []
        orphan_masks: List[Path] = []
        pairs: List[Tuple[Path, Path, Path]] = []

        # images側を正として、maskがあるかチェック
        for rel in sorted(image_map.keys()):
            if rel not in mask_map:
                missing_masks.append(rel)
                continue
            pairs.append((image_map[rel], mask_map[rel], rel))

        # masks側にだけ存在する(孤児mask)をチェック
        for rel in sorted(mask_map.keys()):
            if rel not in image_map:
                orphan_masks.append(rel)

        # 欠損/孤児がある場合は詳細ファイルを出して停止する
        if missing_masks or orphan_masks:
            msg = self.build_pair_error_message(
                images_root=images_root,
                masks_root=masks_root,
                missing_masks=missing_masks,
                orphan_masks=orphan_masks,
            )
            error_file = self.write_pair_check_error_file(output_root, msg, dry_run)
            raise RuntimeError(f"Pair check failed. See: {error_file}\n\n{msg}")

        if not pairs:
            raise RuntimeError("Valid pairs are 0. Check folders and extensions.")

        return pairs

    def collect_relpath_map(self, root: Path) -> Dict[Path, Path]:
        """
        root配下を再帰探索し、対象拡張子ファイルの「相対パス -> 絶対パス」辞書を作る。

        :param root: 探索対象のルートフォルダ
        :type root: Path

        :return: 相対パスをキー、絶対パスを値とする辞書
        :rtype: dict(Path, Path)

        :raises FileNotFoundError: 入力フォルダが存在しない場合
        :raises RuntimeError: 対象ファイルが0件、または相対パス重複がある場合
        """
        if not root.exists():
            raise FileNotFoundError(f"Input dir not found: {root}")

        result: Dict[Path, Path] = {}

        # rglobで再帰走査(フォルダ配下を丸ごと対象にする)
        for p in root.rglob("*"):
            if not p.is_file():
                continue

            # 拡張子フィルタ(画像系だけを対象にする)
            if p.suffix.lower() not in self.IMAGE_EXT:
                continue

            # root からの相対パスをキーにする
            rel = p.relative_to(root)

            # 相対パスが重複するのは、通常あり得ない(入力がおかしい)
            if rel in result:
                raise RuntimeError(f"Duplicate relative path detected: {rel}")

            result[rel] = p

        # 対象が0件なら、パスや拡張子条件が間違っている可能性が高い
        if not result:
            raise RuntimeError(f"No target files found in: {root}")

        return result

    def write_pair_check_error_file(self, output_root: Path, msg: str, dry_run: bool) -> Path:
        """
        ペア不成立の詳細を output_root 直下に pair_check_errors.txt として保存する。

        :param output_root: 出力ルート
        :type output_root: Path
        :param msg: 保存するエラーメッセージ本文
        :type msg: str
        :param dry_run: DRY_RUNフラグ
        :type dry_run: bool

        :return: 出力したファイルパス
        :rtype: Path

        :raises Exception: ファイル出力に失敗した場合
        """
        out_path = output_root / "pair_check_errors.txt"

        if dry_run:
            logger.info(f"[DRY_RUN] write pair check error file: {out_path}")
            return out_path

        output_root.mkdir(parents=True, exist_ok=True)
        out_path.write_text(msg, encoding="utf-8")
        return out_path

    def build_pair_error_message(self, images_root: Path, masks_root: Path, missing_masks: List[Path], orphan_masks: List[Path]) -> str:
        """
        ペア不成立(欠損/孤児)の詳細メッセージを生成する。

        :param images_root: imagesのルート
        :type images_root: Path
        :param masks_root: masksのルート
        :type masks_root: Path
        :param missing_masks: imagesに対応するmaskが無い相対パス一覧
        :type missing_masks: list(Path)
        :param orphan_masks: masksに対応するimageが無い相対パス一覧
        :type orphan_masks: list(Path)

        :return: 改行区切りのメッセージ本文
        :rtype: str
        """
        lines: List[str] = []
        lines.append("Pair check failed.")
        lines.append(f"ImagesRoot: {images_root}")
        lines.append(f"MasksRoot : {masks_root}")
        lines.append("")

        if missing_masks:
            lines.append(f"[Missing masks] count={len(missing_masks)}")
            for rel in missing_masks:
                lines.append(f"  image   : {images_root / rel}")
                lines.append(f"  expected: {masks_root / rel}")
                lines.append("")

        if orphan_masks:
            lines.append(f"[Orphan masks] count={len(orphan_masks)}")
            for rel in orphan_masks:
                lines.append(f"  mask    : {masks_root / rel}")
                lines.append(f"  expected: {images_root / rel}")
                lines.append("")

        return "\n".join(lines)

    # =========================================================
    # グループキー生成
    # =========================================================
    def group_key(self, rel: Path, group_by_parts: int) -> str:
        """
        相対パスからグループキーを生成する。

        例:
          rel="data1/abc/0001.png", group_by_parts=1 -> "data1"
          rel="data1/abc/0001.png", group_by_parts=2 -> "data1/abc"
          rel="0001.png" (ルート直下) -> "(root)"

        :param rel: images_root/masks_root からの相対パス
        :type rel: Path
        :param group_by_parts: 何階層までをグループキーに含めるか
        :type group_by_parts: int

        :return: グループキー文字列
        :rtype: str
        """
        parts = rel.parts

        # ルート直下にファイルがある場合は、特別キーにまとめる
        if len(parts) <= 1:
            return "(root)"

        # group_by_parts は最低1
        n = max(1, group_by_parts)

        # partsの最後はファイル名なので除外したい
        head = parts[: min(n, len(parts) - 1)]
        return "/".join(head)

    # =========================================================
    # グループ単位で train/val 分割
    # =========================================================
    def split_by_group(
        self,
        pairs: List[Tuple[Path, Path, Path]],
        train_percent: int,
        group_by_parts: int,
        split_mode: str,
        train_group_count: int,
        val_group_count: int,
        remainder_mode: str,
        group_order: str,
    ) -> Tuple[List[Tuple[Path, Path, Path]], List[Tuple[Path, Path, Path]], List[Tuple[Path, Path, Path]]]:
        """
        フォルダ(group_key)単位で train/val に分割する。

        分割ルール:
          - pairs から group_key -> pairs を構築
          - group_key の並び順は GROUP_ORDER に従う(SORT/SHUFFLE)
          - PERCENTモード:
              train_percent に応じて n_train_groups を決め、前半をtrain、残りをvalへ
              ※ 0～100を許容し、train=0 や val=0 も許容する(要件: V=0許容)
          - COUNTモード:
              train_group_count(T) + val_group_count(V) を指定した数だけ割り当て
              余りは remainder_mode(IGNORE/TRAIN/VAL/UNUSED) で扱う

        :param pairs: (img_abs, mask_abs, rel_path) のリスト
        :type pairs: list(tuple(Path, Path, Path))
        :param train_percent: PERCENTモード時のtrain割合(0～100)
        :type train_percent: int
        :param group_by_parts: グループ化の階層数
        :type group_by_parts: int
        :param split_mode: "PERCENT" または "COUNT"
        :type split_mode: str
        :param train_group_count: COUNTモード時のtrainグループ数(T)
        :type train_group_count: int
        :param val_group_count: COUNTモード時のvalグループ数(V、0許容)
        :type val_group_count: int
        :param remainder_mode: COUNTモードで余りをどう扱うか
        :type remainder_mode: str
        :param group_order: グループの並び順(SORT/SHUFFLE)
        :type group_order: str

        :return: train_pairs, val_pairs, unused_pairs のタプル
        :rtype: tuple(list(tuple(Path, Path, Path)), list(tuple(Path, Path, Path)), list(tuple(Path, Path, Path)))

        :raises ValueError: 不正なパラメータが指定された場合
        """
        # ---------------------------------------------
        # 1) group_key -> pairs の辞書を作る
        # ---------------------------------------------
        groups: Dict[str, List[Tuple[Path, Path, Path]]] = {}
        for img, msk, rel in pairs:
            g = self.group_key(rel, group_by_parts)
            groups.setdefault(g, []).append((img, msk, rel))

        group_keys = list(groups.keys())
        n_groups = len(group_keys)

        if n_groups <= 0:
            raise RuntimeError("No groups found. Check GROUP_BY_PARTS and input folders.")

        # ---------------------------------------------
        # 2) グループ順序の確定
        # ---------------------------------------------
        if group_order == "SHUFFLE":
            # seedなし(=毎回変わる)でシャッフル
            random.shuffle(group_keys)
        else:
            # SORT(デフォルト): 文字列順で固定
            group_keys = sorted(group_keys)

        # ---------------------------------------------
        # 3) 出力コンテナ
        # ---------------------------------------------
        train_pairs: List[Tuple[Path, Path, Path]] = []
        val_pairs: List[Tuple[Path, Path, Path]] = []
        unused_pairs: List[Tuple[Path, Path, Path]] = []

        # ---------------------------------------------
        # 4) COUNTモード
        # ---------------------------------------------
        if split_mode == "COUNT":
            t = int(train_group_count)
            v = int(val_group_count)

            # T,Vは0以上
            if t < 0 or v < 0:
                raise ValueError(f"TRAIN_GROUP_COUNT / VAL_GROUP_COUNT must be >= 0. got T={t}, V={v}")

            # T+Vは最低1(全てunused/ignoreは仕様として危険なので弾く)
            if (t + v) <= 0:
                raise ValueError(f"TRAIN_GROUP_COUNT + VAL_GROUP_COUNT must be >= 1. got T={t}, V={v}")

            # 指定数がグループ数を超えると割り当て不能
            if (t + v) > n_groups:
                raise ValueError(
                    "TRAIN_GROUP_COUNT + VAL_GROUP_COUNT must be <= number of groups.\n"
                    f"  groups={n_groups}\n"
                    f"  T={t}\n"
                    f"  V={v}"
                )

            # 前半T個をtrain、次のV個をvalへ
            train_keys = set(group_keys[:t])
            val_keys = set(group_keys[t:t + v])

            # 余りグループ
            remainder_keys = list(group_keys[t + v:])

            # 余りの扱い(要件で切替)
            if remainder_keys:
                if remainder_mode == "IGNORE":
                    # 何もしない(余りは捨てる)
                    pass
                elif remainder_mode == "TRAIN":
                    train_keys.update(remainder_keys)
                elif remainder_mode == "VAL":
                    val_keys.update(remainder_keys)
                elif remainder_mode == "UNUSED":
                    # unusedへ積む(後でまとめてコピーする)
                    for g in remainder_keys:
                        group_pairs = sorted(groups[g], key=lambda x: str(x[2]).replace("\\", "/"))
                        unused_pairs.extend(group_pairs)
                else:
                    raise ValueError(f"Unknown REMAINDER_MODE: {remainder_mode}")

            # 最終的に group_keys の順で各グループを出力リストへ積む
            # (グループ内の順序はrel_path文字列で固定)
            for g in group_keys:
                group_pairs = sorted(groups[g], key=lambda x: str(x[2]).replace("\\", "/"))
                if g in train_keys:
                    train_pairs.extend(group_pairs)
                elif g in val_keys:
                    val_pairs.extend(group_pairs)
                else:
                    # IGNOREの場合、ここに落ちる(=何もしない)
                    pass

            return train_pairs, val_pairs, unused_pairs

        # ---------------------------------------------
        # 5) PERCENTモード(従来互換)
        # ---------------------------------------------
        # 0～100を許容し、val=0やtrain=0も許容する(要件: V=0)
        if not (0 <= train_percent <= 100):
            raise ValueError(f"TRAIN_PERCENT must be 0..100. got={train_percent}")

        n_train = int(math.floor(n_groups * (train_percent / 100.0)))

        # 先頭n_trainをtrain、残りをval
        train_keys = set(group_keys[:n_train])
        val_keys = set(group_keys[n_train:])

        for g in group_keys:
            group_pairs = sorted(groups[g], key=lambda x: str(x[2]).replace("\\", "/"))
            if g in train_keys:
                train_pairs.extend(group_pairs)
            elif g in val_keys:
                val_pairs.extend(group_pairs)

        return train_pairs, val_pairs, unused_pairs

    # =========================================================
    # 出力ファイル名（フラット化 + 連番）
    # =========================================================
    def make_output_filename(
        self, 
        original_filename: str,
        seq_index: int,
        seq_prefix: str, 
        seq_digits: int,
        seq_sep: str,
        output_filename_mode: str,
        ) -> str:
        """
        連番ファイル名を生成する。

        :param original_filename: 元ファイル名(rel.name)
        :type original_filename: str
        :param seq_index: 連番(1始まり)
        :type seq_index: int
        :param seq_prefix: 連番プレフィックス
        :type seq_prefix: str
        :param seq_digits: 連番の桁数
        :type seq_digits: int
        :param seq_sep: 連番と元名を繋ぐ区切り
        :type seq_sep: str
        :param output_filename_mode: 出力名モード
        :type output_filename_mode: str

        :return: 出力ファイル名
        :rtype: str
        """
        seq = str(seq_index).zfill(int(seq_digits))

        if output_filename_mode == "SEQ_ONLY":
            # 元名は捨て、拡張子だけ維持する
            ext = Path(original_filename).suffix
            return f"{seq_prefix}{seq}{ext}"

        # SEQ_AND_ORIGINAL(デフォルト)
        return f"{seq_prefix}{seq}{seq_sep}{original_filename}"

    # =========================================================
    # コピー（フラット化 + 連番）
    # =========================================================
    def copy_pairs(
        self,
        pairs: List[Tuple[Path, Path, Path]],
        out_dir: Path,
        seq_prefix: str,
        seq_digits: int,
        seq_sep: str,
        output_filename_mode: str,
        transfer_mode: str,
        dry_run: bool,
        ):
        """
        pairs を out_dir 配下へコピーする。
        出力構造は out_dir/images と out_dir/masks の2つを作り、フラットに出力する。

        :param pairs: (img_abs, mask_abs, rel_path) のリスト
        :type pairs: list(tuple(Path, Path, Path))
        :param out_dir: subset(train/val/unused) の出力先ディレクトリ
        :type out_dir: Path
        :param seq_prefix: 連番プレフィックス
        :type seq_prefix: str
        :param seq_digits: 連番桁数
        :type seq_digits: int
        :param seq_sep: 区切り文字
        :type seq_sep: str
        :param output_filename_mode: 出力名モード
        :type output_filename_mode: str
        :param transfer_mode: 転送モード(AUTO/HARDLINK/SYMLINK/COPY)
        :type transfer_mode: str
        :param dry_run: DRY_RUNフラグ
        :type dry_run: bool

        :return: なし
        :rtype: None

        :raises RuntimeError: 転送に失敗した場合
        """
        # subset配下に images/ と masks/ を作る
        images_dir = out_dir / "images"
        masks_dir = out_dir / "masks"

        if not dry_run:
            images_dir.mkdir(parents=True, exist_ok=True)
            masks_dir.mkdir(parents=True, exist_ok=True)

        # pairsは「(img_abs, mask_abs, rel_path)」の並び
        # 出力ファイル名は rel_path.name から生成する(フラット化のためフォルダ名は捨てる)
        for i, (img_src, msk_src, rel) in enumerate(pairs, start=1):
            out_name = self.make_output_filename(
                original_filename=rel.name,
                seq_index=i,
                seq_prefix=seq_prefix,
                seq_digits=seq_digits,
                seq_sep=seq_sep,
                output_filename_mode=output_filename_mode,
            )

            # images と masks は「同名」で出す(対応関係維持が最重要)
            self.transfer_one(img_src, images_dir / out_name, transfer_mode, dry_run)
            self.transfer_one(msk_src, masks_dir / out_name, transfer_mode, dry_run)

    def transfer_one(self, src: Path, dst: Path, mode: str, dry_run: bool):
        """
        1ファイルを出力する。

        TRANSFER_MODE:
          - AUTO    : hardlink -> symlink -> copy の順に固定で試す
          - HARDLINK: os.link
          - SYMLINK : os.symlink
          - COPY    : shutil.copyfile

        注意:
          - Windowsではsymlinkに権限が必要な場合があるため、AUTOでは失敗したらCOPYへフォールバックする
          - hardlinkは同一ボリュームでないと失敗する(EXDEV)

        :param src: 入力ファイルパス
        :type src: Path
        :param dst: 出力ファイルパス
        :type dst: Path
        :param mode: 転送モード(AUTO/HARDLINK/SYMLINK/COPY)
        :type mode: str
        :param dry_run: DRY_RUNフラグ
        :type dry_run: bool

        :return: なし
        :rtype: None

        :raises RuntimeError: すべての転送方式に失敗した場合
        :raises FileExistsError: 出力先が既に存在する場合
        """
        if dry_run:
            logger.info(f"[DRY_RUN] copy {src} -> {dst}")
            return

        # 同名ファイルがあると「対応関係が壊れる」ので、黙って上書きはしない
        if dst.exists():
            raise FileExistsError(f"Destination already exists: {dst}")

        dst.parent.mkdir(parents=True, exist_ok=True)

        # AUTO_ORDERは廃止したため、AUTOの試行順は固定
        if mode == "AUTO":
            modes = ["HARDLINK", "SYMLINK", "COPY"]
        else:
            modes = [mode]

        errors: List[str] = []

        # 試行順に転送を試す
        for m in modes:
            try:
                if m == "HARDLINK":
                    os.link(str(src), str(dst))
                    return

                if m == "SYMLINK":
                    os.symlink(str(src), str(dst), target_is_directory=False)
                    return

                if m == "COPY":
                    shutil.copyfile(str(src), str(dst))
                    return

                errors.append(f"{m}: unknown mode")

            except OSError as e:
                code = getattr(e, "errno", None)

                # よくある失敗パターンは、メッセージを分けて残すと調査が楽
                if code == errno.EXDEV:
                    errors.append(f"{m}: EXDEV (cross-device) src={src} dst={dst}")
                elif code in (errno.EPERM, errno.EACCES):
                    errors.append(f"{m}: PERMISSION (EPERM/EACCES) src={src} dst={dst}")
                else:
                    errors.append(f"{m}: OSError errno={code} msg={e} src={src} dst={dst}")

        # 全方式ダメなら、試した順序とエラーをまとめて例外化
        raise RuntimeError(
            "Failed to transfer file.\n"
            f"src={src}\n"
            f"dst={dst}\n"
            f"tried={modes}\n"
            "errors:\n" + "\n".join(errors)
        )

    # =========================================================
    # GTあり/なし 判定・混在制御
    # =========================================================
    def mask_has_gt(self, mask_path: Path, black_max_value: int, min_nonblack_pixels: int) -> bool:
        """
        mask画像から「GTあり」を判定する。

        判定ロジック:
          - 画像をグレースケール(L)へ変換して扱う
          - 画素値が black_max_value を超える画素を「黒以外」とみなす
          - 黒以外画素数が min_nonblack_pixels 以上なら True(=GTあり)

        :param mask_path: maskファイルパス
        :type mask_path: Path
        :param black_max_value: 黒扱いの上限値(この値以下は黒)
        :type black_max_value: int
        :param min_nonblack_pixels: 黒以外画素数の閾値
        :type min_nonblack_pixels: int

        :return: GTありならTrue
        :rtype: bool
        """
        try:

            with Image.open(str(mask_path)) as img:
                if img.mode != "L":
                    img = img.convert("L")

                hist = img.histogram()
                if not hist:
                    return False

                idx = max(0, int(black_max_value) + 1)
                nonblack = sum(hist[idx:])
                return (nonblack >= int(min_nonblack_pixels))

        except Exception:
            return False

    def apply_gt_mix(
        self,
        pairs: List[Tuple[Path, Path, Path]],
        gt_mix_mode: str,
        neg_per_pos: float,
        mask_black_max_value: int,
        min_nonblack_pixels: int,
    ) -> Tuple[List[Tuple[Path, Path, Path]], dict]:
        """
        GTあり/なしの混在制御を行う。

        gt_mix_mode:
          - POS_ONLY: GTありのみ残す
          - MIX     : GTあり + GTなしを neg_per_pos の比率で混在する
                      例: neg_per_pos=0.25 なら、GTあり4に対してGTなし1
                      GTなしの採用は「元の並び順の先頭から」行う(無駄なランダム性を入れない)

        :param pairs: (img_abs, mask_abs, rel_path) のリスト
        :type pairs: list(tuple(Path, Path, Path))
        :param gt_mix_mode: ALL / POS_ONLY / MIX
        :type gt_mix_mode: str
        :param neg_per_pos: GTあり1に対するGTなしの混在数(MIX時のみ使用)
        :type neg_per_pos: float
        :param mask_black_max_value: maskの黒扱い上限値
        :type mask_black_max_value: int
        :param min_nonblack_pixels: GTあり判定の閾値
        :type min_nonblack_pixels: int

        :return: (選別後pairs, 集計情報)
        :rtype: tuple(list(tuple(Path, Path, Path)), dict)
        """
        pos: List[Tuple[Path, Path, Path]] = []
        neg: List[Tuple[Path, Path, Path]] = []

        for img, msk, rel in pairs:
            if self.mask_has_gt(msk, mask_black_max_value, min_nonblack_pixels):
                pos.append((img, msk, rel))
            else:
                neg.append((img, msk, rel))

        if gt_mix_mode == "POS_ONLY":
            stats = {
                "total_pairs": len(pairs),
                "pos_pairs": len(pos),
                "neg_pairs": len(neg),
                "requested_neg": 0,
                "selected_neg": 0,
                "selected_pairs": len(pos),
            }
            return pos, stats

        if gt_mix_mode == "MIX":
            if len(pos) <= 0:
                requested_neg = 0
            else:
                requested_neg = int(len(pos) * max(0.0, float(neg_per_pos)))

            selected_neg = min(len(neg), requested_neg)
            chosen_neg = neg[:selected_neg]
            selected = pos + chosen_neg

            stats = {
                "total_pairs": len(pairs),
                "pos_pairs": len(pos),
                "neg_pairs": len(neg),
                "requested_neg": requested_neg,
                "selected_neg": selected_neg,
                "selected_pairs": len(selected),
            }
            return selected, stats

        # ALL(ここは通常呼ばれない想定だが、保険で残す)
        stats = {
            "total_pairs": len(pairs),
            "pos_pairs": len(pos),
            "neg_pairs": len(neg),
            "requested_neg": 0,
            "selected_neg": 0,
            "selected_pairs": len(pairs),
        }
        return pairs, stats

    # =========================================================
    # CSV 出力
    # =========================================================
    def write_csv(
        self,
        csv_path: Path,
        train_pairs: List[Tuple[Path, Path, Path]],
        val_pairs: List[Tuple[Path, Path, Path]],
        unused_pairs: List[Tuple[Path, Path, Path]],
        seq_prefix: str,
        seq_digits: int,
        seq_sep: str,
        output_filename_mode: str,
        group_by_parts: int,
        dry_run: bool,
    ):
        """
        split_list.csv を output_root 直下へ出力する。

        CSVの目的:
          - 元の相対パス(rel_path_original) と、出力ファイル名(filename_output) の対応を追えるようにする
          - subset(train/val/unused) と group_key を残し、後から分割の妥当性を確認できるようにする

        :param csv_path: 出力CSVパス
        :type csv_path: Path
        :param train_pairs: train用ペア一覧
        :type train_pairs: list(tuple(Path, Path, Path))
        :param val_pairs: val用ペア一覧
        :type val_pairs: list(tuple(Path, Path, Path))
        :param unused_pairs: unused用ペア一覧
        :type unused_pairs: list(tuple(Path, Path, Path))
        :param seq_prefix: 連番プレフィックス
        :type seq_prefix: str
        :param seq_digits: 連番桁数
        :type seq_digits: int
        :param seq_sep: 区切り文字
        :type seq_sep: str
        :param output_filename_mode: 出力名モード
        :type output_filename_mode: str
        :param group_by_parts: グループ化単位
        :type group_by_parts: int
        :param dry_run: DRY_RUNフラグ
        :type dry_run: bool

        :return: なし
        :rtype: None
        """
        if dry_run:
            logger.info(f"[DRY_RUN] write csv: {csv_path}")
            return

        csv_path.parent.mkdir(parents=True, exist_ok=True)

        with csv_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)

            # ヘッダ
            w.writerow(["subset", "group_key", "rel_path_original", "filename_output"])

            # train
            for i, (_, _, rel) in enumerate(train_pairs, start=1):
                out_name = self.make_output_filename(
                    original_filename=rel.name,
                    seq_index=i,
                    seq_prefix=seq_prefix,
                    seq_digits=seq_digits,
                    seq_sep=seq_sep,
                    output_filename_mode=output_filename_mode,
                )
                w.writerow([
                    "train",
                    self.group_key(rel, group_by_parts),
                    str(rel).replace("\\", "/"),
                    out_name,
                ])

            # val
            for i, (_, _, rel) in enumerate(val_pairs, start=1):
                out_name = self.make_output_filename(
                    original_filename=rel.name,
                    seq_index=i,
                    seq_prefix=seq_prefix,
                    seq_digits=seq_digits,
                    seq_sep=seq_sep,
                    output_filename_mode=output_filename_mode,
                )
                w.writerow([
                    "val",
                    self.group_key(rel, group_by_parts),
                    str(rel).replace("\\", "/"),
                    out_name,
                ])

            # unused
            for i, (_, _, rel) in enumerate(unused_pairs, start=1):
                out_name = self.make_output_filename(
                    original_filename=rel.name,
                    seq_index=i,
                    seq_prefix=seq_prefix,
                    seq_digits=seq_digits,
                    seq_sep=seq_sep,
                    output_filename_mode=output_filename_mode,
                )
                w.writerow([
                    "unused",
                    self.group_key(rel, group_by_parts),
                    str(rel).replace("\\", "/"),
                    out_name,
                ])