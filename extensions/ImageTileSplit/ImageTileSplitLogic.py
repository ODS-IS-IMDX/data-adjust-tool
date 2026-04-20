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

import logging
import math
import os
import uuid
from importlib import import_module
from pathlib import Path
from typing import List, Tuple

# 外部ライブラリの動的インポート
np = import_module("numpy")
cv2 = import_module("cv2")

logger = logging.getLogger("nifi.python.ImageTileSplit")


class ImageTileSplitLogic:
    """
    ImageTileSplitLogic

    ■ 基本方針
    - FlowFile本文(byte_data)は使用しない(入力はファイルパス指定)。
    - 出力はPNG固定である。
    - タイルサイズ(tile_size)はプロパティTILE_SIZEで指定する。
      未指定の場合は DEFAULT_TILE_SIZE(=256) を使用する。
    - strideはプロパティSTRIDEで指定する。
      未指定の場合は stride=tile_size とし、重なりなしの分割となる。
      stride<tile_size の場合はスライディング(重なりあり)となる。
    - Z(zoom level)は入力画像のピクセル数(W,H)とtile_sizeから自動決定し、単一Zのみを出力する。
      M = max(W, H)
      M <= tile_size の場合: Z = 0
      それ以外: Z = ceil(log2(M / tile_size))

    ■ 出力範囲モード(XYZ_TILES_MODE)
    - xyz:
        従来通り、tile_size * 2^Z の「正方形キャンバス」へ右端/下端をpaddingしてから切り出す。
        (XYZ Tilesの思想に沿う: 1辺が常に2^Z枚になる)
    - tight:
        元画像の範囲を最小限にカバーするだけの「必要最小pad」で切り出す。
        (出力枚数を減らせる。正方形や2^Z枚は保証しない)

    ■ 出力パス(フラット出力)
    - 「XYZ Tiles形式のキー(z,x,y)」は維持するが、フォルダ階層は作らない。
      つまり out_root/{z}/{x}/{y}.png ではなく out_root直下にまとめて置く。
    - ファイル名は以下:
        {out_root_name}_{z}_{x}_{y}{suffix}.png
      out_root_name は設定フォルダ名と同じ文字列である。
      (create_unique でuuidが付く場合、ファイル名にもuuidが反映される)

    ■ ファイル名suffix
      (1) スライディング(stride < tile_size)の場合のみ:
          _ox{ox}_oy{oy}
          ※ox/oyは0でも必ず付与する(同名衝突防止のため)

      (2) 回転(ROTATE_90/180/270)が有効な場合のみ:
          _r90 / _r180 / _r270
          ※0度は互換維持のためサフィックス無し

      (3) 左右反転(MIRROR_LR=true)の追加出力:
          _lr
          ※左右反転は「回転後の画像」に対して適用し、別ファイルとして追加で出力する。

      例:
        {out_root}/{out_root_name}_{z}_{x}_{y}_ox128_oy0_r90_lr.png

    ■ 日本語ファイル名(Windows)対策
    - OpenCVのimread/imwriteは、環境によって日本語パスで失敗する。
      そこで「読み込み: fromfile + imdecode」「書き込み: imencode + write_bytes」に統一する。

    ■ 画像ライブラリ
    - PILは使わない(OpenCV + NumPyで統一)。
    """

    DEFAULT_TILE_SIZE = 256  # 未設定時はデフォルトで256
    OUTPUT_FORMAT = "png"  # 仕様で固定
    RESIZED_TILE_SIZE = 256  # OUTPUT_RESIZED_256=true のときの出力サイズ

    def __init__(self):
        pass

    def __call__(self, byte_data: bytes, attribute: dict, properties: dict):
        """
        NiFiのFlowFileTransformの呼び出し口。

        :param byte_data: FlowFile本文(本仕様では未使用)
        :param attribute: FlowFile属性(入出力の引き継ぎに使う)
        :param properties: プロセッサプロパティ(ユーザ設定値)
        :return: (空bytes, 更新されたattribute)
        """
        try:
            # =========================================================
            # 1) プロパティ取得
            # =========================================================
            logger.info("DEBUG properties: " + str(properties))
            logger.info("DEBUG attributes: " + str(attribute))

            input_path = (properties.get("INPUT_PATH") or "").strip()
            output_base_dir = (properties.get("OUTPUT_BASE_DIR") or "").strip()

            tile_size_raw = (properties.get("TILE_SIZE") or "").strip()
            tile_size = self.DEFAULT_TILE_SIZE if tile_size_raw == "" else int(tile_size_raw)

            stride_raw = (properties.get("STRIDE") or "").strip()
            stride = tile_size if stride_raw == "" else int(stride_raw)

            pad_mode = (properties.get("PADDING_MODE") or "zero").strip()
            if_output_exists = (properties.get("IF_OUTPUT_EXISTS") or "create_unique").strip()

            # 出力範囲のモード
            xyz_tiles_mode = (properties.get("XYZ_TILES_MODE") or "xyz").strip()

            # PNG圧縮レベル(0..9)
            png_compression_level_raw = (properties.get("PNG_COMPRESSION_LEVEL") or "3").strip()
            png_compression_level = self._parse_int_range(
                png_compression_level_raw,
                "PNG_COMPRESSION_LEVEL",
                min_value=0,
                max_value=9,
            )

            # save/encode/none (ベンチやデバッグ用。通常運用はsave)
            save_mode = (properties.get("SAVE_MODE") or "save").strip().lower()
            if save_mode not in ("save", "encode", "none"):
                raise ValueError("SAVE_MODE must be one of: save, encode, none")

            # 出力ファイル数の上限(0なら無制限)
            max_output_files_raw = (properties.get("MAX_OUTPUT_FILES") or "0").strip()
            max_output_files = self._parse_int_range(
                max_output_files_raw,
                "MAX_OUTPUT_FILES",
                min_value=0,
                max_value=10**12,  # 実質上限なし(型チェック目的)
            )

            rotate_0 = self._parse_bool(properties.get("ROTATE_0") or "true", "ROTATE_0")
            rotate_90 = self._parse_bool(properties.get("ROTATE_90") or "false", "ROTATE_90")
            rotate_180 = self._parse_bool(properties.get("ROTATE_180") or "false", "ROTATE_180")
            rotate_270 = self._parse_bool(properties.get("ROTATE_270") or "false", "ROTATE_270")
            mirror_lr = self._parse_bool(properties.get("MIRROR_LR") or "false", "MIRROR_LR")

            # 任意角度の追加回転(0.01度単位: 1度=100)
            # 例: "3000,6050" -> 30.00度, 60.50度
            rotation_angles_raw = (properties.get("ROTATION_ANGLES") or "").strip()
            rotation_angles_x100 = self._parse_rotation_angles_x100(rotation_angles_raw, "ROTATION_ANGLES")

            # 回転は「右方向(時計回り)」を正として扱う
            rotation_x100_fixed = []
            if rotate_0:
                rotation_x100_fixed.append(0)
            if rotate_90:
                rotation_x100_fixed.append(9000)
            if rotate_180:
                rotation_x100_fixed.append(18000)
            if rotate_270:
                rotation_x100_fixed.append(27000)

            if len(rotation_x100_fixed) == 0:
                raise ValueError("At least one rotation must be true (ROTATE_0/90/180/270).")

            # 固定角と任意角が重複する場合は、任意角側を除外する
            rotation_x100_fixed_set = set(rotation_x100_fixed)
            rotation_x100_all = rotation_x100_fixed + [
                a for a in rotation_angles_x100 if a not in rotation_x100_fixed_set
            ]

            # OUTPUT_RESIZED_256 は「別出力」ではなく「通常出力を置き換え」する
            # つまり true のときは、tile_sizeで切ったタイルを 256x256 にしてから保存し、
            # 元のtile_sizeのタイルは保存しない。
            output_resized_256 = self._parse_bool(
                properties.get("OUTPUT_RESIZED_256") or "false",
                "OUTPUT_RESIZED_256",
            )

            # =========================================================
            # 2) 入力チェック
            # =========================================================
            if input_path == "":
                raise ValueError("Input Path is empty")
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"Input file not found: {input_path}")

            if output_base_dir == "":
                raise ValueError("Output Base Directory is empty")

            if tile_size <= 0:
                raise ValueError("TILE_SIZE must be a positive integer")

            # strideはタイルサイズ以下であること (重なりなし～スライディングを許容)
            if stride <= 0 or stride > tile_size:
                raise ValueError("Stride (px) must be in range: 1..tile_size")

            if xyz_tiles_mode not in ("xyz", "tight"):
                raise ValueError("XYZ_TILES_MODE must be one of: xyz, tight")

            in_path = Path(input_path)
            base_dir = Path(output_base_dir)

            # =========================================================
            # 3) 画像を読み込む(OpenCV, 日本語パス対応)
            # =========================================================
            img = self._imread_unicode(in_path)  # BGR(uint8), shape=(H,W,3)
            height, width = img.shape[:2]

            # =========================================================
            # 4) Zを画像サイズから自動決定する(単一Z)
            # =========================================================
            z = self._resolve_zoom_level_auto(width, height, tile_size)

            # =========================================================
            # 5) キャンバスサイズを決定し、必要ならpaddingする
            # =========================================================
            canvas, canvas_w, canvas_h, grid_count_x, grid_count_y, canvas_is_square = (
                self._build_canvas(img, width, height, tile_size, z, xyz_tiles_mode, pad_mode)
            )

            # =========================================================
            # 6) 出力ルートフォルダ(out_root)を作る(manifestなし対策)
            # =========================================================
            out_root_name = (
                f"{in_path.stem}__w{width}__h{height}__z{z}__st{stride}"
                f"__pad{pad_mode}__mode{xyz_tiles_mode}"
            )
            out_root = base_dir / out_root_name

            output_dir_status = "created"
            if out_root.exists():
                if if_output_exists == "overwrite":
                    # overwriteは「既存ファイルを消さずに上書きする」挙動になる。
                    # 余りタイルが残るリスクがあるため、運用上は注意が必要。
                    output_dir_status = "overwrite"
                elif if_output_exists == "create_unique":
                    # create_unique の場合は out_root_name 自体を変える。
                    # (フラット出力の都合で、ファイル名にも out_root_name を埋め込むため)
                    out_root_name = f"{out_root_name}__{uuid.uuid4().hex[:8]}"
                    out_root = base_dir / out_root_name
                    output_dir_status = "created_unique"
                else:
                    raise FileExistsError(f"Output directory already exists: {str(out_root)}")

            out_root.mkdir(parents=True, exist_ok=True)

            # =========================================================
            # 7) start_x/start_yのリストを作る
            # =========================================================
            # canvas_w/canvas_h 上で tile_size x tile_size を切り出せるstart位置を作る。
            # 端まで必ずカバーするため、最後が(last_start)に届いていない場合は追加する。
            starts_x = self._make_starts(canvas_w, stride, tile_size)
            starts_y = self._make_starts(canvas_h, stride, tile_size)

            is_sliding = stride != tile_size

            # =========================================================
            # 8) タイル出力(png固定)
            # =========================================================
            # OpenCVのPNG圧縮(0..9)。数値が大きいほど「小さくなるが遅い」。
            # (benchでも分かる通り、ここは支配的になりやすい)
            encode_params = [int(cv2.IMWRITE_PNG_COMPRESSION), int(png_compression_level)]

            # OUTPUT_RESIZED_256=true の場合、保存される画像の1辺は256になる。
            output_tile_size = self.RESIZED_TILE_SIZE if output_resized_256 else tile_size

            tile_count = 0

            # start座標を走査して切り出し
            for start_y in starts_y:
                for start_x in starts_x:
                    # MAX_OUTPUT_FILES を超えたら打ち切り
                    if max_output_files > 0 and tile_count >= max_output_files:
                        break

                    # タイル番号(x,y): tile_sizeグリッド基準での番号
                    x_idx = start_x // tile_size
                    y_idx = start_y // tile_size

                    # スライディング用のオフセット(px)
                    ox = start_x - (x_idx * tile_size)
                    oy = start_y - (y_idx * tile_size)

                    # tileの切り出し(BGR)
                    # (start_x/start_yはtile_sizeで割り切れない場合があるのでスライスで切る)
                    tile_img = canvas[start_y : start_y + tile_size, start_x : start_x + tile_size].copy()

                    # 回転/反転のバリエーションを出力する
                    for rot_x100 in rotation_x100_all:
                        # ---------------------------------------------
                        # 8-1) 回転
                        # ---------------------------------------------
                        # 回転は「右方向(時計回り)」を正として扱う。
                        # 既存のRotation 0/90/180/270は、ピクセル値を変えないためにcv2.rotateで処理する。
                        if rot_x100 == 0:
                            tile_img_rot = tile_img
                        elif rot_x100 == 9000:
                            # 90度: 時計回り(右回転)
                            tile_img_rot = cv2.rotate(tile_img, cv2.ROTATE_90_CLOCKWISE)
                        elif rot_x100 == 18000:
                            tile_img_rot = cv2.rotate(tile_img, cv2.ROTATE_180)
                        elif rot_x100 == 27000:
                            # 270度(時計回り) = 90度(反時計回り)
                            tile_img_rot = cv2.rotate(tile_img, cv2.ROTATE_90_COUNTERCLOCKWISE)
                        else:
                            # 任意角度は「出力サイズをタイルサイズ固定」にする。
                            # 回転で画像外が入り込む領域は 0(黒) で埋める(固定仕様)。
                            tile_img_rot = self._rotate_image_keep_size(tile_img, rot_x100)

                        # ---------------------------------------------
                        # 8-2) ファイル名を組み立て
                        # ---------------------------------------------
                        # フラット出力:
                        #   out_root/{out_root_name}_{z}_{x}_{y}{suffix}.png
                        base_name = f"{out_root_name}_{z}_{x_idx}_{y_idx}"

                        # スライディング時は ox/oy を付ける(0でも必ず付ける)
                        if is_sliding:
                            base_name = f"{base_name}_ox{ox}_oy{oy}"

                        # 回転(0度はサフィックス無しで互換維持)
                        if rot_x100 != 0:
                            base_name = f"{base_name}_r{self._format_rotation_x100(rot_x100)}"

                        # ---------------------------------------------
                        # 8-3) 通常(反転なし)の出力
                        # ---------------------------------------------
                        tile_to_save = tile_img_rot

                        # OUTPUT_RESIZED_256=true の場合は、保存前に 256x256 にする
                        if output_resized_256 and tile_size != self.RESIZED_TILE_SIZE:
                            tile_to_save = cv2.resize(
                                tile_to_save,
                                (self.RESIZED_TILE_SIZE, self.RESIZED_TILE_SIZE),
                                interpolation=cv2.INTER_AREA,
                            )

                        # 保存処理(save/encode/none)
                        out_path = out_root / f"{base_name}.png"
                        if save_mode == "save":
                            self._imwrite_png_unicode(out_path, tile_to_save, encode_params)
                        elif save_mode == "encode":
                            self._imencode_png(tile_to_save, encode_params)
                        elif save_mode == "none":
                            # 何もしない(ループ性能だけ見たい時)
                            pass
                        else:
                            # ここに来るのはバグ
                            raise ValueError(f"Unknown SAVE_MODE: {save_mode}")

                        tile_count += 1
                        if max_output_files > 0 and tile_count >= max_output_files:
                            break

                        # ---------------------------------------------
                        # 8-4) 左右反転(追加出力)
                        # ---------------------------------------------
                        if mirror_lr:
                            tile_m = cv2.flip(tile_to_save, 1)  # 1: 左右反転

                            out_path = out_root / f"{base_name}_lr.png"
                            if save_mode == "save":
                                self._imwrite_png_unicode(out_path, tile_m, encode_params)
                            elif save_mode == "encode":
                                self._imencode_png(tile_m, encode_params)
                            elif save_mode == "none":
                                pass
                            else:
                                raise ValueError(f"Unknown SAVE_MODE: {save_mode}")

                            tile_count += 1
                            if max_output_files > 0 and tile_count >= max_output_files:
                                break

                    if max_output_files > 0 and tile_count >= max_output_files:
                        break

                if max_output_files > 0 and tile_count >= max_output_files:
                    break

            # =========================================================
            # 9) FlowFile属性の更新(後段が楽になるように必要情報を揃える)
            # =========================================================
            attribute.update(
                {
                    "tile.output_root": str(out_root),
                    "tile.output_root_name": str(out_root_name),
                    "tile.z": str(z),
                    "tile.image_width": str(width),
                    "tile.image_height": str(height),
                    "tile.tile_size": str(output_tile_size),  # 実際に出力されたタイルのサイズ
                    "tile.source_tile_size": str(tile_size),  # 切り出しに使ったタイルサイズ
                    "tile.stride": str(stride),
                    "tile.pad_mode": pad_mode,
                    "tile.output_format": self.OUTPUT_FORMAT,  # 常にpng
                    "tile.is_sliding": "true" if is_sliding else "false",
                    "tile.xyz_tiles_mode": xyz_tiles_mode,
                    "tile.canvas_is_square": "true" if canvas_is_square else "false",
                    "tile.canvas_width": str(canvas_w),
                    "tile.canvas_height": str(canvas_h),
                    "tile.canvas_size": str(max(canvas_w, canvas_h)),  # 互換のため残す(最大辺)
                    "tile.grid_count_x": str(grid_count_x),
                    "tile.grid_count_y": str(grid_count_y),
                    "tile.grid_count": str(max(grid_count_x, grid_count_y)),  # 互換のため残す
                    "tile.start_x_count": str(len(starts_x)),
                    "tile.start_y_count": str(len(starts_y)),
                    "tile.tile_count": str(tile_count),
                    "tile.output_dir_status": output_dir_status,
                    "tile.save_mode": save_mode,
                    "tile.png_compression_level": str(png_compression_level),
                }
            )

            return b"", attribute

        except Exception as e:
            # 失敗時はfailureに流す
            attribute["tile.error"] = str(e)
            raise

    # ---------------------------------------------------------
    # property parser
    # ---------------------------------------------------------
    def _parse_bool(self, v: str, prop_name: str) -> bool:
        """Parse a boolean property.

        このプロセッサのbool系プロパティは、基本的に 'true' / 'false' を想定する。
        それ以外の値は設定ミスとみなして例外にする(曖昧な解釈をしない)。
        """
        s = (v or "").strip().lower()
        if s == "true":
            return True
        if s == "false":
            return False
        raise ValueError(f"{prop_name} must be 'true' or 'false'")

    def _parse_int_range(self, v: str, prop_name: str, min_value: int, max_value: int) -> int:
        """Parse an integer property with range validation."""
        try:
            n = int(v)
        except Exception:
            raise ValueError(f"{prop_name} must be an integer: value={v}")

        if n < min_value or n > max_value:
            raise ValueError(f"{prop_name} out of range: {min_value}..{max_value} value={n}")
        return n

    def _parse_rotation_angles_x100(self, v: str, prop_name: str) -> List[int]:
        """Parse rotation angle list (0.01deg unit) from comma-separated string.

        仕様:
        - カンマ区切りの整数リストを受け付ける
        - 1度=100(0.01度単位)
        - 許容範囲は 1..35999 (0と36000は受け付けない)
        - 重複は排除(入力順は維持)
        """
        s = (v or "").strip()
        if s == "":
            return []

        angles: List[int] = []
        seen = set()

        for part in s.split(","):
            p = part.strip()
            if p == "":
                continue

            try:
                a = int(p)
            except Exception:
                raise ValueError(f"{prop_name} must be comma-separated integers: value={v}")

            if a < 1 or a > 35999:
                raise ValueError(f"{prop_name} out of range: 1..35999 value={a}")

            if a in seen:
                continue
            seen.add(a)
            angles.append(a)

        return angles

    def _format_rotation_x100(self, rot_x100: int) -> str:
        """Format rotation (0.01deg unit) for filename suffix.

        例:
        - 3000 -> "30"
        - 6050 -> "60p50"
        - 5    -> "0p05"
        """
        deg = rot_x100 // 100
        frac = rot_x100 % 100

        if frac == 0:
            return str(deg)

        return f"{deg}p{frac:02d}"

    def _rotate_image_keep_size(self, img: "np.ndarray", rot_x100: int) -> "np.ndarray":
        """Rotate image by arbitrary angle, keeping output size unchanged.

        - 回転中心: 画像中心
        - 出力サイズ: 入力と同じ
        - 回転方向: 右方向(時計回り)を正とする
        - 画像外が入り込む領域: 0で埋める(固定仕様)
        """
        h, w = img.shape[:2]

        # 0.01度 -> 度(float)
        angle_deg = rot_x100 / 100.0

        # OpenCVは正の角度が反時計回りなので、時計回りにしたい場合は符号を反転する
        center = (w / 2.0, h / 2.0)
        m = cv2.getRotationMatrix2D(center, -angle_deg, 1.0)

        # ここは画像(特にマスク)の値を変えたくないので、補間はNEARESTに寄せる
        rotated = cv2.warpAffine(
            img,
            m,
            (w, h),
            flags=cv2.INTER_NEAREST,
            borderMode=cv2.BORDER_CONSTANT,
            borderValue=0,
        )
        return rotated

    # ---------------------------------------------------------
    # zoom level
    # ---------------------------------------------------------
    def _resolve_zoom_level_auto(self, width: int, height: int, tile_size: int) -> int:
        """
        画像サイズからZ(zoom level)を自動決定する。

        ルール:
        - M = max(W,H)
        - M <= tile_size なら Z = 0
        - それ以外は Z = ceil(log2(M / tile_size))

        :param width: 元画像幅W
        :param height: 元画像高さH
        :return: Z(0以上)
        """
        max_side = max(width, height)

        if max_side <= tile_size:
            return 0

        ratio = max_side / float(tile_size)
        z = int(math.ceil(math.log(ratio, 2)))
        return max(0, z)

    # ---------------------------------------------------------
    # canvas / padding
    # ---------------------------------------------------------
    def _build_canvas(
            self,
            img: "np.ndarray",
            width: int,
            height: int,
            tile_size: int,
            z: int,
            xyz_tiles_mode: str,
            pad_mode: str,
    ) -> Tuple["np.ndarray", int, int, int, int, bool]:
        """
        XYZ_TILES_MODE に応じて、キャンバスを作る。

        戻り値:
          - canvas: BGR画像(ndarray)
          - canvas_w/canvas_h: キャンバスの幅/高さ
          - grid_count_x/grid_count_y: tile_size単位での枚数(切り上げ)
          - canvas_is_square: 正方形かどうか
        """
        if xyz_tiles_mode == "xyz":
            # 正方形キャンバス: S = tile_size * 2^Z
            grid_count = 2 ** z
            canvas_size = tile_size * grid_count
            canvas = self._pad_to_canvas(img, canvas_size, canvas_size, pad_mode)
            return canvas, canvas_size, canvas_size, grid_count, grid_count, True

        # tight: 必要最小pad (正方形を強制しない)
        #
        # 重要:
        #   width/height が tile_size の倍数でない場合、canvas_w/canvas_h を画像サイズのままにすると
        #   _make_starts() が最後に「last_start = axis_size - tile_size」を追加し、
        #   start_x/start_y が tile_size の倍数でない値になることがある。
        #
        #   stride == tile_size (is_sliding == false) の場合、ファイル名に ox/oy が付かないため、
        #   x_idx = start_x // tile_size が既存タイルと衝突して上書きが発生し、結果として端の領域が欠ける。
        #
        # 対策:
        #   tightモードでも右端/下端の「切れ端」部分だけ pad し、キャンバスサイズを tile_size の倍数に丸める。
        #   これにより start_x/start_y は tile_size 境界に揃い、上書き衝突が起きない。
        grid_count_x = int(math.ceil(width / float(tile_size)))
        grid_count_y = int(math.ceil(height / float(tile_size)))

        canvas_w = max(tile_size, grid_count_x * tile_size)
        canvas_h = max(tile_size, grid_count_y * tile_size)

        canvas = self._pad_to_canvas(img, canvas_w, canvas_h, pad_mode)
        return canvas, canvas_w, canvas_h, grid_count_x, grid_count_y, False

    def _pad_to_canvas(self, img: "np.ndarray", canvas_w: int, canvas_h: int, pad_mode: str) -> "np.ndarray":
        """
        元画像を右端/下端を埋めて canvas_h x canvas_w にする。
        左上(0,0)に元画像を置き、足りない部分だけpadする。

        pad_mode:
          - zero: 黒で埋める
          - edge: 端の画素を伸ばす
          - reflect: 反射(鏡)で埋める
            幅または高さが1pxの場合はreflectに制約があるのでedgeにフォールバックする
        """
        h, w = img.shape[:2]

        if w > canvas_w or h > canvas_h:
            raise ValueError(
                f"Image larger than canvas. image=({w},{h}) canvas=({canvas_w},{canvas_h})"
            )

        pad_right = canvas_w - w
        pad_bottom = canvas_h - h

        # 既に指定サイズならそのまま返す
        if pad_right == 0 and pad_bottom == 0:
            return img

        pad_spec = ((0, pad_bottom), (0, pad_right), (0, 0))  # (H, W, C)

        if pad_mode == "zero":
            padded = np.pad(img, pad_spec, mode="constant", constant_values=0)
        elif pad_mode == "edge":
            padded = np.pad(img, pad_spec, mode="edge")
        elif pad_mode == "reflect":
            # reflectは寸法1の軸でエラーになりやすいので安全側へ
            if w <= 1 or h <= 1:
                padded = np.pad(img, pad_spec, mode="edge")
            else:
                padded = np.pad(img, pad_spec, mode="reflect")
        else:
            raise ValueError(f"Unknown Padding Mode: {pad_mode}")

        return padded

    # ---------------------------------------------------------
    # start positions
    # ---------------------------------------------------------
    def _make_starts(self, axis_size: int, stride: int, tile_size: int) -> List[int]:
        """
        axis_size幅(または高さ)のキャンバス上で、tile_sizeを切り出せるstart位置を生成する。

        - startは0から始める
        - range(0, last_start+1, stride)で基本列を作る
        - 最後がlast_startでなければlast_startを追加して端まで確実にカバーする

        :param axis_size: キャンバスの幅または高さ
        :param stride: stride(1..tile_size)
        :param tile_size: タイル1辺(px)
        :return: start位置のリスト
        """
        last_start = axis_size - tile_size
        if last_start < 0:
            # axis_size < tile_size の場合は、padでtile_sizeにしているはずだが保険
            return [0]

        starts = list(range(0, last_start + 1, stride))
        if len(starts) == 0:
            starts = [0]

        # 端までカバーするため、最後がlast_startでなければ追加する
        if starts[-1] != last_start:
            starts.append(last_start)

        return starts

    # ---------------------------------------------------------
    # Unicode-safe imread/imwrite
    # ---------------------------------------------------------
    def _imread_unicode(self, path: Path) -> "np.ndarray":
        """
        日本語パスでも確実に読めるimread。

        OpenCVのcv2.imreadは環境によってUnicodeパスで失敗する。
        そのため、Pythonのファイル読み込み + cv2.imdecode で統一する。
        """
        data = np.fromfile(str(path), dtype=np.uint8)
        img = cv2.imdecode(data, cv2.IMREAD_COLOR)
        if img is None:
            raise ValueError(f"Failed to read image by imdecode: {str(path)}")
        return img

    def _imencode_png(self, img: "np.ndarray", encode_params: List[int]) -> bytes:
        """PNGエンコードのみ行い、bytesを返す(保存しない)。"""
        ok, buf = cv2.imencode(".png", img, encode_params)
        if not ok:
            raise ValueError("cv2.imencode failed")
        return buf.tobytes()

    def _imwrite_png_unicode(self, path: Path, img: "np.ndarray", encode_params: List[int]) -> None:
        """
        日本語パスでも確実に書けるPNG保存。

        OpenCVのcv2.imwriteは環境によってUnicodeパスで失敗する。
        そのため、cv2.imencode + pathlib.Path.write_bytes で統一する。
        """
        bs = self._imencode_png(img, encode_params)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(bs)
