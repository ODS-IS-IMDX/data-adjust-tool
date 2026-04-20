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

from pathlib import Path
from typing import Dict, Tuple, Optional, List

from PIL import Image


class ImageTileMergeLogic:
    """
    ImageTileMergeLogic

    ■要点
    - XYZ Tiles(単一Z)を貼り付けて1枚のpngにする
    - 分割時パディング領域は、元画像サイズ(W,H)でクロップして落とす
      -> 出力サイズが元画像と完全一致になる

    ■重要制約
    - スライディング由来(_ox/_oy)が混ざっていたらエラー
    - 出力はpng固定
    """


    def __init__(self):
        pass

    # ---------------------------------------------------------
    # 元サイズ取得
    # ---------------------------------------------------------
    def _get_original_size_value(
        self,
        attribute: dict,
        properties: dict,
        prop_key: str,
        attr_key: str,
    ) -> Optional[int]:
        """
        元サイズ(W/H)を安全に取得する。

        優先順位:
        1) FlowFile属性 attr_key があればそれ
        2) プロパティ prop_key の値があればそれ
        3) どちらも無ければNone
        """
        # 1) attribute優先
        if attr_key in attribute:
            v = str(attribute.get(attr_key) or "").strip()
            if v != "":
                try:
                    return int(v)
                except Exception:
                    raise ValueError(f"FlowFile attribute {attr_key} is not int: {v}")

        # 2) property
        v2 = "" if properties.get(prop_key) == "-1" else properties.get(prop_key).strip()
        if v2 != "":
            try:
                return int(v2)
            except Exception:
                raise ValueError(f"Property {prop_key} is not int: {v2}")

        return None

    # ---------------------------------------------------------
    # タイルサイズ検出
    # ---------------------------------------------------------
    def _detect_tile_size_xyz(self, input_dir: Path, merge_key:str) -> Optional[int]:
        """
        input_dir配下を走査し、対象タイルpngの実サイズからタイルサイズを決定する。

        追加ルール:
        - "_ox" or "_oy" が含まれたらスライディング由来として即エラー
        - タイルは正方形であること
        """
        input_dir = Path(input_dir)
        for f in input_dir.iterdir():
            if not f.is_file():
                continue
            
            # merge_key がファイル名に含まれていなければスキップ
            if merge_key not in f.name:
                continue

            # png固定(要件)
            if f.suffix.lower() != ".png":
                continue

            # スライディング由来の検出(要件)
            name = f.name
            if ("_ox" in name) or ("_oy" in name):
                raise ValueError(
                    f"Sliding-window tile detected (not supported for merge): {str(f)}"
                )

            # 拡張子除去
            stem = f.stem
            parts = stem.split("_")[-2:]
            # _z_x_y が取れないものは無視
            if len(parts) != 2 or not all(p.isdigit() for p in parts):
                continue

            with Image.open(f) as img:
                w, h = img.size
                if w != h:
                    raise ValueError(
                        f"Tile must be square. detected: {str(f)} size={img.size}"
                    )
                if w <= 0:
                    raise ValueError(
                        f"Tile size must be > 0. detected: {str(f)} size={img.size}"
                    )
                return w

        return None

    # ---------------------------------------------------------
    # タイル走査
    # ---------------------------------------------------------
    def _scan_tiles_xyz(self, input_dir: Path, tile_size: int, merge_key:str) -> Tuple[Dict[Tuple[int, int], Path], int, int, int]:
        """
        input_dir配下を走査し、(x,y,z)がファイル名に含まれるtile_path を収集する。

        追加ルール:
        - "_ox" or "_oy" が含まれたらスライディング由来として即エラー

        """
        tile_map: Dict[Tuple[int, int], Path] = {}
        max_x = -1
        max_y = -1
        found_count = 0

        input_dir = Path(input_dir)
        for f in input_dir.iterdir():
            if not f.is_file():
                continue
            
            # merge_key がファイル名に含まれていなければスキップ
            if merge_key not in f.name:
                continue

            # png固定(要件)
            if f.suffix.lower() != ".png":
                continue

            # スライディング由来の検出(要件)
            name = f.name
            if ("_ox" in name) or ("_oy" in name):
                raise ValueError(
                    f"Sliding-window tile detected (not supported for merge): {str(f)}"
                )
                
            # 拡張子除去
            stem = f.stem
            parts = stem.split("_")[-2:]
            # _z_x_y が取れないものは無視
            if len(parts) != 2 or not all(p.isdigit() for p in parts):
                continue

            x, y = map(int, parts)

            # サイズチェック
            with Image.open(f) as img:
                if img.size != (tile_size, tile_size):
                    raise ValueError(
                        f"Tile size is not {tile_size}x{tile_size}: "
                        f"{str(f)} size={img.size}"
                    )

            tile_map[(x, y)] = f
            found_count += 1
            max_x = max(max_x, x)
            max_y = max(max_y, y)

        return tile_map, max_x, max_y, found_count

    # ---------------------------------------------------------
    # 背景色
    # ---------------------------------------------------------
    def _default_background_for_mode(self, mode: str):
        """
        モード別の背景値。
        二値マスクならL(0)が自然、RGBAなら透明が自然、RGBなら黒が自然、という感じ。
        """
        if mode == "RGB":
            return (0, 0, 0)
        if mode == "RGBA":
            return (0, 0, 0, 0)
        if mode == "L":
            return 0
        return 0
    
    
    def __call__(self, byte_data: bytes, attribute: dict, properties: dict):
        """
        :param byte_data: FlowFile本文(未使用)
        :param attribute: FlowFile属性
        :param properties: プロセッサプロパティ
        :return: (空bytes, 更新属性)
        """
        try:
            # =========================================================
            # 1) プロパティ取得
            # =========================================================
            input_base_dir = (properties.get("INPUT_BASE_DIR") or "").strip()
            merge_key = (properties.get("MERGE_KEY") or "").strip()

            tile_size_raw = attribute.get("tile.tile_size") or ""
            tile_size_prop_raw = (properties.get("TILE_SIZE") or "").strip()
            auto_detect_tile_size = str(properties.get("AUTO_DETECT_TILE_SIZE") or "True").lower() == "true"

            tile_size: Optional[int] = None
            if tile_size_raw != "":
                try:
                    tile_size = int(tile_size_raw)
                except Exception:
                    raise ValueError(f"tile.tile_size is not int: {tile_size_raw}")
            else:
                v2 = "" if tile_size_prop_raw == "-1" else tile_size_prop_raw
                if v2 != "":
                    try:
                        tile_size = int(v2)
                    except Exception:
                        raise ValueError(f"Property TILE_SIZE is not int: {v2}")

            # 元サイズ: まずFlowFile属性(tile.image_width/height)を優先し、
            # それが無い場合だけプロパティ値を採用する
            orig_w = self._get_original_size_value(
                attribute=attribute,
                properties=properties,
                prop_key="ORIGINAL_WIDTH",
                attr_key="tile.image_width",
            )
            orig_h = self._get_original_size_value(
                attribute=attribute,
                properties=properties,
                prop_key="ORIGINAL_HEIGHT",
                attr_key="tile.image_height",
            )

            output_path = (properties.get("OUTPUT_PATH") or "").strip()
            overwrite = str(properties.get("OVERWRITE") or "True").lower() == "true"
            missing_policy = (properties.get("MISSING_TILE_POLICY") or "fail").strip()
            output_color_mode = (properties.get("OUTPUT_COLOR_MODE") or "keep").strip()

            # =========================================================
            # 2) 入力チェック
            # =========================================================
            if input_base_dir == "":
                raise ValueError("Input Base Directory is empty")

            base_dir = Path(input_base_dir)
            if not base_dir.exists():
                raise FileNotFoundError(f"Input Base Directory not found: {str(base_dir)}")
            if not base_dir.is_dir():
                raise ValueError(f"Input Base Directory is not a directory: {str(base_dir)}")

            if output_path == "":
                raise ValueError("Output Path is empty")

            out_path = Path(output_path)
            if out_path.exists() and (not overwrite):
                raise FileExistsError(f"Output already exists and Overwrite=False: {str(out_path)}")

            if missing_policy not in ("fail", "blank"):
                raise ValueError(f"Missing Tile Policy must be fail or blank: {missing_policy}")

            if output_color_mode not in ("keep", "RGB", "L"):
                raise ValueError(f"Output Color Mode must be keep/RGB/L: {output_color_mode}")

            # 元サイズは必須
            if orig_w is None or orig_h is None:
                raise ValueError(
                    "Original image size (W,H) is required to crop padded area. "
                    "Please provide tile.image_width/tile.image_height attributes or set properties "
                    "'Original Image Width (px)' and 'Original Image Height (px)'."
                )

            if orig_w <= 0 or orig_h <= 0:
                raise ValueError(f"Original image size must be > 0. given: W={orig_w}, H={orig_h}")

            if tile_size is None:
                if auto_detect_tile_size:
                    tile_size_detected = self._detect_tile_size_xyz(base_dir, merge_key)
                    if tile_size_detected is None:
                        tile_size = 256
                    else:
                        tile_size = tile_size_detected
                else:
                    tile_size = 256

            if tile_size <= 0:
                raise ValueError(f"Tile size must be > 0. given: tile_size={tile_size}")

            # =========================================================
            # 4) タイル一覧を収集
            # =========================================================
            tile_map, max_x, max_y, found_count = self._scan_tiles_xyz(input_base_dir, tile_size, merge_key)

            if found_count == 0:
                raise ValueError(f"No tiles found under: {str(input_base_dir)}")

            # =========================================================
            # 5) クロップ前キャンバスサイズを決定
            #    - 今回は「存在するmax_x/max_y」から決める(必要最小)
            # =========================================================
            tiles_x_count = max_x + 1
            tiles_y_count = max_y + 1
            canvas_w = tiles_x_count * tile_size
            canvas_h = tiles_y_count * tile_size

            # 元画像サイズがキャンバスより大きいのは破綻なので止める
            if orig_w > canvas_w or orig_h > canvas_h:
                raise ValueError(
                    f"Original size is larger than merged canvas. "
                    f"orig(W,H)=({orig_w},{orig_h}) canvas(W,H)=({canvas_w},{canvas_h}). "
                    f"Tiles are insufficient or Z mismatch."
                )

            # =========================================================
            # 6) 出力モードを決めてキャンバスを作る
            # =========================================================
            first_tile_path = next(iter(tile_map.values()))
            with Image.open(first_tile_path) as first_img:
                first_mode = first_img.mode

            if output_color_mode == "keep":
                canvas_mode = first_mode
            else:
                canvas_mode = output_color_mode

            canvas_bg = self._default_background_for_mode(canvas_mode)
            canvas = Image.new(canvas_mode, (canvas_w, canvas_h), canvas_bg)

            # =========================================================
            # 7) 欠損チェック(必要なら)
            #    - 元画像範囲に関係あるグリッドだけチェックする
            #      (orig_w/orig_hまでを覆う最小タイル数)
            # =========================================================
            need_x = (orig_w + tile_size - 1) // tile_size
            need_y = (orig_h + tile_size - 1) // tile_size

            missing_count = 0
            for y in range(0, need_y):
                for x in range(0, need_x):
                    if (x, y) not in tile_map:
                        missing_count += 1

            if missing_count > 0 and missing_policy == "fail":
                raise ValueError(
                    f"Missing tiles detected in original area: missing={missing_count}, policy=fail. "
                    f"(needed grid: x=0..{need_x-1}, y=0..{need_y-1})"
                )

            # =========================================================
            # 8) タイル貼り付け
            # =========================================================
            for (x, y), tile_path in tile_map.items():
                paste_x = x * tile_size
                paste_y = y * tile_size

                # 元画像に関係ない右/下の余白側タイルがあっても貼ってOK
                # 最後にクロップで切り落とす
                with Image.open(tile_path) as tile_img:
                    if tile_img.size != (tile_size, tile_size):
                        raise ValueError(
                            f"Tile size is not {tile_size}x{tile_size}: "
                            f"{str(tile_path)} size={tile_img.size}"
                        )

                    if tile_img.mode != canvas_mode:
                        tile_img = tile_img.convert(canvas_mode)

                    canvas.paste(tile_img, (paste_x, paste_y))

            # =========================================================
            # 9) パディング領域を削除(ここが今回の本体)
            # =========================================================
            final_img = canvas.crop((0, 0, orig_w, orig_h))

            # =========================================================
            # 10) 出力保存(png固定)
            # =========================================================
            out_path.parent.mkdir(parents=True, exist_ok=True)
            final_img.save(out_path, format="PNG")

            # =========================================================
            # 11) 属性更新
            # =========================================================
            attribute.update(
                {
                    "merge.input_base_dir": str(base_dir),
                    "merge.tile_size": str(tile_size),
                    "merge.max_x": str(max_x),
                    "merge.max_y": str(max_y),
                    "merge.canvas_width": str(canvas_w),
                    "merge.canvas_height": str(canvas_h),
                    "merge.original_width": str(orig_w),
                    "merge.original_height": str(orig_h),
                    "merge.output_width": str(orig_w),
                    "merge.output_height": str(orig_h),
                    "merge.tile_count_found": str(found_count),
                    "merge.missing_tile_count": str(missing_count),
                    "merge.output_path": str(out_path),
                }
            )

            return b"", attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
