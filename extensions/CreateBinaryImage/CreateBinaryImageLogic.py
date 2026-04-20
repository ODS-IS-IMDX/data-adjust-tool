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

# -*- coding: utf-8 -*-
"""
CreateBinaryImageLogic.py

目的:
    RGB画像(ndarray)に対して、指定RGB±toleranceに一致する画素を白(255)、
    それ以外を黒(0)にした2値画像(マスク)を作成する。

前提(ここが今回の変更点):
    - FieldSetFile(CSV)の生成/展開は common側(Converter/Executor)がやる
    - このロジックに渡ってくる入力は、以下のどちらかだけを想定する
        A) pandas.Series: FieldSetFileを展開した「1行」(列にpickle bytesが入っている)
        B) bytes: pickle化された numpy.ndarray (ConvertImageToNumpy等の出力)

重要:
    - ピクセル数(H,W)は絶対に変更しない
      (リサイズ/回転/クロップ/パディングは一切しない)
    - 出力は pickle化numpy.ndarray(bytes)
      shape=(H,W), dtype=uint8, values=0 or 255

使い方(Series入力時):
    - properties['fsf_image_src'] で入力画像列名を指定 (既定: 'content')
    - properties['fsf_image_dst'] で出力マスク列名を指定 (既定: 'content')
    - 出力列に mask(bytes) を入れ、'color_space' を 'BINARY' に更新する
"""

from __future__ import annotations

import io
import pickle
from typing import Tuple

import numpy as np
from PIL import Image

# ImageColorExtractionLogic と同じ「Seriesが来る前提」の基底クラス
from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic

# pandasは環境によって import_module にしている実装が多いが、
# このロジック内では型判定にしか使わないため、ここは通常importで書く
import pandas as pd


class CreateBinaryImageLogic(FlexibleRasterVectorLogic):
    """
    CreateBinaryImageLogic
    """

    def __init__(self):
        pass

    def input_check(self, byte_data, attribute):
        """
        入力データと属性の妥当性を確認する。

        - byte_data が None の場合はエラー
        - attribute に ColorSpace が無い場合はエラー
          (このフレームワークでは attribute['ColorSpace'] が前提になりがちなので合わせる)
        """
        if byte_data is None:
            raise Exception("入力データが設定されていません")

        if not attribute or attribute.get("ColorSpace") is None:
            raise Exception("attributeにColorSpaceが設定されていません")

    def __call__(self, byte_data, attribute, properties) -> Tuple[object, dict]:
        """
        Parameters
        ----------
        byte_data : pandas.Series or bytes
            A) pandas.Series の場合: FieldSetFile展開後の1行。画像は任意列(既定 'content')にpickle bytesで入っている想定。
            B) bytes の場合: pickle化 numpy.ndarray の想定。
        attribute : dict
            属性情報。ColorSpace を参照して必要ならBGR->RGBの補正を行う。
        properties : dict
            - target_rgb: "R,G,B" (既定 "0,0,255")
            - tolerance: 0-255 (既定 20)
            - fsf_image_src: 入力画像列名 (Series入力時のみ使用、既定 'content')
            - fsf_image_dst: 出力マスク列名 (Series入力時のみ使用、既定 'content')

        Returns
        -------
        (new_byte_data, attribute)
            - Series入力時: new_byte_data は pandas.Series(入力行を更新して返す)
            - bytes入力時 : new_byte_data は pickle化mask bytes
        """
        self.input_check(byte_data, attribute)

        # =========================================================
        # 1) プロパティ取得
        #    ※既存コードは "TARGET_RGB"/"TOLERANCE" 前提だったが、
        #      他ロジックに合わせて snake_case を基本にしつつ、互換で大文字も拾う
        # =========================================================
        target_rgb_str = (
            (properties.get("target_rgb") or properties.get("TARGET_RGB") or "0,0,255")
        ).strip()

        tol_raw = (properties.get("tolerance") or properties.get("TOLERANCE") or "20")
        tol = self._clamp_int(tol_raw, 0, 255, default=20)

        r, g, b = self._parse_rgb_csv(target_rgb_str)

        # =========================================================
        # 2) 入力からRGB ndarray(H,W,3)を復元
        #    - Seriesなら指定列から取り出す
        #    - bytesならそのままpickle.loads
        # =========================================================
        rgb = self._decode_input_to_rgb_ndarray(byte_data, attribute, properties)

            # =========================================================
        # 3) マスク生成: 一致=255, 不一致=0
            # =========================================================
        mask = self._make_binary_mask(rgb, r=r, g=g, b=b, tol=tol)

            # =========================================================
        # 4) 出力: pickle化ndarray bytes
            # =========================================================
        out_bytes = pickle.dumps(mask, protocol=pickle.HIGHEST_PROTOCOL)

            # =========================================================
        # 5) Series入力の場合は「行に書き戻して返す」
        #    bytes入力の場合は bytesをそのまま返す
            # =========================================================
        new_color_space = "BINARY"

        if isinstance(byte_data, pd.core.series.Series):
            # どの列に出力するか(既定はcontent上書き)
            fsf_image_dst = properties.get("fsf_image_dst", "content")

            # 出力マスクを書き込み
            byte_data[fsf_image_dst] = out_bytes

            # 色空間列も更新(この列名は既存ロジック群に合わせて 'color_space' を採用)
            byte_data["color_space"] = new_color_space

            new_byte_data = byte_data
        else:
            new_byte_data = out_bytes

            # =========================================================
        # 6) attribute更新
            # =========================================================
        attribute["ColorSpace"] = new_color_space
        attribute["BinaryTargetRGB"] = f"{r},{g},{b}"
        attribute["BinaryTolerance"] = str(tol)

        return new_byte_data, attribute

    # ---------------------------------------------------------------------
    # 入力デコード系
    # ---------------------------------------------------------------------
    def _decode_input_to_rgb_ndarray(self, byte_data, attribute: dict, properties: dict) -> np.ndarray:
        """
        Series/bytes どちらの入力でも、最終的にRGB ndarray(H,W,3)へ統一する。

        重要:
            ピクセル数(H,W)は一切変えない。
        """
        # Series入力: 指定列(既定 content)から画像pickle bytesを取り出す
        if isinstance(byte_data, pd.core.series.Series):
            fsf_image_src = properties.get("fsf_image_src", "content")
            try:
                payload = byte_data[fsf_image_src]
            except Exception:
                raise ValueError(f"fsf_image_srcに無効な値が設定されています: {fsf_image_src}")
        else:
            payload = byte_data

        if payload is None or (isinstance(payload, (bytes, bytearray)) and len(payload) == 0):
            raise ValueError("Input payload is empty")

        # まずpickle.loadsを試す(通常は ndarray が返る想定)
        try:
            obj = pickle.loads(payload)
        except Exception:
            obj = None

        # (A) pickle.loadsでndarrayが取れたケース
        if isinstance(obj, np.ndarray):
            return self._ensure_rgb_from_ndarray(obj, attribute)

        # (B) pickle.loadsで bytes が取れたケース(=「画像ファイルbytesをpickle化して持っていた」等)
        if isinstance(obj, (bytes, bytearray)):
            return self._decode_image_bytes_to_rgb(bytes(obj))

        # (C) pickle.loadsできなかったケース
        # ここは「生の画像ファイルbytes」を直接受け取った可能性があるので、PILで開く
        # (Series入力の想定では普通起きないが、保険として入れておく)
        if isinstance(payload, (bytes, bytearray)):
            return self._decode_image_bytes_to_rgb(bytes(payload))

        raise ValueError(f"Unsupported payload type: {type(payload)}")

    def _decode_image_bytes_to_rgb(self, img_bytes: bytes) -> np.ndarray:
        """
        画像ファイルbytes(PNG/JPEG等)をRGB ndarray(H,W,3)へ変換する。
        サイズは変えず、チャンネル数だけ揃える。
        """
        im = Image.open(io.BytesIO(img_bytes))
        im_rgb = im.convert("RGB")  # サイズは維持される。チャンネルだけ3に揃える。
        rgb = np.array(im_rgb, dtype=np.uint8)
        return rgb

    def _ensure_rgb_from_ndarray(self, arr: np.ndarray, attribute: dict) -> np.ndarray:
        """
        ndarray入力をRGB ndarray(H,W,3)へ正規化する。

        想定:
            - (H,W,3) : RGB or BGR
            - (H,W,4) : RGBA
            - (H,W)   : GRAYSCALE/BINARY

        attribute['ColorSpace'] が 'BGR' の場合は、RGBに並び替える。
        """
        if arr.ndim == 2:
            # (H,W) -> (H,W,3) に拡張(ピクセル数は維持)
            a = arr.astype(np.uint8, copy=False)
            rgb = np.stack([a, a, a], axis=2)
            return rgb

        if arr.ndim == 3:
            if arr.shape[2] == 3:
                rgb = arr.astype(np.uint8, copy=False)
                cs = (attribute.get("ColorSpace") or "").upper()
                if cs == "BGR":
                    rgb = rgb[:, :, ::-1]
                return rgb

            if arr.shape[2] == 4:
                rgb = arr[:, :, 0:3].astype(np.uint8, copy=False)
                cs = (attribute.get("ColorSpace") or "").upper()
                if cs == "BGR":
                    rgb = rgb[:, :, ::-1]
        return rgb

        raise ValueError(f"Unsupported ndarray shape: {arr.shape}")

    # ---------------------------------------------------------------------
    # マスク生成系
    # ---------------------------------------------------------------------
    def _make_binary_mask(self, rgb: np.ndarray, r: int, g: int, b: int, tol: int) -> np.ndarray:
        """
        指定RGB±tolに一致する画素を白(255)、それ以外を黒(0)にする2値マスクを作る。

        出力:
            shape=(H,W), dtype=uint8, values=0 or 255
        """
        # uint8のまま引き算/比較をするとアンダーフローが起きるので int16 に上げる
        rr = rgb[:, :, 0].astype(np.int16)
        gg = rgb[:, :, 1].astype(np.int16)
        bb = rgb[:, :, 2].astype(np.int16)

        r_min, r_max = r - tol, r + tol
        g_min, g_max = g - tol, g + tol
        b_min, b_max = b - tol, b + tol

        cond = (
            (rr >= r_min) & (rr <= r_max) &
            (gg >= g_min) & (gg <= g_max) &
            (bb >= b_min) & (bb <= b_max)
        )

        mask = (cond.astype(np.uint8) * 255)
        return mask

    # ---------------------------------------------------------------------
    # ユーティリティ
    # ---------------------------------------------------------------------
    def _parse_rgb_csv(self, s: str) -> Tuple[int, int, int]:
        """
        "R,G,B" を (r,g,b) intへ変換する。
        """
        parts = [p.strip() for p in s.split(",")]
        if len(parts) != 3:
            raise ValueError(f"target_rgb must be 'R,G,B'. got: {s}")

        r = self._clamp_int(parts[0], 0, 255, default=0)
        g = self._clamp_int(parts[1], 0, 255, default=0)
        b = self._clamp_int(parts[2], 0, 255, default=0)
        return r, g, b

    def _clamp_int(self, v, min_v: int, max_v: int, default: int = 0) -> int:
        """
        int変換して範囲クランプする。
        """
        try:
            x = int(str(v).strip())
        except Exception:
            x = default

        if x < min_v:
            x = min_v
        if x > max_v:
            x = max_v
        return x
