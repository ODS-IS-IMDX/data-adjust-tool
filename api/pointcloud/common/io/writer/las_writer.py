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

# api/pointcloud/common/io/writer/las_writer.py
import os, math
import numpy as np
import laspy
from pyproj import CRS
from pointcloud.common.io.writer.base_writer import IPointCloudWriter
from pointcloud.common.converter_utils import is_geographic

class LasPointCloudWriter(IPointCloudWriter):
    """
    LAS/LAZ 書き出しライタ。
    - 小規模: write() 一発
    - 大規模: open()/write_points()/close() の逐次処理
    - version / PDRF は「ユーザ指定を最優先」。未指定時のみ列数から推測する。
    """

    def __init__(self, epsg: int | None = None, scale: float = 0.001, to_laz: bool = False):
        self.epsg = epsg
        self.scale = scale
        self.to_laz = to_laz
        self._writer = None
        self.header = None
        self._last_version: str | None = None
        self._last_pdrf: int | None = None

    def _auto_scales_by_crs(self, epsg: int):
        """
        EPSGに応じてXY/Zそれぞれのスケールを自動推定する。
        - 地理座標系: XYは度、Zはm
        - 投影座標系: XY/Zともm
        """
        # EPSGから一旦CRSを作る（例外は握りつぶしてNoneにする）
        try:
            crs_obj = CRS.from_epsg(int(epsg))
        except Exception:
            crs_obj = None

        # converter_utils.is_geographic で堅牢に判定（CRS優先、ダメならEPSGフォールバック）
        geo = is_geographic(crs_obj=crs_obj, epsg_guess=epsg)

        # 判定結果でスケールを分岐
        if geo:
            # 地理座標系 → XY: degree, Z: meter
            return [1e-7, 1e-7, 0.001]
        else:
            # 投影座標系 → 全軸 meter
            return [0.001, 0.001, 0.001]

    # =========================================================
    # 一括書き出しAPI（旧仕様互換）
    # =========================================================
    def write(self,
            path: str,
            array: np.ndarray,
            epsg: int | None = None,
            version: str | None = None,
            pdrf_id: int | None = None,
            src_header: laspy.LasHeader | None = None) -> None:
        """
        メモリ上の ndarray 全体を LAS/LAZ に書き出す（ワンショット）。
        src_header が与えられた場合は、元LASのヘッダを完全に引き継ぐ。
        """
        path = self._decide_path(path)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        # ---- 元ヘッダを引き継ぐ場合 ----
        if src_header is not None:
            # copy() により完全複製（version, pdrf, vlr, scale, offset, epsg 含む）
            hdr = src_header.copy()
            self.header = hdr
        else:
            # 通常の新規生成ルート
            self._prepare_header(array, epsg=epsg, version=version, pdrf_id=pdrf_id)
            hdr = self.header

        # ---- 書き込み ----
        with laspy.open(path, mode="w", header=hdr) as writer:
            rec = self._make_record(array, hdr)
            writer.write_points(rec)


    # =========================================================
    # ストリーム書き出しAPI（NiFi対応）
    # =========================================================
    def open(self,
             path: str,
             epsg: int | None = None,
             init_points: np.ndarray | None = None,
             version: str | None = None,
             pdrf_id: int | None = None):
        """
        NiFiストリーム向け: 最初のチャンクからヘッダを確定して open。
        version/pdrf_id が与えられたら **最優先** で採用。未指定時のみ列数から推測する。
        """
        path = self._decide_path(path)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._prepare_header(init_points, epsg=epsg, version=version, pdrf_id=pdrf_id)
        self._writer = laspy.open(path, mode="w", header=self.header)
        return self

    def write_points(self, array: np.ndarray, xyz_columns: str = "XYZRGB"):
        if self._writer is None:
            raise RuntimeError("open() が呼ばれていません。")
        rec = self._make_record(array, self.header, xyz_columns)
        self._writer.write_points(rec)

    def close(self):
        """NiFiストリーム向け: クローズ処理"""
        if self._writer:
            self._writer.close()
            self._writer = None

    # =========================================================
    # 内部ヘルパ
    # =========================================================
    def _decide_path(self, path: str) -> str:
        """self.to_laz=True なら .laz に、そうでなければ与えられた拡張子を尊重する。"""
        if self.to_laz:
            root, _ = os.path.splitext(path)
            return root + ".laz"
        return path

    def _prepare_header(self,
                        array: np.ndarray | None,
                        epsg: int | None = None,
                        version: str | None = None,
                        pdrf_id: int | None = None):
        """
        最初のチャンクから LAS ヘッダを確定する。
        - version/pdrf_id が与えられたら **最優先** で使用。
        - 未指定の場合のみ、列数から PDRF を推測し、version 既定は "1.2"。
        - PDRF=8 を指定された場合は version>=1.4 になるよう自動補正。
        """
        # --- 列数に基づく PDRF 推測（未指定時のみ） ---
        if pdrf_id is None and array is not None and array.size > 0:
            ncols = array.shape[1]
            # 列順は XYZ → I → RGB → T を想定
            # 3:XYZ -> 0, 4:XYZI -> 0, 5:XYZIT -> 1, 6:XYZRGB -> 2, 7:XYZIRGB -> 2, 8:XYZIRGBT -> 3
            if ncols <= 3:
                pdrf_id = 0
            elif ncols == 4:
                pdrf_id = 0
            elif ncols == 5:
                pdrf_id = 1
            elif ncols == 6:
                pdrf_id = 2
            elif ncols == 7:
                pdrf_id = 2
            else:  # ncols >= 8
                pdrf_id = 3

        # --- version 既定 ---
        if version is None:
            version = "1.2"
        # --- 互換性ガード ---
        if pdrf_id == 8 and float(version) < 1.4:
            version = "1.4"

        hdr = laspy.LasHeader(point_format=pdrf_id if pdrf_id is not None else 0, version=version)

        # EPSG の付与（引数優先 → インスタンス既定）
        use_epsg = epsg if epsg is not None else self.epsg
        if use_epsg:
            crs_obj = CRS.from_epsg(int(use_epsg))
            hdr.add_crs(crs_obj)
            # laspyの互換性確保：parse_crsも明示的に保持
            hdr.parse_crs = crs_obj

        # スケール・オフセット（init_points が無い場合は 0 原点基準で固定）
        if use_epsg:
            scales = self._auto_scales_by_crs(int(use_epsg))
        else:
            scales = [self.scale, self.scale, self.scale]

        if array is not None and array.size > 0:
            x, y, z = array[:, 0], array[:, 1], array[:, 2]
            hdr.scales = tuple(scales)
            hdr.offsets = (
                math.floor(x.min() / scales[0]) * scales[0],
                math.floor(y.min() / scales[1]) * scales[1],
                math.floor(z.min() / scales[2]) * scales[2],
            )
            # --- min/max も確実に書き込む ---
            hdr.mins = (float(x.min()), float(y.min()), float(z.min()))
            hdr.maxs = (float(x.max()), float(y.max()), float(z.max()))
        else:
            hdr.scales = tuple(scales)
            hdr.offsets = (0.0, 0.0, 0.0)
            hdr.mins = (0.0, 0.0, 0.0)
            hdr.maxs = (0.0, 0.0, 0.0)

        self.header = hdr
        self._last_version = version
        self._last_pdrf = pdrf_id if pdrf_id is not None else 0

    def _make_record(self, array: np.ndarray, hdr, xyz_columns: str = "XYZRGB"):
        """
        入力配列と xyz_columns の定義に基づき、
        LAS の ScaleAwarePointRecord を生成する。

        Parameters
        ----------
        array : np.ndarray
            列順は xyz_columns に準拠した点群データ。
        hdr : laspy.LasHeader
            LASヘッダ。
        xyz_columns : str
            列構成を表す文字列（例：'XYZIRGBT'）。
        """
        n = len(array)
        rec = laspy.ScaleAwarePointRecord.zeros(n, header=hdr)
        dims = set(rec.point_format.dimension_names)

        # --- 汎用マッピング ---
        col_map = {c:i for i, c in enumerate(xyz_columns)}

        # --- XYZ ---
        rec.x = array[:, col_map["X"]]
        rec.y = array[:, col_map["Y"]]
        rec.z = array[:, col_map["Z"]]

        # --- Intensity ---
        if "I" in col_map and "intensity" in dims:
            rec.intensity = array[:, col_map["I"]].astype(np.uint16)

        # --- RGB ---
        if {"R", "G", "B"}.issubset(col_map.keys()) and {"red", "green", "blue"}.issubset(dims):
            rec.red = array[:, col_map["R"]].astype(np.uint16)
            rec.green = array[:, col_map["G"]].astype(np.uint16)
            rec.blue = array[:, col_map["B"]].astype(np.uint16)

        # --- GPS_TIME ---
        if "T" in col_map and "gps_time" in dims:
            rec.gps_time = array[:, col_map["T"]].astype(np.float64)

        return rec

    def flush(self) -> None:
        """
        NiFiストリーム向け: 明示フラッシュ処理。
        - ライタ側に flush() があれば呼び出す。
        - laspy のライタは flush を持たない可能性もあるので hasattr チェックを挟む。
        - OSレベルのバッファリングを強制的に吐き出したいケース向けの“お守り”実装。
        """
        if self._writer is None:
            return

        # laspy の実装に flush があれば呼ぶ（なければ何もしない）
        if hasattr(self._writer, "flush"):
            self._writer.flush()
