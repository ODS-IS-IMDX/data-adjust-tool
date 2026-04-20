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

import glob, math, os, shutil
from importlib import import_module
from pointcloud.common.io.reader.factory import get_reader
from pointcloud.common.io.writer.factory import get_writer
from pointcloud.common.converter_utils import is_geographic

np = import_module("numpy")
jit = import_module("numba").jit
f8 = import_module("numba").float64
i8 = import_module("numba").int64
# 変換は行わないため Transformer は使わない。EPSG存在確認用のCRS型だけ保持。
CRS = import_module("pyproj").CRS
Transformer = import_module("pyproj").Transformer


@jit(i8[:](f8[:], f8, f8), nopython=False)
def _floor_div(arr, origin, size):
    """
    等間隔の格子に割り当てるためのセルインデックス計算。
    - 引数:
      arr    : 対象軸の座標配列（float64想定）
      origin : 原点[m]（格子の基準点）
      size   : セルサイズ[m]
    - 戻り値:
      各点が属するセル番号（int64配列）
    """
    n = arr.shape[0]
    out = np.empty(n, dtype=np.int64)
    for i in range(n):
        out[i] = math.floor((arr[i] - origin) / size)
    return out


def _finite3(x: np.ndarray, y: np.ndarray, z: np.ndarray):
    """NaN/Inf を除外して決定性を担保（入力品質が悪いときの保険）"""
    m = np.isfinite(x) & np.isfinite(y) & np.isfinite(z)
    if m.all():
        return x, y, z, None  # フィルタ無し
    return x[m], y[m], z[m], m


class SplitLasDataByAreaLogic:
    """グリッド割当・断片出力（parse_properties 付き）"""

    # -----------------------------
    # プロパティ解析
    # -----------------------------
    def parse_properties(self, properties: dict) -> dict:
        """
        NiFi の properties から必要項目を取り出し、
        既定値の付与・型変換・妥当性検証・出力先の初期化を行う。
        """

        def _req(name: str) -> str:
            v = properties.get(name)
            if v is None or str(v).strip() == "":
                raise ValueError(f"{name} が未設定です。")
            return str(v).strip()

        # 必須
        p = {"in_dir": _req("INPUT_DIR"), "out_root": _req("OUTPUT_DIR")}

        # 原点（m）
        try:
            p["ox_m"] = float(_req("ORIGIN_X"))
            p["oy_m"] = float(_req("ORIGIN_Y"))
        except Exception:
            raise ValueError("OriginX(m) / OriginY(m) は数値(m)で指定してください。")
        # セルサイズ（整数mm）
        try:
            p["cx_mm"] = int(_req("CELL_X_MM"))
            p["cy_mm"] = int(_req("CELL_Y_MM"))
        except Exception:
            raise ValueError("CellSizeX(mm) / CellSizeY(mm) は整数で指定してください。")

        if p["cx_mm"] <= 0 or p["cy_mm"] <= 0:
            raise ValueError("セルサイズ(mm) は 1 以上の整数で指定してください。")

        p["cx_m"] = p["cx_mm"] / 1000.0
        p["cy_m"] = p["cy_mm"] / 1000.0

        # 任意
        try:
            p["read_chunk"] = int(str(properties.get("READ_CHUNK", "2000000")))
        except Exception:
            raise ValueError("ReadChunkPoints は整数で指定してください。")

        if p["read_chunk"] <= 0:
            raise ValueError("ReadChunkPoints は 1 以上で指定してください。")

        # 出力クリーンアップ（UI名と大文字スネークの両方に対応）
        clean_raw = properties.get("CLEAN_OUT", properties.get("CleanOutputOnStart", "true"))
        p["clean"] = str(clean_raw).lower() == "true"

        # 入出力確認
        if not os.path.isdir(p["in_dir"]):
            raise FileNotFoundError(f"InputDirectory指定先にフォルダが存在しません: {p['in_dir']}")
        if p["clean"] and os.path.isdir(p["out_root"]):
            shutil.rmtree(p["out_root"])
        os.makedirs(p["out_root"], exist_ok=True)

        return p

    # -----------------------------
    # エントリポイント
    # -----------------------------
    def __call__(self, content, attribute, properties):
        """
        - 入力LASを列挙
        - ヘッダを取得（ここを“完全引き継ぎ”の起点にする）
        - XY平面の格子で分割し、各断片を書き出す
        - EPSG変換や座標変換は一切しない（ヘッダ・座標は元のまま）
        """
        p = self.parse_properties(properties)

        files = sorted(glob.glob(os.path.join(p["in_dir"], '**', '*.las'), recursive=True))
        if not files:
            raise FileNotFoundError(f'入力フォルダ内にLasファイルが見つかりません: {p["in_dir"]}')

        # ---- 補助: 緯度(deg)から 1度あたりのメートル量を近似（WGS84近似式）----
        def _meters_per_deg(lat_deg: float):
            import math as _m
            φ = _m.radians(lat_deg)
            # 緯度方向(南北)
            m_per_deg_lat = 111132.92 - 559.82 * _m.cos(2 * φ) + 1.175 * _m.cos(4 * φ) - 0.0023 * _m.cos(6 * φ)
            # 経度方向(東西)
            m_per_deg_lon = 111412.84 * _m.cos(φ) - 93.5 * _m.cos(3 * φ) + 0.118 * _m.cos(5 * φ)
            # 非常に高緯度でcosφ→0になるのを避けるための下限
            if m_per_deg_lon < 1e-6:
                m_per_deg_lon = 1e-6
            return m_per_deg_lat, m_per_deg_lon

        for src_path in files:
            src_base = os.path.splitext(os.path.basename(src_path))[0]

            # --- ファクトリ経由で Reader を取得 ---
            reader = get_reader(src_path)

            # 入力LASのヘッダを取得（このヘッダを“そのまま”継承して書き出す）
            # 既存I/F: reader.get_header(path) を利用（laspyヘッダが返る）
            in_hdr = reader.get_header(src_path)  # 【確認】ヘッダ取得API
            crs = None
            epsg_proj = None
            try:
                crs = in_hdr.parse_crs()
                if isinstance(crs, CRS):
                    epsg_proj = crs.to_epsg()

                    if epsg_proj is None or epsg_proj == 0:
                        raise ValueError(f"EPSG取得エラー: {epsg_proj},{crs}")
                else:
                    raise TypeError("CRS情報がLASヘッダから取得できませんでした。")

            # ヘッダにCRSが無い/壊れていたらエラー
            except Exception as ex:
                raise ValueError(f"EPSG取得エラー: {ex}")

            # 座標系が「地理座標系（度単位）」かを決定
            is_geo = is_geographic(crs, epsg_proj)

            # チャンク単位で読み込み（ここでは座標加工しない）
            for ci, pts in enumerate(reader.iter_chunks(src_path, chunk_points=p["read_chunk"])):
                # XY/ Z を取り出し & NaN/Inf フィルタ
                x = np.ascontiguousarray(np.asarray(pts[:, 0], dtype=np.float64))
                y = np.ascontiguousarray(np.asarray(pts[:, 1], dtype=np.float64))
                z = np.ascontiguousarray(np.asarray(pts[:, 2], dtype=np.float64))
                x, y, z, mask = _finite3(x, y, z)
                if mask is not None:
                    pts = pts[mask]

                # セルサイズの“使用値”を決定
                #   - 通常（投影座標; m単位）    : そのまま p["cx_m"], p["cy_m"]
                #   - 地理座標系（度単位）の場合 : 入力のmm→mに換算済みの p["cx_m"], p["cy_m"] を“度”に変換して用いる
                if is_geo:
                    # チャンク内の代表緯度（平均）を使って m↔deg 換算比を求める
                    # いつ実行: 各チャンクごと（地域が広い場合の緯度依存に追従）
                    lat_ref = float(np.mean(y)) if y.size else 0.0  # yが緯度(deg)軸
                    m_per_deg_lat, m_per_deg_lon = _meters_per_deg(lat_ref)
                    cx_use = max(p["cx_m"] / m_per_deg_lon, 1e-12)  # 経度方向: m → deg
                    cy_use = max(p["cy_m"] / m_per_deg_lat, 1e-12)  # 緯度方向: m → deg
                else:
                    cx_use = p["cx_m"]
                    cy_use = p["cy_m"]

                # 格子インデックス計算（単位は座標系に合わせたcx_use/cy_use）
                ix = _floor_div(x, p["ox_m"], cx_use)
                iy = _floor_div(y, p["oy_m"], cy_use)

                # セルごとに分配
                area = np.vstack((ix, iy)).T
                uniq, inv = np.unique(area, axis=0, return_inverse=True)

                for k, (ixk, iyk) in enumerate(uniq):
                    sel = (inv == k)
                    sub = pts[sel]
                    if len(sub) == 0:
                        continue

                    # 左上
                    lon_left = p["ox_m"] + int(ixk) * p["cx_m"]
                    lat_top = p["oy_m"] + int(iyk) * p["cy_m"]

                    # 小数7桁の文字列変換
                    def _str7(v: float) -> str:
                        return f'{v:.7f}'

                    tile_key = f'{p["cx_mm"]}x{p["cy_mm"]}_Lon{_str7(lon_left)}_Lat{_str7(lat_top)}_epsg{int(epsg_proj) if epsg_proj else 0}'
                    out_name = f'{tile_key}_{src_base}_{ci:04d}_{k:03d}.part.las'
                    out_path = os.path.join(p["out_root"], out_name)

                    # Writer をファクトリで取得し、元ヘッダを“そのまま”適用して書き出す
                    writer = get_writer(out_path)
                    # src_header を渡してヘッダ完全コピー（version/PDRF/VLR/scale/offset/EPSGを維持）
                    writer.write(out_path, sub, src_header=in_hdr)

                    # 代表属性（断片ごと）
                    attribute.update({
                        'tile.key': tile_key,
                        'cell.size.mm.x': str(p["cx_mm"]),
                        'cell.size.mm.y': str(p["cy_mm"]),
                        'origin.x': str(p["ox_m"]),
                        'origin.y': str(p["oy_m"]),
                        'las.epsg': str(epsg_proj),
                    })

        return b'', attribute
