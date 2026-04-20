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

import os
from importlib import import_module
from pointcloud.common.io.reader.factory import get_reader
from pointcloud.common.io.writer.factory import get_writer

np = import_module("numpy")
CRS = import_module("pyproj").CRS
Transformer = import_module("pyproj").Transformer
jit = import_module("numba").jit
f8 = import_module("numba").float64
i8 = import_module("numba").int64
u2 = import_module("numba").uint16

# =========================================================
# ユーティリティ関数
# =========================================================
def _require(path: str):
    """
    指定されたファイルパスの存在を確認し、存在しない場合は例外を送出する
    
    :param path: ファイルパス
    :type path: str

    :return: ファイルパス
    :rtype: str

    :raises Exception: 存在しない場合は例外をスローする
    """
    if not path or not os.path.isfile(path):
        raise ValueError(f"INPUT_PATH は既存の .xyz ファイルを指定してください: {path}")
    return path

def _ensure_dir(p: str) -> None:
    """
    親ディレクトリを再帰的に作成する
    
    :param p: ディレクトリパス
    :type p: str
    """
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)


@jit(u2[:](f8[:], f8, f8), nopython=True)
def _scale_clip_u16(a, vmax, scale):
    """
    RGB/Intensity値を0..65535の範囲に正規化するNumba関数
    
    :param a: 入力配列（RGBやIntensityの生値）
    :type a: np.ndarray
    :param vmax: 出力範囲の最大値
    :type vmax: float
    :param scale: 入力値に掛けるスケール
    :type scale: float
    
    :return: 正規化され、クリップされたuint16配列
    :rtype: np.ndarray
    """
    n = a.size
    out = np.empty(n, dtype=np.uint16)
    for i in range(n):
        v = a[i] * scale
        if v < 0.0:
            v = 0.0
        elif v > vmax:
            v = vmax
        out[i] = int(v + 0.5)
    return out

def _make_scale_conv(mode: str):
    """
    入力値のレンジモードに応じて正規化関数を生成する。
    
    :param mode: 正規化レンジの種類を示す文字列。
    :type mode: str

    :return: 入力 ndarray を正規化する関数（callable）。
            返される関数は ndarray → ndarray の変換を行う。
    :rtype: Callable[[np.ndarray], np.ndarray]
    """
    if mode == "0-1":
        return lambda a: _scale_clip_u16(a.astype(np.float64, copy=False), 65535.0, 65535.0)
    elif mode == "0-255":
        return lambda a: _scale_clip_u16(a.astype(np.float64, copy=False), 65535.0, 257.0)
    else:
        def _auto(a_in):
            a = a_in.astype(np.float64, copy=False)
            vmax = float(a.max()) if a.size else 0.0
            if vmax <= 1.01:
                return _scale_clip_u16(a, 65535.0, 65535.0)
            elif vmax <= 255.5:
                return _scale_clip_u16(a, 65535.0, 257.0)
            else:
                return _scale_clip_u16(a, 65535.0, 1.0)
        return _auto

# =========================================================
# メインロジッククラス
# =========================================================
class ConvertXyzToLasLogic:
    """
    XYZ / CSV 形式の点群を LAS 形式へ変換する処理を行う。

    - チャンク逐次処理に対応（大容量点群に対応可能）
    - Numba による RGB / Intensity の高速正規化
    - 入出力 EPSG の差分がある場合は自動的に座標変換を実施
    - Reader / Writer は共通I/Oファクトリを介して生成される
    """

    def parse_properties(self, props: dict) -> dict:
        """
        必須/任意プロパティの検証・型変換・既定値適用を行う。

        NiFi から渡されるプロパティ(dict形式)を受け取り、
        ファイルパスの存在チェック・数値型への変換・EPSGコードの解釈などを行った上で、
        後続処理で利用しやすい形にまとめた辞書を返す。

        :param properties: NiFi プロセッサから渡されるプロパティの辞書
        :type properties: dict(str, Any)

        :return: 正規化済みのプロパティ情報を格納した辞書
        :rtype: dict(str, Any)

        :raises ValueError: 必須プロパティが未設定、型が不正、または拡張子が不正な場合に例外をスローする。
        :raises FileNotFoundError: 入力ファイルが存在しない場合に例外をスローする。
        """
        params = {}
        params["input_path"]  = _require(str(props.get("INPUT_PATH", "")).strip())
        # ファイルが存在するか
        if not os.path.exists(params["input_path"]):
            raise FileNotFoundError(f'InputPath が存在しません: {params["input_path"]}')

        # ディレクトリではなくファイルであること
        if not os.path.isfile(params["input_path"]):
            raise ValueError(f'InputPath はファイルではありません: {params["input_path"]}')

        # XYZ/CSV ファイルかどうか
        ext = os.path.splitext(params["input_path"])[1].lower()
        if ext not in [".xyz", ".csv"]:
            raise ValueError(f'XYZ/CSV ファイルではありません: {params["input_path"]}（拡張子: {ext}）')

        params["in_format"] = "CSV" if ext == [".csv"] else "XYZ"
        
        params["output_path"] = str(props.get("OUTPUT_PATH", "")).strip()
        if not params["output_path"]:
            raise ValueError("OUTPUT_PATH が未設定です。")
        out_path = params["output_path"]
        # out_path がディレクトリかファイルかを判定
        if out_path.endswith(os.sep) or not os.path.splitext(out_path)[1]:
            # ディレクトリ扱い
            os.makedirs(out_path, exist_ok=True)
        else:
            # ファイル扱い → 親ディレクトリを作成
            parent_dir = os.path.dirname(out_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

        inp_epsg = str(props.get("INPUT_EPSG", "")).strip()
        params["input_epsg"] = int(inp_epsg) if inp_epsg else None
        epsg = str(props.get("OUTPUT_EPSG", "")).strip()
        params["output_epsg"] = int(epsg) if (epsg and epsg != "None") else params["input_epsg"]

        params["chunk_lines"] = int(str(props.get("CHUNK_LINES", "1000000")))
        params["delimiter"] = "," if str(props.get("DELIMITER", "space")).lower() == "comma" else " "


        cols = str(props.get("XYZ_COLUMNS", "XYZ")).upper()
        params["xyz_columns"] = cols
        params["has_i"]   = ("I" in cols)
        params["has_rgb"] = ("RGB" in cols)
        params["has_t"]   = (cols.endswith("T") or cols == "XYZIT")

        params["rgb_range"] = str(props.get("RGB_INPUT_RANGE", "auto")).lower()
        params["i_range"]   = str(props.get("INTENSITY_INPUT_RANGE", "auto")).lower()

        params["strict"] = str(props.get("STRICT_MODE", "true")).lower() == "true"

        # 明示指定の LAS version / PDRF
        ver = str(props.get("OUTPUT_LAS_VERSION", "")).strip()
        params["out_las_version"] = (ver if ver else None)
        pdrf = str(props.get("OUTPUT_LAS_PDRF", "")).strip()
        params["out_las_pdrf"] = (int(pdrf) if pdrf else None)

        # flushメモリ制限（MB指定を Byte へ換算）
        flush_mb_str = str(props.get("FLUSH_BYTES", "")).strip() or "0"
        try:
            flush_mb = float(flush_mb_str)
        except Exception:
            flush_mb = 0.0
        params["flush_bytes"] = int(flush_mb * 1024 * 1024)

        return params

    def __call__(self, byte_data, attribute, properties):
        """
        FlowFile の content/attribute/properties を受け取り、
        互換インターフェースで点群変換を実行する（単一メソッド完結・高速版）。

        テキスト形式（.xyz / .csv）の点群ファイルを、LAS形式に変換する。

        :param content: FlowFile のコンテンツ(本処理では未使用でよい想定)
        :type content: bytes または任意
        :param attribute: FlowFile の属性を格納した辞書。処理結果のメタ情報が追記される
        :type attribute: dict(str, str)
        :param properties: プロセッサプロパティを格納した辞書
        :type properties: dict(str, Any)
        """

        p = self.parse_properties(properties)

        # ---- ローカル関数 -------------------------------------------------
        # 期待列数を事前計算（毎回の計算を回避）
        _valid = {"X","Y","Z","I","R","G","B","T"}
        expected_cols = sum(1 for c in p["xyz_columns"] if c in _valid)

        # 無効列チェック
        invalid_cols = [c for c in p["xyz_columns"] if c not in _valid]
        if invalid_cols:
            raise ValueError(f"無効な列が含まれています: {invalid_cols}")
        # 重複チェック
        duplicates = [c for c in p["xyz_columns"] if p["xyz_columns"].count(c) > 1]
        if duplicates:
            raise ValueError(f"重複した列があります: {set(duplicates)}")
        
        def trim_columns(arr: np.ndarray) -> np.ndarray:
            """
            列数チェックと不要列カット（strict対応）
            
            :param mode: 正規化モード。以下のいずれか
                - "0-1"   : 入力値が0..1の範囲
                - "0-255" : 入力値が0..255の範囲
                - その他  : 自動判定
            :type mode: str
            
            :return: 入力配列を uint16 に正規化して返す関数
            :rtype: Callable[[np.ndarray], np.ndarray]
            """
            if p["strict"] and arr.shape[1] != expected_cols:
                raise ValueError(f"列数不一致: 期待 {expected_cols}, 実際 {arr.shape[1]}")
            if arr.shape[1] < expected_cols:
                raise ValueError(f"不足列: 期待 {expected_cols}, 実際 {arr.shape[1]}")
            if arr.shape[1] > expected_cols:
                arr = arr[:, :expected_cols]
            return arr

        # 列インデックスを一度だけ作る
        idx_map = {c:i for i, c in enumerate(p["xyz_columns"])}

        def split_columns(arr: np.ndarray):
            """
            p["xyz_columns"] の並び順に従って XYZ/I/RGB/T を抽出する。
            例: "XYZIRGB" → X,Y,Z,I,R,G,B の順で抽出。
            
            入力配列から p["xyz_columns"] の定義に従って XYZ/I/R/G/B/T の各列を抽出する。

            :param arr: 入力の 2 次元配列。行は点データ、列は各属性。
            :type arr: np.ndarray
            
            :return: 各列の配列。存在しない列は None。
                    順番は (x, y, z, intensity, r, g, b, gps_time)
            :rtype: Tuple[Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray], 
                        Optional[np.ndarray], Optional[np.ndarray], Optional[np.ndarray], 
                        Optional[np.ndarray], Optional[np.ndarray]]
            
            :raises ValueError: 配列の列数が p["xyz_columns"] の定義と一致しない場合
            """
            ncol = arr.shape[1]
            if len(p["xyz_columns"]) != ncol:
                raise ValueError(f"列数不一致: 定義={len(p['xyz_columns'])}, 実データ={ncol}")

            get = idx_map.get
            x = arr[:, get("X")] if get("X") is not None else None
            y = arr[:, get("Y")] if get("Y") is not None else None
            z = arr[:, get("Z")] if get("Z") is not None else None
            inten = arr[:, get("I")] if get("I") is not None else None
            r = arr[:, get("R")] if get("R") is not None else None
            g = arr[:, get("G")] if get("G") is not None else None
            b = arr[:, get("B")] if get("B") is not None else None
            gps_time = arr[:, get("T")] if get("T") is not None else None
            return x, y, z, inten, r, g, b, gps_time

        rgb_conv = _make_scale_conv(p["rgb_range"]) if p["has_rgb"] else None
        i_conv   = _make_scale_conv(p["i_range"])   if p["has_i"]   else None

        # 出力列数を固定算出（flush分割の精度向上）
        out_cols_const = (3
                          + (1 if p["has_i"] else 0)
                          + (3 if p["has_rgb"] else 0)
                          + (1 if p["has_t"] else 0))

        def compose_output(x, y, z, inten, r, g, b, gps_time):
            """
            XYZ → I → RGB → T の順に column_stack
            
            :param x: X座標配列
            :type x: np.ndarray
            :param y: Y座標配列
            :type y: np.ndarray
            :param z: Z座標配列
            :type z: np.ndarray
            :param inten: Intensity 配列（存在する場合）
            :type inten: Optional[np.ndarray]
            :param r: R値配列（存在する場合）
            :type r: Optional[np.ndarray]
            :param g: G値配列（存在する場合）
            :type g: Optional[np.ndarray]
            :param b: B値配列（存在する場合）
            :type b: Optional[np.ndarray]
            :param gps_time: GPS時間配列（存在する場合）
            :type gps_time: Optional[np.ndarray]
            
            :return: p["xyz_columns"] の順序に従って結合された 2次元配列
            :rtype: np.ndarray
            """
            # 事前確保して代入（コピー回数を削減）
            n = x.shape[0]
            out = np.empty((n, out_cols_const), dtype=np.float64)

            # 変換結果を先に作っておく（何度も呼ばない）
            inten_scaled = i_conv(inten) if (inten is not None and i_conv is not None) else None
            r_scaled = rgb_conv(r) if (r is not None and rgb_conv is not None) else None
            g_scaled = rgb_conv(g) if (g is not None and rgb_conv is not None) else None
            b_scaled = rgb_conv(b) if (b is not None and rgb_conv is not None) else None

            # xyz_columns の並び順どおりに詰める
            for j, c in enumerate(p["xyz_columns"]):
                if c == "X":
                    out[:, j] = x
                elif c == "Y":
                    out[:, j] = y
                elif c == "Z":
                    out[:, j] = z
                elif c == "I" and inten_scaled is not None:
                    out[:, j] = inten_scaled
                elif c == "R" and r_scaled is not None:
                    out[:, j] = r_scaled
                elif c == "G" and g_scaled is not None:
                    out[:, j] = g_scaled
                elif c == "B" and b_scaled is not None:
                    out[:, j] = b_scaled
                elif c == "T" and gps_time is not None:
                    out[:, j] = gps_time
            return out

        def iter_subarrays(arr: np.ndarray):
            """
            flush制限に応じて arr をそのまま or 分割して返すジェネレータ
            
            :param arr: 入力配列（2次元 float64 想定）
            :type arr: np.ndarray
            :yield: flush_bytes 制限に従って分割された部分配列
            :rtype: np.ndarray
            """
            limit = p.get("flush_bytes", 0)
            if limit <= 0 or arr.nbytes <= limit:
                yield arr
                return
            row_bytes = out_cols_const * 8  # float64 前提（Writer に合わせる）
            max_rows = max(1, limit // row_bytes)
            for i in range(0, arr.shape[0], max_rows):
                yield arr[i:i+max_rows, :]


        # ---- EPSG 変換器 -------------------------------------------------
        transformer = None
        if p["input_epsg"] and p["output_epsg"] and p["input_epsg"] != p["output_epsg"]:
            transformer = Transformer.from_crs(
                p["input_epsg"],
                p["output_epsg"],
                always_xy=True,
            )

        # ---- 出力パス ----------------------------------------------------
        src = p["input_path"]
        base = os.path.splitext(os.path.basename(src))[-2]
        out_name = base + ".las"
        out_path = os.path.join(p["output_path"], out_name) if os.path.isdir(p["output_path"]) else p["output_path"]
        _ensure_dir(os.path.dirname(out_path) or out_path)

        # ---- Reader / Writer ---------------------------------------------
        reader_cls = get_reader(src)
        reader = reader_cls() if isinstance(reader_cls, type) else reader_cls
        if hasattr(reader, "delimiter"): reader.delimiter = p["delimiter"]
        if hasattr(reader, "strict"): reader.strict = p["strict"]
        if hasattr(reader, "chunk_lines"): reader.chunk_lines = p["chunk_lines"]
        writer = get_writer("LAS")

        # ---- LAS version / PDRF ------------------------------------------
        pdrf_id = p["out_las_pdrf"]
        ver_str = p["out_las_version"]

        # ---- メインループ（flush有無を単一ループで処理） ----------------
        total_points = 0
        header_committed = False
        las_header = None

        for  _, arr in enumerate(reader.iter_chunks(p["input_path"]), 1):
            if arr.ndim == 1:
                arr = arr.reshape(1, -1)
            arr = trim_columns(arr)

            for  _, sub_arr in enumerate(iter_subarrays(arr), 1):

                # 座標・属性抽出
                x, y, z, inten, r, g, b, gps_time = split_columns(sub_arr)
                if transformer is not None:
                    x, y, z = transformer.transform(x, y, z)

                # 最初の到着でヘッダ確定
                if not header_committed:
                    #   init_points=np.column_stack((x, y, z))  → 変換後(4326)の座標
                    init_points = np.column_stack((x, y, z))
                    writer.open(
                        out_path,
                        epsg=p["output_epsg"],
                        init_points=init_points,
                        version=ver_str,
                        pdrf_id=pdrf_id,
                    )
                    if p["output_epsg"] and hasattr(writer, "header") and writer.header is not None:
                        writer.header.add_crs(CRS.from_epsg(p["output_epsg"]))
                    header_committed = True
                    las_header = getattr(writer, "header", None)

                # 出力配列を作成して書込
                out_arr = compose_output(x, y, z, inten, r, g, b, gps_time)
                writer.write_points(out_arr, xyz_columns=p["xyz_columns"])
                total_points += len(out_arr)

        # ---- 終了処理 ----------------------------------------------------
        writer.close()

        # ---- NiFi属性返却 -----------------------------------------------
        attribute.update({
            "in.format": p["in_format"],
            "in.columns": p["xyz_columns"],
            "in.epsg": str(p["input_epsg"] or "-"),
            "out.points": str(total_points),
            "out.epsg": str(p["output_epsg"] or "-"),
            "out.path": out_path,
            "out.las.version": ver_str,
            "out.las.pdrf": str(pdrf_id),
        })
        if getattr(las_header, "scales", None):
            attribute["out.scale"] = ",".join(str(v) for v in las_header.scales)
        if getattr(las_header, "offsets", None):
            attribute["out.offset"] = ",".join(str(v) for v in las_header.offsets)

        return b"", attribute
