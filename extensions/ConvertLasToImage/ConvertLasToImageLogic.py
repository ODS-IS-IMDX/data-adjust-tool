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

import os, io, math, json
from importlib import import_module
from typing import Tuple, Dict, Any
from pointcloud.common.io.reader.factory import get_reader

np = import_module("numpy")
cv2 = import_module("cv2")
Affine = import_module("affine").Affine
CRS = import_module("pyproj").CRS
Transformer = import_module("pyproj").Transformer
jit = import_module("numba").jit
prange = import_module("numba").prange
f8 = import_module("numba").float64
i8 = import_module("numba").int64
u1 = import_module("numba").uint8
u4 = import_module("numba").uint32


# ========= Numbaカーネル =========
@jit((
i8[:], i8[:], f8[:], u1[:], u1[:], u1[:], f8[:, :], f8, f8[:, :], u4[:, :], u4[:, :], u4[:, :], u4[:, :], u1[:, :]),
    nopython=False, parallel=True)
def _accumulate(ix, iy, z, r, g, b, minZ, alpha, sumZ, sumR, sumG, sumB, cnt, mask):
    """
    各点群サンプルをグリッドに集約し、Z・RGB の合計値とカウントを更新する。

    NumPy 配列として渡された各種情報(グリッドインデックス・標高・RGB)をもとに、
    "最小標高(minZ)" と "しきい値(alpha)" に従って有効な点だけを集約する。
    OpenCV 画像および DEM を後段で生成するための下準備となるコア処理。

    :param ix: X方向のグリッドインデックス(列インデックス)を格納した1次元配列
    :type ix: numpy.ndarray
    :param iy: Y方向のグリッドインデックス(行インデックス)を格納した1次元配列
    :type iy: numpy.ndarray
    :param z: 各点の標高値(Z値)を格納した1次元配列
    :type z: numpy.ndarray
    :param r: 各点のR成分(0〜255)を格納した1次元配列
    :type r: numpy.ndarray
    :param g: 各点のG成分(0〜255)を格納した1次元配列
    :type g: numpy.ndarray
    :param b: 各点のB成分(0〜255)を格納した1次元配列
    :type b: numpy.ndarray
    :param minZ: 各グリッドセルの最小標高値を保持する2次元配列
    :type minZ: numpy.ndarray
    :param alpha: 地物抽出の許容高さしきい値[m]。minZからの差分がこの値以下の点だけを集約対象とする
    :type alpha: float
    :param sumZ: 各グリッドセルの標高値合計を保持する2次元配列
    :type sumZ: numpy.ndarray
    :param sumR: 各グリッドセルのR成分合計を保持する2次元配列
    :type sumR: numpy.ndarray
    :param sumG: 各グリッドセルのG成分合計を保持する2次元配列
    :type sumG: numpy.ndarray
    :param sumB: 各グリッドセルのB成分合計を保持する2次元配列
    :type sumB: numpy.ndarray
    :param cnt: 各グリッドセルに集約された有効点の数を保持する2次元配列
    :type cnt: numpy.ndarray
    :param mask: 有効セルかどうかを示すマスク配列。0 のセルはスキップする
    :type mask: numpy.ndarray

    :return: なし(配列はインプレースで更新される)
    :rtype: None

    :raises Exception: numba 実行時の内部エラーが発生した場合に例外がスローされる可能性がある。
    """
    n = ix.shape[0]
    for i in prange(n):
        ty, tx = iy[i], ix[i]
        if ty < 0 or tx < 0 or ty >= minZ.shape[0] or tx >= minZ.shape[1]:
            continue
        if mask[ty, tx] == 0:
            continue
        zi = z[i]
        if zi < minZ[ty, tx]:
            minZ[ty, tx] = zi
        dz = zi - minZ[ty, tx]
        if 0.0 <= dz <= alpha:
            sumZ[ty, tx] += zi
            sumR[ty, tx] += r[i]
            sumG[ty, tx] += g[i]
            sumB[ty, tx] += b[i]
            cnt[ty, tx] += 1


def _u16_to_u8_auto(a: np.ndarray) -> np.ndarray:
    """
    16bit 画像(または配列)を最大値に応じて 8bit 表現に自動ダウンサンプリングする。

    16bit の強度やRGB成分を、その最大値のレンジに合わせて右シフトし、
    0〜255 の範囲に収まる uint8 配列に変換する。

    :param a: 入力の16bit配列(画像の1チャンネル等)
    :type a: numpy.ndarray

    :return: 0〜255に正規化された uint8 配列
    :rtype: numpy.ndarray

    :raises ValueError: 入力配列の形状が不正な場合に例外をスローする可能性がある。
    """
    if a.size == 0: return np.zeros(0, np.uint8)
    vmax = float(a.max())
    if vmax <= 255:  return a.astype(np.uint8, copy=False)
    if vmax <= 4095: return (a.astype(np.uint16, copy=False) >> 4).astype(np.uint8, copy=False)
    return (a.astype(np.uint16, copy=False) >> 8).astype(np.uint8, copy=False)


def _stretch_u8_by_percentile(x: np.ndarray, lo=1.0, hi=99.0) -> np.ndarray:
    """
    パーセンタイルに基づいて画素値を線形ストレッチし、0〜255 の uint8 に変換する。

    指定された下位・上位パーセンタイル値をもとに、入力配列を線形変換して、
    コントラストを強調した 8bit 画像用配列を生成する。

    :param x: 入力の画素値配列(任意のスケール)
    :type x: numpy.ndarray
    :param lo: 下限側のパーセンタイル(例: 1.0)
    :type lo: float
    :param hi: 上限側のパーセンタイル(例: 99.0)
    :type hi: float

    :return: 0〜255に線形スケーリングされた uint8 配列
    :rtype: numpy.ndarray

    :raises ValueError: lo と hi の設定が不正な場合に例外をスローする可能性がある。
    """
    if x.size == 0: return np.zeros(0, np.uint8)
    p_lo = float(np.percentile(x, lo))
    p_hi = float(np.percentile(x, hi))
    if p_hi <= p_lo: return np.zeros_like(x, np.uint8)
    y = np.clip((x - p_lo) * (255.0 / (p_hi - p_lo)), 0, 255)
    return y.astype(np.uint8, copy=False)

def _rgb_from_chunk(chunk) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    チャンク単位の点群データから RGB 各チャンネルの配列を生成する。

    入力が numpy.ndarray の場合は [X,Y,Z,R,G,B] または [X,Y,Z,Intensity] 想定で、
    laspy のチャンクオブジェクトの場合は point_format/dimension_names を参照して
    RGB または Intensity から 8bit の R/G/B 配列を作成する。

    ・XYZRGB の場合:
        - R/G/B 列は 8bit または 16bit の整数値を想定し、
          タイル間で色味が変わらないように「パーセンタイルによるストレッチは行わず」
          `_u16_to_u8_auto` によるビットシフトのみで 0〜255 に正規化する。
    ・XYZI の場合:
        - Intensity を 1ch グレースケールとして扱い、可視化しやすくするために
          パーセンタイルストレッチ `_stretch_u8_by_percentile` を適用する。

    :param chunk: 点群チャンク。numpy.ndarray または laspy.points.PointRecord
    :type chunk: numpy.ndarray または laspy.points.PointRecord

    :return: R,G,B 各チャンネルの 1次元配列(長さ = 点数)
    :rtype: tuple(numpy.ndarray, numpy.ndarray, numpy.ndarray)

    :raises ValueError: 入力の列数や属性が想定外の場合に例外をスローする可能性がある。
    """
    # -----------------------------------------------------------
    # 1) numpy.ndarray（LasPointCloudReader.iter_chunks の戻り値）を扱う場合
    # -----------------------------------------------------------
    # iter_chunks(path, xyz_columns="XYZRGB") は
    #   [X, Y, Z, R, G, B] （整数値→float64）の ndarray を返す実装を想定している。
    # この場合、RGB はタイル間で色味が変わらないよう、
    # 以前のようなパーセンタイルストレッチは行わず、16bit→8bit の縮約のみにとどめる。

    if isinstance(chunk, np.ndarray):
        n = chunk.shape[0]
        if n == 0:
            # 空チャンクなら長さ0の配列を返す
            return (np.zeros(0, np.uint8), np.zeros(0, np.uint8), np.zeros(0, np.uint8))

        cols = chunk.shape[1]

        # 列数 >= 6 のとき: [X,Y,Z,R,G,B] とみなす
        #  - R/G/B は 0〜255 または 0〜65535 の整数値を想定
        #  - 「タイルごとにコントラストが変わる」ことを避けるため、
        #    パーセンタイルストレッチではなく `_u16_to_u8_auto` による固定的な縮約のみ行う。
        if cols >= 6:
            r_raw = chunk[:, 3]
            g_raw = chunk[:, 4]
            b_raw = chunk[:, 5]

            # float64 などで来ても整数値を想定して uint16 にキャストした上で縮約する
            r = _u16_to_u8_auto(r_raw.astype(np.uint16, copy=False))
            g = _u16_to_u8_auto(g_raw.astype(np.uint16, copy=False))
            b = _u16_to_u8_auto(b_raw.astype(np.uint16, copy=False))
            return r, g, b

        # 列数 >= 4 のとき: [X,Y,Z,Intensity] 相当とみなしてグレースケール画像にする
        # こちらは「強度値の分布を見やすくする」ことを優先し、
        # 従来どおりパーセンタイルストレッチを適用する。
        if cols >= 4:
            val = _stretch_u8_by_percentile(chunk[:, 3].astype(np.float64))
            return val, val, val

        # XYZ しかない場合は一律128のグレー
        val = np.full(n, 128, np.uint8)
        return val, val, val

    # -----------------------------------------------------------
    # 2) 従来通り、laspy の chunk（point_format を持つオブジェクト）の場合
    # -----------------------------------------------------------
    dims = set(chunk.point_format.dimension_names)
    n = len(chunk)
    if {"red", "green", "blue"}.issubset(dims):
        r = _u16_to_u8_auto(np.asarray(chunk.red))
        g = _u16_to_u8_auto(np.asarray(chunk.green))
        b = _u16_to_u8_auto(np.asarray(chunk.blue))
        return r.astype(np.uint8, copy=False), g.astype(np.uint8, copy=False), b.astype(np.uint8, copy=False)
    if "intensity" in dims:
        val = _stretch_u8_by_percentile(np.asarray(chunk.intensity, np.float64))
        return val, val, val
    val = np.full(n, 128, np.uint8)
    return val, val, val

def _map_product_to_kind(s: str) -> str:
    """
    UI上のラスタ種別文字列から内部処理用の種別コード(Z/RGB/INTENSITY)を判定する。

    "標高(DEM)" や "カラー画像(RGB)" などのディスプレイ用文字列を受け取り、
    処理ロジック内で使用する 'Z', 'RGB', 'INTENSITY' にマッピングする。

    :param s: ラスタ種別を表す文字列
    :type s: str

    :return: 内部処理用の種別コード('Z' / 'RGB' / 'INTENSITY')
    :rtype: str

    :raises Exception: 特殊な形式の文字列でパースに失敗した場合に例外をスローする可能性がある。
    """
    s = (s or '').strip()
    if 'DEM' in s or '標高' in s: return 'Z'
    if 'RGB' in s or 'カラー' in s: return 'RGB'
    if 'Intensity' in s or '反射強度' in s or '強度' in s: return 'INTENSITY'
    u = s.upper()
    if 'Z' in u: return 'Z'
    if 'INT' in u: return 'INTENSITY'
    return 'RGB'


def _compute_bounds(reader, input_path: str, header: Any | None = None):
    """
    LAS 全体の XY 範囲を決定するヘルパ。

    座標変換の有無に応じて処理を分岐し、DEM/RGB 生成に必要な
    最小/最大の X・Y を求める。ヘッダの mins/maxs を使用する。

    :param reader: LAS を読み出す共通リーダインスタンス
    :type reader: 任意(共通Reader。LasPointCloudReader など)
    :param input_path: 入力 LAS ファイルパス
    :type input_path: str
    :param header: 事前に取得済みの LAS ヘッダ。None の場合は関数内で取得する
    :type header: 任意 または None

    :return: (xmin, xmax, ymin, ymax) のタプル
    :rtype: tuple(float, float, float, float)

    """
    # header が渡されていれば再利用／無ければここで get_header する
    h = header if header is not None else reader.get_header(input_path)
    xmin, ymin, _ = h.mins
    xmax, ymax, _ = h.maxs
    return float(xmin), float(xmax), float(ymin), float(ymax)


class ConvertLasToImageLogic:
    """LasToImage の実処理。OpenCVイメージのバイト列＋属性を返す。"""

    # -----------------------------
    # プロパティ解析
    # -----------------------------
    def parse_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
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

        def _req(key: str) -> str:
            v = properties.get(key)
            if v is None or str(v).strip() == "":
                raise ValueError(f"{key} が未設定です。")
            return str(v).strip()

        p: Dict[str, Any] = {}
        p["input_path"] = _req("INPUT_PATH")

        # UI表示名ベースの種別（例：'標高（DEM）' / 'カラー画像（RGB）' / '反射強度（Intensity）'）
        p["product_display"] = str(properties.get("RASTER_PRODUCT", "標高（DEM）")).strip()
        p["kind"] = _map_product_to_kind(p["product_display"])

        # mm系は float
        try:
            p["res_mm"] = float(str(properties.get("RES_MM", "50.0")))
        except Exception:
            raise ValueError("Resolution (mm/px) は数値(mm)で指定してください。")
        try:
            p["alpha_mm"] = float(str(properties.get("ALPHA_MM", "50.0")))
        except Exception:
            raise ValueError("Alpha (mm) は数値(mm)で指定してください。")

        # チャンク点数
        try:
            p["chunk_points"] = int(str(properties.get("CHUNK", "5000000")))
        except Exception:
            raise ValueError("Chunk Size は整数で指定してください。")
        if p["chunk_points"] <= 0:
            raise ValueError("Chunk Size は 1 以上で指定してください。")

        # ファイルが存在するか
        if not os.path.exists(p["input_path"]):
            raise FileNotFoundError(f'InputPath が存在しません: {p["input_path"]}')

        # ディレクトリではなくファイルであること
        if not os.path.isfile(p["input_path"]):
            raise ValueError(f'InputPath はファイルではありません: {p["input_path"]}')

        # LAS ファイルかどうか
        ext = os.path.splitext(p["input_path"])[1].lower()
        if ext not in [".las"]:
            raise ValueError(f'LAS ファイルではありません: {p["input_path"]}（拡張子: {ext}）')
        
        return p

    # -----------------------------
    # エントリポイント
    # -----------------------------
    def __call__(self, content, attribute, properties):
        """
        FlowFile の content/attribute/properties を受け取り、LAS→DEM/画像変換処理を実行する。

        NiFi から渡される入力を前提としつつ、直接 Python からも呼び出せるようになっている。
        LAS を読み込み、指定した解像度・ラスタ種別に従って DEM と RGB を生成し、
        OpenCV 画像(または NPY)のバイト列と更新済み属性を返す。

        :param content: FlowFile のコンテンツ(本処理では未使用でよい想定)
        :type content: bytes または任意
        :param attribute: FlowFile の属性を格納した辞書。処理結果のメタ情報が追記される
        :type attribute: dict(str, str)
        :param properties: プロセッサプロパティを格納した辞書
        :type properties: dict(str, Any)

        :return: (出力コンテンツのバイト列, 更新済み属性辞書)
        :rtype: tuple(bytes, dict(str, str))

        :raises ValueError: プロパティが不正、またはグリッドサイズが0以下などの場合に例外をスローする。
        :raises FileNotFoundError: 入力 LAS ファイルが存在しない場合に例外をスローする。
        :raises RuntimeError: PNG エンコードに失敗した場合に例外をスローする。
        :raises Exception: リーダや座標変換、ファイル書き込みなどで予期しないエラーが発生した場合に例外をスローする。
        """
        p = self.parse_properties(properties)

        # 共通Reader利用
        reader = get_reader(p["input_path"])
        header = reader.get_header(p["input_path"])
        src_crs = header.parse_crs()
        dst_epsg = src_crs.to_epsg() if src_crs else None

        res = p["res_mm"] / 1000.0
        alpha = p["alpha_mm"] / 1000.0

        xmin, xmax, ymin, ymax = _compute_bounds(reader, p["input_path"], header)

        nx = int(math.ceil((xmax - xmin) / res))
        ny = int(math.ceil((ymax - ymin) / res))
        if nx <= 0 or ny <= 0:
            raise ValueError("空のグリッド（範囲と解像度を確認）")

        minZ = np.full((ny, nx), np.inf, np.float64)
        sumZ = np.zeros((ny, nx), np.float64)
        sumR = np.zeros((ny, nx), np.uint32)
        sumG = np.zeros((ny, nx), np.uint32)
        sumB = np.zeros((ny, nx), np.uint32)
        cnt = np.zeros((ny, nx), np.uint32)
        mask = np.ones((ny, nx), np.uint8)

        total_pts = 0
        # Intensity プロダクトのときだけ XYZI 構成でチャンクを読む。
        if p["kind"] == "INTENSITY":
            chunk_iter = reader.iter_chunks(
                p["input_path"],
                chunk_points=p["chunk_points"],
                xyz_columns="XYZI",  # [X, Y, Z, Intensity] 想定
            )
        else:
            chunk_iter = reader.iter_chunks(
                p["input_path"],
                chunk_points=p["chunk_points"],
            )

        for ch in chunk_iter:
            arr = np.asarray(ch)
            x = arr[:, 0]
            y = arr[:, 1]
            z = arr[:, 2]

            ix = np.floor((x - xmin) / res).astype(np.int64, copy=False)
            iy = np.floor((ymax - y) / res).astype(np.int64, copy=False)

            r, g, b = _rgb_from_chunk(ch)
            _accumulate(ix, iy, z, r, g, b, minZ, alpha, sumZ, sumR, sumG, sumB, cnt, mask)
            total_pts += len(ch)

        # 最終化
        valid = cnt > 0
        dem = np.full((ny, nx), -9999.0, np.float32)
        dem[valid] = (sumZ[valid] / cnt[valid]).astype(np.float32)

        R = np.zeros((ny, nx), np.uint8)
        G = np.zeros_like(R)
        B = np.zeros_like(R)
        R[valid] = (sumR[valid] / cnt[valid]).astype(np.uint8)
        G[valid] = (sumG[valid] / cnt[valid]).astype(np.uint8)
        B[valid] = (sumB[valid] / cnt[valid]).astype(np.uint8)
        rgb_bgr = np.dstack([B, G, R]).astype(np.uint8, copy=False)
        gray = cv2.cvtColor(rgb_bgr, cv2.COLOR_BGR2GRAY)

        # エンコード（フロー図仕様：OpenCVデータ＋属性）
        if p["kind"] == 'Z':
            buf = io.BytesIO()
            np.save(buf, dem)
            content_bytes = buf.getvalue()
            content_type = 'application/x-npy'
            kind_disp = '標高（DEM）'
            kind_out = 'DEM'
        elif p["kind"] == 'RGB':
            ok, enc = cv2.imencode('.png', rgb_bgr)
            if not ok: raise RuntimeError('PNGエンコード失敗(RGB)')
            content_bytes = enc.tobytes()
            content_type = 'image/png'
            kind_disp = 'カラー画像（RGB）'
            kind_out = 'RGB'
        else:
            ok, enc = cv2.imencode('.png', gray)
            if not ok: raise RuntimeError('PNGエンコード失敗(Intensity)')
            content_bytes = enc.tobytes()
            content_type = 'image/png'
            kind_disp = '反射強度（Intensity）'
            kind_out = 'Int'

        # アフィン（GDAL）
        transform = Affine.translation(xmin, ymax) * Affine.scale(res, -res)
        a, b, c, d, e, f = transform.to_gdal()

        attribute.update({
            'product.kind_display':kind_disp, 
            'product.kind':kind_out, 
            'width':str(nx), 
            'height':str(ny),
            'transform.gdal': json.dumps([a, b, c, d, e, f]),
            'crs.epsg': str(dst_epsg),
            'res.mm': str(p["res_mm"]), 
            'alpha.mm':str(p["alpha_mm"]),
            'points.total': f'{total_pts:,}', 
            'in.path':p["input_path"],
            'content.type': content_type})
        return content_bytes, attribute
