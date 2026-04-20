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

import io, json, os
from typing import Any, Dict, List, Tuple
from importlib import import_module

cv2 = import_module("cv2")
np = import_module("numpy")
rasterio = import_module("rasterio")
Affine = import_module("affine").Affine
CRS = import_module("pyproj").CRS


def _ensure_dir(path: str) -> None:
    """
    指定パスの親ディレクトリを作成するユーティリティ。
    :param path: ファイルパス
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)


def _affine_from_gdal_list(gdal_list: List[float]) -> Affine:
    """
    GDAL の 6要素配列から Affine オブジェクトを生成する。
    :param gdal_list: [a, b, c, d, e, f]
    :return: Affine 変換
    """
    a, b, c, d, e, f = [float(x) for x in gdal_list]
    return Affine.from_gdal(a, b, c, d, e, f)


def _pick_crs(attrs: Dict[str, Any], out_epsg: int | None) -> CRS:
    """
    出力 CRS を決定するユーティリティ。
    優先度: プロパティ OUT_EPSG > 属性 crs.epsg

    :param attrs: FlowFile 属性
    :param out_epsg: プロパティから渡された EPSG（任意）
    :return: pyproj.CRS オブジェクト
    """
    if out_epsg:
        return CRS.from_epsg(int(out_epsg))
    epsg_attr = attrs.get('crs.epsg')
    if epsg_attr not in (None, ""):
        return CRS.from_epsg(int(epsg_attr))
    raise ValueError("CRS が未特定（OutputEPSG も crs.epsg もありません）。")


def _bands_dtype(arr: np.ndarray) -> Tuple[int, str]:
    """
    配列からバンド数と dtype を判定する。

    :param arr: 画像配列 (H,W) or (3,H,W)
    :return: (バンド数, dtype文字列)
    """
    if arr.ndim == 2:
        return 1, str(arr.dtype)
    if arr.ndim == 3 and arr.shape[0] == 3:
        return 3, str(arr.dtype)
    raise ValueError(f"配列shape不正: {arr.shape}")


def _write_geotiff(path: str, array: np.ndarray, transform: Affine, crs: CRS,
                   compress: str, nodata: float | None, photometric: str | None) -> None:
    """
    rasterio を用いて GeoTIFF を書き出すユーティリティ。

    :param path: 出力ファイルパス
    :param array: 画像配列 (H,W) or (3,H,W)
    :param transform: Affine 変換
    :param crs: 出力 CRS
    :param compress: 圧縮方式（DEFLATE/LZW/ZSTD/NONE）
    :param nodata: NoData 値（DEM用）
    :param photometric: フォトメトリック解釈（RGB のとき 'RGB'）
    """
    profile = {
        "driver": "GTiff",
        "height": array.shape[-2],
        "width": array.shape[-1],
        "count": 1 if array.ndim == 2 else 3,
        "dtype": array.dtype,
        "transform": transform,
        "crs": crs
    }
    if compress and compress.upper() != 'NONE':
        profile["compress"] = compress.upper()
    if photometric:
        profile["photometric"] = photometric

    _ensure_dir(path)

    with rasterio.open(path, "w", **profile) as dst:
        if array.ndim == 2:
            dst.write(array, 1)
            if nodata is not None and np.issubdtype(array.dtype, np.floating):
                dst.nodata = nodata
        else:
            dst.write(array[0], 1)
            dst.write(array[1], 2)
            dst.write(array[2], 3)

def _compose_basename(attrs: Dict[str, Any], default_base: str, kind: str) -> str:
    """
    FlowFile属性と種別コードから、GeoTIFF のベースファイル名を組み立てる

    属性に含まれる「res.mm」「alpha.mm」を参照し、
    それぞれが存在する場合は "Res{res}mm" / "Alpha{alpha}mm" の形でサフィックスに付与する。
    そのうえで、種別コード(kind)に応じて "_DEM" / "_RGB" / "_Int" を末尾に追加して返す。

    :param attrs: FlowFile属性。res.mm / alpha.mm を取得するために参照する。
    :param default_base: プロパティ BASENAME から渡されるベースファイル名。
    :param kind: ラスタ種別コード。"DEM" / "RGB" / "Int" のいずれかを想定する。
    :return: 解像度・アルファ・種別サフィックスを付与したベースファイル名。
    """
    res = attrs.get('res.mm')
    alp = attrs.get('alpha.mm')

    # 共通の suffix をまず作る
    suffix_parts = []
    if res:
        suffix_parts.append(f"Res{res}mm")
    if alp:
        suffix_parts.append(f"Alpha{alp}mm")
    suffix = "_" + "_".join(suffix_parts) if suffix_parts else ""

    if kind == 'DEM':
        return f"{default_base}{suffix}_DEM"
    elif kind == 'RGB':
        return f"{default_base}{suffix}_RGB"
    else:  # Int
        return f"{default_base}{suffix}_Int"

class ConvertImageToRasterLogic:
    """
    ConvertImageToRaster の実処理クラス。
    FlowFile のコンテンツ＋属性から GeoTIFF を生成し、出力パスを属性に付与する。
    """

    # -----------------------------
    # プロパティ解析
    # -----------------------------
    def parse_properties(self, attribute: Dict[str, Any], properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        必須/任意プロパティと属性を検証し、型変換・既定値適用した辞書を返す。

        - OUTPUT_PATH（必須: 出力先ディレクトリ）
        - BASENAME（必須: ファイル名ベース。kind/res/alpha に応じてサフィックスを付与）
        - OUT_EPSG（任意）
        - COMPRESSION（任意）

        併せて FlowFile 属性:
        - product.kind（必須: DEM/RGB/Int）
        - transform.gdal（必須: GDAL 6要素）
        - res.mm / alpha.mm（任意: ファイル名サフィックスに利用）

        :param attribute: FlowFile属性(dict)
        :param properties: プロセッサのプロパティ(dict)
        :return: 解析済みパラメータを格納した dict
        """

        def _req_prop(key: str) -> str:
            v = properties.get(key)
            if v is None or str(v).strip() == "":
                raise ValueError(f"{key} が未設定です。")
            return str(v).strip()

        out_root = _req_prop('OUTPUT_PATH')
        os.makedirs(out_root, exist_ok=True)

        epsg_str = str(properties.get('OUT_EPSG', '')).strip()
        out_epsg = int(epsg_str) if epsg_str and epsg_str != 'None' else None

        comp = str(properties.get('COMPRESSION', 'DEFLATE')).strip()

        kind = attribute.get('product.kind')
        if kind not in ('DEM', 'RGB', 'Int'):
            raise ValueError("product.kind が DEM/RGB/Int のいずれでもありません。")

        if 'transform.gdal' not in attribute:
            raise ValueError("transform.gdal 属性がありません。")

        try:
            gdal_list = json.loads(attribute['transform.gdal'])
            if not (isinstance(gdal_list, (list, tuple)) and len(gdal_list) == 6):
                raise ValueError
            gdal_list = [float(x) for x in gdal_list]
        except Exception:
            raise ValueError("transform.gdal の形式が不正です。（GDAL 6要素の配列が必要）")

        # 出力ファイル名のベース（BASENAME は必須プロパティ）
        base_raw = _req_prop('BASENAME')
        base = _compose_basename(attribute, base_raw, kind)

        return {
            "out_root": out_root,
            "out_epsg": out_epsg,
            "compress": comp,
            "kind": kind,
            "gdal_list": gdal_list,
            "base": base
        }

    # -----------------------------
    # エントリポイント
    # -----------------------------
    def __call__(self, content: bytes, attribute: dict, properties: dict):
        """
        NiFi FlowFile入力を受け取り、画像コンテンツを GeoTIFF に変換して出力する。

        :param content: FlowFile本文（DEM は .npy、RGB/Int は PNG のバイト列）
        :param attribute: FlowFile属性(dict)
        :param properties: プロセッサのプロパティ(dict)
        :return: (空バイト列, 更新後属性dict)
        """
        p = self.parse_properties(attribute, properties)

        if not content:
            raise ValueError("FlowFile コンテンツが空です。")

        # --- 画像復元 ---
        if p["kind"] == 'DEM':
            arr = np.load(io.BytesIO(content))  # (H,W) float32
            if arr.ndim != 2:
                raise ValueError("DEM 配列は (H,W) を想定しています。")
            nodata = -9999.0
            photometric = None
        else:
            buf = np.frombuffer(content, dtype=np.uint8)
            img = cv2.imdecode(
                buf,
                cv2.IMREAD_COLOR if p["kind"] == 'RGB' else cv2.IMREAD_GRAYSCALE
            )
            if img is None:
                raise RuntimeError('PNG デコードに失敗しました。')

            if p["kind"] == 'RGB':
                rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
                arr = np.transpose(rgb, (2, 0, 1))  # (3,H,W)
                photometric = "RGB"
                nodata = None
            else:
                arr = img.astype(np.uint8, copy=False)  # (H,W)
                photometric = None
                nodata = None

        # --- メタ確定 ---
        transform = _affine_from_gdal_list(p["gdal_list"])
        crs = _pick_crs(attribute, p["out_epsg"])

        out_path = os.path.join(p["out_root"], f"{p['base']}.tif")

        # --- 書出し ---
        _write_geotiff(out_path, arr, transform, crs, p["compress"], nodata, photometric)

        # --- 属性付与 ---
        bands, dtype = _bands_dtype(arr)
        attribute.update({
            'geotiff.path': out_path,
            'bands': str(bands),
            'dtype': dtype,
            'crs.epsg.out': str(crs.to_epsg() or ''),
            'compress': p["compress"]
        })
        return b"", attribute
