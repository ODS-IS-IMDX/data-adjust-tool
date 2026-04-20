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

"""
フォーマット変換で横断的に使う小物関数群。
Numbaで高速化するものはここに寄せる。
"""

import os
import numpy as np
from typing import Optional  # 追加: 型注釈のため

# 追加: pyprojは実行環境によっては未導入の可能性があるので、任意依存にする
try:
    from pyproj import CRS
    _PYPROJ_OK = True
except Exception:
    CRS = None  # 型的には未解決でよい（実行時には使わないように守る）
    _PYPROJ_OK = False

try:
    from numba import jit, f8, u2, b1
    _NUMBA_OK = True
except Exception:
    _NUMBA_OK = False
    def jit(*args, **kwargs):
        def _wrap(fn):
            return fn
        return _wrap
    f8 = u2 = b1 = None


def sniff_format_from_path(path: str) -> str:
    """
    ファイルパスからおおよその点群フォーマットを推測する。
    拡張子ベースなので100%ではない。
    """
    from pointcloud.common.constants import EXT_TO_FMT
    ext = os.path.splitext(path)[1].lower()
    return EXT_TO_FMT.get(ext, "")


@jit(f8[:](f8[:], f8, f8), nopython=True)
def scale_and_clip_to_range(values, max_val, scale):
    """
    values を scale 倍して 0..max_val にクリップする。
    LASのRGB変換などで使う。
    """
    n = values.size
    out = np.empty(n, dtype=np.float64)
    for i in range(n):
        v = values[i] * scale
        if v < 0.0:
            v = 0.0
        elif v > max_val:
            v = max_val
        out[i] = v
    return out


def is_geographic(crs_obj: Optional["CRS"], epsg_guess: Optional[int] = None) -> bool:
    """
    地理座標系(Geographic CRS)かどうかを堅牢に判定する。

    優先順位:
      1) ヘッダ等から受け取った CRS オブジェクトの .is_geographic を信頼
      2) epsg_guess が与えられていれば CRS.from_epsg(epsg_guess).is_geographic を参照
      3) どちらも不可なら False（=投影とみなす）を返す

    注意:
      - pyproj が未導入の環境では、CRSを安全に扱えないため常に False を返す。
      - 軸の単位名(degree/meter)だけでの判定は行わない（誤判定を避けるため）。
    """
    # pyproj が無い場合は判定不能なので被害の小さい False に倒す
    if not _PYPROJ_OK:
        return False

    # 1) 受け取った CRS を最優先で評価（LASのWKT/GeoTIFF由来を想定）
    try:
        if crs_obj is not None:
            return bool(crs_obj.is_geographic)
    except Exception:
        # 壊れたCRS/WKTでも落ちないように握りつぶす
        pass

    # 2) EPSGのヒントがあればフォールバック
    try:
        if epsg_guess is not None:
            return bool(CRS.from_epsg(int(epsg_guess)).is_geographic)
    except Exception:
        # 不正EPSGや辞書未登録でも落とさない
        pass

    # 3) 最終フォールバック。誤って度扱いにする被害を避けるため False。
    return False
