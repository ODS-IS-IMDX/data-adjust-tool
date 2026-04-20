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
LAS / LAZ を読み込むリーダ。
laspy に依存する。
大容量ファイル対応のため iter_chunks() を実装。
"""

import numpy as np
import laspy
from pointcloud.common.io.reader.base_reader import IPointCloudReader


class LasPointCloudReader(IPointCloudReader):
    """
    LAS/LAZ 読み込みリーダ。
    laspy を使って点群を読み込み、XYZ_COLUMNS に従った ndarray を返す。
    """

    def __init__(self):
        # 今は特に状態を持たないので何もしない
        pass

    def iter_chunks(
        self,
        path: str,
        chunk_points: int = 1_000_000,
        xyz_columns: str = "XYZRGB",
        **kwargs,
    ) -> np.ndarray:
        """
        LAS ファイルをチャンク単位で読み取るジェネレータ。
        xyz_columns に基づいて出力列を構成する。

        chunk_points, xyz_columns は NiFi ロジックから keyword 引数で渡される前提。
        **kwargs は将来の拡張用に受けるだけで無視している。
        """
        with laspy.open(path) as las:
            for chunk in las.chunk_iterator(chunk_points):
                yield self._extract_columns(chunk, xyz_columns)

    def read(self, path: str, **kwargs) -> np.ndarray:
        """
        一括読み込み（チャンクなし）。
        NiFi 側からは xyz_columns="XYZRGB" のように keyword で渡される想定。

        kwargs:
            xyz_columns: 取り出したい列構成 (既定 "XYZRGB")
        """
        xyz_columns = kwargs.get("xyz_columns", "XYZRGB")
        las = laspy.read(path)
        return self._extract_columns(las, xyz_columns)

    def _extract_columns(self, las, xyz_columns: str = "XYZRGB") -> np.ndarray:
        """
        laspy.LasData から指定された列構成に従って配列を抽出。
        高速性を維持するため、必要な列だけをまとめて結合する。
        """
        dims = set(las.point_format.dimension_names)

        # まず主要列を取り出しておく（copyしない）
        x, y, z = las.x, las.y, las.z
        inten = las.intensity if "intensity" in dims else None
        red = las.red if "red" in dims else None
        green = las.green if "green" in dims else None
        blue = las.blue if "blue" in dims else None
        gps = las.gps_time if "gps_time" in dims else None

        # 参照マップ
        colmap = {"X": x, "Y": y, "Z": z, "I": inten, "R": red, "G": green, "B": blue, "T": gps}

        # NiFiプロパティの列順に従って必要列だけを選ぶ
        cols = []
        for c in xyz_columns.upper():
            arr = colmap.get(c)
            # 要求された列が LAS に無い場合は、ここで早めにエラーにしておくか、
            # 現状どおり「None は飛ばす」かは方針次第。
            if arr is None:
                # 今は ConvertLasToXyzLogic 側で列数チェックしてくれるので、
                # ここでは「飛ばす」動作を維持している。
                continue
            cols.append(arr)

        if not cols:
            raise ValueError(f"指定列に対応するデータが存在しません: {xyz_columns}")

        return np.vstack(cols).T.astype(np.float64, copy=False)

    def get_header(self, path: str):
        """
        LAS/LAZ ファイルのヘッダを laspy 経由で取得する。
        共通I/Fとして提供（他形式は未実装で構わない）。
        """
        try:
            with laspy.open(path) as lfile:
                return lfile.header
        except Exception as e:
            raise ValueError(f"LASヘッダの取得に失敗しました: {path} ({e})")
