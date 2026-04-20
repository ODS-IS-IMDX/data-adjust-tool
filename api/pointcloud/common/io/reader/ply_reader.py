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
PLY を読み込むリーダ。
open3d に依存する。
"""

import numpy as np

try:
    import open3d as o3d
except Exception:
    o3d = None

from pointcloud.common.io.reader.base_reader import IPointCloudReader


class PlyPointCloudReader(IPointCloudReader):
    """
    PLY (Polygon File Format / Point Cloud) ファイルを読み込むリーダクラス。

    - open3d.io.read_point_cloud() を使って PLY を読み込む。
    - 返り値は (N, 3) または (N, 6) の ndarray。
      - 前半3列: XYZ 座標 (float64)
      - 後半3列: RGB カラー (float64, 0〜1 の正規化値)
    """

    def read(self, path: str) -> np.ndarray:
        """
        PLY ファイルを一括で読み込み、NumPy 配列に変換して返す。

        Parameters
        ----------
        path : str
            入力 PLY ファイルのパス。

        Returns
        -------
        np.ndarray
            形状 (N, 3) または (N, 6) の配列。
            - XYZ のみ: (N, 3)
            - XYZ + RGB: (N, 6)
        """
        if o3d is None:
            raise RuntimeError("open3d が利用できません。PLY を読むには open3d をインストールしてください。")

        # PLY を open3d で読み込み
        pc = o3d.io.read_point_cloud(path)

        # 座標を float64 の (N, 3) 配列に変換
        pts = np.asarray(pc.points, dtype=np.float64)

        # カラーが付いている場合は (N, 3) を横結合して (N, 6) にする
        if pc.has_colors():
            cols = np.asarray(pc.colors, dtype=np.float64)
            arr = np.hstack([pts, cols])
        else:
            arr = pts

        return arr
