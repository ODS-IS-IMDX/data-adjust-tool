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
PLY に書き出すライタ。
"""

import os
import numpy as np

try:
    import open3d as o3d
except Exception:
    o3d = None

from pointcloud.common.io.writer.base_writer import IPointCloudWriter


class PlyPointCloudWriter(IPointCloudWriter):
    """
    PLY 書き出しライタ。

    - 小規模データ:
        write(path, array) で一発書き出し。
    - ストリーミング利用:
        open(path) → write_points(array) を複数回 → close()
        内部バッファにためて、close() 時にまとめて1ファイルを書き出す実装。
        （PLY も基本的にストリーム前提ではないため）
    """

    def __init__(self) -> None:
        # ストリーミング用の出力パス
        self._path: str | None = None
        # ストリーミング時に append していくバッファ
        self._buffers: list[np.ndarray] = []

    # ==============================
    # 一括書き出し API
    # ==============================
    def write(self, path: str, array: np.ndarray) -> None:
        """
        array 全体を指定パスの PLY に一括書き出しする。
        ストリーミングを使わないシンプルなケース向け。
        """
        if o3d is None:
            raise RuntimeError("open3d が利用できません。PLY出力には open3d が必要です。")

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        # 座標（必須）: 先頭3列を XYZ とみなす
        pts = array[:, :3]
        pc = o3d.geometry.PointCloud()
        pc.points = o3d.utility.Vector3dVector(pts.astype(float))

        # 4〜6列目に RGB が入っている想定（あれば色も付与）
        if array.shape[1] >= 6:
            colors = array[:, 3:6].astype(float)
            # 0-255 の場合は 0-1 に正規化
            if colors.max() > 1.5:
                colors = colors / 255.0
            pc.colors = o3d.utility.Vector3dVector(colors)

        # PLY 書き出し（バイナリ）
        o3d.io.write_point_cloud(path, pc, write_ascii=False)

    # ==============================
    # ストリーミング API
    # ==============================
    def open(self, path: str) -> None:
        """
        ストリーミング書き出し開始。
        - path を覚えておき、write_points で受け取った配列を
          内部バッファに溜める。
        - 実ファイル書き出しは close() 時に一度だけ行う。
        """
        self._path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._buffers = []

    def write_points(self, array: np.ndarray) -> None:
        """
        ストリーミング用 1チャンク分の点群をバッファへ追加する。
        - PLY も PCD 同様、基本は一括書き出しなので、
          ここではメモリ上に溜め込むだけにしておく。
        """
        if self._path is None:
            raise RuntimeError("PlyPointCloudWriter.open() が呼ばれていません。")

        if array is None or array.size == 0:
            return

        # 呼び出し側で array が再利用されても安全なように copy して保持
        self._buffers.append(np.array(array, copy=True))

    def flush(self) -> None:
        """
        ストリーミングバッファのフラッシュ。
        - 現状の設計では、close() でしか実ファイル書き出しを行わないため、
          flush() は no-op として実装しておく。
        """
        return

    def close(self) -> None:
        """
        ストリーミング書き出しの終了処理。
        - バッファに溜まっている配列を vstack して 1 本の PLY として書き出す。
        """
        if self._path is None:
            return  # open されていない場合は何もしない

        try:
            if not self._buffers:
                # 1度も write_points されていない場合は何も書かない
                return

            # 全チャンクを結合
            all_points = np.vstack(self._buffers)
            # 一括書き出しロジックを再利用
            self.write(self._path, all_points)
        finally:
            # 後片付け
            self._buffers = []
            self._path = None
