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
XYZ / CSV に書き出すライタ（ストリーム対応版）。
- open(path) でファイルを開く
- write_points(array) で追記
- close() でクローズ
- 1回で全部書きたいときは write(path, array) もそのまま使える
"""

import os
import numpy as np
from pointcloud.common.io.writer.base_writer import IPointCloudWriter


class XyzPointCloudWriter(IPointCloudWriter):
    def __init__(
        self,
        path: str | None = None,
        delimiter: str = " ",
        decimals: int = 6,
        include_header: bool = False,
        columns: str = "XYZ"
    ):
        # 出力先（openで決めてもいい）
        self.path = path
        # 区切り
        self.delimiter = delimiter
        # 小数桁
        self.decimals = int(decimals)
        # 1行目に列名を出すか
        self.include_header = include_header
        # 列名（"XYZRGB" みたいなやつ）
        self.columns = columns
        # 内部ファイルハンドル
        self._fh = None
        # 最初のチャンクかどうか
        self._first = True

    # --------------------------------------------------
    # 既存互換：一発書き
    # --------------------------------------------------
    def write(self, path: str, array: np.ndarray) -> None:
        """
        旧実装互換。1回で全部書き出すときだけ使う。
        """
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fmt = f"%.{self.decimals}f"
        if self.delimiter == " ":
            np.savetxt(path, array, fmt=fmt)
        else:
            np.savetxt(path, array, fmt=fmt, delimiter=self.delimiter)

    def open(self, path: str) -> None:
        """追記モードで開く（ヘッダは最初のチャンクだけ出す）"""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.path = path
        self._fh = open(path, "w", encoding="utf-8")
        self._first = True

        # 列名に基づいて1行のフォーマット文字列を事前生成する。
        # 例）"%.3f %.3f %.3f %d %d %d" のような形
        fmt_parts = []
        for c in self.columns.upper():
            if c in ("X", "Y", "Z", "T"):  # 座標とGPS time → float
                fmt_parts.append(f"%.{self.decimals}f")
            else:                          # I, R, G, B → integer
                fmt_parts.append("%d")

        # delimiter で結合したフォーマット文字列
        # （例）"%.3f,%.3f,%.3f,%d,%d,%d"
        self._compiled_fmt = self.delimiter.join(fmt_parts)


    def write_points(self, array: np.ndarray) -> None:
        """
        - フォーマット文字列は open() 時にプリコンパイル済み
        """

        if self._fh is None:
            if not self.path:
                raise RuntimeError("XyzPointCloudWriter: path が未設定です。open() を先に呼んでください。")
            self.open(self.path)

        # --- ヘッダ行（最初の1回のみ） ---
        if self._first and self.include_header:
            header = self._make_header_from_columns()
            self._fh.write(header + "\n")

        # --- フォーマット文字列を使って1行ずつ生成 ---
        fmt = self._compiled_fmt   # open() 時に作っておく

        # array.shape = (N, M)
        for row in array:
            # rowごとにフォーマット適用。これが最速。
            line = fmt % tuple(row.tolist())
            self._fh.write(line + "\n")

        self._first = False

    def close(self) -> None:
        """ファイルを閉じる"""
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def flush(self) -> None:
        """
        ファイルバッファを明示的にフラッシュする。
        - ConvertLasToXyzLogic などから hasattr(writer, "flush") 前提で呼ばれる。
        - 下層のファイルオブジェクト(self._fh)があれば、その flush() をそのまま呼び出す。
        """
        if self._fh is not None:
            self._fh.flush()

    # --------------------------------------------------
    def _make_header_from_columns(self) -> str:
        """
        "XYZRGB" → "X Y Z R G B" みたいな感じで列名を作る。
        XYZIT とかも一応作れるようにしておく。
        """
        # だいたいこのパターンしか使わんはずなので素朴にやる
        colmap = {
            "X": "X",
            "Y": "Y",
            "Z": "Z",
            "I": "Intensity",
            "R": "Red",
            "G": "Green",
            "B": "Blue",
            "T": "GpsTime",
            "N": "NIR",
        }
        cols = []
        # "XYZRGB" みたいな連続文字列を3文字ずつ…はややこしいので
        # よく使うやつだけ分岐
        s = self.columns.upper()

        # 典型パターンだけ先に
        if s in ("XYZ", "XYZI", "XYZRGB", "XYZIRGB", "XYZIT", "XYZIRGBT"):
            seq = []
            # XYZ
            seq.extend(["X", "Y", "Z"])
            if "I" in s:
                seq.append("I")
            if "RGB" in s:
                seq.extend(["R", "G", "B"])
            if s.endswith("T"):
                seq.append("T")
            for c in seq:
                cols.append(colmap.get(c, c))
            return (self.delimiter if self.delimiter != " " else " ").join(cols)

        # それ以外は1文字ずつ読む（保険）
        for ch in s:
            cols.append(colmap.get(ch, ch))
        return (self.delimiter if self.delimiter != " " else " ").join(cols)
