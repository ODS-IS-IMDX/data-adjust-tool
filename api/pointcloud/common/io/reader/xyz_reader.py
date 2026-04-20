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
XYZ / CSV / TXT などのテキスト点群を読むリーダ。
"""

import numpy as np
from pointcloud.common.io.reader.base_reader import IPointCloudReader


class XyzPointCloudReader(IPointCloudReader):
    """
    最も単純なテキスト点群リーダ。
    - read(path): 従来どおり一括で読み込む（小〜中規模向け）
    - iter_chunks(path): 行数ベースで分割して読む（大規模向け）
    """

    def __init__(
        self,
        delimiter: str = " ",
        strict: bool = False,
        expected_cols: int | None = None,
        chunk_lines: int = 0,
    ):
        """
        :param delimiter: 区切り文字 (" " or ",")
        :param strict: 列数が違ったら例外にするか
        :param expected_cols: 想定する列数（XYZ, XYZRGB, XYZIT...）
        :param chunk_lines: 0 のときは一括読み、>0 のときは iter_chunks() で使う想定
        """
        self.delimiter = delimiter
        self.strict = strict
        self.expected_cols = expected_cols
        self.chunk_lines = int(chunk_lines) if chunk_lines else 0

    # ---------------------------------------------------------
    # ① 従来どおりの「全部読む」版
    # ---------------------------------------------------------
    def read(self, path: str) -> np.ndarray:
        """小さいファイル向け。一発で ndarray を返す。"""
        if self.delimiter == " ":
            arr = np.loadtxt(path)
        else:
            arr = np.loadtxt(path, delimiter=self.delimiter)

        arr = self._postprocess_array(arr)
        return arr

    # ---------------------------------------------------------
    # ② NiFi向けの「チャンクで読む」版
    # ---------------------------------------------------------
    def iter_chunks(self, path: str):
        """
        大きいXYZを行単位で分割して返すジェネレータ。
        各yieldは ndarray（形状はそのチャンク分）。
        """
        if self.chunk_lines <= 0:
            # チャンク指定がない場合は一括読みして1回だけ返す
            yield self.read(path)
            return

        delim = (None if self.delimiter == " " else self.delimiter)
        buf: list[str] = []
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                buf.append(line)
                if len(buf) >= self.chunk_lines:
                    arr = np.loadtxt(buf, delimiter=delim)
                    arr = self._postprocess_array(arr)
                    yield arr
                    buf.clear()
            if buf:
                arr = np.loadtxt(buf, delimiter=delim)
                arr = self._postprocess_array(arr)
                yield arr

    # ---------------------------------------------------------
    # 共通の後処理
    # ---------------------------------------------------------
    def _postprocess_array(self, arr: np.ndarray) -> np.ndarray:
        """列数チェックや 1次元→2次元化などをここに寄せる。"""
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)

        if self.expected_cols is not None:
            if self.strict and arr.shape[1] != self.expected_cols:
                raise ValueError(f"列数が一致しない: 期待={self.expected_cols}, 実際={arr.shape[1]}")
            elif arr.shape[1] > self.expected_cols:
                # 厳密でなければ、余分な列はとりあえず切る
                arr = arr[:, :self.expected_cols]

        return arr
