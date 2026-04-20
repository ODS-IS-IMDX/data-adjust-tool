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
全ての点群ライタのベースクラス。
"""

from abc import ABC, abstractmethod
import numpy as np


class IPointCloudWriter(ABC):
    """
    点群を書き出すためのベースインターフェース。
    - 小さなデータは write(path, array) だけを使う一括書き出し
    - 大きなデータは open/write_points/flush/close を使うストリーミング書き出し
    """

    @abstractmethod
    def write(self, path: str, array: np.ndarray) -> None:
        """
        array を指定パスに一括書き出しするための最小インターフェース。
        ストリーミング不要な簡易Writer向け。
        """
        raise NotImplementedError()

    # --- 以下はストリーミングWriter用の拡張インターフェース ---
    def open(self, path: str) -> None:
        """
        ストリーミング書き出し開始用。
        既定実装は何もしない(no-op)。
        ストリーム対応Writerだけオーバーライドすればよい。
        """
        pass

    def write_points(self, array: np.ndarray) -> None:
        """
        ストリーミング書き出し時に、1チャンク分の点群を追加する。
        既定実装は write(path, array) を想定していないので何もしない(no-op)。
        """
        pass

    def flush(self) -> None:
        """
        バッファをフラッシュする。
        既定実装は何もしない(no-op)。
        """
        pass

    def close(self) -> None:
        """
        リソースを解放する。
        既定実装は何もしない(no-op)。
        """
        pass
