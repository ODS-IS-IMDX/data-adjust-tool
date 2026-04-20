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
全ての点群リーダのベースクラス。
各フォーマットごとにこれを継承して実装する。
"""

from abc import ABC, abstractmethod
import numpy as np


class IPointCloudReader(ABC):
    """
    点群を読み取って NumPy 配列などの統一形式で返すためのインターフェース。
    ここでは (N, M) 形状の ndarray を基本とする。
    カラム順は呼び出し側で決める。

    - 小さなファイル: read(path, ...) で一括読み込み
    - 大きなファイル: iter_chunks(path, ...) で逐次読み込み
      （デフォルト実装は「1チャンクで全部返す」）
    """

    @abstractmethod
    def read(self, path: str, **kwargs) -> np.ndarray:
        """
        path で指定された点群ファイルを読み取り、ndarray を返す。

        kwargs:
            実装クラスごとに任意:
            - xyz_columns="XYZRGB" のような列指定
            - delimiter="space" などの区切り指定
            - strict=True/False などの挙動指定

        失敗した場合は例外を送出する。
        """
        raise NotImplementedError()

    def iter_chunks(self, path: str, **kwargs):
        """
        逐次読み込み用のジェネレータ。

        既定実装:
            - read(path, **kwargs) で全データを一括読み込み
            - それを1チャンクとして yield するだけ

        大容量対応したいリーダは、このメソッドをオーバーライドして
        chunk_lines やバイナリストリームに応じて複数チャンクを yield する。
        """
        yield self.read(path, **kwargs)
