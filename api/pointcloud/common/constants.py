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
点群モジュールで共通に使う定数定義。
"""

import os

# サポートするフォーマットの識別子
FMT_XYZ = "xyz"
FMT_CSV = "csv"
FMT_TXT = "txt"
FMT_LAS = "las"
FMT_LAZ = "laz"
FMT_PCD = "pcd"
FMT_PLY = "ply"
FMT_E57 = "e57"

# 標準のXYZカラム
DEFAULT_XYZ_COLUMNS = "XYZRGB"


def get_file_ext(path_or_fmt: str) -> str:
    """
    パス or フォーマット名から「拡張子っぽいもの」を取り出す。
    - 拡張子があればドットなしで返す（.csv → csv）
    - 拡張子がなければ末尾のファイル名だけを小文字で返す

    例:
        "C:/data/pt.xyz"      → "xyz"
        "/tmp/out.CSV"        → "csv"
        "tile.laz"            → "laz"
        "csv"                 → "csv"      ← 形式名で渡されたとみなす
        "C:\\aaa\\bbb\\ccc"   → "ccc"      ← 拡張子なしなので末尾名を返す
        "noext"               → "noext"    ← そのまま
    """
    # まず拡張子を取る
    _, ext = os.path.splitext(path_or_fmt)
    if ext:
        return ext.lstrip(".").lower()

    # 拡張子が無い場合 → ファイル名だけにして返す
    base = os.path.basename(path_or_fmt)  # ディレクトリを落とす
    if not base:  # 念のため
        return ""
    return base.lower()
