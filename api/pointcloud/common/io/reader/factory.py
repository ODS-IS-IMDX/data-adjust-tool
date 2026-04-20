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
フォーマット名・拡張子から適切なリーダを返すファクトリ。
NiFi の各プロセッサからはここだけを触る想定。
"""

from pointcloud.common.constants import (
    FMT_XYZ,
    FMT_CSV,
    FMT_TXT,
    FMT_LAS,
    FMT_LAZ,
    FMT_PCD,
    FMT_PLY,
    FMT_E57,
    get_file_ext,  
)
from pointcloud.common.io.reader.xyz_reader import XyzPointCloudReader
from pointcloud.common.io.reader.las_reader import LasPointCloudReader
from pointcloud.common.io.reader.pcd_reader import PcdPointCloudReader
from pointcloud.common.io.reader.ply_reader import PlyPointCloudReader
# E57は後で実装する
# from api.pointcloud.common.io.reader.e57_reader import E57PointCloudReader


def get_reader(fmt_or_path: str):
    """
    - パスが来た場合：拡張子から "xyz" "csv" "las" などを推測する
    - 形式名が来た場合：その文字列を小文字にして使う
    - ディレクトリ込みのパスで拡張子なしの場合：末尾のファイル名を形式名として扱う
    """
    fmt = get_file_ext(fmt_or_path) 

    # テキスト系（xyz / csv / txt）は共通リーダ
    if fmt in (FMT_XYZ, FMT_CSV, FMT_TXT, "txt"):
        return XyzPointCloudReader()

    # LAS / LAZ
    if fmt in (FMT_LAS, FMT_LAZ):
        return LasPointCloudReader()

    # PCD
    if fmt == FMT_PCD:
        return PcdPointCloudReader()

    # PLY
    if fmt == FMT_PLY:
        return PlyPointCloudReader()

    # E57（まだ）
    if fmt == FMT_E57:
        raise NotImplementedError("E57 Reader はまだ実装していない。")

    # ここまで来たら未対応
    raise ValueError(f"対応していないフォーマットです: {fmt_or_path}")
