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
Writer のファクトリ。
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
from pointcloud.common.io.writer.las_writer import LasPointCloudWriter
from pointcloud.common.io.writer.pcd_writer import PcdPointCloudWriter
from pointcloud.common.io.writer.ply_writer import PlyPointCloudWriter
from pointcloud.common.io.writer.xyz_writer import XyzPointCloudWriter


def get_writer(fmt_or_path: str, **kwargs):
    """
    フォーマット名または出力パスから適切なWriterを返す。
    kwargs は各Writerにそのまま渡す。
    - "C:/tmp/out.xyz" → xyz → XyzPointCloudWriter(...)
    - "out.csv"        → csv → XyzPointCloudWriter(...)
    - "CSV"            → csv → XyzPointCloudWriter(...)
    - "C:/tmp/out.laz" → laz → LasPointCloudWriter(to_laz=True, ...)
    """
    # 拡張子あり・なし・形式名すべてをここで正規化
    fmt = get_file_ext(fmt_or_path)

    # LAS/LAZ
    if fmt in (FMT_LAS, FMT_LAZ):
        to_laz = (fmt == FMT_LAZ)
        return LasPointCloudWriter(to_laz=to_laz, **kwargs)

    # PCD
    if fmt == FMT_PCD:
        return PcdPointCloudWriter()

    # PLY
    if fmt == FMT_PLY:
        return PlyPointCloudWriter()

    # E57
    if fmt == FMT_E57:
        raise NotImplementedError("E57 Writer はまだ実装していない。")

    # テキスト系（xyz / csv / txt）は同じWriterで行く
    if fmt in (FMT_XYZ, FMT_CSV, FMT_TXT, ""):
        return XyzPointCloudWriter(**kwargs)

    raise ValueError(f"対応していない出力フォーマットです: {fmt_or_path}")
