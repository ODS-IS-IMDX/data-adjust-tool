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

import os, math, time, shutil
from pointcloud.common.io.reader.factory import get_reader
from pointcloud.common.io.writer.factory import get_writer

class SplitLasDataLogic:
    """SplitLasData の実処理"""

    # -----------------------------
    # プロパティ解析
    # -----------------------------
    def parse_properties(self, properties: dict) -> dict:
        """
        NiFi の properties から必要項目を取り出し、
        既定値の付与・型変換・妥当性検証を行って返す。
        """

        def _req(key: str) -> str:
            # 必須キーを取り出す。空なら即エラーにする。
            v = properties.get(key)
            if v is None or str(v).strip() == "":
                raise ValueError(f"{key} が未設定です。")
            return str(v).strip()

        params = {}
        params["input_path"] = _req("INPUT_PATH")
        params["output_dir"] = _req("OUTPUT_DIR")
        params["split_by"] = str(properties.get("SPLIT_BY", "PointsPerChunk")).strip()

        # 許容値チェック。想定以外は落とす。
        if params["split_by"] not in ("PointsPerChunk", "MaxSizeMB"):
            raise ValueError("SPLIT_BY は PointsPerChunk / MaxSizeMB のいずれかを指定してください。")

        # 数値系。int変換できないものは即エラー。
        try:
            params["chunk_size"] = int(properties.get("CHUNK_SZ", "5000000"))
        except Exception:
            raise ValueError("ChunkSize は整数で指定してください。")
        try:
            params["max_size_mb"] = int(properties.get("MAX_MB", "100"))
        except Exception:
            raise ValueError("MAX_MB は整数で指定してください。")

        if params["chunk_size"] <= 0:
            raise ValueError("CHUNK_SZ は 1 以上で指定してください。")
        if params["max_size_mb"] <= 0:
            raise ValueError("MAX_MB は 1 以上で指定してください。")

        # 入力ファイルの存在確認
        ip = params["input_path"]
        if not os.path.isfile(ip):
            raise FileNotFoundError(f"INPUT_PATH が存在しない: {ip}")
        # .las のみ対象
        ext = os.path.splitext(ip)[1].lower()
        if ext != ".las":
            raise ValueError(f"INPUT_PATH は .las ファイルを指定してください: {ip}")

        # 出力ディレクトリを安全にクリアしてから作成する
        if os.path.exists(params["output_dir"]):
            try:
                shutil.rmtree(params["output_dir"])
            except PermissionError as e:
                # Windowsで開きっぱなし等の可能性があるため警告のみ
                print(f"WARNING: 出力ディレクトリ削除に失敗しました。ファイルが開かれている可能性があります: {e}")
        os.makedirs(params["output_dir"], exist_ok=True)

        return params

    # -----------------------------
    # エントリポイント
    # -----------------------------
    def __call__(self, content, attribute, properties):
        """
        NiFiから実行されるエントリポイント。
        - 入力の .las を単一指定で受け取る（INPUT_PATH）
        - 元ファイルのLASヘッダを読み取り、必要な項目を抽出
        - チャンク分割の閾値を決定
        - 各チャンクを「ヘッダ引き継ぎ」で個別ファイルへ書き出し
        - 座標変換は行わない（今回の要件）
        """
        t0 = time.time()
        p = self.parse_properties(properties)

        # 単一ファイルを直接処理
        f = p["input_path"]
        base = os.path.splitext(os.path.basename(f))[0]

        # Readerをファクトリ経由で取得（既存の設計を踏襲）
        reader = get_reader(f)

        # 入力LASのヘッダを取得（共通I/F利用）
        hdr = reader.get_header(f)

        # 1点あたりのバイト数を取得し、MaxSizeMB指定時のポイント数見積もりに使う
        try:
            point_size_bytes = int(hdr.point_size)
        except Exception:
            point_size_bytes = int(hdr.point_format.size)

        # 分割粒度の決定
        if p["split_by"] == "PointsPerChunk":
            points_per_chunk = p["chunk_size"]
        else:
            est_pts = math.floor((p["max_size_mb"] * 1024 * 1024) / max(point_size_bytes, 1))
            points_per_chunk = max(1, est_pts)

        total_out_points = 0
        total_out_files = 0

        # ここからチャンクループ。recはnumpy配列で返る想定（XYZが先頭3列）
        idx = 1
        for rec in reader.iter_chunks(f, chunk_points=points_per_chunk):
            # 今回は座標変換を行わないため、recへの加工はしない

            out_name = f"{base}_chunk_{idx:04d}.las"
            out_path = os.path.join(p["output_dir"], out_name)

            # --- Writerをファクトリ経由で取得 ---
            writer = get_writer(out_path)
            
            # 一括書き（チャンク単位）
            writer.write(out_path, rec, src_header=hdr)

            # NiFi属性の更新（モニタ・デバッグ用）
            try:
                crs = hdr.parse_crs()
                epsg = crs.to_epsg() if crs else ""
                ver = f"{hdr.version.major}.{hdr.version.minor}"
                pdrf = str(hdr.point_format.id)
            except Exception:
                epsg, ver, pdrf = "", "", ""

            attribute.update({
                "chunk.index": f"{idx:04d}",
                "chunk.points": str(len(rec)),
                "parent.filename": os.path.basename(f),
                "las.epsg": str(epsg),
                "split.by": p["split_by"],
                "points.per.chunk": str(points_per_chunk),
                "las.version": ver,
                "las.pdrf": pdrf,
            })

            total_out_points += len(rec)
            total_out_files += 1
            idx += 1

        attribute.update({
            "out.points.total": str(total_out_points),
            "out.files.total": str(total_out_files),
            "elapsed.sec": f"{time.time() - t0:.3f}",
        })
        return b"", attribute
