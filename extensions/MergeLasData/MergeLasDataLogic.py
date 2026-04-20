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

import os, glob, time, uuid
from importlib import import_module
from typing import Tuple
from collections import defaultdict
from pointcloud.common.io.reader.factory import get_reader
from pointcloud.common.io.writer.factory import get_writer

np = import_module("numpy")
jit = import_module("numba").jit
f8 = import_module("numba").float64
i8 = import_module("numba").int64

# ---- ヘッダ署名（Numbaで軽量ハッシュ化） ----
@jit(i8(f8, f8, f8, f8, i8, i8), nopython=False)
def _pack_signature(sx, sy, ox, oy, pfid, ver):

    # 小数は 1e9 スケーリングで整数化（1nm 解像度相当）
    isx = i8(sx * 1_000_000_000.0)
    isy = i8(sy * 1_000_000_000.0)
    iox = i8(ox * 1_000_000_000.0)
    ioy = i8(oy * 1_000_000_000.0)
    sig = (pfid & 0xFFFF) | ((ver & 0xFFFF) << 16)
    sig ^= (isx ^ (isy << 1)) ^ (iox << 2) ^ (ioy << 3)
    return sig


def _header_signature(h) -> Tuple[int, int, float, float, float, float, int]:
    pfid = int(h.point_format.id)
    ver = (h.version.major * 100) + h.version.minor
    sx, sy = float(h.scales[0]), float(h.scales[1])
    ox, oy = float(h.offsets[0]), float(h.offsets[1])
    sig = int(_pack_signature(sx, sy, ox, oy, pfid, ver))
    return (pfid, ver, sx, sy, ox, oy, sig)


class MergeLasDataLogic:
    """MergeLasTiles の実処理である。"""

    # -----------------------------
    # プロパティ解析
    # -----------------------------
    def parse_properties(self, properties: dict) -> dict:
        """
        必須プロパティの存在確認、型変換、既定値の適用を行い辞書で返す。
        """

        def _req(key: str) -> str:
            v = properties.get(key)
            if v is None or str(v).strip() == "":
                raise ValueError(f"{key} が未設定です。")
            return str(v).strip()

        params = {}
        params["input_dir"] = _req("INPUT_DIR")
        params["output_dir"] = _req("OUTPUT_DIR")

        params["header_check"] = str(properties.get("HDR_CHECK", "true")).lower() == "true"
        params["delete_parts"] = str(properties.get("DEL_PARTS", "false")).lower() == "true"

        try:
            params["chunk_write_points"] = int(properties.get("CHUNK_WRITE", "2000000"))
        except Exception:
            raise ValueError("CHUNK_WRITE は整数で指定してください。")
        if params["chunk_write_points"] <= 0:
            raise ValueError("CHUNK_WRITE は 1 以上で指定してください。")

        if not os.path.isdir(params["input_dir"]):
            raise FileNotFoundError(f"INPUT_DIR が存在しません: {params['input_dir']}")

        os.makedirs(params["output_dir"], exist_ok=True)
        return params

    def make_temp_output_path(self, out_dir: str) -> str:
        """
        一時出力に使うUUIDファイルパスを生成する関数。
        - out_dir: 出力ディレクトリの絶対または相対パス
        戻り値:
          例) C:/out/4f2d3a....  （拡張子なし）
        """
        name = uuid.uuid4().hex
        return os.path.join(out_dir, name)

    # -----------------------------
    # エントリポイント
    # -----------------------------
    def __call__(self, content, attribute, properties):
        t0 = time.time()
        p = self.parse_properties(properties)

        # フォルダ内のすべての .part.las を取得
        all_parts = sorted(glob.glob(os.path.join(p["input_dir"], '*.part.las')))
        if not all_parts:
            raise FileNotFoundError(f'.part.las が見つからない: {p["input_dir"]}')

        # '_chunk_' 以前の文字列をキーにグループ化
        groups = defaultdict(list)
        for part in all_parts:
            base = os.path.basename(part)
            tile_key = base.split('_chunk_')[0]  # ← _chunk_ は含めない
            groups[tile_key].append(part)

        merged_tiles = 0
        total_points = 0
        total_parts = 0

        for tile_key, parts in groups.items():
            if not parts:
                continue

            # --- 共通Reader取得 ---
            reader_first = get_reader(parts[0])
            base_hdr = reader_first.get_header(parts[0])
            pfid, ver, sx, sy, ox, oy, sig = _header_signature(base_hdr)

            # ヘッダ検証
            if p["header_check"]:
                for pp in parts[1:]:
                    reader_chk = get_reader(pp)
                    hdr_chk = reader_chk.get_header(pp)
                    pfid2, ver2, sx2, sy2, ox2, oy2, sig2 = _header_signature(hdr_chk)
                    # offsets は LAS ファイルごとに異なることがあるため比較対象から外す
                    if (pfid, ver, sx, sy) != (pfid2, ver2, sx2, sy2):
                        raise ValueError(f'ヘッダ不一致: {pp}  '
                                         f'(pf={pfid2},ver={ver2},scale=({sx2},{sy2}))')

            # 統合
            out_name = f'{tile_key}.las'
            out_path = os.path.join(p["output_dir"], out_name)

            # --- UUID方式で一時ファイル作成 ---
            tmp_path = self.make_temp_output_path(p["output_dir"])

            #   1) 全partを順に読み、ndarrayをリストに貯める
            #   2) 最後に np.vstack で結合
            #   3) out_path の拡張子から Writer を決定し、tmp_path に 1回だけ書き出す
            tile_points = 0
            arrays = []  # ndarray断片の一時バッファ
            try:
                reader_part = get_reader(parts[0])

                # 1) 読み集め（順序は parts の並び × チャンク順）
                for pp in parts:
                    print(f"[DEBUG] start loading {pp}")
                    chunk_index = 0
                    for rec in reader_part.iter_chunks(pp, chunk_points=p["chunk_write_points"]):
                        chunk_index += 1
                        # rec は (N, D) のndarray想定。座標変換・属性改変はしない。
                        arrays.append(rec)
                        tile_points += len(rec)

                        # --- デバッグログ追加 ---
                        print(f"[DEBUG] loaded chunk {chunk_index}: {len(rec)} points "
                              f"from {os.path.basename(pp)} "
                              f"(chunk_write_points={p['chunk_write_points']})")

                    print(f"[DEBUG] finished {pp}, total {chunk_index} chunks read.")

                # 2) 結合（空配列対策も入れる）
                if len(arrays) == 0:
                    concat = np.empty((0, 3), dtype=np.float64)  # 次工程が困らない最小形
                elif len(arrays) == 1:
                    concat = arrays[0]
                else:
                    concat = np.vstack(arrays)

                # 3) ワンショット出力。ヘッダは“完全継承”のみを使用
                writer = get_writer(out_path)
                writer.write(tmp_path, concat, src_header=base_hdr)

                # 原子的置換は行わず、既存ファイルを上書き
                if os.path.exists(out_path):
                    os.remove(out_path)
                os.replace(tmp_path, out_path)
            finally:
                # 一時ファイルの掃除（失敗時も念のため）
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except:
                        pass

            # 断片削除（任意）
            if p["delete_parts"]:
                for pp in parts:
                    try:
                        os.remove(pp)
                    except Exception:
                        pass

            # 属性付与（代表値：直近タイル）
            attribute.update({'tile.key':tile_key, 'merged.points':str(tile_points), 'parts.count':str(len(parts)),
                'header.signature':      f'pf={pfid},ver={ver},scale=({sx},{sy}),offset=({ox},{oy})'})

            merged_tiles += 1
            total_points += tile_points
            total_parts += len(parts)

        attribute.setdefault('merge.elapsed.sec', f'{time.time() - t0:.3f}')
        attribute.update({'merge.tiles.count':str(merged_tiles), 'merge.parts.total':str(total_parts),
            'merge.points.total':             str(total_points), })
        return b'', attribute

