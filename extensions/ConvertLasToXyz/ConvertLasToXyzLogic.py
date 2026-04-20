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

import os, math, time

from importlib import import_module
np = import_module("numpy")

pointcloud_reader = import_module("pointcloud.common.io.reader.factory")
get_reader = pointcloud_reader.get_reader

pointcloud_writer = import_module("pointcloud.common.io.writer.factory")
get_writer = pointcloud_writer.get_writer

numba = import_module("numba")
jit = numba.jit
u1 = numba.u1
u2 = numba.u2
f8 = numba.f8
b1 = numba.b1


# =========================================================
# Numba kernels
# =========================================================
@jit(u1[:](u2[:]), nopython=True, cache=True)
def _u16_to_u8(v16):
    n = v16.shape[0]
    out = np.empty(n, dtype=np.uint8)
    for i in range(n):
        out[i] = u1(v16[i] >> 8)
    return out


@jit(b1(f8[:]), nopython=True, cache=True)
def _isfinite_all(arr):
    n = arr.shape[0]
    for i in range(n):
        if not math.isfinite(arr[i]):
            return False
    return True


# =========================================================
# Utility
# =========================================================
def _require(path: str):
    if not path or not os.path.isfile(path):
        raise ValueError(f"InputPath は既存の .las または .laz ファイルを指定してください。: {path}")
    return path


def _ensure_dir(p: str) -> None:
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)


def _fmt_delim(name: str) -> str:
    name = str(name).lower()
    if name == "comma": return ","
    return " "


def _process_channel(chan0, do_downcast):
    """RGB1チャンネルを 8bit/16bit に応じて適切処理する統合関数."""

    if chan0 is None:
        return None

    dt = chan0.dtype

    # 8bit入力はそのまま
    if dt == np.uint8:
        return chan0

    # 16bit入力
    if dt == np.uint16:
        if do_downcast:
            # 16bit → 8bitへ縮約
            return _u16_to_u8(chan0)
        else:
            # 16bitのまま保持
            return chan0

    # 上記以外の型は uint16にクリップ変換
    ch16 = np.clip(chan0, 0, 65535).astype(np.uint16, copy=False)
    if do_downcast:
        return _u16_to_u8(ch16)
    else:
        return ch16


# =========================================================
# Main Logic
# =========================================================
class ConvertLasToXyzLogic:

    def parse_properties(self, props: dict) -> dict:
        p = {}
        p["input_path"] = _require(str(props.get("INPUT_PATH", "")).strip())
        p["output_path"] = str(props.get("OUTPUT_PATH", "")).strip()
        if not p["output_path"]:
            raise ValueError("OUTPUT_PATH が未設定です。")

        p["output_format"] = str(props.get("OUTPUT_FORMAT", "XYZ")).upper()
        p["delimiter"] = _fmt_delim(props.get("DELIMITER", "space"))
        p["decimals"] = int(str(props.get("DECIMALS", "3")))

        p["include_header"] = str(props.get("INCLUDE_HEADER", "false")).lower() == "true"

        # -------------------------------
        # OUTPUT_COLUMNS
        # -------------------------------
        cols_raw = str(props.get("OUTPUT_COLUMNS", "")).strip().upper()
        if not cols_raw:
            raise ValueError("OutputColumns が未設定です。XYZIRGBT の任意並びを指定してください。")

        valid_chars = set("XYZIRGBT")
        filtered = []
        invalid = set()

        for ch in cols_raw:
            if ch in valid_chars:
                filtered.append(ch)
            elif not ch.isspace():
                invalid.add(ch)

        if invalid:
            raise ValueError(
                f"OutputColumns に不正な文字が含まれています: {''.join(sorted(invalid))}  (指定={cols_raw})")

        if not filtered:
            raise ValueError("OUTPUT_COLUMNS には XYZIRGBT のいずれかを含めてください。")

        seen = set()
        dup = set()
        for ch in filtered:
            if ch in seen:
                dup.add(ch)
            else:
                seen.add(ch)
        if dup:
            raise ValueError(f"OutputColumns に重複した文字があります。: {''.join(sorted(dup))}  (指定={cols_raw})")

        for axis in "XYZ":
            if axis not in filtered:
                raise ValueError(f"OutputColumns に {axis} が含まれていません。XYZ は必須です。 (指定={cols_raw})")

        if len(filtered) > 8:
            raise ValueError(f"OutputColumns は最大8文字まで入力できます。: {cols_raw}")

        p["columns"] = "".join(filtered)

        p["chunk_points"] = int(str(props.get("CHUNK_POINTS", "1000000")))
        p["flush_bytes"] = int(float(str(props.get("FLUSH_BYTES", "0") or "0")) * 1024 * 1024)

        p["strict_mode"] = str(props.get("STRICT_MODE", "false")).lower() == "true"
        p["rgb_downcast"] = str(props.get("RGB_DOWNCAST", "true")).lower() == "true"
        return p

    def __call__(self, byte_data, attribute, properties):
        t0 = time.time()
        p = self.parse_properties(properties)

        reader_obj = get_reader(p["input_path"])
        reader = reader_obj() if isinstance(reader_obj, type) else reader_obj

        out_path = p["output_path"]
        _ensure_dir(out_path)

        # ======================================================
        # OUTPUT_FORMAT（XYZ/CSV）で拡張子を強制上書き
        # ======================================================
        root, ext = os.path.splitext(out_path)

        if p["output_format"] == "XYZ":
            out_path = root + ".xyz"
        elif p["output_format"] == "CSV":
            out_path = root + ".csv"

        writer = get_writer(
            p["output_format"],
            path=out_path,
            delimiter=p["delimiter"],
            decimals=p["decimals"],
            include_header=p["include_header"],
            columns=p["columns"],
        )

        writer.open(out_path)

        total_points = 0
        limit_bytes = p["flush_bytes"]
        cols_spec = p["columns"].upper()

        chunks = reader.iter_chunks(p["input_path"], chunk_points=p["chunk_points"], xyz_columns=cols_spec)

        for chunk_idx, arr in enumerate(chunks, 1):
            t1 = time.time()

            if arr.ndim == 1:
                arr = arr.reshape(1, -1)

            if arr.shape[1] != len(cols_spec):
                raise ValueError(
                    f"LASリーダから返された列数が OutputColumns と一致しません。期待={len(cols_spec)} 列, 実際={arr.shape[1]} 列, OutputColumns={cols_spec}")

            x0 = y0 = z0 = None
            inten0 = r0 = g0 = b0 = t0_arr = None

            for idx, ch in enumerate(cols_spec):
                col = arr[:, idx]
                if ch == "X":
                    x0 = col
                elif ch == "Y":
                    y0 = col
                elif ch == "Z":
                    z0 = col
                elif ch == "I":
                    inten0 = col
                elif ch == "R":
                    r0 = col
                elif ch == "G":
                    g0 = col
                elif ch == "B":
                    b0 = col
                elif ch == "T":
                    t0_arr = col

            if x0 is None or y0 is None or z0 is None:
                raise ValueError(
                    f"LASデータから X/Y/Z のいずれかが取得できませんでした。chunk={chunk_idx}, OutputColumns={cols_spec}")

            if p["strict_mode"]:
                if not (_isfinite_all(x0) and _isfinite_all(y0) and _isfinite_all(z0)):
                    raise ValueError("XYZ に非有限値(NaN,Inf)が含まれています。")

            x = x0.astype(np.float64, copy=False) if x0.dtype != np.float64 else x0
            y = y0.astype(np.float64, copy=False) if y0.dtype != np.float64 else y0
            z = z0.astype(np.float64, copy=False) if z0.dtype != np.float64 else z0
            inten = inten0.astype(np.uint32, copy=False) if inten0 is not None else None

            # RGB処理（新しい関数で安全に処理）
            R = _process_channel(r0, p["rgb_downcast"])
            G = _process_channel(g0, p["rgb_downcast"])
            B = _process_channel(b0, p["rgb_downcast"])

            T = t0_arr.astype(np.float64, copy=False) if t0_arr is not None and t0_arr.dtype != np.float64 else t0_arr

            colmap = {"X": x, "Y": y, "Z": z, "I": inten, "R": R, "G": G, "B": B, "T": T}

            out_cols = []
            for c in cols_spec:
                arr_col = colmap.get(c)
                if arr_col is None:
                    raise ValueError(
                        f"LASデータから {c} 列を取得できませんでした。OutputColumns={cols_spec}, chunk={chunk_idx}")
                out_cols.append(arr_col)

            n_rows = out_cols[0].shape[0]
            total_nbytes = sum(col.nbytes for col in out_cols)
            row_bytes = int(total_nbytes // max(1, n_rows))
            nb_per_block = max(1,
                               int(limit_bytes // row_bytes)) if limit_bytes and row_bytes > 0 and total_nbytes > limit_bytes else n_rows

            wrote_this_chunk = 0
            for beg in range(0, n_rows, nb_per_block):
                end = min(n_rows, beg + nb_per_block)
                block_cols = [c[beg:end] for c in out_cols]
                block_arr = np.column_stack(block_cols)
                writer.write_points(block_arr)
                wrote_this_chunk += (end - beg)

            writer.flush()

            total_points += wrote_this_chunk

        writer.flush()
        writer.close()

        attribute.update({
            "in.format": "LAS",
            "out.format": p["output_format"],
            "out.points": str(total_points),
            "out.path": out_path,
            "elapsed.sec": f"{time.time() - t0:.3f}",
        })
        return b"", attribute
