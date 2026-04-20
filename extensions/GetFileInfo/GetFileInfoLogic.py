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

import os
import json
import ctypes
import platform
from importlib import import_module

ImageModule = import_module("PIL.Image")


class GetFileInfoLogic:
    """
    プロパティで設定されたフォルダパス、もしくはファイルパスからファイル情報を取得するクラス。
    フォルダバスの場合はInput Path配下のファイル情報を再帰的に探索して出力する。
    
    ・ファイル一覧は拡張子フィルタ(INCLUDE_EXTENSIONS)に従って取得する。
    ・ファイルサイズは INCLUDE_FILE_SIZE プロパティに従って取得する。
    ・画像ファイル(既定の拡張子セット)については、ピクセルサイズ(幅・高さ)の取得をベストエフォートで試みる。
      - Pillow(PIL) がインストールされていない環境では、このモジュール読み込み時点で ImportError となる想定。
      - 壊れた画像などの場合は、そのファイルのピクセル情報は取得できないが、例外にはせず None として扱う。
    ・image_width / image_height は常に JSON に出力する。
      - 画像でないファイルや取得失敗時は対応する要素が null(PythonではNone) になる。
      - 型は file_size と同様に「成功時は文字列(str)・失敗時は null」で統一する。
    """

    # 画像として扱う拡張子の既定セット（プロパティでは公開しない）
    DEFAULT_IMAGE_EXTENSIONS = {
        "jpg", "jpeg", "png", "tif", "tiff", "bmp", "gif", "webp"
    }

    def __init__(self):
        """現状は特に状態を持たない初期化のみを行うコンストラクタ。"""
        pass

    def __call__(self, byte_data: bytes, attribute: dict, properties: dict):
        """
        NiFi 指定ディレクトリ以下のファイル情報をJSONで出力する。

        :param byte_data: FlowFile本文(JSON)
        :param attribute: FlowFile属性(dict)
        :param properties: プロセッサのプロパティ(dict)
        :return: (更新出力JSON bytes, 属性 dict)
        """
        try:

            # === プロパティ取得 ===
            input_path = properties.get("INPUT_PATH", "")
            include_exts = properties.get("INCLUDE_EXTENSIONS", "")
            recursive = str(properties.get("RECURSIVE_SEARCH", "True")).lower() == 'true'
            ignore_hidden = str(properties.get("IGNORE_HIDDEN", "True")).lower() == 'true'
            include_file_size = str(properties.get("INCLUDE_FILE_SIZE", "True")).lower() == 'true'

            if not input_path or not os.path.exists(input_path):
                raise FileNotFoundError(f"指定パスが存在しません: {input_path}")
            
            # 列挙対象ファイルの拡張子フィルタ
            # 例: "jpg,png" -> ["jpg","png"]
            extensions = (
                [ext.strip().lower() for ext in include_exts.split(',') if ext.strip()]
                if include_exts else None
            )

            system = platform.system()

            # === 出力用リスト ===
            fileList  = []

            # ファイル個別処理
            def handle_file(full_path: str, rel_base: str):
                name = os.path.basename(full_path)
                ext = os.path.splitext(name)[1][1:].lower()
                record = {}
                
                # --- 隠しファイル ---
                if ignore_hidden:
                    if system == "Windows":
                        file_attribute_hidden = 0x2
                        attrs = ctypes.windll.kernel32.GetFileAttributesW(str(full_path))
                        if attrs != -1 and (attrs & file_attribute_hidden) != 0:
                            return
                    else:
                        if name.startswith('.'):
                            return

                # --- 拡張子フィルタ ---
                if extensions and ext not in extensions:
                    return

                # --- 相対パス追加（単体ファイルでも統一） ---
                record["file_path"] = os.path.relpath(full_path, rel_base)

                # --- サイズ ---
                if include_file_size:
                    try:
                        record["file_size"] = str(os.stat(full_path).st_size)
                    except:
                        record["file_size"] = None

                # --- 画像サイズ ---
                if ext in self.DEFAULT_IMAGE_EXTENSIONS:
                    try:
                        with ImageModule.open(full_path) as img:
                            w, h = img.size
                        record["image_widths"] =  str(w)
                        record["image_heights"] =  str(h)
                    except:
                        record["image_widths"] =  None
                        record["image_heights"] =  None
                else:
                    record["image_widths"] =  None
                    record["image_heights"] =  None
                
                fileList.append(record)

            if os.path.isfile(input_path):
                # ファイル単体のみ処理
                handle_file(input_path, os.path.dirname(input_path))

            else:
                # フォルダ → 再帰 or 非再帰で処理
                for root, dirs, files in os.walk(input_path):
                    if not recursive:
                        dirs[:] = []

                    for name in files:
                        full_path = os.path.join(root, name)
                        handle_file(full_path, input_path)
                        
            byte_data =  json.dumps(fileList, ensure_ascii=False)
                
            return byte_data, attribute

        except Exception as e:
            raise Exception(f"[GetFileInfoLogic_Exception]: {str(e)}")
