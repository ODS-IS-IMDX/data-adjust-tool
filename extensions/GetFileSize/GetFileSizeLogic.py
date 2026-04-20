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
import os.path

from PIL import Image

class GetFileSizeLogic:
    """
    FlowFile属性(absolute.path + filename)をもとに、対象ファイルのメタ情報を取得して
    FlowFile属性に書き戻すためのロジッククラス。

    ■今回の仕様(混乱しない版)
    - 出力するファイルサイズは「file_size」1つだけにする
      → 単位は「file_size_unit」に必ずセットする
    - FILE_SIZE_UNIT で "byte" / "KB" / "MB" を選択する
      → file_size はその単位に換算した値(文字列)を出す
    - 画像の縦横(px)はフラグ(GET_IMAGE_DIMENSIONS)で取得する/しないを切り替える
      → 非画像/壊れ画像でも落とさず、取得できない場合は 0 を入れる

    ■固定で書き込むFlowFile属性名(仕様として固定)
    - file_size       : FILE_SIZE_UNITで選んだ単位に換算したサイズ値(文字列)
    - file_size_unit  : file_sizeの単位("byte" / "KB" / "MB")
    - image_width_px  : 画像の幅(ピクセル) ※取れない場合は0
    - image_height_px : 画像の高さ(ピクセル) ※取れない場合は0
    """

    # ============================================================
    # 出力するFlowFile属性名(固定)
    # ============================================================
    ATTR_FILE_SIZE = "file_size"
    ATTR_FILE_SIZE_UNIT = "file_size_unit"

    ATTR_IMAGE_WIDTH_PX = "image_width_px"
    ATTR_IMAGE_HEIGHT_PX = "image_height_px"

    # ============================================================
    # 入力プロパティのキー名(入口側のPropertyDescriptor変数名に合わせる)
    # ============================================================
    PROP_FILE_SIZE_UNIT = "FILE_SIZE_UNIT"
    PROP_GET_IMAGE_DIMENSIONS = "GET_IMAGE_DIMENSIONS"

    # ============================================================
    # 単位の許容値(入口側でも allowable_values で縛る前提)
    # ============================================================
    UNIT_BYTE = "byte"
    UNIT_KB = "KB"
    UNIT_MB = "MB"

    def __init__(self):
        pass

    def __call__(self, content: bytes, attribute: dict, properties: dict):
        """
        FlowFileTransformから呼ばれる想定の処理本体。

        Parameters
        ----------
        content : bytes
            FlowFile本文(このロジックでは未使用)
        attribute : dict
            FlowFile属性(ここに結果を追記して返す)
        properties : dict
            プロセッサのプロパティ(設定値)
        """
        try:
            # ============================================================
            # 1) プロパティ取得
            # ============================================================
            # サイズの表示単位
            # - NiFi側で allowable_values で縛る前提
            # - defaultは "byte"
            file_size_unit = str(properties.get(self.PROP_FILE_SIZE_UNIT, self.UNIT_BYTE))

            # 画像寸法取得フラグ
            # - "true" のときだけ取得する(バリデータでtrue/falseのみの前提)
            get_image_dims = (properties.get(self.PROP_GET_IMAGE_DIMENSIONS, "true") == "true")

            # ============================================================
            # 2) FlowFile属性からファイルパスを組み立てる
            # ============================================================
            file_name = attribute.get("filename", "").strip()
            absolute_path = attribute.get("absolute.path", "").strip()

            if not file_name or not absolute_path:
                raise AttributeError("FlowFile に filename または absolute.path 属性がありません")

            file_path = os.path.join(str(absolute_path), str(file_name))

            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"ファイルが存在しません: {file_path}")

            # ============================================================
            # 3) ファイルサイズ取得(内部はbytesで取得して、出力はunitに合わせて換算)
            # ============================================================
            # 出力属性を1つに統一するため、内部では bytes を取得して換算する。
            # ただし出力には bytes を別属性で出さない(混乱回避の方針)。
            size_bytes = os.path.getsize(file_path)

            # まず unit は必ず出す(後段で "この数字は何単位？" にならないように)
            attribute[self.ATTR_FILE_SIZE_UNIT] = file_size_unit

            # unitに合わせて file_size を作る
            if file_size_unit == self.UNIT_KB:
                # KB = 1024 bytes
                value = float(size_bytes) / 1024.0
                attribute[self.ATTR_FILE_SIZE] = f"{value:.3f}"

            elif file_size_unit == self.UNIT_MB:
                # MB = 1024 * 1024 bytes
                value = float(size_bytes) / (1024.0 * 1024.0)
                attribute[self.ATTR_FILE_SIZE] = f"{value:.3f}"

            else:
                # "byte" 想定
                # - byteは整数が自然なので整数文字列にする
                attribute[self.ATTR_FILE_SIZE_UNIT] = self.UNIT_BYTE
                attribute[self.ATTR_FILE_SIZE] = str(int(size_bytes))

            # ============================================================
            # 4) 画像の縦横取得(必要なら)
            # ============================================================
            # 要望:
            # - 壊れた画像 / 画像ではないファイル でも「落とさない」
            # - 取得できなかったら 0 を入れる
            if get_image_dims:
                attribute[self.ATTR_IMAGE_WIDTH_PX] = "0"
                attribute[self.ATTR_IMAGE_HEIGHT_PX] = "0"

                try:
                    with Image.open(file_path) as img:
                        width_px, height_px = img.size

                    attribute[self.ATTR_IMAGE_WIDTH_PX] = str(int(width_px))
                    attribute[self.ATTR_IMAGE_HEIGHT_PX] = str(int(height_px))

                except Exception:
                    # 非画像/壊れ画像等はここに入る
                    # 目的は「落とさない」なので握りつぶす(0のまま通す)
                    pass

            return content, attribute

        except Exception as e:
            raise Exception(f"[GetFileSizeLogic_Exception]: {str(e)}")
