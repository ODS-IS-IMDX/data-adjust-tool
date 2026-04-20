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
import logging
import uuid
from importlib import import_module

# 外部ライブラリの動的インポート（モジュール先頭で実施）
Image = import_module("PIL.Image")
pdfium = import_module("pypdfium2")

# ---------------------------------------------------------------
# デフォルトロガー設定（NiFi実行時はComponentLogが上書きされる）
# ---------------------------------------------------------------
logger = logging.getLogger(__name__)


class ConvertImageFormatLogic:
    """
    画像ファイルを指定形式に変換するロジック。
    FlowFile本文は使用せず、入力・出力ともにファイルパスを扱う。
    """

    # PDFレンダリング用のデフォルト設定
    _PDF_RENDER_DPI = 200
    _PDF_PAGE_GAP = 10
    _PDF_BACKGROUND_RGB = (255, 255, 255)
    _PDF_MAX_TOTAL_PIXELS = 20000 * 20000  # 安全対策（約4億ピクセル）

    def __init__(self):
        """
        ConvertImageFormatLogicの初期化を行う。

        本クラスはNiFi上での実行を想定しているが、Python単体実行でも動作するように、
        デフォルトでは標準loggingのロガーを利用する。

        :return: None
        :rtype: None
        """
        # Python単体実行時は標準loggingを使用
        self._logger = logger

    # -----------------------------------------------------------
    # NiFi側から ComponentLog を注入するためのメソッド
    # -----------------------------------------------------------
    def set_logger(self, nifi_logger):
        """
        NiFiのComponentLog(または互換のロガー)を注入する。

        NiFi実行時は、NiFiが提供するComponentLogを利用してログ出力するのが自然であるため、
        本メソッドで外部からロガーを差し替え可能にしている。

        :param nifi_logger: NiFiのComponentLog(または互換のloggerオブジェクト)
        :type nifi_logger: object
        :return: None
        :rtype: None
        """
        """NiFiのComponentLogを注入"""
        self._logger = nifi_logger

    # -----------------------------------------------------------
    # 安全に画像を開く関数
    # -----------------------------------------------------------
    def safe_open_image(self, input_path: str):
        """
        画像ファイルを安全に開き、後続処理で扱いやすい状態(RGBA中心)に正規化して返す。

        重要:
          - Pillowは画像形式やmodeにより、decode後のmodeが(P/LA等)になることがある。
          - 特にWebPの一部(modeがP/LA)は、そのままの扱いで不具合が出やすいためRGBAへ変換する。
          - withブロック外で画像を利用するため、最後にcopyして返す(ファイルハンドルを閉じるため)。

        :param input_path: 入力画像ファイルパス
        :type input_path: str
        :return: 画像オブジェクト(PIL Image)。原則RGBA(例外的にRGB/RGBAのcopy)
        :rtype: PIL.Image.Image
        :raises FileNotFoundError: 指定パスが存在しない場合(呼び元で起き得る)
        :raises OSError: 画像として読み込めない場合(Pillow由来)
        """
        """WebPなどを安全に開く（RGBA化して戻す）"""
        with Image.open(input_path) as img:
            img.load()
            self._logger.debug(f"[DEBUG] Opened image: format={img.format}, mode={img.mode}, size={img.size}")

            if img.format == "WEBP" and img.mode in ("P", "LA"):
                self._logger.debug("[DEBUG] Converting WebP (P/LA) -> RGBA for safe processing")
                img = img.convert("RGBA")
            elif img.mode not in ("RGB", "RGBA"):
                self._logger.debug(f"[DEBUG] Normalizing image mode: {img.mode} -> RGBA")
                img = img.convert("RGBA")
            else:
                img = img.copy()  # withブロック外で利用するためコピー

            return img

    # -----------------------------------------------------------
    # 単一画像の出力パスを決定する共通処理
    # -----------------------------------------------------------
    def _decide_output_path_single(self, input_path, output_path, ext, base_name, overwrite):
        """
        単一ファイル出力時の出力パスを決定する。

        仕様(現行実装準拠):
          - overwrite=False の場合:
            - 入力ファイルと同じディレクトリ配下に "Rename" フォルダを作成し、そこへ出力する。
            - 既に同名ファイルが存在する場合は "(1)", "(2)" のように連番を付けて回避する。
          - overwrite=True の場合:
            - OUTPUT_PATHが指定されていればそれを優先し、拡張子だけはoutput_formatに合わせて補正する。
            - OUTPUT_PATH未指定なら入力ファイルと同一ディレクトリに base_name + ext で出力する。
          - 返却前に出力先ディレクトリを必ず作成する。

        :param input_path: 入力ファイルパス
        :type input_path: str
        :param output_path: 出力ファイルパス(空やNoneの場合は未指定扱い)
        :type output_path: str | None
        :param ext: 出力拡張子(例: ".jpg")
        :type ext: str
        :param base_name: 出力ファイル名のベース(拡張子なし)
        :type base_name: str
        :param overwrite: 上書き可否。FalseのときはRenameフォルダへ退避し重複回避する
        :type overwrite: bool
        :return: 実際に使用する出力ファイルパス(ディレクトリ作成済み)
        :rtype: str
        """
        """
        1ファイルだけを出力する場合の出力パスを決定する。
        既存実装のロジックをそのまま共通化したもの。
        """
        file_name = base_name + ext
        explicit_out = bool(output_path and len(str(output_path).strip()) > 0)

        if not overwrite:
            # overwrite=False のときは Rename フォルダを強制使用
            rename_dir = os.path.join(os.path.dirname(input_path), "Rename")
            os.makedirs(rename_dir, exist_ok=True)
            output_path_local = os.path.join(rename_dir, file_name)

            counter = 1
            while os.path.exists(output_path_local):
                name, _ = os.path.splitext(file_name)
                output_path_local = os.path.join(rename_dir, f"{name}({counter}){ext}")
                counter += 1
        else:
            # overwrite=True のときは通常通り explicit_out 優先
            if explicit_out:
                out_root, out_ext = os.path.splitext(output_path)
                if out_ext.lower() != ext.lower():
                    output_path_local = out_root + ext
                else:
                    output_path_local = output_path
            else:
                input_dir = os.path.dirname(input_path)
                output_path_local = os.path.join(input_dir, file_name)

        os.makedirs(os.path.dirname(output_path_local), exist_ok=True)
        return output_path_local

    # -----------------------------------------------------------
    # 単一画像を保存する共通処理
    # -----------------------------------------------------------
    def _save_single_image(self, img, output_path, output_format,
                           quality, color_mode, transparent_bg):
        """
        1枚の画像(Pillow Image)を指定フォーマットでディスクに保存する。

        主な処理:
          - 透過を保持できない形式(JPEG等)へ出力する場合の背景合成(白背景)を行う。
          - color_mode指定がある場合、必要に応じてmode変換を行う(透過を壊しにくい範囲で制御)。
          - 非透過形式(JPEG等)にRGBAが残っている場合はRGBへ落とす。
          - フォーマット別の保存オプション(quality, optimize等)を適用する。
          - 一時ファイルへ保存してからos.replaceで原子的に置換する(途中で落ちても壊れにくい)。

        :param img: 入力画像(Pillow Image)
        :type img: PIL.Image.Image
        :param output_path: 出力ファイルパス
        :type output_path: str
        :param output_format: 出力フォーマット名(例: "JPEG", "PNG", "WEBP", "GIF")
        :type output_format: str
        :param quality: JPEG/WEBP品質(0-100を想定)。値域は呼び元が保証する前提
        :type quality: int
        :param color_mode: Pillowのmode指定(例: "RGB", "RGBA")。Noneなら変換しない
        :type color_mode: str | None
        :param transparent_bg: 透過背景を扱うかどうか。非透過形式では白背景合成になる
        :type transparent_bg: bool
        :return: None
        :rtype: None
        :raises OSError: 保存に失敗した場合(Pillow/OS由来)
        """
        """
        1枚のPillow画像を指定フォーマットでディスクに保存する共通処理。
        元の実装のロジックをそのまま切り出している。
        """
        # === 背景処理 ===
        alpha_ok = output_format in ['PNG', 'WEBP', 'GIF']

        # RGBAなどアルファ付き画像を、アルファなしのモード(RGBなど)で出力したい場合は、
        # output_formatがPNGでも「color_modeを優先」する。
        # Transparent Background=True のときは、白背景に合成してから変換する。
        if color_mode and 'A' in img.getbands() and 'A' not in color_mode:
            if transparent_bg:
                white_bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
                img = Image.alpha_composite(white_bg, img.convert("RGBA")).convert(color_mode)
            else:
                img = img.convert(color_mode)

        elif transparent_bg and not alpha_ok:
            # 透過を保持できない形式（JPEGなど）の場合は白背景で合成してRGBに落とす
            white_bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
            img = Image.alpha_composite(white_bg, img.convert("RGBA")).convert("RGB")

        # === カラーモード変換 ===
        if color_mode and img.mode != color_mode:
            img = img.convert(color_mode)

        # === RGBA→RGB変換（非透過形式用） ===
        if not alpha_ok and img.mode == 'RGBA':
            img = img.convert("RGB")

        # === 保存オプション設定 ===
        save_opts = {}
        if output_format in ['JPEG', 'WEBP']:
            save_opts['quality'] = quality
            save_opts['subsampling'] = 0
        elif output_format == 'PNG':
            save_opts['optimize'] = True
            save_opts['compress_level'] = 6
        elif output_format == 'GIF':
            save_opts['optimize'] = True

        # === 画像保存（原子的置換） ===
        tmp_path = f"{output_path}.{uuid.uuid4().hex}.tmp"
        try:
            img.save(tmp_path, format=output_format, **save_opts)
            os.replace(tmp_path, output_path)
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    # -----------------------------------------------------------
    # PDFをページごとにレンダリングしてPIL画像リストとして取得
    # -----------------------------------------------------------
    def _render_pdf_pages(self, input_path, dpi=None):
        """
        PDFをページごとにレンダリングし、PIL.Imageのリストとして返す。

        仕様:
          - pypdfium2を用いてPDFを開き、全ページをbitmapへレンダリングする。
          - dpiは72dpi基準のscaleに変換して指定する(scale = dpi / 72.0)。
          - 返却する画像は後続の透過処理に合わせてRGBAに正規化する。
          - page/bitmap/pdfは明示的にcloseしてリソースリークを防ぐ。

        :param input_path: 入力PDFファイルパス
        :type input_path: str
        :param dpi: レンダリング解像度(dpi)。Noneの場合はクラス定数_PDF_RENDER_DPIを使う
        :type dpi: int | None
        :return: 各ページのレンダリング結果(PIL Image)のリスト
        :rtype: list[PIL.Image.Image]
        :raises ImportError: pypdfium2が利用できない場合
        :raises ValueError: PDFが0ページの場合
        :raises Exception: レンダリング中に発生した例外
        """
        """
        指定したPDFの全ページをPIL.Imageのリストとして取得する。
        """
        if pdfium is None:
            raise ImportError("pypdfium2 is required to handle PDF input.")

        dpi = dpi or self._PDF_RENDER_DPI

        pdf = pdfium.PdfDocument(input_path)
        try:
            page_count = len(pdf)
            if page_count == 0:
                raise ValueError(f"PDF has no pages: {input_path}")

            # 72dpiを基準としたスケール指定
            scale = dpi / 72.0
            images = []

            for i in range(page_count):
                page = pdf[i]
                try:
                    bitmap = page.render(scale=scale)
                    try:
                        pil_img = bitmap.to_pil()
                    finally:
                        # render結果(ビットマップ)を明示解放
                        try:
                            bitmap.close()
                        except Exception:
                            pass
                finally:
                    # pageも明示解放
                    try:
                        page.close()
                    except Exception:
                        pass

                if pil_img.mode != "RGBA":
                    pil_img = pil_img.convert("RGBA")
                images.append(pil_img)

            self._logger.debug(f"[DEBUG] Rendered PDF pages: count={len(images)}, dpi={dpi}, scale={scale}")
            return images

        finally:
            # PdfDocument(=ファイルハンドル)を必ず閉じる
            try:
                pdf.close()
            except Exception:
                pass


    # -----------------------------------------------------------
    # PDFページ群を縦または横に結合して1枚の画像にする
    # -----------------------------------------------------------
    def _merge_images(self, images, orientation, gap, background_rgb):
        """
        複数ページ分の画像を、縦または横に並べて1枚のRGBA画像に結合する。

        仕様:
          - orientation="vertical": 最大幅に合わせて各ページをリサイズし、上から順に貼り付ける。
          - orientation="horizontal": 最大高さに合わせて各ページをリサイズし、左から順に貼り付ける。
          - ページ間にはgapピクセルの余白を入れる。
          - 背景色はbackground_rgbで指定したRGBを不透明(alpha=255)として使う。
          - 極端に大きい結合画像(ピクセル数)は安全対策として例外にする。

        :param images: 結合対象の画像リスト(PIL Image)
        :type images: list[PIL.Image.Image]
        :param orientation: "vertical" または "horizontal"
        :type orientation: str
        :param gap: ページ間の余白(px)
        :type gap: int
        :param background_rgb: 背景色(R, G, B)
        :type background_rgb: tuple[int, int, int]
        :return: 結合済みRGBA画像
        :rtype: PIL.Image.Image
        :raises ValueError: imagesが空、orientationが不正、または結合後サイズが過大な場合
        """
        """
        PDF各ページの画像を縦または横に並べて1枚のRGBA画像に結合する。
        """
        if not images:
            raise ValueError("No images to merge.")

        orientation = orientation.lower()
        if orientation not in ("vertical", "horizontal"):
            raise ValueError(f"Invalid orientation: {orientation}")

        # サイズを揃えながらリサイズ
        if orientation == "vertical":
            target_w = max(im.width for im in images)
            resized = []
            for im in images:
                if im.width == target_w:
                    r = im.copy()
                else:
                    new_h = int(im.height * (target_w / im.width))
                    r = im.resize((target_w, new_h), Image.LANCZOS)
                if r.mode != "RGBA":
                    r = r.convert("RGBA")
                resized.append(r)

            total_w = target_w
            total_h = sum(im.height for im in resized) + gap * (len(resized) - 1 if len(resized) > 1 else 0)
        else:  # horizontal
            target_h = max(im.height for im in images)
            resized = []
            for im in images:
                if im.height == target_h:
                    r = im.copy()
                else:
                    new_w = int(im.width * (target_h / im.height))
                    r = im.resize((new_w, target_h), Image.LANCZOS)
                if r.mode != "RGBA":
                    r = r.convert("RGBA")
                resized.append(r)

            total_h = target_h
            total_w = sum(im.width for im in resized) + gap * (len(resized) - 1 if len(resized) > 1 else 0)

        # サイズチェック（極端に巨大な画像を防ぐ）
        if total_w * total_h > self._PDF_MAX_TOTAL_PIXELS:
            raise ValueError(f"Merged image is too large: {total_w} x {total_h} pixels")

        bg_rgba = (background_rgb[0], background_rgb[1], background_rgb[2], 255)
        merged = Image.new("RGBA", (total_w, total_h), bg_rgba)

        if orientation == "vertical":
            y = 0
            for im in resized:
                merged.paste(im, (0, y))
                y += im.height + gap
        else:
            x = 0
            for im in resized:
                merged.paste(im, (x, 0))
                x += im.width + gap

        self._logger.debug(f"[DEBUG] Merged image created: size={merged.size}, orientation={orientation}")
        return merged

    # -----------------------------------------------------------
    # 非PDF入力の処理
    # -----------------------------------------------------------
    def _process_non_pdf(self, input_path, output_path, output_format,
                         quality, color_mode, transparent_bg,
                         overwrite, ext, base_name, attribute):
        """
        非PDF(通常画像)入力を変換し、単一ファイルとして保存する。

        処理の流れ:
          1) 出力パスを決定する(_decide_output_path_single)
          2) 画像を安全に読み込む(safe_open_image)
          3) 指定フォーマットで保存する(_save_single_image)
          4) attributeに結果情報を格納して返す

        attribute更新(現行実装準拠):
          - result_path: 出力ファイルパス
          - result_format: 出力フォーマット名
          - result_quality: qualityを文字列化したもの
          - result_transparent: str(not transparent_bg).lower() を設定(現行仕様)

        :param input_path: 入力画像ファイルパス
        :type input_path: str
        :param output_path: 出力ファイルパス(未指定可)
        :type output_path: str | None
        :param output_format: 出力フォーマット名(例: "JPEG", "PNG", "WEBP", "GIF")
        :type output_format: str
        :param quality: JPEG/WEBP品質
        :type quality: int
        :param color_mode: Pillowのmode指定(例: "RGB")。Noneなら変換しない
        :type color_mode: str | None
        :param transparent_bg: 透過背景を扱うかどうか
        :type transparent_bg: bool
        :param overwrite: 上書き可否
        :type overwrite: bool
        :param ext: 出力拡張子(例: ".jpg")
        :type ext: str
        :param base_name: 出力ファイル名ベース(拡張子なし)
        :type base_name: str
        :param attribute: FlowFile属性(dict)。本関数内でupdateする
        :type attribute: dict
        :return: (空bytes, 更新済みattribute)
        :rtype: tuple(bytes, dict)
        :raises Exception: 読み込み/保存/パス決定に失敗した場合
        """
        """
        非PDF画像ファイルを変換する処理。
        """
        # 出力パス決定（単一ファイル）
        output_path_local = self._decide_output_path_single(
            input_path=input_path,
            output_path=output_path,
            ext=ext,
            base_name=base_name,
            overwrite=overwrite,
        )

        # 画像読み込み
        img = self.safe_open_image(input_path)

        # 保存
        self._save_single_image(
            img=img,
            output_path=output_path_local,
            output_format=output_format,
            quality=quality,
            color_mode=color_mode,
            transparent_bg=transparent_bg,
        )

        # 属性更新
        attribute.update({
            'result_path': output_path_local,
            'result_format': output_format,
            'result_quality': str(quality),
            'result_transparent': str(not transparent_bg).lower(),
        })

        return b"", attribute

    # -----------------------------------------------------------
    # PDF 1ページの処理
    # -----------------------------------------------------------
    def _process_pdf_single_page(self, input_path, output_path, output_format,
                                 quality, color_mode, transparent_bg,
                                 overwrite, ext, base_name,
                                 attribute, pdf_page_mode, images):
        """
        1ページだけのPDFを処理し、単一画像ファイルとして保存する。

        仕様:
          - PDF_PAGE_MODEの値にかかわらず、出力は必ず1ファイルである。
          - ファイル名/出力先は単一出力の決定ロジック(_decide_output_path_single)に従う。
          - pdf_page_modeが"SEPARATE_FILES"のときだけ、互換目的でページ情報も属性に付与する。

        attribute更新(現行実装準拠):
          - result_path, result_format, result_quality, result_transparent
          - pdf_page_mode == 'SEPARATE_FILES' の場合のみ:
            - result_paths: result_pathと同値
            - result_page_count: "1"

        :param input_path: 入力PDFファイルパス
        :type input_path: str
        :param output_path: 出力ファイルパス(未指定可)
        :type output_path: str | None
        :param output_format: 出力フォーマット名(例: "JPEG", "PNG", "WEBP", "GIF")
        :type output_format: str
        :param quality: JPEG/WEBP品質
        :type quality: int
        :param color_mode: Pillowのmode指定(例: "RGB")。Noneなら変換しない
        :type color_mode: str | None
        :param transparent_bg: 透過背景を扱うかどうか
        :type transparent_bg: bool
        :param overwrite: 上書き可否
        :type overwrite: bool
        :param ext: 出力拡張子
        :type ext: str
        :param base_name: 出力ファイル名ベース(拡張子なし)
        :type base_name: str
        :param attribute: FlowFile属性(dict)。本関数内でupdateする
        :type attribute: dict
        :param pdf_page_mode: PDFページの出力モード("MERGED_VERTICAL"/"MERGED_HORIZONTAL"/"SEPARATE_FILES")
        :type pdf_page_mode: str
        :param images: _render_pdf_pagesで得たページ画像リスト(ここでは先頭1枚のみ使用)
        :type images: list[PIL.Image.Image]
        :return: (空bytes, 更新済みattribute)
        :rtype: tuple(bytes, dict)
        :raises Exception: 保存やパス決定に失敗した場合
        """
        """
        1ページだけのPDFを処理する。
        PDF_PAGE_MODE にかかわらず出力ファイルは1つ・ファイル名も共通。
        """
        output_path_local = self._decide_output_path_single(
            input_path=input_path,
            output_path=output_path,
            ext=ext,
            base_name=base_name,
            overwrite=overwrite,
        )

        img = images[0]

        self._save_single_image(
            img=img,
            output_path=output_path_local,
            output_format=output_format,
            quality=quality,
            color_mode=color_mode,
            transparent_bg=transparent_bg,
        )

        # 共通属性
        attribute.update({
            'result_path': output_path_local,
            'result_format': output_format,
            'result_quality': str(quality),
            'result_transparent': str(not transparent_bg).lower(),
        })

        # PDF_PAGE_MODE が SEPARATE_FILES のときだけ、ページ情報も付与
        if pdf_page_mode == 'SEPARATE_FILES':
            attribute['result_paths'] = output_path_local
            attribute['result_page_count'] = '1'

        return b"", attribute

    # -----------------------------------------------------------
    # PDFをページごとに分割ファイルとして保存する処理（複数ページ専用）
    # -----------------------------------------------------------
    def _process_pdf_separate_files(self, input_path, output_path, output_format,
                                    quality, color_mode, transparent_bg,
                                    overwrite, ext, base_name,
                                    attribute, explicit_out, images, page_count):
        """
        複数ページPDFを、ページごとに個別ファイルとして保存する。

        発動条件(呼び元の制御):
          - 入力がPDF
          - PDF_PAGE_MODE == "SEPARATE_FILES"
          - page_count >= 2

        出力ファイル名:
          - 基本は "{base_root}_{idx:04d}{ext}" (例: xxx_0001.jpg, xxx_0002.jpg)
          - overwrite=False の場合、同名が存在するとさらに "_{counter:04d}" を付けて回避する

        出力先の決定:
          - overwrite=True:
            - output_path指定あり(explicit_out=True)なら、その拡張子を除いたbase_rootを起点に連番出力する
            - output_path未指定なら入力ディレクトリ + base_name を起点にする
          - overwrite=False:
            - 入力ディレクトリ配下の "Rename" フォルダに出力する(常に)
            - output_path指定ありならその拡張子を除いたbase_rootを使う(ただし出力先はRename)

        attribute更新(現行実装準拠):
          - result_path: 1ページ目の出力パス(後方互換用)
          - result_paths: 全ページの出力パスを";"区切りで連結
          - result_page_count: ページ数
          - result_format, result_quality, result_transparent

        :param input_path: 入力PDFファイルパス
        :type input_path: str
        :param output_path: 出力ファイルパス(未指定可)
        :type output_path: str | None
        :param output_format: 出力フォーマット名(例: "JPEG", "PNG", "WEBP", "GIF")
        :type output_format: str
        :param quality: JPEG/WEBP品質
        :type quality: int
        :param color_mode: Pillowのmode指定(例: "RGB")。Noneなら変換しない
        :type color_mode: str | None
        :param transparent_bg: 透過背景を扱うかどうか
        :type transparent_bg: bool
        :param overwrite: 上書き可否
        :type overwrite: bool
        :param ext: 出力拡張子
        :type ext: str
        :param base_name: 出力ファイル名ベース(拡張子なし)
        :type base_name: str
        :param attribute: FlowFile属性(dict)。本関数内でupdateする
        :type attribute: dict
        :param explicit_out: OUTPUT_PATHが明示指定されているかどうか
        :type explicit_out: bool
        :param images: _render_pdf_pagesで得たページ画像リスト
        :type images: list[PIL.Image.Image]
        :param page_count: ページ数(len(images)と一致する前提)
        :type page_count: int
        :return: (空bytes, 更新済みattribute)
        :rtype: tuple(bytes, dict)
        :raises Exception: 保存やパス決定に失敗した場合
        """
        """
        PDF入力かつ PDF_PAGE_MODE=SEPARATE_FILES かつ 2ページ以上のときの処理。
        各ページを個別の画像ファイルとして保存し、関連属性を更新する。
        """
        # ベースパス決定
        if overwrite:
            # 上書きモードでは、OUTPUT_PATHがあればそれをベースに、なければ入力ファイル名ベース
            if explicit_out:
                base_root, _ = os.path.splitext(output_path)
                base_dir = os.path.dirname(base_root) or os.path.dirname(input_path)
            else:
                base_dir = os.path.dirname(input_path)
                base_root = os.path.join(base_dir, base_name)
        else:
            # overwrite=False のときは Rename フォルダ配下に必ず出力
            base_dir = os.path.join(os.path.dirname(input_path), "Rename")
            os.makedirs(base_dir, exist_ok=True)
            if explicit_out:
                base_root, _ = os.path.splitext(output_path)
            else:
                base_root = os.path.join(base_dir, base_name)

        os.makedirs(base_dir, exist_ok=True)

        result_paths = []

        for idx, img in enumerate(images, start=1):
            # 4桁ゼロ埋めの連番付きファイル名（0001, 0002, ...）
            numbered_root = f"{base_root}_{idx:04d}"
            page_output_path = f"{numbered_root}{ext}"

            if not overwrite:
                # 既存ファイルがある場合は _0001, _0002 ... を付けて重複回避
                candidate = page_output_path
                counter = 1
                while os.path.exists(candidate):
                    name, ext_local = os.path.splitext(page_output_path)
                    candidate = f"{name}_{counter:04d}{ext_local}"
                    counter += 1
                page_output_path = candidate

            os.makedirs(os.path.dirname(page_output_path), exist_ok=True)
            self._save_single_image(
                img=img,
                output_path=page_output_path,
                output_format=output_format,
                quality=quality,
                color_mode=color_mode,
                transparent_bg=transparent_bg,
            )
            result_paths.append(page_output_path)

        # 属性更新（1ページ目のパスは後方互換用に result_path にも入れる）
        attribute.update({
            'result_path': result_paths[0],
            'result_paths': ";".join(result_paths),
            'result_page_count': str(page_count),
            'result_format': output_format,
            'result_quality': str(quality),
            'result_transparent': str(not transparent_bg).lower(),
        })

        return b"", attribute

    # -----------------------------------------------------------
    # PDF複数ページを結合して1枚として保存する処理
    # -----------------------------------------------------------
    def _process_pdf_merged(self, input_path, output_path, output_format,
                            quality, color_mode, transparent_bg,
                            overwrite, ext, base_name,
                            attribute, pdf_page_mode, images, page_count):
        """
        複数ページPDFを結合し、1枚の画像として保存する。

        発動条件(呼び元の制御):
          - 入力がPDF
          - page_count >= 2
          - PDF_PAGE_MODE in {"MERGED_VERTICAL", "MERGED_HORIZONTAL"}

        結合方法:
          - MERGED_VERTICAL: 縦方向に結合
          - MERGED_HORIZONTAL: 横方向に結合
          - 結合処理は _merge_images に委譲する(リサイズ/ギャップ/背景色/サイズ上限制御を含む)

        出力:
          - 単一出力の決定ロジック(_decide_output_path_single)で保存先を決める
          - attributeに結果情報を格納して返す

        :param input_path: 入力PDFファイルパス
        :type input_path: str
        :param output_path: 出力ファイルパス(未指定可)
        :type output_path: str | None
        :param output_format: 出力フォーマット名(例: "JPEG", "PNG", "WEBP", "GIF")
        :type output_format: str
        :param quality: JPEG/WEBP品質
        :type quality: int
        :param color_mode: Pillowのmode指定(例: "RGB")。Noneなら変換しない
        :type color_mode: str | None
        :param transparent_bg: 透過背景を扱うかどうか
        :type transparent_bg: bool
        :param overwrite: 上書き可否
        :type overwrite: bool
        :param ext: 出力拡張子
        :type ext: str
        :param base_name: 出力ファイル名ベース(拡張子なし)
        :type base_name: str
        :param attribute: FlowFile属性(dict)。本関数内でupdateする
        :type attribute: dict
        :param pdf_page_mode: "MERGED_VERTICAL" または "MERGED_HORIZONTAL"
        :type pdf_page_mode: str
        :param images: _render_pdf_pagesで得たページ画像リスト
        :type images: list[PIL.Image.Image]
        :param page_count: ページ数(len(images)と一致する前提)
        :type page_count: int
        :return: (空bytes, 更新済みattribute)
        :rtype: tuple(bytes, dict)
        :raises Exception: 結合/保存/パス決定に失敗した場合
        """
        """
        PDF入力かつ PDF_PAGE_MODE が MERGED_VERTICAL / MERGED_HORIZONTAL のときの処理。
        全ページを1枚の画像に結合して出力する。
        """
        orientation = "vertical" if pdf_page_mode == 'MERGED_VERTICAL' else "horizontal"

        merged_img = self._merge_images(
            images=images,
            orientation=orientation,
            gap=self._PDF_PAGE_GAP,
            background_rgb=self._PDF_BACKGROUND_RGB,
        )

        output_path_local = self._decide_output_path_single(
            input_path=input_path,
            output_path=output_path,
            ext=ext,
            base_name=base_name,
            overwrite=overwrite,
        )

        self._save_single_image(
            img=merged_img,
            output_path=output_path_local,
            output_format=output_format,
            quality=quality,
            color_mode=color_mode,
            transparent_bg=transparent_bg,
        )

        attribute.update({
            'result_path': output_path_local,
            'result_format': output_format,
            'result_quality': str(quality),
            'result_transparent': str(not transparent_bg).lower(),
        })
        # 必要ならここに result_page_count を付けてもよい（今は付けない）

        return b"", attribute

    # -----------------------------------------------------------
    # メイン処理
    # -----------------------------------------------------------
    def __call__(self, byte_data: bytes, attribute: dict, properties: dict):
        """
        NiFi(Python Processor)から呼ばれるエントリポイント。

        重要:
          - FlowFileのcontent(byte_data)は使用しない(戻り値は常に空bytes)。
          - INPUT_PATH/OUTPUT_PATHを使って「ファイル→ファイル」の変換を行う。
          - 入力がPDFの場合は、PDF_PAGE_MODEに応じて出力形態を変える。

        入力の取得優先順位:
          - input_path = properties["INPUT_PATH"] or attribute["Input_path"]
          - output_path = properties["OUTPUT_PATH"] or attribute["Output_path"]

        対応する主なプロパティ(現行実装準拠):
          - OUTPUT_FORMAT: "JPEG"/"PNG"/"WEBP"/"GIF" (default: "JPEG")
          - QUALITY: int (default: 90) ※JPEG/WEBP向け
          - COLOR_MODE: Pillow mode (default: None)
          - TRANSPARENT_BG: "true"/"false"文字列 (default: "True")
          - OVERWRITE: "true"/"false"文字列 (default: "True")
          - PDF_PAGE_MODE: "MERGED_VERTICAL"/"MERGED_HORIZONTAL"/"SEPARATE_FILES" (default: "MERGED_VERTICAL")
            - 想定外の値はMERGED_VERTICALへフォールバックする

        出力(属性):
          - 非PDF: result_path, result_format, result_quality, result_transparent
          - PDF(1ページ): 上記 + (pdf_page_modeがSEPARATE_FILESのときだけ) result_paths, result_page_count
          - PDF(複数ページ + SEPARATE_FILES): result_path(1ページ目), result_paths(";"), result_page_count, result_format, result_quality, result_transparent
          - PDF(複数ページ + MERGED_*): result_path, result_format, result_quality, result_transparent

        例外時:
          - 例外を投げず、attribute["convert_error"]にエラー情報を格納して返す。

        :param byte_data: FlowFile content(未使用)
        :type byte_data: bytes
        :param attribute: FlowFile属性(dict)。結果やエラー情報をupdateして返す
        :type attribute: dict
        :param properties: プロパティ(dict)。NiFiの設定値が入る
        :type properties: dict
        :return: (空bytes, 更新済みattribute)
        :rtype: tuple(bytes, dict)
        """
        try:
            # === パラメータ取得 ===
            input_path = properties.get('INPUT_PATH') or attribute.get('Input_path')
            output_path = properties.get('OUTPUT_PATH') or attribute.get('Output_path')
            output_format = (properties.get('OUTPUT_FORMAT') or 'JPEG').upper()
            quality = int(properties.get('QUALITY') or 90)
            color_mode = properties.get('COLOR_MODE') or None
            transparent_bg = str(properties.get('TRANSPARENT_BG') or 'True').lower() == 'true'
            overwrite = str(properties.get('OVERWRITE') or 'True').strip().lower() in ['true', '1', 'yes']
            pdf_page_mode = (properties.get('PDF_PAGE_MODE') or 'MERGED_VERTICAL').upper()

            # 想定外の値が来た場合は MERGED_VERTICAL にフォールバック
            if pdf_page_mode not in ('MERGED_VERTICAL', 'MERGED_HORIZONTAL', 'SEPARATE_FILES'):
                pdf_page_mode = 'MERGED_VERTICAL'

            self._logger.debug(f"input_path={input_path}")
            self._logger.debug(f"output_format={output_format}")
            self._logger.debug(f"quality={quality}")
            self._logger.debug(f"color_mode={color_mode}")
            self._logger.debug(f"transparent_bg={transparent_bg}")
            self._logger.debug(f"overwrite={overwrite}")
            self._logger.debug(f"pdf_page_mode={pdf_page_mode}")

            if not input_path or not os.path.exists(input_path):
                raise FileNotFoundError(f"Input file not found: {input_path}")

            # === 拡張子マップ ===
            ext_map = {'JPEG': '.jpg', 'PNG': '.png', 'WEBP': '.webp', 'GIF': '.gif'}
            in_ext = os.path.splitext(input_path)[1]
            ext = ext_map.get(output_format, in_ext if in_ext else '.jpg')

            # === 出力ファイル名用の情報 ===
            base_name = os.path.splitext(os.path.basename(input_path))[0]
            explicit_out = bool(output_path and len(str(output_path).strip()) > 0)
            is_pdf = in_ext.lower() == ".pdf"

            # ---------------------------------------------------
            # 非PDF → 専用関数に丸投げ
            # ---------------------------------------------------
            if not is_pdf:
                return self._process_non_pdf(
                    input_path=input_path,
                    output_path=output_path,
                    output_format=output_format,
                    quality=quality,
                    color_mode=color_mode,
                    transparent_bg=transparent_bg,
                    overwrite=overwrite,
                    ext=ext,
                    base_name=base_name,
                    attribute=attribute,
                )

            # ---------------------------------------------------
            # PDF → まず全ページをレンダリング
            # ---------------------------------------------------
            images = self._render_pdf_pages(input_path, dpi=self._PDF_RENDER_DPI)
            page_count = len(images)

            # 1ページPDF → モードに関わらず1ファイル出力（ファイル名も共通）
            if page_count == 1:
                return self._process_pdf_single_page(
                    input_path=input_path,
                    output_path=output_path,
                    output_format=output_format,
                    quality=quality,
                    color_mode=color_mode,
                    transparent_bg=transparent_bg,
                    overwrite=overwrite,
                    ext=ext,
                    base_name=base_name,
                    attribute=attribute,
                    pdf_page_mode=pdf_page_mode,
                    images=images,
                )

            # 複数ページPDF + SEPARATE_FILES → 連番で複数ファイル出力
            if pdf_page_mode == 'SEPARATE_FILES':
                return self._process_pdf_separate_files(
                    input_path=input_path,
                    output_path=output_path,
                    output_format=output_format,
                    quality=quality,
                    color_mode=color_mode,
                    transparent_bg=transparent_bg,
                    overwrite=overwrite,
                    ext=ext,
                    base_name=base_name,
                    attribute=attribute,
                    explicit_out=explicit_out,
                    images=images,
                    page_count=page_count,
                )

            # 複数ページPDF + MERGED_VERTICAL/HORIZONTAL → 結合して1枚ファイル出力
            return self._process_pdf_merged(
                input_path=input_path,
                output_path=output_path,
                output_format=output_format,
                quality=quality,
                color_mode=color_mode,
                transparent_bg=transparent_bg,
                overwrite=overwrite,
                ext=ext,
                base_name=base_name,
                attribute=attribute,
                pdf_page_mode=pdf_page_mode,
                images=images,
                page_count=page_count,
            )

        except Exception as e:
            # ログ出力を削除し、属性にエラーだけ保持
            attribute['convert_error'] = f"{e.__class__.__name__}: {e or 'INPUT_PATH is empty'}"
            return b"", attribute