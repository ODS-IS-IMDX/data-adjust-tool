# MIT License
# 
# Copyright (c) 2025 NTT InfraNet
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

# Python標準ライブラリ
import pickle
from importlib import import_module
import math

# 外部ライブラリの動的インポート
Image = import_module("PIL.Image")
np = import_module("numpy")


from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ImageOCRLogic(FlexibleRasterVectorLogic):
    def input_check(self, byte_data, attribute):
        """
        入力データと属性の妥当性を確認します。

        Parameters
        ----------
        byte_data : bytes
            入力される画像データのバイト列です。
        attribute : dict
            画像の属性情報を含む辞書です。ColorSpace 属性が含まれている必要があります。

        Raises
        ------
        Exception
            byte_data が None の場合や ColorSpace が設定されていない場合に例外を発生させます。
        """
        if byte_data is None:
            raise Exception('入力データが設定されていません')
        if not attribute or attribute.get('ColorSpace') is None:
            raise Exception('attributeにColorSpaceが設定されていません')

    def revert_bboxes_to_original_expand_false(self, results, angle_deg, original_width, original_height):
        """
        Pillowの rotate(angle, expand=False) で回転した画像上の座標を、
        回転前の画像座標系に戻すための関数。

        Parameters
        ----------
        results: 
            EasyOCR等のOCR結果の list:
                [
                    [ [x1,y1], [x2,y2], [x3,y3], [x4,y4] ],  # バウンディングボックス4点
                    認識テキスト,
                    確信度
                ], ...
        angle_deg: 
            回転角度(度数法, +の場合は反時計回り)
        original_width: 
            回転前の画像の幅
        original_height: 
            回転前の画像の高さ

        Returns
        -------
        list
            new_results
                バウンディングボックスを回転前座標に戻した結果 (results と同形式)
        """
        # 画像の中心 (cx, cy)
        cx = original_width / 2.0
        cy = original_height / 2.0

        # 回転角をラジアンに変換
        # Pillowの rotate(angle_deg, expand=False) は
        # 「画像中心を軸に +angle_deg（反時計回り）回転」した座標系
        rad = math.radians(angle_deg)

        new_results = []

        for bbox, text, conf in results:
            # bbox = [[x1,y1],[x2,y2],[x3,y3],[x4,y4]]
            reverted_bbox = []
            for (xr, yr) in bbox:
                # 1) まず「画像中心(cx, cy)」が原点になるようシフト
                xr_shift = xr - cx
                yr_shift = yr - cy

                # 2) +angle_deg 回転の逆なので、-angle_deg で回転する
                #    ※ sin(-θ) = -sin(θ), cos(-θ) = cos(θ)
                #      ただしこの例では明示的に -rad を使うか、
                #      cos(rad), -sin(rad) を直接使うどちらかでもOK
                x_orig_shift = xr_shift * math.cos(rad) - yr_shift * math.sin(rad)
                y_orig_shift = xr_shift * math.sin(rad) + yr_shift * math.cos(rad)

                # 3) 再び中心(cx, cy)を足して戻す
                x_orig = x_orig_shift + cx
                y_orig = y_orig_shift + cy

                reverted_bbox.append([x_orig, y_orig])

            new_results.append([reverted_bbox, text, conf])

        return new_results

    def __call__(self, byte_data, attribute, properties):
        """
        画像に含まれる文字を、指定された OCR エンジン（EasyOCR・Tesseract）で解析し、
        認識結果を返します。OCR の実行後、抽出したテキスト情報は以下の形式に統一して格納します。

            [
                [
                    ([x1, y1], [x2, y2], [x3, y3], [x4, y4]),
                    "認識テキスト",
                    確信度 (float)
                ],
                ...
            ]

        対応可能なOCRエンジンには、EasyOCR・TesseractOCR があり、
        それぞれ異なるアルゴリズムで文字認識を行います。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace の指定はありません。
        properties : dict
            プロパティ設定を含む辞書です。
            - ocr_engine: string
                使用するOCRエンジンの種類（'EasyOCR'、'TesseractOCR''）が設定されます。

            - fsf_image_src: string
                処理する画像を取得するFieldSetFileの項目名が指定されます。

            - fsf_output: string
                処理した結果を出力するFieldSetFileの項目名が指定されます。

            - line_coords_suffix: string
                旗上げ線座標の識別名(入力データ)が指定されます。

        Returns
        -------
        tuple
            new_byte_data : bytes
                文字認識結果[(top_left, top_right, bottom_right, bottom_left), 認識したテキスト, 確信度]のバイト列です。

            dict
                更新された属性情報を含む辞書です。ColorSpace は 元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        # 入出力項目取得
        fsf_image_src = properties.get('fsf_image_src', 'content')
        fsf_output = properties.get('fsf_output', 'content')

        try:
            image = pickle.loads(byte_data[fsf_image_src])
        except Exception as e:
            raise ValueError(f'入力画像の読み込みに失敗しました。fsf_image_srcに設定した名称を確認してください: {fsf_image_src}')

        original_height, original_width = image.shape[:2]

        # 旗上げ線座標
        line_coords = None
        line_coords_suffix = properties.get('line_coords_suffix', None)
        if line_coords_suffix is not None and line_coords_suffix != '':
            try:
                line_coords = eval(byte_data[line_coords_suffix])
                line_coords = pickle.loads(line_coords)
            except Exception as e:
                raise Exception(f'line_coordsの旗上げ線の座標に無効な値が設定されています: {str(e)}')
            for xy in line_coords:
                for num in xy:
                    if not isinstance(num, int) or num < 0:
                        raise ValueError("line_coordsの旗上げ線の座標は0以上である必要があります")

        if line_coords is not None and len(line_coords) == 2:
            # 旗上げ線がある場合は旗上げ線を水平にするように回転する
            (x1, y1), (x2, y2) = line_coords
            # ベクトル (x2 - x1, y2 - y1) の角度を arctan2 で求める
            angle = math.degrees(math.atan2(y2 - y1, x2 - x1))

            img_pil = Image.fromarray(image)

            # 画像を angle で回転(ベクトルが水平になるよう補正)
            img_pil_rotated = img_pil.rotate(angle, expand=False)

            image = np.array(img_pil_rotated)

        ocr_engine = properties.get('ocr_engine', 'EasyOCR')
        if ocr_engine == 'EasyOCR':
            # EasyOCR
            easyocr = import_module("easyocr")
            torch = import_module("torch")

            # GPU使用判定
            use_gpu = torch.cuda.is_available()

            reader = easyocr.Reader(['ja', 'en'], gpu=use_gpu)

            # 文字認識を実行
            results = reader.readtext(image)

        elif ocr_engine == 'TesseractOCR':
            # Tesseract OCR
            pytesseract = import_module("pytesseract")
            Output = pytesseract.Output

            # pytesseract で結果を辞書形式で取得 (バウンディングボックスやテキスト、信頼度等)
            tess_data = pytesseract.image_to_data(
                image,
                lang='jpn+eng',          # 日本語+英語
                config='--psm 3',       # ページセグメンテーションモード等の設定（必要に応じて調整）
                output_type=Output.DICT  # 出力形式を辞書に指定
            )

            # EasyOCR と同等の形式に合わせたい場合
            # results = [ [ [x1,y1], [x2,y2], [x3,y3], [x4,y4] ], text, confidence ], ... ]
            results = []
            n_boxes = len(tess_data['text'])
            for i in range(n_boxes):
                text = tess_data['text'][i].strip()
                conf = tess_data['conf'][i]

                # 空文字や無効な領域を除外したい場合
                if text == '' or conf == '-1':  # conf が -1 の場合は認識失敗
                    continue

                # バウンディングボックス情報を取得
                x = tess_data['left'][i]
                y = tess_data['top'][i]
                w = tess_data['width'][i]
                h = tess_data['height'][i]

                # confidence(文字列)をfloat変換できるならする
                try:
                    confidence = float(conf) / 100.0
                except ValueError:
                    confidence = 0.0

                # EasyOCR互換の4点 bbox
                bbox = [
                    [x, y],  # top_left
                    [x + w, y],  # top_right
                    [x + w, y + h],  # bottom_right
                    [x, y + h]  # bottom_left
                ]
                results.append([bbox, text, confidence])
        else:
            raise ValueError(f'不正なOCRエンジンが指定されています: {ocr_engine}')

        if line_coords is not None and len(line_coords) == 2:
            # 回転して水平にしてからOCRを行った後、Bounding Boxの座標を回転して元に戻す必要がある
            results = self.revert_bboxes_to_original_expand_false(
                results,
                angle,
                original_width=original_width,
                original_height=original_height)

        byte_data[fsf_output] = pickle.dumps(results)
        new_byte_data = byte_data

        return new_byte_data, attribute

