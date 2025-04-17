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
import os
import base64
import pickle
import io
from importlib import import_module

# 外部ライブラリの動的インポート
Image = import_module("PIL.Image")

from raster_to_vector.common.base_raster_vector_logic import BaseRasterVectorLogic


class ImageLinkProcessorLogic(BaseRasterVectorLogic):
    def __init__(self):
        pass

    def __call__(self, byte_data, attribute, properties):
        """
        バイナリ化された画像データを受け取り、HTMLに埋め込んだ画像を表示するためのリンクを生成し、
        画像ファイルも保存するメソッドです。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace の指定はありません。
        properties : dict
            プロパティ設定を含む辞書です。
            - output_dir: string
                出力ディレクトリが設定されます。
                
            - html_filename: string
                HTMLファイル名（拡張子は含めません）が設定されます。

        Returns
        -------
        tuple
            - 新しいコンテンツ（バイナリデータ）と属性（HTMLファイルと画像ファイルへのリンク）

        Raises
        ------
        Exception
            - ディレクトリが作成できない場合や、ファイルの書き込みに失敗した場合には例外を発生させます。
        """
        # 画像バイナリデータをデシリアライズ
        image_data = pickle.loads(byte_data)

        # 画像データをPILのImageオブジェクトに変換
        img = Image.fromarray(image_data)

        # 保存するディレクトリをプロパティから取得
        output_dir = properties.get('output_dir', 'tmp/nifi_images')

        # 相対パスの場合はカレントディレクトリで補完する
        if not os.path.isabs(output_dir):
            output_dir = os.path.abspath(output_dir)

        # ディレクトリが存在しない場合は作成
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # ファイル名を生成
        base_filename = properties.get("html_filename", "image_preview")
        image_file_name = f'{base_filename}.png'
        html_file_name = f'{base_filename}.html'

        image_file_path = os.path.join(output_dir, image_file_name)
        html_file_path = os.path.join(output_dir, html_file_name)

        # 画像を保存
        img.save(image_file_path, format="PNG")

        # バッファから画像データをBase64でエンコード
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        base64_image = base64.b64encode(buffered.getvalue()).decode('utf-8')

        # 画像を表示するHTMLを作成
        html_content = f'''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Image Preview</title>
        </head>
        <body>
            <h1>Image Preview</h1>
            <img src="data:image/png;base64,{base64_image}" alt="Image"/>
            <p>Direct image link: <a href="file://{image_file_path}">{image_file_name}</a></p>
        </body>
        </html>
        '''

        # HTMLファイルを保存
        with open(html_file_path, 'w') as html_file:
            html_file.write(html_content)

        # 新しいFlowFileの属性にローカルのHTMLファイルパスと画像ファイルパスを追加
        # ローカルファイルを開くために 'file://' を使う
        file_url = f'file://{html_file_path}'
        image_url = f'file://{image_file_path}'

        new_attributes = {
            'html.link': file_url,       # Data Provenanceに表示するローカルパス (HTML)
            'html.filepath': html_file_path,
            'image.link': image_url,     # 画像のローカルパス
            'image.filepath': image_file_path
        }
        attribute.update(new_attributes)

        return byte_data, attribute
