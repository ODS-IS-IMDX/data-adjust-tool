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

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from raster_to_vector.common.base_processor import BaseProcessor


class ImageLinkProcessor(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageLinkProcessor

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像をhtmlとpngファイルに保存するプロセッサクラスです。

    **処理概要:**

    - 入力データ（バイナリ化された画像）と属性の妥当性をチェックします。
    - 画像をディスクに保存し、Base64形式にエンコードされた画像を含むHTMLを生成します。
    - 出力ディレクトリが存在しない場合、自動的に作成されます。
    - output_dirプロパティとhtml_filenameプロパティを使用して、出力先やファイル名を指定します。
    
    タグ
    --------------------------------
    * image processing
    * html generation
    * file output

    プロパティ
    --------------------------------
    **Output Directory**

    **必須入力  直接入力**

    - 出力ディレクトリを指定します。
    - 画像およびHTMLファイルを保存するディレクトリです。
    - デフォルトは tmp/nifi_images です。

    **HTML Filename**

    **任意入力  直接入力**

    - HTMLファイル名を指定します（拡張子は含めません）。
    - デフォルトはimage_previewです。

    リレーションシップ
    --------------------------------
    * success: 画像の保存とHTML生成が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * なし

    Writes Attributes
    --------------------------------
    * html.link: 生成されたHTMLファイルへのローカルファイルURL
    * html.filepath: 生成されたHTMLファイルへのファイルパス
    * image.link: 保存された画像ファイルへのローカルファイルURL
    * image.filepath: 保存された画像ファイルへのファイルパス

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(色空間の指定なし)である必要があります。

    System Resource Considerations
    --------------------------------
    * ディスク容量: 画像とHTMLファイルを保存するため、十分なディスク容量が必要です。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'Processor that converts binary image to HTML link'

    output_dir = PropertyDescriptor(
        name="Output Directory",
        description="出力ディレクトリを指定します。\
                    画像およびHTMLファイルを保存するディレクトリです。\
                    デフォルトは tmp/nifi_images です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="tmp/nifi_images",
        sensitive=False
    )

    html_filename = PropertyDescriptor(
        name="HTML Filename",
        description="HTMLファイル名を指定します（拡張子は含めません）。\
                    デフォルトはimage_previewです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="image_preview",
        sensitive=False
    )

    property_descriptors = [output_dir, html_filename]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageLinkProcessorLogic.ImageLinkProcessorLogic')
    def __init__(self, **kwargs):
        """
        初期化メソッド。
        """
        super().__init__(**kwargs)
