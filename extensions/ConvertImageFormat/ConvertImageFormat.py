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

from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope
from raster_to_vector.common.base_processor import BaseProcessor

class ConvertImageFormat(BaseProcessor):
    """
    ファイルパス参照方式で画像形式を変換するNiFi Python Processor。
    FlowFileのコンテンツは使用せず、Pillowで直接ディスク上の画像を変換する。
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "1.0.0"
        description = "FlowFileのコンテンツを使用せず、ファイルパスを指定して画像形式を変換する。PDF入力にも対応。"
        tags = ["image", "convert", "pillow", "path", "jpeg", "png", "webp", "gif", "pdf"]

    # === プロパティ定義 ===
    INPUT_PATH = PropertyDescriptor(
        name='Input Path',
        description='入力画像ファイルの絶対パス',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    OUTPUT_PATH = PropertyDescriptor(
        name='Output Path',
        description='出力画像ファイルの絶対パス',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    OUTPUT_FORMAT = PropertyDescriptor(
        name='Output Format',
        description='出力フォーマット（例: JPEG, PNG, WEBP, GIF）',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        allowable_values=[
            "JPEG",
            "PNG",
            "WEBP",
            "GIF",
        ],
        default_value='PNG',
        required=False
    )

    QUALITY = PropertyDescriptor(
        name='Quality',
        description='出力品質（JPEG/WEBPのみ有効）1〜100',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value='90',
        required=False
    )

    COLOR_MODE = PropertyDescriptor(
        name='Color Mode',
        description='出力時のカラーモード（例: RGB, L, CMYK）',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value='RGB',
        required=False
    )

    TRANSPARENT_BG = PropertyDescriptor(
        name='Transparent Background',
        description='透過画像を白背景に変換するか',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        allowable_values=["True", "False"],
        default_value='True',
        required=False
    )

    OVERWRITE = PropertyDescriptor(
        name='Overwrite',
        description='出力先ファイルを上書きするかしないか(しない場合はRenameフォルダが新しく作成され、その中に格納される)',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        allowable_values=["True", "False"],
        default_value='True',
        required=False
    )

    PDF_PAGE_MODE = PropertyDescriptor(
        name='PDF Page Mode',
        description='PDF入力時のページの扱い方 (MERGED_VERTICAL: 縦に連結, MERGED_HORIZONTAL: 横に連結, SEPARATE_FILES: ページごとに別ファイル出力)',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        allowable_values=[
            "MERGED_VERTICAL",
            "MERGED_HORIZONTAL",
            "SEPARATE_FILES",
        ],
        default_value='MERGED_VERTICAL',
        required=False
    )

    property_descriptors = [
        INPUT_PATH,
        OUTPUT_PATH,
        OUTPUT_FORMAT,
        QUALITY,
        COLOR_MODE,
        TRANSPARENT_BG,
        OVERWRITE,
        PDF_PAGE_MODE,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ConvertImageFormatLogic.ConvertImageFormatLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
