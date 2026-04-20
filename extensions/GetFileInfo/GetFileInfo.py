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


class GetFileInfo(BaseProcessor):
    """
    FlowFileの本文(JSON)から input_path を取得し、
    指定フォルダ以下のファイル情報を収集するNiFi Python Processor。
    """
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = 'FlowFile本文内のJSONからinput_pathを読み取り、指定フォルダ以下のファイル一覧情報(JSON)を出力する。'
        tags = ['file', 'json', 'list', 'info']

    # === プロパティ定義 ===
    INPUT_PATH = PropertyDescriptor(
        name='Input Path',
        description='フォルダパス、もしくはファイルパス。',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value='',
        required=True
    )
        
    INCLUDE_EXTENSIONS = PropertyDescriptor(
        name='Include Extensions',
        description='カンマ区切りで指定された拡張子のみを対象とする（例: jpg,png,tif,bmp）。空欄なら全て対象。',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value='',
        required=False
    )

    RECURSIVE_SEARCH = PropertyDescriptor(
        name='Recursive Search',
        description='サブフォルダを再帰的に探索するかどうか。',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        allowable_values=['True', 'False'],
        default_value='True',
        required=True
    )

    IGNORE_HIDDEN = PropertyDescriptor(
        name='Ignore Hidden Files',
        description='隠しファイルを無視するかどうか。',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        allowable_values=['True', 'False'],
        default_value='True',
        required=True
    )

    INCLUDE_FILE_SIZE = PropertyDescriptor(
        name='Include File Size',
        description='出力JSONにファイルサイズ(image_size)を含めるかどうか。',
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        allowable_values=['True', 'False'],
        default_value='True',
        required=True
    )

    # === プロパティ一覧 ===
    property_descriptors = [
        INCLUDE_EXTENSIONS,
        RECURSIVE_SEARCH,
        IGNORE_HIDDEN,
        INCLUDE_FILE_SIZE,
        INPUT_PATH
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('GetFileInfoLogic.GetFileInfoLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
