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


class GetJsonWorkContent(BaseProcessor):
    """
    引数のjson_filenameを取得し、そのファイル内容より、WorkContentを抜き出して属性設定にあうもののみに
    抜き出して返す、NiFi Python Processor。
    """
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = 'FlowFile本文内のJSONからjson_filenameを読み取り、そのファイル内容より、WorkContentを抜き出してattributeのwork_contentを書き換える。'
        tags = ['file', 'json', 'list', 'info']

    # === プロパティ定義 ===
    JSON_FILENAME = PropertyDescriptor(
        name='Json Filename',
        description='GEOJSONのファイル名',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value='',
        required=True
    )

    WORK_TYPE = PropertyDescriptor(
        name='Work Type',
        description='作業種別',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value='',
        required=True
    )

    MISSING_JSON_POLICY = PropertyDescriptor(
        name='Missing Json Policy',
        description='JSONファイルが存在しない場合の挙動',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value='default',
        required=False
    )

    INPUT_FIELD_NAMES = PropertyDescriptor(
        name='Input Field Names',
        description='設定する属性の種類リスト',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value='',
        required=False
    )

    # === プロパティ一覧 ===
    property_descriptors = [
        JSON_FILENAME,
        WORK_TYPE,
        MISSING_JSON_POLICY,
        INPUT_FIELD_NAMES
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('GetJsonWorkContentLogic.GetJsonWorkContentLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
