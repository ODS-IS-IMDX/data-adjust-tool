# MIT License
#
# Copyright (c) 2026 NTT InfraNet
# Copyright (c) 2026 NTT DATA Japan Co., Ltd.
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

from nifiapi.properties import PropertyDescriptor
from raster_to_vector.common.base_processor import BaseProcessor


class UpdateAttributeFromJson(BaseProcessor):
    """
    FlowFileのコンテンツに格納されているJSONを解析し、キーバリューの形式でFlowFileのアトリビュートに登録/更新するプロセッサ。
    抽出結果をFlowFileのアトリビュートとして設定する。(キー,キー,キー,・・・,キー＝Value)
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = 'Flatten nested JSON content and write key-value pairs as FlowFile attributes.'
        tags = ['json', 'attributes', 'transform']

    # -----------------------------
    # プロパティ定義
    # -----------------------------
    CLEAR_CONTENTS = PropertyDescriptor(
        name='ClearContents',
        description='処理後に content を空にするか指定 (true=空にする / false=空にしない)',
        required=True,
        default_value='true',
        allowable_values=['true', 'false'],
    )

    KEY_NAME_PREFIX = PropertyDescriptor(
        name='KeyNamePrefix',
        description='登録する Attribute の前に付けるJSONファイル名を入力(例：test → test.属性名)',
        required=False,
        default_value='',
        allowable_values=None,
    )

    UPDATE_MODE = PropertyDescriptor(
        name='UpdateMode',
        description='同一キーが既存属性と重複した場合の対応の指定。(overwrite:値を上書きする/skip:値を上書きしない/addsuffix:キーに接尾語を付与(1~の連番)/failure:Failure(異常終了)にする)',
        required=True,
        default_value='overwrite',
        allowable_values=['overwrite', 'skip', 'addsuffix', 'failure'],
    )

    property_descriptors = [CLEAR_CONTENTS, UPDATE_MODE, KEY_NAME_PREFIX]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    # -----------------------------
    # リレーションシップ
    # -----------------------------
    class Relationships:
        success = 'success'
        failure = 'failure'

    # -----------------------------
    # Logicクラスとの接続
    # -----------------------------
    @BaseProcessor.set_logic_class('UpdateAttributeFromJsonLogic.UpdateAttributeFromJsonLogic')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
