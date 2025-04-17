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

from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
import cad.common.cad_utils as CU

import re


class ValidateAlignMapGrid(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「AlignMapGrid」の入力データのチェックを行う'''
        tags = [
            "fieldsetfile",
            "dataframe",
            'python',
            'validate'
        ]

    def replace_sections(self, text):
        """
        dwh名を被らないようにする
        
        :param text: FieldSetFile
        :type text: bytes
        
        :return: 修正後のFieldSetFile
        :rtype: bytes
        """
        try:
            # 文字列にデコード
            text = text.decode('utf-8')

            # "文字列/geometry" -> "文字列(geometry)/geometry"
            text = re.sub(r"(\w+)/geometry", r"\1(geometry)/geometry", text)

            # "文字列/mesh" -> "文字列(mesh)/geometry"
            text = re.sub(r"(\w+)/mesh", r"\1(mesh)/geometry", text)

            # 結果をバイト列に戻して返す
            return text.encode('utf-8')
        except Exception as e:
             raise Exception(f"[replace_sections]: {str(e)}")

    def validate_data(self, context, flowfile):
        """
        入力データのチェックを実行する

        :param context: プロセッサの設定値が格納されているデータ
        :param flowfile: プロセッサに渡されるデータ

        :return: チェック結果 正常="success"、異常="failure"
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # インプットデータ取得
            fsf = flowfile.getContentsAsBytes()

            # データの存在チェック
            if not self.validate_empty_data(fsf):
                return self.RESULT_FAILURE

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(fsf)
            if not result:
                return self.RESULT_FAILURE
            
            # meshとgeometryが含まれているかチェック
            if self.validate_exist_attribute(df, ["mesh", "geometry"]):
                fsf = self.replace_sections(fsf)
                df = CU.field_set_file_to_dataframe(fsf)
            else:
                return self.RESULT_FAILURE

            # バリデータログ作成
            if not self.validate_log_of_fsf(fsf, df):
                return self.RESULT_FAILUER

            return self.RESULT_SUCCESS

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
