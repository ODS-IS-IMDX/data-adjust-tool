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
from importlib import import_module

# 外部ライブラリの動的インポート
ezdxf = import_module("ezdxf")

from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from common.error_code_list import ErrorCodeList


class ValidateConvertFromDxfToGeoDataFrame(CadBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """プロセッサー「ConvertFromDxfToGeoDataFrame」の入力データのチェックを行う"""
        tags = ["ezdxf", "dxf", "python", "validate"]

    def validate_dxf_file_content(self, file_path):
        """
        dxfファイルの内容が正しいかチェックする

        :param file_path: dxfのフルパス
        :type file_path: str

        :return: チェック結果 正常=True、異常=False
        :rtype: bool

        :raises Exception: dxfファイルの形式チェック処理中にエラーが発生した場合に例外をスローする
        """
        try:
            ezdxf.readfile(file_path)
            return True
        except Exception as e:
            self.validate_logger.write_log(ErrorCodeList.EC00006, **{"ファイル内容": "Dxf形式ではありません。"})
            return False

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
            # ファイル名の拡張子がdxfか確認
            filename = flowfile.getAttribute("filename")
            dir_path = flowfile.getAttribute("absolute.path")
            dxf_file_path = os.path.join(dir_path, filename)

            ext = os.path.splitext(filename)[1]
            if ext != '.dxf':
                self.validate_logger.write_log(ErrorCodeList.EC00006, **{"ファイル拡張子": ext})
                return self.RESULT_FAILURE

            # ファイルの内容がCADか確認
            if not self.validate_dxf_file_content(dxf_file_path):
                return self.RESULT_FAILURE
            return self.RESULT_SUCCESS
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
