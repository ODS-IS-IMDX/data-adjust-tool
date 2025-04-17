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

from common.error_code_list import ErrorCodeTable, ErrorCodeList


class BaseValidateLogger:

    """
    NiFiプロセッサでのエラーやイベントを記録するための共通ロギングクラス。
    """
    def __init__(self, logger):
        self.logger = logger

    def write_normal_log(self, error_code: ErrorCodeList, record_number: int = None, **kwargs):
        """
        通常のログを出力する。

        :param error_code: エラー番号
        :type error_code: Enum
        :param record_number: レコード番号
        :type record_number:int
        :param **kwargs: キーワード引数
        :type **kwargs:dict[str, any]
        """
        self.write_log(error_code=error_code, record_number=record_number,
                       ** kwargs
                       )

    def write_fsf_log(self, error_code: ErrorCodeList, column_name: str = None, fid: int = None,
                      **kwargs
                      ):
        """
        FieldSetFileのログを出力する。

        :param error_code: エラー番号
        :type error_code: Enum
        :param column_name: 列名称
        :type column_name: str
        :param fid: FID
        :type fid:int
        :param **kwargs: キーワード引数
        :type **kwargs:dict[str, any]
        """
        self.write_log(error_code=error_code, column_name=column_name, fid=fid,
                       **kwargs
                       )

    def write_log(self, error_code: ErrorCodeList, column_name: str = None, fid: int = None, record_number: int = None,
                  **kwargs
                  ):
        """
        エラーログを記録する共通関数。

        :param error_code: エラー番号
        :type error_code: Enum
        :param column_name: 列名称
        :type column_name: str
        :param fid: FID
        :type fid:int
        :param record_number: レコード番号
        :type record_number:int
        :param **kwargs: キーワード引数
        :type **kwargs:dict[str, any]
        """
        error_message = ErrorCodeTable.get_error_message(error_code)
        messages = [f"[{error_code.value}]", f"[{error_message}]"]
        if column_name:
            messages.append(f"[Dwh列:{column_name}]")
        if fid:
            messages.append(f"[FID:{fid}]")
        if record_number:
            messages.append(f"[レコード番号:{record_number}]")
        if kwargs:
            messages.append("".join(f"[{key}:{value}]" for key, value in kwargs.items()))
        log_message = " - ".join(messages)

        self.logger.error(log_message)
