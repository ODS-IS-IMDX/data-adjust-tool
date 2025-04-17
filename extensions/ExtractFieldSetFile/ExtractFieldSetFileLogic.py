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

import cad.common.cad_utils as CU


class ExtractFieldSetFileLogic:

    def extract_field_set_file(self, input_fsf, geometry_name):
        """
        入力データのFieldSetFileから指定されたジオメトリデータ名のレコードのみを抽出し、
        対象データのみを持つFieldSetFileを生成する。

        :param input_fsf: 入力データであるFieldSetFile。
        :type input_fsf: bytes
        :param geometry_name: 抽出対象のジオメトリデータ名。
        :type geometry_name: str

        :return: 対象データのみを持つFieldSetFile。
        :rtype: str

        :raises Exception:
            プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 入力データのFieldSetFileをDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(input_fsf)

            # DataFrameのDwh列の値を基に、対象のレコードを抽出する
            target_dataframe = dataframe[dataframe["Dwh"].str.split("/").str[0] == geometry_name].reset_index(drop=True)
            if len(target_dataframe) == 0:
                # 指定されたジオメトリデータ名と一致するレコードが無い場合、エラーを投げる
                raise Exception(f"Undefined geometry name: {geometry_name}.")

            # 抽出したDataFrameからFieldSetFileを生成
            field_set_file = CU.dataframe_to_field_set_file(target_dataframe)

            return field_set_file

        except Exception as e:
            raise Exception(f"[extract_field_set_file Exception]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: バイナリ形式のFieldSetFile
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 処理結果のFieldSetFileと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティ値取得
            geometry_name = properties["GEOMETRY_NAME"]

            # メイン処理を実行
            results = self.extract_field_set_file(byte_data, geometry_name)

            return results, attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
