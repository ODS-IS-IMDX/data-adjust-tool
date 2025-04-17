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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 属性のユニーク値を確認するためのプロセッサ。
# FieldSetFileのValue列のユニーク値を取得し、その個数をカウントする。
# その後、FieldSetFileのDwh列と結合する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
np = import_module("numpy")


class GetFieldUniqueValue(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        FieldSetFileに格納されているフィールドの、ユニーク値とその個数を取得する。
                        ①input: 任意のフィールドを持った1行のFieldSetFile。
                        ②output: フィールドのユニーク値とその個数が記入されたテキストファイル。
                      """
        tags = ['Unique', 'Python']

    property_descriptors = []

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def format_unique_value_counts(self, target_value, target_dwh):
        """
        概要:
            target_value(list)のユニーク値とその個数をフォーマットし文字列にする関数。
        引数:
            target_value: field_set_file内のValue列の値(list)
            target_dwh: field_set_file内のDwh列の値(Dwhファイル名)
        戻り値:
            formatted_result_by_unique_value_and_counts: フォーマットされた文字列
        """

        # target_valueのユニーク値をunique_value_arrayへ、ユニーク値の個数をvalue_count_arrayへ
        unique_value_array, \
            value_count_array\
            = np.unique(np.array(target_value, dtype=str)[:, 1], return_counts=True)

        # target_dwhの最後に区切り文字として@を加える
        target_dwh_at = target_dwh + "@"

        # ユニーク値の配列と、その個数の配列を文字列型でlist化
        unique_list = list(unique_value_array.astype(str))
        unique_count_list = list(value_count_array.astype(str))

        # DWH名　ユニーク値　ユニーク値の個数
        # の3つを要素間の区切り文字を","に設定し、結合する。
        formatted_result_by_unique_value_and_counts = target_dwh_at + \
            ','.join(unique_list) + "@" + ','.join(unique_count_list) + "\n"

        return formatted_result_by_unique_value_and_counts

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # flowfileからCSV形式のFieldSetFileを取得し、DataFrameに加工した後、各列の値を取得する
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                target_value\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # target_value(list or array)のユニーク値とその個数をフォーマットし文字列にする
            formatted_result_by_unique_value_and_counts = WM.calc_func_time(
                self.logger)(self.format_unique_value_counts)(target_value, target_dwh)

            return FlowFileTransformResult(relationship="success", contents=formatted_result_by_unique_value_and_counts, attributes={'filename': target_dwh + "_unique.txt"})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
