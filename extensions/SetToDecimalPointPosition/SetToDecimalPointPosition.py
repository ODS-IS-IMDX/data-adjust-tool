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

# -----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 小数点以下の桁数をプロパティで入力した桁数に設定するプロセッサ。
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")


class SetToDecimalPointPosition(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        フィールドデータを指定の小数点位置で四捨五入する。
                        ①input: 数値型のフィールドを持った1行のFieldSetFile。
                        ②output: 数値型のフィールドを持った1行のFieldSetFile。
                      """
        tags = ['DecimalPoint', 'Python']

    # 小数点を第何位に置くのか
    ROUND_NUMBER = PropertyDescriptor(
        name="Round Number",
        description="出力データの小数点以下の桁数",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [ROUND_NUMBER]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ

        戻り値:
            round_number: 小数点第何位に置くのかが書かれた整数(例:2  (第二位まで表示、第三位を四捨五入する))
        """

        round_number = int(context.getProperty(self.ROUND_NUMBER).getValue())

        return round_number

    def process_tuple_values_to_pairs(self, target_value, round_number):
        """
        概要:
            タプルの2番目の要素が欠損値 "nan" か "None" の空文字列の場合、タプルの1番目の要素と空文字列をペアとして追加
            それ以外の場合は、タプルの1番目の要素と2番目の要素を浮動小数点数に変換し、指定された小数点以下の桁数で四捨五入した値としてペアを追加

        引数:
            target_value: 処理対象のタプルのリスト
            round_number: 四捨五入する桁数

        戻り値:
            rounded_list: 処理されたペアのリスト
        """

        rounded_list = [(ti[0], "") if str(ti[1]) == "nan" or str(ti[1]) == "None" or str(
            ti[1]) == "" else (ti[0], np.round(float(ti[1]), round_number)) for ti in target_value]

        return rounded_list

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで入力した小数点の位置を定義。
            round_number = WM.calc_func_time(
                self.logger)(self.get_property)(context)

            # FieldSetFileをDataFrameにし、各列の値を抜き出す。Value列だけデシリアライズ、デコードを行う
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                target_value\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # FieldSetFileのValuerの列target_valueのタプルのリストを処理し、各タプルの2要素を指定された桁数で四捨五入し、ペアのリストを生成
            rounded_list = WM.calc_func_time(self.logger)(
                self.process_tuple_values_to_pairs)(target_value, round_number)

            # set_field_set_file引数用にリスト化
            target_dwh_list = [target_dwh]
            target_type_list = [target_type]
            rounded_number_list = [rounded_list]

            # 加算された配列をFieldSetFileに加工
            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(target_dwh_list,
                                                                                           target_type_list,
                                                                                           rounded_number_list)

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
