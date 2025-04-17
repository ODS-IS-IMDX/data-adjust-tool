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
# CSVの入荷プロセッサ。
# CSVを入荷し、DataFrameに変換し、定義通りに型変更を行う。
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")

TRUE_COLUMN_FLAG = "カラムあり"
FALSE_COLUMN_FLAG = "カラムなし"


class ConvertCSVToDataFrame(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        CSVファイルをDataFrameに変換する。
                        ①input: 任意のCSVファイル。
                        ②output: CSVから作成したDataFrame。
                      """
        tags = ['CSV', 'DataFrame', 'Python']

    # インプットCSVファイルのデコードに用いる文字コード
    INPUT_CSV_ENCODING_CODE = PropertyDescriptor(
        name='Input CSV Encoding Code',
        description='CSVのデコードに用いる文字コード',
        default_value='utf-8',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # CSVを区切り、値を格納するためのデータフレームの定義。
    DEFINITION_CSV = PropertyDescriptor(
        name='Definition CSV',
        description='CSV形式のデータフレームの定義（"カラム名","データ型"が必須）',
        default_value="""下記を参考にして修正すること

==========ここから==========
カラム名,データ型
項目A,1
項目B,2
項目C,3
==========ここまで==========

 [データ型列入力規則]
    1…整数型
    2…浮動小数点型
    3…文字列型

 ※Empty string setの場合は自動的に型を設定
                      """,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # inputのCSVにカラムがあるのかのflag
    COLUMN_FLAG = PropertyDescriptor(
        name='Column Flag',
        description='入力データのCSVにカラムが書かれているかどうか',
        allowable_values=[TRUE_COLUMN_FLAG, FALSE_COLUMN_FLAG],
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    property_descriptors = [INPUT_CSV_ENCODING_CODE,
                            DEFINITION_CSV,
                            COLUMN_FLAG]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに渡されるデータ

        戻り値:
            input_csv_encoding_code: 入荷CSVの文字コード
            definition_csv: データ定義のCSV
            column_flag: 入荷CSVにカラムがあるのかのフラグ
        """
        input_csv_encoding_code = context.getProperty(
            self.INPUT_CSV_ENCODING_CODE).getValue()

        definition_csv = context.getProperty(self.DEFINITION_CSV).getValue()

        column_flag = context.getProperty(self.COLUMN_FLAG).getValue()

        return input_csv_encoding_code, definition_csv, column_flag

    def transform(self, context, flowfile):
        try:

            # --------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # --------------------------------------------------------------------------
            input_csv_encoding_code, \
                definition_csv, \
                column_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # --------------------------------------------------------------------------
            # definition_csvプロパティが入力されなかった場合: 型指定せずDataFrameに
            # --------------------------------------------------------------------------
            if not definition_csv:
                target_csv = flowfile.getContentsAsBytes().decode(input_csv_encoding_code)
                target_dataframe = pd.read_csv(
                    io.StringIO(target_csv))

            # --------------------------------------------------------------------------
            # definition_csvプロパティがされている場合: 型指定してDataFrameに
            # --------------------------------------------------------------------------
            else:

                # --------------------------------------------------------------------------
                # # 定義CSVをDataFrameにし、各列をlist化
                # --------------------------------------------------------------------------
                definition_dataframe = pd.read_csv(io.StringIO(definition_csv))
                column_list = definition_dataframe["カラム名"].to_list()
                datatype_list = definition_dataframe["データ型"].to_list()

                # --------------------------------------------------------------------------
                # それぞれの型のindexのlistを取得
                # --------------------------------------------------------------------------
                indices_of_int = [index for index,
                                  value in enumerate(datatype_list) if value == "1"]
                indices_of_float = [index for index,
                                    value in enumerate(datatype_list) if value == "2"]
                indices_of_string = [index for index,
                                     value in enumerate(datatype_list) if value == "3"]

                # --------------------------------------------------------------------------
                # CSVデータの入荷
                # --------------------------------------------------------------------------
                target_csv = flowfile.getContentsAsBytes().decode(input_csv_encoding_code)

                # --------------------------------------------------------------------------
                # columnがある場合そのままDataFrameに加工
                # --------------------------------------------------------------------------
                if column_flag == TRUE_COLUMN_FLAG:
                    target_dataframe = pd.read_csv(
                        io.StringIO(target_csv), dtype=str)

                # --------------------------------------------------------------------------
                # columnがない場合、プロパティから取得したcolumn_listをcolumnとして扱う
                # --------------------------------------------------------------------------
                else:
                    target_dataframe = pd.read_csv(
                        io.StringIO(target_csv), header=None, dtype=str)
                    target_dataframe.columns = column_list

                # --------------------------------------------------------------------------
                # 各行をプロパティから取得した型に変更
                # --------------------------------------------------------------------------
                # int型に変換
                if indices_of_int:
                    for column_index in indices_of_int:
                        target_dataframe.iloc[:, column_index] = target_dataframe.iloc[:, column_index].astype(
                            int)

                # float型に変換
                if indices_of_float:
                    for column_index in indices_of_float:
                        target_dataframe.iloc[:, column_index] = target_dataframe.iloc[:, column_index].astype(
                            float)

            return FlowFileTransformResult(relationship="success",
                                           contents=pickle.dumps(target_dataframe))

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
