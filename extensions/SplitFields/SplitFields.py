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

# ----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 同じFindexを持つ属性に文字列を分割する
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")


class SplitFields(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        文字列のフィールドデータを、指定の区切り文字で分割する。
                        ①input: JoinFieldsで結合された1行のFieldSetFile。
                        ②output: 分割されたフィールドを持った、複数行のFieldSetFile。
                      """
        tags = ['JoinFields', 'Python']

    # CSVを区切り、値を格納するためのデータフレームの定義
    DEFINITION_CSV = PropertyDescriptor(
        name='Definition CSV',
        description='文字列分割後のフィールドの定義',
        default_value='''
下記を参考にして修正すること

==========ここから==========
DWH名,データ型
項目A,3
項目B,1
項目C,1
==========ここまで==========

※1行目のカラムは固定かつ必須
    2行目以降は定義項目を入力

    [データ型列入力規則]
    1…整数型
    2…浮動小数点型
    3…文字列型
    4…ブール型
                      ''',
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=True
    )

    # 文字列分割時の区切り文字
    FIELDS_DELIMITER = PropertyDescriptor(
        name="Fields Delimiter",
        description="文字列分割時の区切り文字",
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=True
    )

    property_descriptors = [DEFINITION_CSV,
                            FIELDS_DELIMITER]

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            definition_csv: CSV形式のデータフレームの定義
            fields_delimiter: 文字列分割時の区切り文字
        """

        # プロパティで設定した値を取得
        definition_csv = context.getProperty(
            self.DEFINITION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        fields_delimiter = context.getProperty(
            self.FIELDS_DELIMITER).evaluateAttributeExpressions(flowfile).getValue()

        return definition_csv, fields_delimiter

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):

        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            definition_csv, fields_delimiter = WM.calc_func_time(
                self.logger)(self.get_property)(context, flowfile)

            # flowfileからCSV形式のFieldSetFileを取得し、DataFrameに加工した後、各列の値を取得する
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                target_value\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 定義CSVをDataFrameにし、各列をリスト化
            definition_dataframe = pd.read_csv(io.StringIO(definition_csv))
            dwh_name_list = definition_dataframe["DWH名"].to_list()
            datatype_list = definition_dataframe["データ型"].to_list()

            attributes_dataframe = pd.DataFrame(
                target_value, columns=['index', 'values'])

            # 文字列データを区切り文字で分割して各列に格納
            # 例) 区切り文字"-"
            #    index values Field1 Field2 Field3
            # 0      1   A-B-C      A      B      C
            # 1      2   D-E-F      D      E      F
            # 2      3   G-H-I      G      H      I
            attributes_dataframe[dwh_name_list] = attributes_dataframe['values'].str.split(
                fields_delimiter, expand=True)

            # 元のvalues列を削除
            attributes_dataframe = attributes_dataframe.drop(columns=[
                                                             'values'])

            attributes_dataframe = attributes_dataframe.set_index('index')

            # 各カラムに対してデータ型に基づき変換を実施
            for col, dtype in zip(dwh_name_list, datatype_list):
                if dtype == "1":
                    attributes_dataframe[col] = attributes_dataframe[col].astype(
                        int)
                elif dtype == "2":
                    attributes_dataframe[col] = attributes_dataframe[col].astype(
                        float)
                elif dtype == "3":
                    attributes_dataframe[col] = attributes_dataframe[col].astype(
                        str)
                elif dtype == "4":
                    attributes_dataframe[col] = attributes_dataframe[col] = attributes_dataframe[col].apply(
                        lambda x: str(x).strip().lower() == 'true')

            # 分割されたフィールドをindexと共にリスト化
            output_value_list = [list(zip(attributes_dataframe.index, attributes_dataframe[col]))
                                 for col in attributes_dataframe.columns]

            output_type_list = [target_type]*len(output_value_list)

            # FieldSetFile構成要素の各リストをFieldSetFileに変換
            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_name_list,
                                                                                           output_type_list,
                                                                                           output_value_list
                                                                                           )

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
