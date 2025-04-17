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

# --------------------------------------------------------------------------------------------#-----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 同じFindexを持つ属性を文字列結合する
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")


class JoinFields(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        フィールドをインデックスごとに文字列結合する。
                        ①input: 同じインデックスを持つフィールド（結合したいフィールド）を持った複数行のFieldSetFile。
                        ②output: 結合されたフィールドを持った、1行のFieldSetFile。
                      """
        tags = ['SplitFields', 'Python']

    # 結合の順番を1列で入力
    JOIN_ORDER_DWH_NAME = PropertyDescriptor(
        name="Join Order DWH Name",
        description="1列でDWH名を入力(結合したい順番)",
        default_value='''下記を参考にして修正すること

==========ここから==========
項目A
項目B
項目C
==========ここまで==========
                      ''',
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 文字列結合字時の区切り文字
    FIELDS_DELIMITER = PropertyDescriptor(
        name="Fields Delimiter",
        description="文字列結合時の区切り文字",
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 出荷時ののDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [JOIN_ORDER_DWH_NAME,
                            FIELDS_DELIMITER,
                            OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):

        try:
            join_order_dwh_name = context.getProperty(
                self.JOIN_ORDER_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            join_order_dwh_dataframe \
                = pd.read_csv(io.StringIO(join_order_dwh_name), header=None)

            join_order_dwh_list = join_order_dwh_dataframe[0].to_list()

            fields_delimiter = context.getProperty(
                self.FIELDS_DELIMITER).getValue()

            output_dwh_name = context.getProperty(
                self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # flowfileから、CSV形式のFieldSetFileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # CSVファイルの読み込み
            fieldsetdataframe = pd.read_csv(io.StringIO(input_field_set_file))

            # 空のDataFrameを準備
            attributes_dataframe = pd.DataFrame()

            for index, row in fieldsetdataframe.iterrows():
                # 列名の取得
                attributes_dwh = row["Dwh"]

                # 値のデコード
                decoded_value = pickle.loads(base64.b64decode(row["Value"]))

                # 新しい列としてDataFrameに変換
                merged_dataframe = pd.DataFrame(
                    decoded_value, columns=["Findex", attributes_dwh])

                # 最初のループでは空のDataFrameに直接代入、以降は列を追加していく
                if attributes_dataframe.empty:
                    attributes_dataframe = merged_dataframe
                else:
                    attributes_dataframe = attributes_dataframe.merge(
                        merged_dataframe, on='Findex')

            # もし指定されたカラムが存在しない場合のエラーチェック
            missing_columns = [
                col for col in join_order_dwh_list if col not in attributes_dataframe.columns]
            if missing_columns:
                self.logger.error(f"columnが存在しません:{missing_columns}")

            # Findex列を別に取り出しておく
            findex_column = attributes_dataframe['Findex']

            # DataFrameを指定カラムで並べ替え
            sorted_dataframe = attributes_dataframe[join_order_dwh_list]

            # Concatenated列を追加
            sorted_dataframe.loc[:, 'Concatenated'] \
                = sorted_dataframe.apply(lambda row: fields_delimiter.join(str(x) for x in row), axis=1)

            # Findex列を並べ替え後のDataFrameに再度追加
            sorted_dataframe['Findex'] = findex_column

            # Concatenated列とindexをタプルにまとめてリストに変換
            result_list = [(row['Findex'], row['Concatenated'])
                           for i, row in sorted_dataframe.iterrows()]

            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)([output_dwh_name],
                                                                                           ["object"],
                                                                                           [result_list])

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
