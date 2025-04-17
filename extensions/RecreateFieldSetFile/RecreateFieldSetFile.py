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
# 分割データの再構成を行う。
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


class RecreateFieldSetFile(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        レコードをまたいで分割されたデータを、継続判定項目を用いて結合する。
                        ①input: フィールドデータと、その継続判定項目、主キーを持った3行以上のFieldSetFile。
                        ②output: 結合したフィールドデータと主キー持った、2行のFieldSetFile。
                      """
        tags = ['Binary', 'Python']

    # 入荷時の継続判定項目のDWH名
    INPUT_CONTINUATION_DECISION_DWH = PropertyDescriptor(
        name="Input Continuation Decision DWH",
        description="入力データの継続判定項目のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # 継続判定項目値
    CONTINUATION_DECISION_VALUE = PropertyDescriptor(
        name="Continuation Decision Value",
        description="継続判定値",
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=True
    )

    # 入荷時の分割されたバイナリ項目のDWH名
    INPUT_PARTITIONED_BINARY_DWH = PropertyDescriptor(
        name="Input Partitioned Binary DWH",
        description="入力データの分割バイナリ項目のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 入荷時の主キーのDWH名
    INPUT_PRIMARY_KEY_DWH = PropertyDescriptor(
        name="Input Primary Key DWH",
        description="入力データ主キーのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 出荷時のバイナリデータのDWH名
    OUTPUT_BINARY_DWH = PropertyDescriptor(
        name="Output Binary DWH",
        description="出力のバイナリデータのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 出荷時の主キーのDWH名
    OUTPUT_PRIMARY_KEY_DWH = PropertyDescriptor(
        name="Output Primary Key DWH",
        description="出力データの主キーのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    property_descriptors = [INPUT_CONTINUATION_DECISION_DWH,
                            CONTINUATION_DECISION_VALUE,
                            INPUT_PARTITIONED_BINARY_DWH,
                            INPUT_PRIMARY_KEY_DWH,
                            OUTPUT_BINARY_DWH,
                            OUTPUT_PRIMARY_KEY_DWH]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティの値を取得する。

        引数:
            context: フローファイルのコンテキスト
            flowfile: 対象のFlowFile

        戻り値:
            input_continuation_decision_dwh: 入荷時の継続判定項目DWH名
            continuation_decision_value: 継続判定値
            input_partitioned_binary_dwh: 荷時の分割されたバイナリ項目のDWH名
            input_primary_key_DWH: 入荷時の主キーのDWH名
            output_binary_dwh: 出荷時のバイナリのDWH名
            output_primary_key_dwh: 出荷時の主キーのDWH名
        """

        # 入荷時の継続判定項目
        input_continuation_decision_dwh \
            = context.getProperty(self.INPUT_CONTINUATION_DECISION_DWH).evaluateAttributeExpressions(flowfile).getValue()

        # 継続判定項目値
        continuation_decision_value \
            = context.getProperty(self.CONTINUATION_DECISION_VALUE).getValue()

        # 入荷時の分割されたバイナリ項目のDWH名
        input_partitioned_binary_dwh \
            = context.getProperty(self.INPUT_PARTITIONED_BINARY_DWH).evaluateAttributeExpressions(flowfile).getValue()

        # 入荷時の主キーのDWH名
        input_primary_key_DWH \
            = context.getProperty(self.INPUT_PRIMARY_KEY_DWH).evaluateAttributeExpressions(flowfile).getValue()

        # 出荷時のバイナリのDWH名
        output_binary_dwh \
            = context.getProperty(self.OUTPUT_BINARY_DWH).evaluateAttributeExpressions(flowfile).getValue()

        # 出荷時の主キーのDWH名
        output_primary_key_dwh \
            = context.getProperty(self.OUTPUT_PRIMARY_KEY_DWH).evaluateAttributeExpressions(flowfile).getValue()

        return input_continuation_decision_dwh, \
            continuation_decision_value, \
            input_partitioned_binary_dwh, \
            input_primary_key_DWH, \
            output_binary_dwh, \
            output_primary_key_dwh

    def create_new_index(self, attributes_dataframe, input_continuation_decision_dwh, continuation_decision_value):
        """
        概要:
            継続判定項目に基づいて新しいインデックスを作成する。

        引数:
            attributes_dataframe: 処理するDataFrame
            input_continuation_decision_dwh: 継続判定項目のDWH名
            continuation_decision_value: 継続判定値

        戻り値:
            new_index_list: 新しく作成したインデックスのリスト
        """

        # new_index列を作成
        result_df_copy = attributes_dataframe.copy()
        new_index_list = []
        current_index = 0

        for value in result_df_copy[input_continuation_decision_dwh]:
            if str(value) == continuation_decision_value:

                # 現在のインデックスを追加
                new_index_list.append(current_index)
            else:
                new_index_list.append(current_index)
                # 0の時は現在のインデックスを追加し,次のインデックスに進める
                current_index += 1
        return new_index_list

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):

        try:
            # プロパティで入力した値を取得
            input_continuation_decision_dwh, \
                continuation_decision_value, \
                input_partitioned_binary_dwh, \
                input_primary_key_DWH, \
                output_binary_dwh, \
                output_primary_key_dwh \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

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

            new_index_list \
                = WM.calc_func_time(self.logger)(self.create_new_index)(attributes_dataframe, input_continuation_decision_dwh, continuation_decision_value)

            # new_index列を作成
            result_df_copy = attributes_dataframe.copy()
            new_index_list = []
            current_index = 0

            for value in result_df_copy[input_continuation_decision_dwh]:
                if str(value) == continuation_decision_value:

                    # 現在のインデックスを追加
                    new_index_list.append(current_index)
                else:
                    new_index_list.append(current_index)
                    # 0の時は現在のインデックスを追加し,次のインデックスに進める
                    current_index += 1

            # new_index列作成
            result_df_copy['new_index'] = new_index_list
            agg_dict = {input_partitioned_binary_dwh: "sum"}  # バイナリデータはリストを結合
            for col in result_df_copy.columns:
                if col != 'new_index' and col != input_partitioned_binary_dwh:
                    agg_dict[col] = 'first'  # 他の列は最初の値を取得

            result_df_copy = result_df_copy.groupby('new_index').agg(agg_dict)

            # Findexとバイナリデータのタプルをまとめたリスト
            findex_bin_tuples = list(
                zip(result_df_copy['Findex'], result_df_copy[input_partitioned_binary_dwh]))

            # Findexと結合済みキー項目のタプルをまとめたリスト
            findex_key_tuples = list(
                zip(result_df_copy['Findex'], result_df_copy[input_primary_key_DWH]))

            output_dwh_list = [output_binary_dwh, output_primary_key_dwh]
            output_type_list = ["object", "object"]
            output_value_list = [findex_bin_tuples, findex_key_tuples]

            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_list,
                                                                                           output_type_list,
                                                                                           output_value_list)

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
