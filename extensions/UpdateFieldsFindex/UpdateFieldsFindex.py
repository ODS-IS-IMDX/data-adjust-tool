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
# 属性とresults型データを取得し、属性のFindexをresults型データから取得した新しいFindexに置き換える
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")


class UpdateFieldsFindex(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        要素の個数が更新される処理を行った後に、インデックス更新データを用いてインデックスを整える。
                        ①input: インデックス更新用データと1つ以上のフィールドを持った複数行のFieldSetFile。
                        ②output: インデックスが更新された座標配列とフィールドを持った、1行以上のFieldSetFile。
                      """
        tags = ['Results', 'Python']

    # 座標データ(データフレーム)のDWH名
    RESULTS_DWH_NAME = PropertyDescriptor(
        name="Index Data DWH Name",
        description="インデックス更新用データのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [RESULTS_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            results_dwh_name: インデックス更新用データのDWH名
        """

        # プロパティで設定した値を取得
        results_dwh_name = context.getProperty(
            self.RESULTS_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return results_dwh_name

    def extract_and_process_flowfile(self, flowfile, results_dwh_name, feature_id_column_name='FID'):
        """
        概要:
            FlowFileからデータを抽出し、FieldSetFileをDataFrameに加工し、必要なデータを取得する関数

        引数:
            flowfile: プロセッサに入ってくるデータ
            results_dwh_name: インデックス更新用データのDWH名
            feature_id_column_name: ID関連データのカラム名（デフォルトは 'FID'）

        戻り値:
            results_dataframe: Findex-findexのDataFrame
            attributes_dataframe: 読み込んだ全属性のDataFrame
            attributes_dwh_name_list: 属性のDWH名のlist
        """

        # flowfileから、csv形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        # field_set_file_data_frameからresults型データを抽出
        fid_fid_tuple = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == results_dwh_name, "Value"].values[0]))

        # DataFrameに変換
        results_dataframe = pd.DataFrame(
            fid_fid_tuple, columns=["Findex", "findex"])
        results_dataframe["findex"] = results_dataframe["findex"].astype(float)

        # 属性のvalueのリストを作成
        attributes_dataframe_list = []
        attributes_dwh_name_list = []

        for index, row in field_set_file_data_frame.iterrows():

            # 属性データである場合
            if row["Dwh"] != results_dwh_name:

                attributes_dwh = row["Dwh"]

                attributes_dwh_name_list.append(attributes_dwh)

                decoded_value = pickle.loads(base64.b64decode(row["Value"]))

                # FIDの場合 "findex"は右
                if feature_id_column_name in attributes_dwh:
                    attributes_dataframe = pd.DataFrame(
                        decoded_value, columns=[attributes_dwh, "findex"])

                # FIDでない場合 "findex"は左
                else:
                    attributes_dataframe = pd.DataFrame(
                        decoded_value, columns=["findex", attributes_dwh])

                attributes_dataframe["findex"] = attributes_dataframe["findex"].astype(
                    float)

                attributes_dataframe_list.append(attributes_dataframe)

        # 最終的に全ての属性 DataFrame を結合
        for i, dataframe in enumerate(attributes_dataframe_list):

            if i == 0:

                attributes_dataframe = dataframe

            else:

                attributes_dataframe = attributes_dataframe.merge(
                    dataframe, on="findex", how='left')

        return results_dataframe, attributes_dataframe, attributes_dwh_name_list

    def merge_results_dataframe_and_attributes_dataframe(self, results_dataframe, attributes_dataframe):
        """
        概要:
            results型データから作成したDataFrameと属性データから作成したDataFrameをマージし、重複するFindexを削除して返す

        引数:
            results_dataframe: Findex-findex の dataframe
            attributes_dataframe: 属性のdataframedatafram

        戻り値:
            unique_merged_dataframe: マージ後、重複するFindexを削除したデータフレーム
        """

        merged_dataframe = pd.merge(
            results_dataframe, attributes_dataframe, on="findex", how='left')

        merged_dataframe = merged_dataframe.drop(columns=["findex"])

        # Findexで重複を削除
        unique_merged_dataframe = merged_dataframe.drop_duplicates(
            subset='Findex', keep='first')

        return unique_merged_dataframe

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで設定した値を取得
            results_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileからデータを抽出し、FieldSetFileをDataFrameに加工
            results_dataframe, attributes_dataframe, attributes_dwh_name_list \
                = WM.calc_func_time(self.logger)(self.extract_and_process_flowfile)(flowfile, results_dwh_name)

            # results型データから作成したDataFrameと属性データから作成したDataFrameをマージし、重複するFindexを削除して返す
            merged_dataframe \
                = WM.calc_func_time(self.logger)(self.merge_results_dataframe_and_attributes_dataframe)(results_dataframe,
                                                                                                        attributes_dataframe)

            # GeoDataFrameからフィールド値リストとフィールドタイプリストを取得する
            field_value_list, \
                field_type_list \
                = WM.calc_func_time(self.logger)(NSP.write_field_file)(merged_dataframe,
                                                                       attributes_dwh_name_list
                                                                       )

            # get_field_set_file : pickleによるシリアライズ、base64でのデコード、utf-8でのエンコード処理を行う
            output_field_set_file = WM.calc_func_time(self.logger)(NSP.get_field_set_file)(field_type_list,
                                                                                           field_value_list,
                                                                                           attributes_dwh_name_list
                                                                                           )

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
