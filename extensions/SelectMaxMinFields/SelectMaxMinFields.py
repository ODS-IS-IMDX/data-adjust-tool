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
# 複数のFieldsデータを読み込み、最大値または最小値を求めて新たな結果を生成する。
# 例）
# input
# 属性A [(0, 10.0), (1, 50.0)...]
# 属性B [(0, 20.0), (1, 10.0)...]
# 属性C [(0, 30.0), (1, 10.0)...]
#              ↓
# output
# 最大の場合[(0, 30.0), (1, 50.0)...]
# 最小の場合[(0, 10.0), (1, 10.0)...]
# ---------------------------------------------------------------------------------------------------------
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


class SelectMaxMinFields(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        インデックスを基準に複数のフィールドデータを比較し、最大値または最小値を抽出したフィールドデータを作成する。
                        ①input: 1つ以上の比較したいフィールドを持った複数行のFieldSetFile。
                        ②output: 最小値もしくは最大値で構成されたフィールドを持った1行のFieldSetFile。
                      """
        tags = ["Select", "Python"]

    PRIORITY_TYPE = PropertyDescriptor(
        name="Max Or Min",
        description="最大値または最小値を計算する優先順位",
        allowable_values=["最大値", "最小値"],
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False
    )

    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [PRIORITY_TYPE,
                            OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def extract_and_process_flowfile(self, flowfile):
        """
        概要:
            FlowFile からデータを抽出し、抽出したデータから全てのFieldsデータをマージしたデータフレーム作成

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            attributes_dataframe: インプットのフィールドデータをマージしたDataFrame
        """

        # flowfileから、CSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        # 各フィールドのDataFrame格納用リスト
        attributes_dataframe_list = []

        for _, row in field_set_file_data_frame.iterrows():

            attributes_dwh = row["Dwh"]
            decoded_value = pickle.loads(base64.b64decode(row["Value"]))

            # DataFrameの列名としてFindex（インデックス）とDWH名を使用
            attributes_dataframe = pd.DataFrame(
                decoded_value, columns=["Findex", attributes_dwh])
            # Findex列のデータ型をfloat型に変換
            attributes_dataframe["Findex"] = attributes_dataframe["Findex"].astype(
                float)

            attributes_dataframe_list.append(attributes_dataframe)

        # 最終的に全ての属性DataFrameを結合
        for i, dataframe in enumerate(attributes_dataframe_list):

            if i == 0:
                attributes_dataframe = dataframe
            else:
                attributes_dataframe = attributes_dataframe.merge(
                    dataframe, on="Findex", how="left")

        return attributes_dataframe

    def select_max_min_fields(self, attributes_dataframe, priority_type, output_dwh_name):
        """
        概要:
            与えられたDataFrameに基づいて、指定された優先順位（最大値または最小値）に従い
            インデックスごとの各行の最大値または最小値を計算

        引数:
            attributes_dataframe: フィールドデータを含むDataFrame
            priority_type: 優先順位を示す値（最大値, 最小値）
            output_dwh_name: 出力する際のDWH名

        戻り値:
            max_values_df: 各行の最大値または最小値を計算した結果を含むDataFrame
        """
        # Findexと選択された値を格納するためのリスト
        max_values_list = []

        for i, row in attributes_dataframe.iterrows():

            # priority_typeプロパティの値が最大値選択の場合
            if priority_type == "最大値":
                selected_value = row[1:].max()

            # priority_typeプロパティの値が最小値選択の場合
            if priority_type == "最小値":
                selected_value = row[1:].min()

            max_values_list.append((i, selected_value))

        max_values_df = pd.DataFrame(max_values_list, columns=[
                                     "Findex", output_dwh_name])

        return max_values_df

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # --------------------------------------------------------------------------
            priority_type = context.getProperty(
                self.PRIORITY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

            output_dwh_name = context.getProperty(
                self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # インプットのフィールドデータをマージしたDataFrame取得
            # --------------------------------------------------------------------------
            attributes_dataframe\
                = WM.calc_func_time(self.logger)(self.extract_and_process_flowfile)(flowfile)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 与えられたDataFrameに基づいて、指定された優先順位に従いインデックスごとの各行の最大値または最小値を計算
            # --------------------------------------------------------------------------
            max_values_df\
                = WM.calc_func_time(self.logger)(self.select_max_min_fields)(attributes_dataframe, priority_type, output_dwh_name)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # GeoDataFrameからフィールド値リストとフィールドタイプリストを取得する
            # --------------------------------------------------------------------------
            field_value_list, \
                field_type_list \
                = WM.calc_func_time(self.logger)(NSP.write_field_file)(max_values_df,
                                                                       [output_dwh_name])
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # set_field_set_file : pickleによるシリアライズ、base64でのデコード、utf-8でのエンコード処理を行う
            # --------------------------------------------------------------------------
            output_field_set_file = WM.calc_func_time(self.logger)(NSP.get_field_set_file)(field_type_list,
                                                                                           field_value_list,
                                                                                           [output_dwh_name])
            # --------------------------------------------------------------------------

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
