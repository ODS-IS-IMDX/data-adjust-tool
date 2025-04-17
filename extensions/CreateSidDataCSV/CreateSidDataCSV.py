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
# 属性値を読み込み、整備済空間ID(地物事象)データを作成。CSV形式で出力
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")

# Constants
COLUMN_OBJECT_ID = 'オブジェクトID'
COLUMN_FEATURE_TYPE_ID = '設備種別ID'
COLUMN_FEATURE_ATTRIBUTE_INFOMATION = '設備属性情報'


class CreateSidDataCSV(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        フィールドから、整備済空間ID(地物事象)データをCSV形式で出力する。
                        ①input: 地物事象CSVに必要なフィールド(必須: オブジェクトID)のFieldSetFile。
                        ②output: 作成した地物事象CSV。
                      """
        tags = ['CSV', 'Python']

    # 設備種別ID
    FEATURE_TYPE_ID = PropertyDescriptor(
        name="Feature Type ID",
        description="設備種別ID（ 1:管 2:マンホール）",
        required=True,
        sensitive=False,
        default_value="1",
        allowable_values=["1", "2"],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [FEATURE_TYPE_ID]

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
            feature_type_id: 設備種別ID
        """

        feature_type_id\
            = context.getProperty(self.FEATURE_TYPE_ID).evaluateAttributeExpressions(flowfile).getValue()

        return feature_type_id

    def get_attributes_index(self, flowfile):
        """
        概要:
            データ定義ファイルタイプに -2 が含まれる際の get_data_definition_index 関数で使用するための属性のindexを取得

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            attributes_index: 属性のindex
        """

        # データ定義ファイルタイプに -2 が含まれる際の get_data_definition_index 関数で使用
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        attributes_data = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[[0], "Value"].values[0]))

        attributes_index = set(map(lambda x: x[0], attributes_data))

        return attributes_index

    def update_data_frame(self, temp_data_frame, feature_type_id, json_column):
        """
        DataFrameに設備種別ID列を追加し、指定されたカラム（`json_column`）をJSON形式に変換して新しいカラム（`設備属性情報`）に追加。
        `設備属性情報` カラムには、`json_column` に対応するデータをJSON文字列として格納。

        引数:
            temp_data_frame : 更新対象の元となる属性のDataFrame
            feature_type_id : 設備種別ID（1: 管、2: マンホール）
            json_column : JSON形式に変換するために使用するカラム名のリスト

        戻り値:
            pandas.DataFrame: 設備種別IDと`設備属性情報`カラムが追加されたDataFrame

        注記:
            - `設備属性情報` カラムには、指定した `json_column` のデータをJSON文字列として格納
            - `json_column` から `オブジェクトID` カラムは除外される
        """

        # プロパティで設定した規定値を指定
        temp_data_frame[COLUMN_FEATURE_TYPE_ID] = feature_type_id

        # データ型を文字列型で明示的に指定
        temp_data_frame[COLUMN_FEATURE_TYPE_ID] = temp_data_frame[COLUMN_FEATURE_TYPE_ID].astype(
            str)

        # 設備属性情報用にJSONデータ（文字列）を作成
        json_data = temp_data_frame[json_column].to_json(
            orient='records', lines=True, force_ascii=False)

        # リストにJSON文字列を格納
        json_list = json_data.strip().split('\n')

        # DataFrameに格納
        temp_data_frame[COLUMN_FEATURE_ATTRIBUTE_INFOMATION] = json_list

        return temp_data_frame

    def create_csv(self, column_updated_data_frame, feature_csv_column_name):
        """
        概要:
            更新されたDataFrameをCSVとして出力する

        引数:
            column_updated_data_frame: 更新されたDataFrame
            feature_csv_column_name: 地物事象のCSVカラム名のリスト
            feature_link_csv_column_name: 地物事象リンクのCSVカラム名のリスト
            common_name: 共通名
            output_folder_path: 出力先フォルダのパス

        戻り値:
            なし
        """

        # 重複データを削除
        feature_data_frame = column_updated_data_frame[feature_csv_column_name].drop_duplicates(
        )

        # StringIOオブジェクトを作成
        feature_csv = io.StringIO()

        # 地物事象CSVを出力
        feature_data_frame.to_csv(
            feature_csv, index=False, quoting=1, lineterminator='\n', encoding='utf-8')

        # StringIOの内容を取得
        feature_csv_content = feature_csv.getvalue()

        return feature_csv_content

    def transform(self, context, flowfile):

        try:

            feature_type_id\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # FlowFileの属性からデータ定義を取得
            data_definition = flowfile.getAttribute("DataDefinition")
            data_definition_stream = io.StringIO(data_definition)

            attributes_index\
                = WM.calc_func_time(self.logger)(self.get_attributes_index)(flowfile)

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameから属性取り出し（データ定義書による属性指定）
            # 属性のDataFrame作成
            # --------------------------------------------------------------------------
            geometry_type_list, \
                geometry_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list\
                = WM.calc_func_time(self.logger)(DCP.get_data_definition_index)(data_definition_stream,
                                                                                data_definition_delimiter='Comma',
                                                                                data_definition_encoding='UTF-8',
                                                                                )

            field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(DCP.convert_flowfile_to_field_set_file_dataframe)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # 製品データ定義ファイルに指定された属性項目ファイルをすべて読み込み一つのDataFrameとする
            # -----------------------------------------------------------------------------------------------------------

            all_attribute_dataframe\
                = WM.calc_func_time(self.logger)(DCP.create_attribute_dataframe)(field_set_file_dataframe,
                                                                                 dwh_file_name_list,
                                                                                 attribute_name_list,
                                                                                 attribute_const_value_list,
                                                                                 attribute_file_type_list,
                                                                                 len(attributes_index),
                                                                                 input_file_type=1,
                                                                                 )

            # '設備属性情報'(COLUMN_FEATURE_ATTRIBUTE_INFOMATION)のJSONのカラムを作成
            json_column = all_attribute_name_list

            json_column.remove(COLUMN_OBJECT_ID)

            temp_data_frame = WM.calc_func_time(self.logger)(self.update_data_frame)(
                all_attribute_dataframe, feature_type_id, json_column)

            feature_csv_column_name = [
                COLUMN_OBJECT_ID, COLUMN_FEATURE_TYPE_ID, COLUMN_FEATURE_ATTRIBUTE_INFOMATION]

            feature_csv_content = WM.calc_func_time(self.logger)(
                self.create_csv)(temp_data_frame, feature_csv_column_name)

            return FlowFileTransformResult(relationship="success", contents=feature_csv_content)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
