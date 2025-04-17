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
# DataFrameを操作し、FieldSetFileを生成するためのプロセッサ。
# DataFrameをデシリアライズし、データ定義を取得し、フィールドの情報を処理している。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
from io import StringIO
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")


class ConvertDataFrameToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データ定義記載項目に従い、DataFrameをFieldSetFileに変換する。
                        ①input: DataFrameか、DataFrameを持ったFieldSetFile。
                        ②output: 出力したいデータ項目を持ったFieldSetFile。
                      """
        tags = ['DataFrame', 'Python']

    # データ定義の区切り文字を指定するためのPropertyDescriptorを定義
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_data_definition_delimiter(self, context):
        """
        プロパティからデータ定義の区切り文字を取得し、ログ出力

        引数
            context - processorの設定値が格納されているデータ
        戻り値
            input_data_type - フローファイルに入ってくる種類
            data_definition_delimiter - 取得したデータ定義の区切り文字の値
        """

        # プロパティからデータ定義の区切り文字を取得する
        data_definition_delimiter = context.getProperty(
            self.DATA_DEFINITION_DELIMITER).getValue()

        # 区切り文字をログ出力する
        self.logger.info(
            f"[Property], Data Definition Delimiter: {data_definition_delimiter}")

        return data_definition_delimiter

    def extract_geodata_from_flowfile(self, flowfile):
        """
        FlowFileからシリアライズされたGeoDataFrameのバイトデータを取得し、DataFrameを復元する or
        FlowFileからCSV形式のFieldSetFileを受け取り、value列からDataFrameを復元する

        引数
            flowfile - プロセッサに入ってくるデータ
        戻り値
            dataframe - 復元されたDataFrameオブジェクト
            data_definition_stream - データ定義をStringIOストリームとして扱うオブジェクト
        """

        # FlowFileからシリアライズされたDataFrameのバイトデータを取得する
        dataframe = WM.calc_func_time(self.logger, False)(
            PBP.get_value_from_field_Set_file_or_serialized)(flowfile)

        # FlowFileの属性からデータ定義を取得する
        data_definition = flowfile.getAttribute("DataDefinition")

        # データ定義をStringIOストリームとして扱う
        data_definition_stream = StringIO(data_definition)

        return dataframe, data_definition_stream

    def add_index_column(self, dataframe):
        """
        DataFrameにインデックス列"Findex"を追加する

        引数
            dataframe - インデックス列を追加する前のDataFrame
        戻り値
            dataframe - インデックス列を追加した後のGeoDataFrame
        """

        # インデックス列"Findex"を追加
        index_list = np.arange(0, len(dataframe), dtype="float")

        dataframe["Findex"] = index_list

        return dataframe

    def transform(self, context, flowfile):
        try:

            # 地物IDのカラム名とデータ定義の区切り文字を取得する
            data_definition_delimiter \
                = WM.calc_func_time(self.logger)(self.get_data_definition_delimiter)(context)

            # flowfileからDataFrameとデータ定義を取得する
            dataframe, data_definition_stream = WM.calc_func_time(
                self.logger)(self.extract_geodata_from_flowfile)(flowfile)

            # DataFrameにインデックス列"Findex"を追加
            dataframe = WM.calc_func_time(self.logger)(
                self.add_index_column)(dataframe)

            # データ定義からジオメトリタイプlist、フィールド名リスト、DWHファイル名リストを取得する
            # get_data_definition_indexの戻り値に合わせて今回使用しない変数も設定している。
            geometry_type_list, \
                geometry_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list\
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index)(data_definition_stream,
                                                                                data_definition_delimiter=data_definition_delimiter
                                                                                )

            # GeoDataFrameからフィールド値リストとフィールドタイプリストを取得する
            field_value_list, \
                field_type_list\
                = WM.calc_func_time(self.logger)(NSP.write_field_file)(dataframe,
                                                                       all_attribute_name_list
                                                                       )

            # FieldSetFileに変換する
            field_set_file = WM.calc_func_time(self.logger)(NSP.get_field_set_file)(field_type_list,
                                                                                    field_value_list,
                                                                                    dwh_file_name_list
                                                                                    )

            # 成功リレーションと共に処理結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=field_set_file)

            # エラーが発生した場合、失敗リレーションを返す
        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
