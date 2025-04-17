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
# flowfileのデータを製品定義ファイルに記載されている属性値をCSVにまとめるプロセッサ。
# geometry以外のデータ（地物の属性）をまとめている。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
import io
import base64
import traceback

from importlib import import_module


# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
pd = import_module("pandas")


class CreateCSVFromFields(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データ定義記載項目に従い、フィールドをCSV形式に変換する。
                        ①input: 出荷定義に書かれたフィールドを持った複数行のFieldSetFile。
                        ②output: データ定義記載データをまとめたCSVファイル。
                      """
        tags = ['CSV', 'Python']

    # プロパティで入力するものはない。
    property_descriptors = []

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_field_set_file_and_datadefinition(self, flowfile):
        """
        概要:
            flowfileからFieldSetFile(.csv)を取得し、DataFrameに加工する。
            また"DataDefinition"というAttributeに直接書かれているデータ定義ファイル(.csv)を抽出し、パスにする。

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            field_set_data_frame: FieldSetFileをDataFrameに加工した物
            data_definition_stream: CSV形式のデータ定義ファイル
        """

        # flowfileから、CSV形式のFieldSetFileを取得に変更
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # input_field_set_fileを、DataFrameに加工する。
        field_set_data_frame = pd.read_csv(io.StringIO(input_field_set_file))

        # flowfileから、DataDefinitionというAttributeのキーからデータ定義ファイルの値を取得する。
        data_definition = flowfile.getAttribute("DataDefinition")

        # 取得したデータ定義ファイルのデータをパスにする。
        data_definition_stream = io.StringIO(data_definition)

        return field_set_data_frame, data_definition_stream

    def process_dataframe_to_csv(self,
                                 field_set_data_frame,
                                 attribute_dwh_file_name_list,
                                 attribute_distribution_name_list,
                                 attribute_data_type_list):
        """
        概要:
            field_set_data_frameから必要な情報を抜き出し、出荷定義ファイルの要件に則した形のCSVに変換する関数。

        引数:
            field_set_data_frame: FieldSetFileをDataFrameに加工した物
            attribute_dwh_file_name_list: データ定義ファイルの属性のDwhファイル名list
            attribute_distribution_name_list: データ定義ファイルの属性名list
            attribute_data_type_list: データ定義ファイルの属性の型list

        戻り値:
            result_csv: 各属性の値が入ったCSV
        """

        # field_set_data_frameのDwh列,Value列をリスト化
        dwh_list = field_set_data_frame["Dwh"].tolist()
        value_list = field_set_data_frame["Value"].tolist()

        # FIDを適した形に変形する。
        FID_dwh_name = attribute_dwh_file_name_list[0].split("/")[0] + "/FID"

        # dwh_listのFIDのインデックスを抽出する(value_listのFIDのインデックスも同じもの)
        geometry_index = dwh_list.index(FID_dwh_name)

        # dwh_list,value_listからgeometry_index(FID)のインデックスの値を削除
        del dwh_list[geometry_index]
        del value_list[geometry_index]

        # Attributeの値を入れるデータフレームを定義
        field_data_frame = None

        # dwh_listの個数ループし、Findexと値が入ったデータフレームを作り、Findexをキーとしmergeする。
        for i in range(len(dwh_list)):

            # value_listの要素ごとにデコードデシリアライズし。リスト化(例:[(0,254),(0,854)...]FindexとAttributeの値が入る)
            target_list = pickle.loads(base64.b64decode(value_list[i]))

            # field_set_fileのリストと、データ定義ファイルのリストのインデックスの順番が別なので、
            # field_set_fileのdwh_listから、データ定義ファイルのattribute_dwh_file_name_listのDWH名を抜き出す。
            dwh_index = attribute_dwh_file_name_list.index(dwh_list[i])

            # 1つ目の2列のDataFrameを作る。
            if field_data_frame is None:

                # 値はtarget_listで、カラムが左側はFindexで右側がデータ定義ファイルで取得したattribute_distribution_name_listのインデックスが[dwh_index](例:外径m)
                # dtypeで型を指定する。指定する型はデータ定義ファイルで取得した型
                field_data_frame = pd.DataFrame(target_list,
                                                columns=["Findex",
                                                         attribute_distribution_name_list[dwh_index]],
                                                dtype=type(
                                                    attribute_data_type_list[dwh_index])
                                                )

                # Findexをキーとして扱うために、Findexの方はint64にする。
                field_data_frame = field_data_frame.astype({"Findex": "int64"})

            # 2つ目以降の(mergeする)DataFrameを作成しfield_data_frameに結合
            else:
                # 上の処理と同じ
                merge_data_frame = pd.DataFrame(target_list,
                                                columns=["Findex",
                                                         attribute_distribution_name_list[dwh_index]],
                                                dtype=type(
                                                    attribute_data_type_list[dwh_index])
                                                )

                merge_data_frame = merge_data_frame.astype({"Findex": "int64"})

                # field_data_frameと、merge_data_frameを内部結合。キーはFindex
                field_data_frame = pd.merge(
                    field_data_frame, merge_data_frame, on="Findex", how="inner")

            # field_set_fileの順番にmergeしたが、それだと順番がランダムの為、カラム列をソートしアルファベット順にソート。
            sort_columns_list = sorted(field_data_frame.columns)

            # sort_columns_listの順番に並び替える。
            field_data_frame = field_data_frame.reindex(
                sort_columns_list, axis=1)

            # Findexは処理の中で使っていた値で本来はいらないので消し、CSV化
            result_csv = field_data_frame.drop(
                columns="Findex").to_csv(index=False)

        return result_csv

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------

    def transform(self, context, flowfile):
        try:

            # flowfileからFieldSetFileを取得。
            # またflowfileのAttributeからデータ定義を取得する。
            field_set_data_frame, \
                data_definition_stream\
                = WM.calc_func_time(self.logger)(self.get_field_set_file_and_datadefinition)(flowfile)

            # データ定義ファイルの情報をListにして出力する
            # このプロセッサで使う戻り値は、属性のDwhファイル名List、属性の流通項目名List、属性のデータ型List
            geometry_type_list, \
                geometry_dwh_file_name_list, \
                geometry_distribution_name_list, \
                attribute_file_type_list, \
                attribute_dwh_file_name_list, \
                attribute_distribution_name_list, \
                attribute_const_value_list, \
                attribute_data_type_list = WM.calc_func_time(self.logger)(NSP.get_data_definition_index_datatype)(data_definition_stream,
                                                                                                                  data_definition_delimiter=DDC.DELIMITER_COMMA,
                                                                                                                  data_definition_encoding="utf-8"
                                                                                                                  )

            # DataFrameからCSVに変換する。
            result_csv = WM.calc_func_time(self.logger)(self.process_dataframe_to_csv)(field_set_data_frame,
                                                                                       attribute_dwh_file_name_list,
                                                                                       attribute_distribution_name_list,
                                                                                       attribute_data_type_list
                                                                                       )

            return FlowFileTransformResult(relationship="success", contents=result_csv)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
