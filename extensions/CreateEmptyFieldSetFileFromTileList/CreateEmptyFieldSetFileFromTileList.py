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
# GeoDataFrameの分割処理で出力された複数のタイルリストデータをもとにDWH名のみ記述された空のFieldSetFileを出力する。
# ---------------------------------------------------------------------------------------------------------
# Python標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")


class CreateEmptyFieldSetFileFromTileList(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        GeoDataFrameの分割処理で出力されたタイルリストデータをもとにDWH名のみ記述された空のFieldSetFileを出力する。
                        ①input: タイルリストデータを持った複数行のFieldSetFile。
                        ②output: DWH名のみ記述されたタイル数分のFieldSetFile。
                      """
        tags = ["SplitGeoDataFrame", "CityGML", "SpatialID", "Python"]

    property_descriptors = []

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_tile_data_from_field_set_file(self, flowfile):
        """
        概要:
            flowfileからタイルリスト情報を抽出し、データフレームとしてリストで返す

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            tile_name_dataframe_list: DWH名をカラム名として持つDataFrameのリスト
        """

        # flowfileから、CSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        # 各フィールドのDataFrame格納用リスト
        tile_name_dataframe_list = []

        for _, row in field_set_file_data_frame.iterrows():

            dwh_name = row["Dwh"]
            decoded_value = pickle.loads(base64.b64decode(row["Value"]))

            # DataFrameの列名としてFindex（インデックス）とDWH名を使用
            tile_name_dataframe = pd.DataFrame(
                decoded_value, columns=[dwh_name])

            tile_name_dataframe_list.append(tile_name_dataframe)

        return tile_name_dataframe_list

    def get_unique_tile_name(self, tile_name_dataframe_list):
        """
        概要:
            複数のDataFrame間で一致しないタイル名を抽出し、それぞれのDataFrameのユニークなタイル名をリスト化する。

        引数:
            tile_name_dataframe_list: タイル名を含むDataFrameのリスト（2つのDataFrame）

        戻り値:
            output_dwh_name_list: 一致しないユニークなタイル名のリスト
        """

        # tile_name_dataframe_list内の全てのDataFrameから値を取得し、setに変換して重複を排除
        values_list = list(
            set(value for df in tile_name_dataframe_list for value in df.values.flatten()))

        # values_listをDataFrameに変換
        df_values = pd.DataFrame(values_list, columns=['key'])

        # "key" と同じ列を基に結合
        for i, tile_name_dataframe in enumerate(tile_name_dataframe_list):
            if i == 0:
                # 1ループ目は"key"列付DataFrameにtile_name_dataframeの1要素目を結合
                merge_df = pd.merge(
                    df_values, tile_name_dataframe, left_on=df_values.columns[0], right_on=tile_name_dataframe.columns[0], how='outer')

            else:
                # 2ループ目以降上記の条件で作成したDataFrameに結合していく
                merge_df = pd.merge(
                    merge_df, tile_name_dataframe, left_on=merge_df.columns[0], right_on=tile_name_dataframe.columns[0], how='outer')

        output_dwh_name_list = []

        for _, row in merge_df.iterrows():

            # "key"列はスキップ
            for column_with_dwh_name in merge_df.columns[1:]:

                # NaNの場合
                if pd.isna(row[column_with_dwh_name]):

                    # 出力用DWH名作成(元のDWH名＋図郭名)
                    output_dwh_name_list.append(
                        f"{column_with_dwh_name}_{row['key']}")

        return output_dwh_name_list

    def create_field_set_file(self, output_dwh_name_list):
        """
        概要:
            ユニークなタイル名リストをもとに、Type列とValue列が空のFieldSetFileを生成する

        引数:
            output_dwh_name_list: DWH名のリスト

        戻り値:
            output_field_set_file: DWH列のみの空のFieldSetFile
        """

        output_dwh_name_length = len(output_dwh_name_list)

        output_type_list = [""] * output_dwh_name_length
        output_value_list = [""] * output_dwh_name_length

        data_dict = {
            "Dwh": output_dwh_name_list,
            "Type": output_type_list,
            "Value": output_value_list
        }

        # data_dictをFieldSetFile(CSV形式)に加工
        result_dataframe = pd.DataFrame(data_dict)
        output_field_set_file = result_dataframe.to_csv(index=False)

        return output_field_set_file

    def transform(self, context, flowfile):
        try:

            # --------------------------------------------------------------------------
            # FieldSetFileからTileList情報をDataFrameとして取得
            # --------------------------------------------------------------------------
            tile_name_dataframe_list\
                = WM.calc_func_time(self.logger)(self.get_tile_data_from_field_set_file)(flowfile)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 作成したTileListのDataFrameから一致する図郭が存在しないタイル名を取得
            # --------------------------------------------------------------------------
            output_dwh_name_list\
                = WM.calc_func_time(self.logger)(self.get_unique_tile_name)(tile_name_dataframe_list)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # Type列,Value列が空のFieldSetFileを作成
            # --------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(self.create_field_set_file)(output_dwh_name_list)
            # --------------------------------------------------------------------------

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
