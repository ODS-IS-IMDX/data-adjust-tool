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
# GeoDataFrameをプロパティで指定した分割法で、分けるプロセッサ。
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback
import io
import pickle

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")
np = import_module("numpy")


class SplitGeoDataFrameByFieldsValue(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        GeoDataFrameを指定したフィールド値を基に分割する。
                        ①input: GeoDataFrame。
                        ②output: 分割した複数のGeoDataFrameを持った、複数行のFieldSetFile。
                      """
        tags = ["GeoDataFrame", "Python"]

    # 分割基準のフィールド名
    SPLIT_FIELD_NAME = PropertyDescriptor(
        name="Split Field Name",
        description="分割基準のフィールド名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 分割基準の値
    SPLIT_FIELD_VALUE = PropertyDescriptor(
        name="Split Field Value",
        description="分割基準の値。複数を含める場合はカンマ区切りで指定、複数に分割する場合は改行して指定。",
        default_value="""
下記を参考にして修正すること

==========ここから==========
10,50
20,
30,60
==========ここまで==========

上記の例では、10,50の値を含むGeoDataFrame、20の値を含むGeoDataFrame
30,60の値を含むGeoDataFrame、その他の値が存在する場合は残りのデータでまとめたGeoDataFrameに分割される。
""",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    property_descriptors = [SPLIT_FIELD_NAME,
                            SPLIT_FIELD_VALUE]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context):
        """
        プロパティで入力した値を取得する関数

        引数
            context: プロセッサの設定値が格納されているデータ

        戻り値
            split_field_name: 分割基準のフィールド名
            split_field_value: 分割基準の値

        """
        split_field_name = context.getProperty(
            self.SPLIT_FIELD_NAME).getValue()

        split_field_value = context.getProperty(
            self.SPLIT_FIELD_VALUE).getValue()

        return split_field_name, split_field_value

    def deserialize_geodataframe(self, flowfile):
        """
        flowfileからシリアライズされたGeoDataFrameのバイトデータを取得し、バイトデータからGeoDataFrameを復元

        引数
            flowfile: プロセッサに入ってくるデータ
        戻り値
            geodataframe: デシリアライズされたGeoDataFrame
        """

        # FlowFileからシリアライズされたGeoDataFrameのバイトデータを取得する
        serialize_dataframe = flowfile.getContentsAsBytes()

        # バイトデータからGeoDataFrameをデシリアライズ
        geodataframe = pickle.loads(serialize_dataframe)

        return geodataframe

    def transform(self, context, flowfile):
        try:

            # flowfileの属性からfilenameを取得(DWH名作成用)
            filename = flowfile.getAttribute("filename")

            # --------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # --------------------------------------------------------------------------
            split_field_name, \
                split_field_value, \
                = WM.calc_func_time(self.logger)(self.get_property)(context)

            # --------------------------------------------------------------------------
            # バイトデータからGeoDataFrameを復元
            # --------------------------------------------------------------------------
            geodataframe\
                = WM.calc_func_time(self.logger)(self.deserialize_geodataframe)(flowfile)

            try:
                # 処理後半に文字列型に変換したsplit_field_name列を元の型に戻すために保持
                str_string = str(geodataframe[split_field_name].dtype)
            except KeyError:
                self.logger.error(
                    f"Split Field Nameプロパティで指定されたフィールド名 '{split_field_name}' がGeoDataFrameに存在しません")
                return FlowFileTransformResult(relationship="failure")

            # split_field_name列の型変換(split_field_valueとデータ型を一致させるため)
            geodataframe[split_field_name] = geodataframe[split_field_name].astype(
                str)

            # split_field_name列のからユニーク値取得(split_field_valueと比較するため)
            unique_geodataframe_value = geodataframe[split_field_name].unique()

            # プロパティ指定のsplit_field_valueを文字列型のDataFrameで読み込み
            try:
                partition_definition_data_frame = pd.read_csv(
                    io.StringIO(split_field_value), header=None, dtype=str, na_filter=False)
            except Exception as e:
                self.logger.error(f"CSVの読み込みエラー: {str(e)}")
                return FlowFileTransformResult(relationship="failure")

            # 文字列型で読み込んだためNaN->""になるため、後続処理でnp.nanを処理するためにnp.nanに戻す
            partition_definition_data_frame = partition_definition_data_frame.replace(
                "", np.nan)

            # split_field_valueで指定した全ての値を取得
            all_split_field_value = partition_definition_data_frame.values.flatten().tolist()

            # NaNの削除
            all_split_field_value = [
                x for x in all_split_field_value if pd.notna(x)]

            unique_geodataframe_value_set = set(unique_geodataframe_value)
            unique_split_field_value_set = set(all_split_field_value)

            # unique_valuesとunique_split_field_valueの差分をリストで取得
            difference_unique_geodataframe_to_split_field = list(
                unique_geodataframe_value_set - unique_split_field_value_set)

            # split_field_valueで指定した値を行ごとにリストで取得
            # 例) ↓partition_definition_data_frame
            #     0    1
            # 0  10   50
            # 1  20   60
            # 2  30  NaN
            # ↓partition_definition_data_frame_rows_list
            # [['10', '50'], ['20', '60'], ['30']]
            partition_definition_data_frame_rows_list = [row.dropna().tolist() for _,
                                                         row in partition_definition_data_frame.iterrows()]

            # 一致する値が1つでもあればTrueを返す
            if not unique_geodataframe_value_set.isdisjoint(unique_split_field_value_set):
                is_match = True
            else:
                is_match = False

            if is_match == False:
                partition_definition_data_frame_rows_list = []

            for row_list in partition_definition_data_frame_rows_list:
                # 一致しない要素を削除
                row_list[:] = [
                    item for item in row_list if item in unique_geodataframe_value_set]

                # 空リストを除外
                if row_list:
                    # 空でないリストのみpartition_definition_data_frame_rows_listに追加
                    continue
                else:
                    partition_definition_data_frame_rows_list.remove(row_list)

            geodataframe_list = []

            for row_list in partition_definition_data_frame_rows_list:

                filtered_gdf_list = []

                for item in row_list:

                    # GeoDataFrameのsplit_field_name列で一致するデータがあるか確認
                    matched_gdf = geodataframe[geodataframe[split_field_name] == item]

                    # 一致するデータが存在しない場合は、このアイテムをスキップ
                    if matched_gdf.empty:
                        continue

                    # 一致するデータがあれば、filtered_gdf_listに追加
                    filtered_gdf_list.append(matched_gdf)

                combined_gdf = pd.concat(filtered_gdf_list, ignore_index=True)
                geodataframe_list.append(combined_gdf)

            filtered_gdf_list = []

            # difference_unique_geodataframe_to_split_fieldが空でない場合に処理を実行
            if difference_unique_geodataframe_to_split_field:

                # プロパティで指定されなかった残りのGeoDataFrame作成
                for item in difference_unique_geodataframe_to_split_field:

                    # GeoDataFrameのsplit_field_name列で一致するデータがあるか確認
                    matched_gdf = geodataframe[geodataframe[split_field_name] == item]

                    # 一致するデータが存在しない場合は、このアイテムをスキップ
                    if matched_gdf.empty:
                        continue

                    # 一致するデータがあれば、filtered_gdf_listに追加
                    filtered_gdf_list.append(matched_gdf)

                combined_gdf = pd.concat(filtered_gdf_list, ignore_index=True)
                geodataframe_list.append(combined_gdf)

            # Dwhの作成(ファイル名_指定属性_属性値)
            output_dwh_list = []

            for gdf in geodataframe_list:

                # GeoDataFrameのsplit_field_name列ユニーク値を取得
                unique_values_in_gdf = gdf[split_field_name].unique()

                # 一致するsplit_field_valueを取得
                matching_values = [
                    val for val in unique_values_in_gdf if val in unique_split_field_value_set]

                # 一致するsplit_field_valueを"@"で結合
                matching_values_str = "@".join(matching_values)

                # 一致する値があれば、それに基づきファイル名を作成
                if matching_values:

                    # filename_属性名_属性値形式でDWH名を作成
                    dwh_name = f"{filename}_{split_field_name}_{matching_values_str}"
                    output_dwh_list.append(dwh_name)

                # 一致する値がない場合、_otherを付けDWH名を作成
                else:
                    dwh_name = f"{filename}_{split_field_name}_other"
                    output_dwh_list.append(dwh_name)

            output_type_list = ["GeoDataFrame"] * \
                len(geodataframe_list)

            for gdf in geodataframe_list:

                # split_field_name列のデータ型を元に戻す
                gdf[split_field_name] = gdf[split_field_name].astype(
                    str_string)

            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_list,
                                                                                           output_type_list,
                                                                                           geodataframe_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
