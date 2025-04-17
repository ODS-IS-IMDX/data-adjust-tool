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

import pickle
from abc import abstractmethod
import re
import io
import base64
import os
from collections import defaultdict
from pathlib import Path

from common.base_validate_processor import BaseValidateProcessor
from common.error_code_list import ErrorCodeList
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP

import cad.common.cad_utils as CU
from importlib import import_module

pd = import_module("pandas")
gpd = import_module("geopandas")
Polygon = import_module("shapely.geometry").Polygon
np = import_module("numpy")
LineString = import_module("shapely.geometry").LineString
Point = import_module("shapely.geometry").Point
BaseGeometry = import_module("shapely.geometry.base").BaseGeometry
Delaunay = import_module("scipy.spatial").Delaunay
CRS = import_module("pyproj").CRS


class DataProcessingBaseValidateProcessor(BaseValidateProcessor):

    @abstractmethod
    def validate_data(self, context, flowfile):
        pass

    def validate_empty_data(self, data):
        """
        dataが空かどうかを検証する
        :param data: 検証対象データ
        :return bool: 検証結果 正常=True、異常=False
        """
        if data is None:
            self.validate_logger.write_log(ErrorCodeList.EC00001)
            return False
        return True

    def validate_fsf_format(self, fsf, is_pickle=True):
        """
        FieldSetFileの形式が正しいかを検証する
        :param fsf: 検証対象データ
        :param is_pickle: Valueがpickleされているかどうか
        :return bool: 検証結果 正常=True、異常=False
        :return DataFrame or None: 正常に変換されたDataFrame 正常=DataFrame、異常=None
        """
        result = True
        try:
            df = CU.field_set_file_to_dataframe(fsf, is_decode=False)
        except:
            args = {"error_code": ErrorCodeList.EC00006, "対象": "FieldSetFile"}
            self.validate_logger.write_log(**args)
            result = False
            return result, None

        # 列名称の有無を検証
        required_columns = ["Dwh", "Type", "Value"]
        if not all(col in df.columns for col in required_columns):
            args = {"error_code": ErrorCodeList.EC00009}
            self.validate_logger.write_log(**args)
            result = False
            return result, None

        # Dwhに"/"スラッシュまたは"-"ハイフンのいずれかが含まれているか検証。
        # いずれも含まれていなければエラーとする。
        dwh_values = df["Dwh"].tolist()  # Dwh列の値をリストに変換
        slash_flag = True
        for idx, dwh_value in enumerate(dwh_values):
            # まずスラッシュが含まれていないか確認
            if "/" not in dwh_value:
                # スラッシュが含まれていなければ、ハイフン(-)が含まれているか確認
                if "-" not in dwh_value:
                    args = {
                        "error_code": ErrorCodeList.EC00006,
                        "column_name": dwh_value,
                        "record_number": idx,
                    }
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return result, None
            else:
                # ハイフン(-)が含まれている場合はフラグをFalseにする
                slash_flag = False

        # Dwh検証用の正規表現パターンを設定する
        # 正規表現パターンは、"/"スラッシュと"-"ハイフンのどちらを含むかにより変化する
        if slash_flag:
            dwh_pattern = re.compile(r"^[^/]+/[^/]+$")  # 任意の文字列/任意の文字列 の形式
        else:
            dwh_pattern = re.compile(r"^[^-]+/[^-]+$")  # 任意の文字列-任意の文字列 の形式

        # Dwhの形式検証
        # 正規表現パターンに一致しなければエラーとする。
        for idx, row in enumerate(df.values):
            if not dwh_pattern.match(row[0]):
                args = {
                    "error_code": ErrorCodeList.EC00006,
                    "column_name": row[0],
                    "record_number": idx,
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return result, None

        # レコード数の一致検証
        # group列としてdwhの左辺だけを取得する
        count_df = df.copy()
        count_df["group"] = df["Dwh"].str.split("/").str[0]
        # group列でデータを分割してリストに格納
        grouped_dfs = [
            group_df[group_df["Type"] != "results"]
            for _, group_df in count_df.groupby("group")
        ]

        for _, group_df in enumerate(grouped_dfs):
            value_list = group_df[group_df["Type"]
                                  != "results"]["Value"].tolist()
            if not len(
                [value for value in value_list if isinstance(value, str)]
            ) == len(group_df):
                args = {
                    "error_code": ErrorCodeList.EC00004,
                    "column_name": group_df["group"].unique()[0],
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return result, None

        if is_pickle:
            # Valueの値の検証
            for idx in range(len(df)):
                try:
                    if isinstance(df.loc[idx, "Value"], str):
                        df.loc[idx, "Value"] = CU.decode_value(
                            df.loc[idx, "Value"])
                    else:
                        args = {
                            "error_code": ErrorCodeList.EC00003,
                            "column_name": df.loc[idx, "Dwh"],
                            "record_number": idx,
                        }
                        self.validate_logger.write_log(**args)
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return result, None
                except Exception as e:
                    args = {
                        "error_code": ErrorCodeList.EC00007,
                        "column_name": df.loc[idx, "Dwh"],
                        "record_number": idx,
                    }
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return result, None

        res_val = None
        if result:
            res_val = df
        return result, res_val

    def validate_serialized_data(self, input_data):
        """
        インプットがシリアライズされたデータか検証する
        :param input_data: 検証対象データ
        :return bool: 検証結果 正常=True、異常=False
        :return デシリアライズされたデータ or None: 正常=デシリアライズされたデータ、異常=None
        """
        try:
            return True, pickle.loads(input_data)
        except Exception:
            self.validate_logger.write_log(ErrorCodeList.EC00015)
            return False, None

    def validate_input_type(self, target_value, check_type):
        """
        概要
            inputされたデータの形を検証する
        引数
            target_value: 検証対象データ(FieldSetFile内に格納されているデータ)
            check_type: target_valueの形
        戻り値
            bool: True or False  check_typeと一致=True、不一致=False
        """
        if check_type in {"coordinates_array", "DEM_array"}:
            if isinstance(target_value, np.ndarray):
                return True
            else:
                self.validate_logger.write_log(ErrorCodeList.ED00002)
                return False
        elif check_type == "GeoDataFrame":
            if isinstance(target_value, gpd.GeoDataFrame):
                return True
            else:
                self.validate_logger.write_log(ErrorCodeList.ED00005)
                return False
        elif check_type == "geometry":
            if isinstance(target_value, list) and all(isinstance(obj, BaseGeometry) for obj in target_value):
                return True
            else:
                self.validate_logger.write_log(ErrorCodeList.ED00003)
                return False
        elif check_type == "fields":
            if isinstance(target_value, list) and all(isinstance(obj, tuple) for obj in target_value):
                return True
            else:
                self.validate_logger.write_log(ErrorCodeList.ED00004)
                return False
        elif check_type == "DataFrame":
            if isinstance(target_value, pd.DataFrame):
                return True
            else:
                self.validate_logger.write_log(ErrorCodeList.ED00054)
        else:
            raise Exception("対象外が発生")

    def get_clockwise_bool(self, tin_array):
        """
        概要
            TINの三次元配列から時計回りか検証する

        引数
            tin_array : ドロネー座標配列

        戻り値
            bool : tin_arrayが時計回りであればTrue 反時計回りの場合はFalseを返す
        """
        # 外積の計算
        ab_array = tin_array[:, 1, :] - tin_array[:, 0, :]
        bc_array = tin_array[:, 2, :] - tin_array[:, 1, :]
        cross_product_array = np.cross(ab_array, bc_array)

        # whereで正負によって振り分け
        result_array = np.where(cross_product_array < 0, 0, 1)

        return result_array

    def validate_correct_GCP(self,
                             base_coordinates_array,
                             target_coordinates_array
                             ):
        """
        概要
            GCPが有効な形か検証する

        引数
            base_coordinates_array : 位置基準点の座標配列
            target_coordinates_array : 補正対象点の座標配列

        戻り値
            bool : GCPが有効であればTrue 無効であればFalseを返す
        """
        # 同一構成点か検証
        if len(base_coordinates_array) != len(target_coordinates_array):

            self.validate_logger.write_log(ErrorCodeList.ED00060)
            return False

        # base_coordinates_arrayに重複する構成点がないか検証
        if not self.validate_iterate_line_coordinates(base_coordinates_array):
            self.logger.error("baseの座標配列に重複する構成点があります。")
            return False

        # target_coordinates_arrayに重複する構成点がないか検証
        if not self.validate_iterate_line_coordinates(target_coordinates_array):
            self.logger.error("targetの座標配列に重複する構成点があります。")
            return False
        # -----------------------------------------------------------------------------------------------------------
        # ドロネーオブジェクト作成 TINの2次元配列生成[TINの構成点]>[TINのID + xy]
        # -----------------------------------------------------------------------------------------------------------
        tri = Delaunay(base_coordinates_array[:, 1:3].copy())

        # ドロネーの組み合わせ取得
        gcp_index_array = tri.simplices
        base_tin_array = base_coordinates_array[gcp_index_array].copy()
        target_tin_array = target_coordinates_array[gcp_index_array].copy()

        # 回転方向
        base_clockwise_bool = self.get_clockwise_bool(
            base_tin_array[:, :, 1:3])
        target_clockwise_bool = self.get_clockwise_bool(
            target_tin_array[:, :, 1:3])

        # base_clockwise_bool と target_clockwise_bool に False が含まれているか検知
        if not np.all(base_clockwise_bool) or not np.all(target_clockwise_bool):
            self.validate_logger.write_log(ErrorCodeList.ED00001)

            result = False
        else:
            result = True

        return result

    def validate_fields_type(self, target_dwh, fields_list, check_type):
        """
        概要
            フィールドが指定された型か検証をする

        引数
            target_dwh : 指定されたDWH
            fields_list : フィールドが格納されたリスト
            check_type : 指定されたデータ型
        
        戻り値
            bool : フィールドが指定された型であればTrue フィールドが指定された型でなければFalse
        """

        for fields_tuple in fields_list:
            if not isinstance(fields_tuple[1], check_type):
                args = {
                    "error_code": ErrorCodeList.ED00006,
                    "target_dwh": target_dwh,
                    "type": type(fields_tuple[1]).__name__,
                }
                self.validate_logger.write_log(**args)
                result = False
                return result

        # 型がすべてあっていればTrueを返す
        result = True
        return result

    def validate_coordinates_is_3D(self, coordinates_array):
        """
        概要
            座標配列が3Dか検証

        引数
            coordinates_array : 座標配列

        戻り値
            bool : 座標配列が3DであればTrue 2D、マルチパッチの場合はFalse
        """
        try:
            # 最初の要素が4つかどうかを判定
            # ID, x, y, z
            return len(coordinates_array[0]) == 4
        except (IndexError, TypeError):
            self.validate_logger.write_log(ErrorCodeList.ED00008)
            # 配列の構造が期待通りでない場合は False を返す
            return False

    def validate_fields_type_is_num(self, target_dwh, fields_list):
        """
        概要
            フィールドが、数字か検証

        引数
            target_dwh : 指定されたDWH名
            fields_list : フィールドが格納されたリスト

        戻り値
            bool : フィールドが数字であれば True 数字でなければFalse
        """
        try:
            fields_list = [(left, float(right)) for left, right in fields_list]

        except Exception:
            args = {
                "error_code": ErrorCodeList.ED00007,
                "target_dwh": target_dwh,
                "type": type(fields_list[0][1]).__name__,
            }
            self.validate_logger.write_log(**args)
            result = False
            return result

        # 型がすべてあっていればTrueを返す
        result = True
        return result

    def validate_data_definition_delimiter(self,
                                           data_definition,
                                           data_definition_delimiter
                                           ):
        """
        概要
            データ定義ファイルが指定した区切り文字か検証

        引数
            data_definition : データ定義ファイル
            data_definition_delimiter : データ定義ファイルの区切り文字

        戻り値
            bool : 有効な区切り文字であれば True 無効な区切り文字であればFalse
        """
        try:
            lines = data_definition.splitlines()[:10]

            # 各行の区切り文字の頻度をカウント
            comma_count = sum(line.count(',') for line in lines)
            tab_count = sum(line.count('\t') for line in lines)

            # 判定不能の場合、試行でデリミタを決定
            data_definition_data_frame = None

            if comma_count > tab_count:
                if not data_definition_delimiter == DDC.DELIMITER_COMMA:
                    self.validate_logger.write_log(ErrorCodeList.ED00009)
                    return False
                else:
                    data_definition_data_frame = pd.read_csv(
                        io.StringIO(data_definition), delimiter=",")
            elif tab_count > comma_count:
                if not data_definition_delimiter == DDC.DELIMITER_TAB:
                    self.validate_logger.write_log(ErrorCodeList.ED00009)
                    return False
                else:
                    data_definition_data_frame = pd.read_csv(
                        io.StringIO(data_definition), delimiter="\t")
            else:
                self.logger.error("データ定義の区切り文字が不正です。")
                self.validate_logger.write_log(ErrorCodeList.ED00010)
                return False  # 判定不能

            return True

        except Exception as e:
            self.logger(f"Error: {e}")
            return None

    def validate_data_definition_column(self,
                                        data_definition_data_frame,
                                        data_type_flag):
        """
        概要
            データ定義ファイルに必要なカラムがあるか検証

        引数
            data_definition_data_frame : DataFrameに加工されたデータ定義ファイル
            data_type_flag : データ定義ファイルに"データ型"カラムがあるかのフラグ

        戻り値
            bool : 有効なデータ定義ファイルであれば True 無効なデータ定義ファイルであればFalse
        """
        try:
            if data_type_flag:
                # 必須カラムリスト
                col_list = [
                    DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME,
                    DDC.DATA_DEFINITION_DWH_COLUMN_NAME,
                    DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME,
                    DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME,
                    DDC.DATA_DEFINITION_DATA_TYPE_COLUMN_NAME,
                ]

            else:
                # 必須カラムリスト(データ型無し)
                col_list = [
                    DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME,
                    DDC.DATA_DEFINITION_DWH_COLUMN_NAME,
                    DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME,
                    DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME,
                ]

            # 不足カラムの確認
            missing_columns = [
                col for col in col_list if col not in data_definition_data_frame.columns]

            if missing_columns:
                # 不足しているカラムがある場合、ログを書き込む
                self.validate_logger.write_log(ErrorCodeList.ED00011)
                return False

            else:
                return True

        except Exception as e:
            self.logger(f"Error: {e}")
            return None

    def validate_data_definition_in_fsf(self,
                                        data_definition,
                                        data_definition_delimiter,
                                        field_set_data_frame):
        """
        概要
            データ定義ファイルがFieldSetFileに適した形になっているか検証

        引数
            data_definition : データ定義ファイル
            data_definition_delimiter : データ定義ファイルの区切り文字
            field_set_data_frame : DataFrameに加工されたFieldSetFile

        戻り値
            bool : 有効なデータ定義ファイルであれば True 無効なデータ定義ファイルであればFalse
        """
        try:
            geometry_type_list, \
                geometry_dwh_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list, \
                = NSP.get_data_definition_index(io.StringIO(data_definition),
                                                data_definition_delimiter=data_definition_delimiter
                                                )

            if not dwh_file_name_list:
                self.validate_logger.write_log(ErrorCodeList.ED00077)

                if self.mode_value == self.MODE_STOP:
                    return False

            for i in range(len(dwh_file_name_list)):
                # 既定値の場合は属性値列にデータがあるか
                if attribute_file_type_list[i] == '-2':
                    if np.isnan(attribute_const_value_list[i]):
                        args = {"error_code": ErrorCodeList.ED00040,
                                "target_dwh": dwh_file_name_list[i]}
                        self.validate_logger.write_log(**args)
                        return False

                else:
                    try:
                        # FieldSetFile内にDWH名があるかどうか検証
                        temp_list = pickle.loads(base64.b64decode(
                            field_set_data_frame.loc[field_set_data_frame['Dwh'] == dwh_file_name_list[i], 'Value'].values[0]))
                        if not self.validate_input_type(temp_list, "fields"):
                            return False
                    except Exception:
                        args = {"error_code": ErrorCodeList.ED00013,
                                "target_dwh": dwh_file_name_list[i]}
                        self.validate_logger.write_log(**args)
                        return False

            return True

        except Exception:
            pass

    def validate_data_definition_in_geometry_fsf(self,
                                                 data_definition,
                                                 data_definition_delimiter,
                                                 field_set_data_frame,
                                                 data_type_flag=True,
                                                 multipatch_flag=True):
        """
        概要
            データ定義ファイルがFieldSetFileに適した形になっているか検証
            座標配列が入っているか検証

        引数
            data_definition : データ定義ファイル
            data_definition_delimiter : データ定義ファイルの区切り文字
            field_set_data_frame : DataFrameに加工されたFieldSetFile
            data_type_flag : データ定義ファイルに"データ型"カラムがあるかのフラグ
            multipatch_flag : マルチパッチが格納されているかのフラグ


        戻り値
            bool : 有効なデータ定義ファイルであれば True 無効なデータ定義ファイルであればFalse
        """

        try:
            # -----------------------------------------------------------------------------------------------------------
            # 【取得】製品データ定義ファイルから出力時ジオメトリ項目ファイル名List、出力時属性項目ファイル名List、入力ファイル元の属性名List取得を取得
            # -----------------------------------------------------------------------------------------------------------
            if data_type_flag:
                geometry_type_list, \
                    geometry_dwh_file_name_list, \
                    geometry_distribution_name_list, \
                    attribute_file_type_list, \
                    attribute_dwh_file_name_list, \
                    attribute_distribution_name_list, \
                    attribute_const_value_list, \
                    attribute_data_type_list\
                    = NSP.get_data_definition_index_datatype(io.StringIO(data_definition),
                                                             data_definition_delimiter=data_definition_delimiter)

                valid_types = {"int",
                               "float",
                               "str",
                               "object"}

                non_convertible_types = [
                    item for item in attribute_data_type_list if item not in valid_types]

                if non_convertible_types:
                    self.validate_logger.write_log(ErrorCodeList.ED00023)
                    return False

                if not attribute_dwh_file_name_list:
                    self.validate_logger.write_log(ErrorCodeList.ED00077)

                    if self.mode_value == self.MODE_STOP:
                        return False

                for i in range(len(attribute_dwh_file_name_list)):
                    # 既定値の場合は属性値列にデータがあるか
                    if attribute_file_type_list[i] == '-2':

                        if np.isnan(attribute_const_value_list[i]):
                            args = {"error_code": ErrorCodeList.ED00040,
                                    "target_dwh": attribute_dwh_file_name_list[i]}
                            self.validate_logger.write_log(**args)
                            return False

                    else:
                        try:
                            temp_list = pickle.loads(base64.b64decode(
                                field_set_data_frame.loc[field_set_data_frame['Dwh'] == attribute_dwh_file_name_list[i], 'Value'].values[0]))
                            if not self.validate_input_type(temp_list, "fields"):
                                return False
                        except Exception:
                            args = {"error_code": ErrorCodeList.ED00013,
                                    "target_dwh": attribute_dwh_file_name_list[i]}
                            self.validate_logger.write_log(**args)
                            return False

                for i in range(len(geometry_dwh_file_name_list)):
                    try:
                        temp_array = pickle.loads(base64.b64decode(
                            field_set_data_frame.loc[field_set_data_frame['Dwh'] == geometry_dwh_file_name_list[i], 'Value'].values[0]))
                    except Exception:
                        args = {"error_code": ErrorCodeList.ED00013,
                                "target_dwh": geometry_dwh_file_name_list[i]}
                        self.validate_logger.write_log(**args)
                        return False

                    try:
                        if not self.validate_input_type(temp_array, "coordinates_array"):
                            return False

                        if multipatch_flag:
                            if not self.validate_coordinates_array(temp_array, multiflag=True):
                                return False

                        else:
                            # 座標配列の形か検証
                            if not self.validate_coordinates_array(temp_array, multiflag=False):
                                return False

                            # ポイントに変換できるか検証
                            if geometry_type_list[0] == DDC.POINT_GEOMETRY_TYPE:
                                if not self.validate_point_coordinates(temp_array):
                                    return False

                            # ラインに変換できるか検証
                            elif geometry_type_list[0] == DDC.LINESTRING_GEOMETRY_TYPE:
                                if not self.validate_line_coordinates(temp_array):
                                    return False

                            # ポリゴンに変換できるか検証
                            elif geometry_type_list[0] == DDC.POLYGON_GEOMETRY_TYPE:
                                if not self.validate_polygon_coordinates(temp_array):
                                    return False

                    except Exception:
                        args = {"error_code": ErrorCodeList.EC00017,
                                "target_dwh": geometry_dwh_file_name_list[i]}
                        self.validate_logger.write_log(**args)
                        return False

            else:
                geometry_type_list, \
                    geometry_dwh_file_name_list, \
                    geometry_distribution_name_list, \
                    dwh_file_name_list, \
                    attribute_name_list, \
                    attribute_const_value_list, \
                    attribute_file_type_list, \
                    all_attribute_name_list, \
                    all_dwh_file_name_list, \
                    = NSP.get_data_definition_index(io.StringIO(data_definition),
                                                    data_definition_delimiter=data_definition_delimiter
                                                    )

                for i in range(len(all_dwh_file_name_list)):
                    if attribute_file_type_list[i] == '-2':
                        if np.isnan(attribute_const_value_list[i]):
                            args = {"error_code": ErrorCodeList.ED00040,
                                    "target_dwh": dwh_file_name_list[i]}
                            self.validate_logger.write_log(**args)
                    else:
                        try:
                            temp_list = pickle.loads(base64.b64decode(
                                field_set_data_frame.loc[field_set_data_frame['Dwh'] == dwh_file_name_list[i], 'Value'].values[0]))
                            if not self.validate_input_type(temp_list, "fields"):
                                return False
                        except Exception:
                            args = {"error_code": ErrorCodeList.ED00012,
                                    "target_dwh": dwh_file_name_list[i]}
                            self.validate_logger.write_log(**args)
                            return False

                for i in range(len(geometry_dwh_file_name_list)):
                    try:
                        temp_array = pickle.loads(base64.b64decode(
                            field_set_data_frame.loc[field_set_data_frame['Dwh'] == geometry_dwh_file_name_list[i], 'Value'].values[0]))
                    except Exception:
                        args = {"error_code": ErrorCodeList.ED00013,
                                "target_dwh": geometry_dwh_file_name_list[i]}
                        self.validate_logger.write_log(**args)
                        return False

                    try:
                        if not self.validate_input_type(temp_array, "coordinates_array"):
                            return False

                        if multipatch_flag:
                            if not self.validate_coordinates_array(temp_array, multiflag=True):
                                return False

                        else:
                            # 座標配列の形か検証
                            if not self.validate_coordinates_array(temp_array, multiflag=False):
                                return False

                            # ポイントに変換できるか検証
                            if geometry_type_list[0] == DDC.POINT_GEOMETRY_TYPE:
                                if not self.validate_point_coordinates(temp_array):
                                    return False

                            # ラインに変換できるか検証
                            elif geometry_type_list[0] == DDC.LINESTRING_GEOMETRY_TYPE:
                                if not self.validate_line_coordinates(temp_array):
                                    return False

                            # ポリゴンに変換できるか検証
                            elif geometry_type_list[0] == DDC.POLYGON_GEOMETRY_TYPE:
                                if not self.validate_polygon_coordinates(temp_array):
                                    return False

                    except Exception:
                        args = {"error_code": ErrorCodeList.EC00017,
                                "target_dwh": geometry_dwh_file_name_list[i]}
                        self.validate_logger.write_log(**args)
                        return False

            return True

        except Exception as e:
            raise Exception(f"Error: {e}")

    def validate_data_definition(self,
                                 data_definition,
                                 data_definition_delimiter,
                                 field_set_data_frame,
                                 data_type_flag=True):
        """
        概要
            データ定義ファイルの区切り文字が正しいか、データ定義ファイルがFieldSetFileに適した形になっているか検証
        引数
            data_definition : データ定義ファイル
            data_definition_delimiter : データ定義ファイルの区切り文字
            field_set_data_frame : DataFrameに加工されたFieldSetFile
            data_type_flag : データ定義ファイルに"データ型"カラムがあるかのフラグ
        
        戻り値
            bool : 有効なデータ定義ファイルであれば True 無効なデータ定義ファイルであればFalse

        """

        try:
            # データを改行で分割して、最初の10行だけ取得
            lines = data_definition.splitlines()[:10]

            # 各行の区切り文字の頻度をカウント
            comma_count = sum(line.count(',') for line in lines)
            tab_count = sum(line.count('\t') for line in lines)

            # 判定不能の場合、試行でデリミタを決定
            data_definition_data_frame = None

            if comma_count > tab_count:
                if not data_definition_delimiter == DDC.DELIMITER_COMMA:
                    self.validate_logger.write_log(ErrorCodeList.ED00009)
                    return False
                else:
                    data_definition_data_frame = pd.read_csv(
                        io.StringIO(data_definition), delimiter=",")
            elif tab_count > comma_count:
                if not data_definition_delimiter == DDC.DELIMITER_TAB:
                    self.validate_logger.write_log(ErrorCodeList.ED00009)
                    return False
                else:
                    data_definition_data_frame = pd.read_csv(
                        io.StringIO(data_definition), delimiter="\t")
            else:
                self.validate_logger.write_log(ErrorCodeList.ED00010)
                return False  # 判定不能

            if data_definition_data_frame is not None:
                if data_type_flag:
                    # 必須カラムリスト
                    col_list = [
                        DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DWH_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME,
                        DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DATA_TYPE_COLUMN_NAME,
                    ]

                else:
                    # 必須カラムリスト(データ型無し)
                    col_list = [
                        DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DWH_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME,
                        DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME,
                    ]
                # 不足カラムの確認
                missing_columns = [
                    col for col in col_list if col not in data_definition_data_frame.columns]

                if missing_columns:
                    # 不足しているカラムがある場合、ログを書き込む
                    self.validate_logger.write_log(ErrorCodeList.ED00011)
                    return False

            geometry_type_list, \
                geometry_dwh_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list, \
                = NSP.get_data_definition_index(io.StringIO(data_definition),
                                                data_definition_delimiter=data_definition_delimiter
                                                )

            for i in range(len(dwh_file_name_list)):
                if attribute_file_type_list[i] == '-2':
                    pass
                else:
                    try:
                        temp_list = pickle.loads(base64.b64decode(
                            field_set_data_frame.loc[field_set_data_frame['Dwh'] == dwh_file_name_list[i], 'Value'].values[0]))
                        if not self.validate_input_type(temp_list, "fields"):
                            return False
                    except Exception:
                        args = {"error_code": ErrorCodeList.ED00012,
                                "target_dwh": dwh_file_name_list[i]}
                        self.validate_logger.write_log(**args)
                        return False

            return True

        except Exception as e:
            self.logger(f"Error: {e}")
            return None

    def validate_data_definition_and_geometry(self,
                                              data_definition,
                                              data_definition_delimiter,
                                              field_set_data_frame,
                                              multipatch_flag=True,
                                              data_type_flag=True):
        """
        概要
            データ定義ファイルの区切り文字が正しいか、データ定義ファイルがFieldSetFileに適した形になっているか検証
            座標配列が入っているか検証
        引数
            data_definition : データ定義ファイル
            data_definition_delimiter : データ定義ファイルの区切り文字
            field_set_data_frame : DataFrameに加工されたFieldSetFile
            multipatch_flag : マルチパッチが格納されているかのフラグ
            data_type_flag : データ定義ファイルに"データ型"カラムがあるかのフラグ
        
        戻り値
            bool : 有効なデータ定義ファイルであれば True 無効なデータ定義ファイルであればFalse

        """
        try:
            # データを改行で分割して、最初の10行だけ取得
            lines = data_definition.splitlines()[:10]

            # 各行の区切り文字の頻度をカウント
            comma_count = sum(line.count(',') for line in lines)
            tab_count = sum(line.count('\t') for line in lines)

            # 判定不能の場合、試行でデリミタを決定
            data_definition_data_frame = None

            if comma_count > tab_count:
                if not data_definition_delimiter == DDC.DELIMITER_COMMA:
                    self.validate_logger.write_log(ErrorCodeList.ED00009)
                    return False
                else:
                    data_definition_data_frame = pd.read_csv(
                        io.StringIO(data_definition), delimiter=",")
            elif tab_count > comma_count:
                if not data_definition_delimiter == DDC.DELIMITER_TAB:
                    self.validate_logger.write_log(ErrorCodeList.ED00009)
                    return False
                else:
                    data_definition_data_frame = pd.read_csv(
                        io.StringIO(data_definition), delimiter="\t")
            else:
                self.validate_logger.write_log(ErrorCodeList.ED00010)
                return False  # 判定不能

            if data_definition_data_frame is not None:
                if data_type_flag:
                    # 必須カラムリスト
                    col_list = [
                        DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DWH_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME,
                        DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DATA_TYPE_COLUMN_NAME,
                    ]

                else:
                    # 必須カラムリスト(データ型無し)
                    col_list = [
                        DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DWH_COLUMN_NAME,
                        DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME,
                        DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME,
                    ]
                # 不足カラムの確認
                missing_columns = [
                    col for col in col_list if col not in data_definition_data_frame.columns]

                if missing_columns:
                    # 不足しているカラムがある場合、ログを書き込む
                    self.validate_logger.write_log(ErrorCodeList.ED00011)
                    return False

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】製品データ定義ファイルから出力時ジオメトリ項目ファイル名List、出力時属性項目ファイル名List、入力ファイル元の属性名List取得を取得
            # -----------------------------------------------------------------------------------------------------------
            if data_type_flag:
                geometry_type_list, \
                    geometry_dwh_file_name_list, \
                    geometry_distribution_name_list, \
                    attribute_file_type_list, \
                    attribute_dwh_file_name_list, \
                    attribute_distribution_name_list, \
                    attribute_const_value_list, \
                    attribute_data_type_list\
                    = NSP.get_data_definition_index_datatype(io.StringIO(data_definition),
                                                             data_definition_delimiter=data_definition_delimiter)

                valid_types = {"int",
                               "float",
                               "str",
                               "object"}

                non_convertible_types = [
                    item for item in attribute_data_type_list if item not in valid_types]

                if non_convertible_types:
                    self.validate_logger.write_log(ErrorCodeList.ED00023)
                    return False

                for i in range(len(attribute_dwh_file_name_list)):
                    if attribute_file_type_list[i] == '-2':
                        pass
                    else:
                        try:
                            temp_list = pickle.loads(base64.b64decode(
                                field_set_data_frame.loc[field_set_data_frame['Dwh'] == attribute_dwh_file_name_list[i], 'Value'].values[0]))
                            if not self.validate_input_type(temp_list, "fields"):
                                return False
                        except Exception:
                            args = {"error_code": ErrorCodeList.ED00012,
                                    "target_dwh": attribute_dwh_file_name_list[i]}
                            self.validate_logger.write_log(**args)
                            return False

                try:
                    temp_array = pickle.loads(base64.b64decode(
                        field_set_data_frame.loc[field_set_data_frame['Dwh'] == geometry_dwh_file_name_list[0], 'Value'].values[0]))
                    if not self.validate_input_type(temp_array, "coordinates_array"):
                        return False

                    if multipatch_flag:
                        if not self.validate_coordinates_array(temp_array, multiflag=True):
                            return False

                    else:
                        if not self.validate_coordinates_array(temp_array, multiflag=False):
                            return False

                except Exception:
                    args = {"error_code": ErrorCodeList.ED00012,
                            "target_dwh": geometry_dwh_file_name_list[0]}
                    self.validate_logger.write_log(**args)
                    return False

                return True

            else:
                geometry_type_list, \
                    geometry_dwh_file_name_list, \
                    geometry_distribution_name_list, \
                    dwh_file_name_list, \
                    attribute_name_list, \
                    attribute_const_value_list, \
                    attribute_file_type_list, \
                    all_attribute_name_list, \
                    all_dwh_file_name_list, \
                    = NSP.get_data_definition_index(io.StringIO(data_definition),
                                                    data_definition_delimiter=data_definition_delimiter
                                                    )

                for i in range(len(dwh_file_name_list)):
                    if attribute_file_type_list[i] == '-2':
                        pass
                    else:
                        try:
                            temp_list = pickle.loads(base64.b64decode(
                                field_set_data_frame.loc[field_set_data_frame['Dwh'] == dwh_file_name_list[i], 'Value'].values[0]))
                            if not self.validate_input_type(temp_list, "fields"):
                                return False
                        except Exception:
                            args = {"error_code": ErrorCodeList.ED00012,
                                    "target_dwh": dwh_file_name_list[i]}
                            self.validate_logger.write_log(**args)
                            return False

                try:
                    temp_array = pickle.loads(base64.b64decode(
                        field_set_data_frame.loc[field_set_data_frame['Dwh'] == geometry_dwh_file_name_list[0], 'Value'].values[0]))
                    if not self.validate_input_type(temp_array, "coordinates_array"):
                        return False
                    if multipatch_flag:
                        if not self.validate_coordinates_array(temp_array, multiflag=True):
                            return False

                    else:
                        if not self.validate_coordinates_array(temp_array, multiflag=False):
                            return False

                except Exception:
                    args = {"error_code": ErrorCodeList.ED00012,
                            "target_dwh": geometry_dwh_file_name_list[0]}
                    self.validate_logger.write_log(**args)
                    return False

                return True

        except Exception as e:
            raise Exception(f"Error: {e}")

    def validate_target_in_fsf(self, target_dwh, field_set_data_frame):
        """
        概要
            field_set_data_frame内に、指定したDWH名があるか検証

        引数
            target_dwh : 指定されたDWH名
            field_set_data_frame : FieldSetFileをDataFrameに加工したデータ
        戻り値
            bool : 有効なDWH名であればTrue 無効であればFalse
        """
        try:
            pickle.loads(base64.b64decode(
                field_set_data_frame.loc[field_set_data_frame['Dwh'] == target_dwh, 'Value'].values[0]))
            return True
        except Exception:
            args = {"error_code": ErrorCodeList.ED00013,
                    "target_dwh": target_dwh}
            self.validate_logger.write_log(**args)
            return False

    def check_closed_and_self_intersecting_multi_patch(self, arr):
        """
        概要
            マルチパッチの4点の閉鎖性を検証する。

        引数
            arr : マルチパッチ
        
        戻り値
            bool : マルチパッチの四点の閉鎖性が確保できればTrue 閉鎖性を確保できない場合Falseを返す
        """
        df = pd.DataFrame(
            arr, columns=["id", "x", "y", "z", "c1", "c2", "c3", "four_id"])

        for id, df_id in df.groupby("id"):  # ID ごとに処理
            for four_id, group in df_id.groupby("four_id"):  # four_id ごとに処理
                coords = group[["x", "y", "z"]].values

                # four_id に対応する点が 4 つなければエラーを返す
                if len(coords) != 4:
                    args = {"error_code": ErrorCodeList.ED00047,
                            "id": id,
                            "連番_id": four_id}
                    self.validate_logger.write_log(**args)
                    return False

                # 始点と終点が異なる場合エラーを返す
                if not np.array_equal(coords[0], coords[3]):
                    # ここはログに出す
                    args = {"error_code": ErrorCodeList.ED00048,
                            "id": id,
                            "連番_id": four_id}
                    self.validate_logger.write_log(**args)
                    return False

        return True  # すべて正常なら True を返す

    def validate_coordinates_array(self, coordinates_array, multiflag=False):
        """
        概要
            座標配列 or マルチパッチか検証

        引数:
            coordinates_array : マルチパッチ or 座標配列
            multiflag : マルチパッチかのフラグ

        戻り値:
            bool: 座標配列であれば True、そうでなければ False。
        """
        try:
            if multiflag:
                # 配列の次元が2で、各行の要素数が8であることを確認
                if coordinates_array.ndim == 2 and coordinates_array.shape[1] == 8:
                    if not self.check_closed_and_self_intersecting_multi_patch(coordinates_array):
                        return False
                else:
                    self.validate_logger.write_log(ErrorCodeList.ED00014)
                    return False

            else:

                # 配列の次元が2で、各行の要素数が4であることを確認
                if coordinates_array.ndim == 2 and coordinates_array.shape[1] != 4:
                    self.validate_logger.write_log(ErrorCodeList.ED00015)
                    return False
                else:
                    return True
            return True

        except Exception as e:
            self.logger.error(f"Error: {e}")
            return None

    def validate_is_valid_directory(self, path):
        """
        概要
            指定されたパスが有効なフォルダかを検証する。

        引数:
            path : 検証するフォルダパス。

        戻り値:
            bool: 有効なフォルダであれば True、そうでなければ False。
        """
        if os.path.isdir(path):
            return True
        else:
            self.validate_logger.write_log(ErrorCodeList.ED00016)
            return False

    def validate_geometry_is_valid(self, geom):
        """
        概要
            ジオメトリが無効ではないか検証

        引数
            geom_list: ジオメトリが格納されたlist

        戻り値
            bool: 有効なジオメトリであれば True、そうでなければ False。

        """
        if not geom.is_valid:
            self.validate_logger.write_log(ErrorCodeList.ED00017)
            return False

        return True

    def validate_geometry_has_z(self, geom):
        """
        概要
            ジオメトリがZ値があるか検証

        引数
            geom_list: ジオメトリが格納されたlist

        戻り値
            bool: ジオメトリにZ値があれば True、そうでなければ False。

        """
        if not geom.has_z:
            self.validate_logger.write_log(ErrorCodeList.ED00018)
            return False

        return True

    def validate_geometry_not_null(self, geom):
        """
        概要
            ジオメトリにnullがないか検証

        引数
            geom_list: ジオメトリが格納されたlist

        戻り値
            bool: ジオメトリにnullがなければ True、そうでなければ False。

        """
        if geom is None:
            self.validate_logger.write_log(ErrorCodeList.ED00019)
            return False

        return True

    def validate_geometry_valid_z(self, geom):
        """
        概要
            ジオメトリのZ値が既定値ではないか検証

        引数
            geom_list: ジオメトリが格納されたlist

        戻り値
            bool: ジオメトリのZ値が既定値がなければ True、そうでなければ False。

        """
        # Z 値を coords から取得 (3D ジオメトリを前提)
        z_value = geom.coords[0][2]

        if z_value == -9999:
            self.validate_logger.write_log(ErrorCodeList.ED00020)
            return False

        return True

    def validate_path(self, target_file_path):
        """
        指定されたファイルパスが存在するか検証

        引数
            target_file_path : 指定されたファイルパス

        戻り値
            bool : 有効なファイルであれば True、そうでなければ False。
        """
        path = Path(target_file_path)

        # パスが存在しない、またはファイルでない場合
        if not path.exists() or not path.is_file():
            self.validate_logger.write_log(ErrorCodeList.ED00016)
            return False  # 存在しないパスがあれば即座にFalseを返す

        return True  # 全て有効な場合はTrueを返す

    def validate_input_data_definition(self,
                                       data_definition,
                                       data_definition_delimiter,
                                       geodata_frame,
                                       geometry_flag=True
                                       ):
        """
        概要
            GeoDataFrameにデータ定義ファイルに書かれたカラムがあるか検証

        引数
            data_definition : データ定義ファイル
            data_definition_delimiter : データ定義ファイルの区切り文字
            geodata_frame : インプットされるGeoDataFrame
            geometry_flag : ジオメトリがあるかのフラグ
        戻り値
            bool : 有効なデータ定義であればTrue 無効であればFalse
        """
        try:

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】製品データ定義ファイルから出力時ジオメトリ項目ファイル名List、出力時属性項目ファイル名List、入力ファイル元の属性名List取得を取得
            # -----------------------------------------------------------------------------------------------------------
            # データ定義からジオメトリタイプlist、フィールド名リスト、DWHファイル名リストを取得する
            geometry_type_list, \
                geometry_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list, \
                = NSP.get_data_definition_index(io.StringIO(data_definition),
                                                data_definition_delimiter=data_definition_delimiter
                                                )

            if geometry_flag:
                if geometry_type_list[0] not in [DDC.POINT_GEOMETRY_TYPE, DDC.LINESTRING_GEOMETRY_TYPE, DDC.POLYGON_GEOMETRY_TYPE]:
                    self.validate_logger.write_log(ErrorCodeList.ED00043)
                    return False

                if geometry_type_list[0] == DDC.POINT_GEOMETRY_TYPE:
                    for geom in geodata_frame.geometry:
                        if geom.geom_type in ["Point", "MultiPoint"]:
                            pass
                        else:
                            self.validate_logger.write_log(
                                ErrorCodeList.ED00044)
                            return False

                elif geometry_type_list[0] == DDC.LINESTRING_GEOMETRY_TYPE:
                    for geom in geodata_frame.geometry:
                        if geom.geom_type in ["LineString", "MultiLineString"]:
                            pass
                        else:
                            self.validate_logger.write_log(
                                ErrorCodeList.ED00045)
                            return False

                elif geometry_type_list[0] == DDC.POLYGON_GEOMETRY_TYPE:
                    for geom in geodata_frame.geometry:
                        if geom.geom_type in ["Polygon", "MultiPolygon"]:
                            pass
                        else:
                            self.validate_logger.write_log(
                                ErrorCodeList.ED00046)
                            return False

                fields_name_list = geodata_frame.columns.to_list()
                all_attribute_name_list.append("geometry")
                for field in all_attribute_name_list:
                    if field not in fields_name_list:
                        args = {"error_code": ErrorCodeList.ED00022,
                                "流通項目名": field}
                        self.validate_logger.write_log(**args)
                        return False

                return True
            else:
                fields_name_list = geodata_frame.columns.to_list()
                for field in all_attribute_name_list:
                    if field not in fields_name_list:
                        args = {"error_code": ErrorCodeList.ED00022,
                                "流通項目名": field}
                        self.validate_logger.write_log(**args)
                        return False

                return True

        except Exception as e:
            raise Exception(f"Error: {e}")
            # return None

    def check_epsg(self, epsg_code):
        """
        概要
            入力されたEPSGが正しいか検証

        引数
            epsg_code : 入力されるEPSGコード

        戻り値
            bool : 有効なEPSGコードであればTrue 無効であればFalseを返す
        """
        try:
            # EPSGコードを元にCRSオブジェクトを作成
            crs = CRS.from_epsg(epsg_code)
            return True
        except Exception:
            args = {"error_code": ErrorCodeList.ED00025,
                    "input_crs": epsg_code}
            self.validate_logger.write_log(**args)
            return False

    def validate_polygon_coordinates(self, target_value):
        """
        概要
            座標配列がポリゴンジオメトリに変換できるか検証

        引数
            coordinates_array : 座標配列

        戻り値
            bool : ポリゴンジオメトリにできればTrue　できなければFalseを返す
        """

        # 座標を格納する辞書
        coordinates_dict = defaultdict(list)

        # 辞書に座標を追加
        for entry in target_value:
            line_id, x, y, z = entry
            coordinates_dict[line_id].append((x, y, z))

        for line_id, points in coordinates_dict.items():
            # 最初と最後の点が異なる場合、最初の点を末尾に追加して閉じたポリゴンにする
            if points[0] != points[-1]:
                points.append(points[0])
            try:
                poly = Polygon(points)

                # 無効なジオメトリではないか検証
                if poly.is_valid:
                    pass
                else:
                    self.validate_logger.write_log(ErrorCodeList.ED00032)

            # ポリゴンにならなかった場合にエラーを返す
            except ValueError:
                self.validate_logger.write_log(ErrorCodeList.ED00029)
                return False

        return True

    def validate_point_coordinates(self, coordinates_array):
        """
        概要
            座標配列がポイントジオメトリに変換できるか検証

        引数
            coordinates_array : 座標配列

        戻り値
            bool : ポイントジオメトリにできればTrue　できなければFalseを返す
        """
        coordinates_dict = NSP.split_geometry_points_dict(
            coordinates_array[:, :4])
        try:
            coordinates_shapely_array = np.array(
                NSP.convert_dict_to_point_geometries(coordinates_dict))[:, 1]
            return True

        except Exception:
            self.validate_logger.write_log(ErrorCodeList.ED00027)
            return False

    def validate_line_coordinates(self, coordinates_array):
        """
        概要
            座標配列がラインジオメトリに変換できるか検証

        引数
            coordinates_array : 座標配列

        戻り値
            bool : ラインジオメトリにできればTrue　できなければFalseを返す
        """
        coordinates_dict = NSP.split_geometry_points_dict(
            coordinates_array[:, :4])
        try:
            coordinates_shapely_array = np.array(
                NSP.convert_dict_to_linestring_geometries(coordinates_dict))[:, 1]
            return True

        except Exception:
            self.validate_logger.write_log(ErrorCodeList.ED00028)
            return False

    def validate_iterate_line_coordinates(self, data: np.ndarray) -> bool:
        """
        概要
            同じ ID 内で (x, y, z) が重複している点を検証する。
            エラーがあれば False を返し、なければ True を返す。

        引数
            data (np.ndarray): 2D numpy 配列 (ID, x, y, z)

        戻り値
            bool: エラーなしなら True, エラーありなら False
        """
        unique_ids = np.unique(data[:, 0])  # ユニークな ID を取得

        for id_ in unique_ids:
            subset = data[data[:, 0] == id_][:, 1:]  # ID ごとの (x, y, z) を取得
            unique_rows, counts = np.unique(
                subset, axis=0, return_counts=True)  # (x, y, z) の重複か検証

            duplicates = unique_rows[counts > 1]  # 2回以上出現したものを取得
            if duplicates.size > 0:

                args = {"error_code": ErrorCodeList.ED00057,
                        "対象のID": id_}
                self.validate_logger.write_log(**args)
                return False

        return True

    def validate_field_null(self, field_list):
        """
        概要
            (ID, 値) のリストから、値が None または np.nan の ID を抽出する関数。

        引数
            field_list (list of tuple): (ID, 値) のリスト

        戻り値
            list: Null 値のある ID のリスト
        """
        # None を np.nan に変換し、numpy 配列に変換
        field_array = np.array([(id_, np.nan if v is None else v)
                               for id_, v in field_list], dtype=np.float64)

        # NaN のある ID を抽出
        nan_ids = field_array[np.isnan(field_array[:, 1]), 0].astype(int)

        if nan_ids.size > 0:
            args = {"error_code": ErrorCodeList.ED00059,
                    "対象のID": nan_ids.tolist()}
            self.validate_logger.write_log(**args)
            return False

        else:
            return True

    def validate_geodataframe_conversion(self,
                                         geodataframe,
                                         extension,
                                         encoding,
                                         write_args=[]):
        """
        概要
            GeoDataFrameをCSV,GeoJSON,GeoPackageに変換できるか検証する。

        引数:
            geodataframe: 検証するフォルダパス。
            extension: ファイル拡張子（「.」付き）
            encoding: エンコーディング方法

        戻り値:
            bool: 有効なGeoDataFrameであれば True、そうでなければ False。
        """

        # CSVの場合
        if extension == DDC.CSV_EXTENSION:

            try:
                result_object = geodataframe.to_csv(index=False)

            except Exception:
                self.validate_logger.write_log(ErrorCodeList.ED00062)
                return False

        # GeoJSONの場合
        elif extension == DDC.GEOJSON_EXTENSION:

            try:
                result_object = geodataframe.to_json(
                    drop_id=True).encode(encoding)

            except LookupError:
                self.validate_logger.write_log(ErrorCodeList.EC00007)
                return False

            except Exception:
                return False

        # GeoPackageの場合
        elif extension == DDC.GPKG_EXTENSION:

            temp_object = io.BytesIO()
            try:
                geodataframe.to_file(temp_object,
                                     encoding=encoding,
                                     driver='GPKG',
                                     layer=write_args[0])

                temp_object.close()

            except LookupError:
                self.validate_logger.write_log(ErrorCodeList.EC00007)
                return False

            except Exception:
                self.validate_logger.write_log(ErrorCodeList.ED00064)
                return False

        return True

    def validate_2d_or_3d_coordinates_array(self, coordinates_array):
        """
        概要
            座標配列が2Dもしくは3Dの座標配列かどうか検証する。

        引数
            coordinates_array: 座標配列

        戻り値
            bool: 有効な座標配列であれば True、そうでなければ False。
        """

        # 配列の次元が2であることを確認
        if coordinates_array.ndim != 2:
            self.validate_logger.write_log(ErrorCodeList.ED00074)
            return False

        # 各行の要素数が4または3であることを確認
        if coordinates_array.shape[1] not in [3, 4]:
            self.validate_logger.write_log(ErrorCodeList.ED00075)
            return False

        return True
