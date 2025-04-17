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

# Python標準ライブラリ
import pickle
from abc import abstractmethod
import re
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")
np = import_module("numpy")
Point = getattr(import_module("shapely.geometry"), "Point")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
LineString = getattr(import_module("shapely.geometry"), "LineString")

from common.base_validate_processor import BaseValidateProcessor
from common.error_code_list import ErrorCodeList
import cad.common.cad_utils as CU


class CadBaseValidateProcessor(BaseValidateProcessor):

    @abstractmethod
    def validate_data(self, context, flowfile):
        pass

    def validate_log_of_fsf(self, fsf, df, expected_type=None):
        """
        入力データがFieldSetFileの場合のValidateLogを生成する

        :param fsf: 入力データのFieldSetFile
        :type fsf: bytes
        :param df: 入力データのFieldSetFileをDataFrameにしたもの
        :type df: pandas.DataFrame
        :param expected_type: ジオメトリ有効チェックの際に期待するジオメトリの型
        :type expected_type: type|None

        :return: 処理結果
        :rtype: bool
        """
        # FieldSetFileをGeoDataFrameに変換
        gdf = CU.field_set_file_to_geodataframe(fsf)

        # GeoDataFrameのジオメトリの有効チェック
        if not self.validate_geodataframe_geometry(gdf, expected_type):
            return False

        # レイヤ名（Dwhの「/」の左側の値）の一意のリストを生成
        layer_list = gdf["layer"].unique()

        # レイヤ名（データ名）の数分ループ
        for layer in layer_list:
            # 特定のデータ名の属性リストを取得（※results型のデータは省く） e.g. ['geometry', '高さ']
            # FieldSetFile->GeoDataFrame変換の際、FieldSetFileに存在しない属性が生成される為
            attr_list = (
                df[(df["Dwh"].str.split("/").str[0] == layer) & (~df["Type"].isin(["results", "geometry"]))]["Dwh"]
                .apply(lambda dwh: dwh.split("/")[1])
                .tolist()
            )
            attr_list.append("geometry")  # TypeがgeometryのデータはDwhの値に関わらずgeometryカラムとして処理されるため
            # 行数・列数チェック対象のGeoDataFrameを抽出
            # レイヤ名（データ名）がループ中のものと一致、かつ、属性リストに存在するデータでフィルタリング
            filtered_gdf = gdf[gdf["layer"] == layer][attr_list]
            # 行数・列数チェックを実施
            if not self.validate_gdf_shape(filtered_gdf, layer):
                return False

        return True

    def validate_empty_data(self, data):
        """
        dataが空かどうかをチェックする

        :param data: チェック対象データ
        :type data: Any

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        if data is None:
            self.validate_logger.write_log(ErrorCodeList.EC00001)
            return False
        return True

    def grouping_data_name(self, data):
        """
        複数ジオメトリが存在する場合にデータ名(Dwhの/の左側)でグルーピングする

        :param data: チェック対象データ
        :type data: geopandas.GeoDataFrame|pandas.DataFrame

        :return グルーピング後のデータ
        :rtype: list|None
        """
        if isinstance(data, gpd.GeoDataFrame):
            if "layer" in data.columns:
                grouped = data.groupby("layer")
                grouped_data = [group for _, group in grouped]
            else:
                grouped_data = data
        # FieldSetFile(DataFrameに変換したもの)の場合
        elif isinstance(data, pd.DataFrame):
            if not data.empty:
                count_df = data.copy()
                layers = count_df["Dwh"].str.split("/").str[0]
                count_df["group"] = layers
                # group列でデータを分割してリストに格納
                grouped_data = [group_df for _, group_df in count_df.groupby("group")]
            else:
                grouped_data = data
        else:
            return None
        return grouped_data

    def validate_exist_grouping_geometry(self, data):
        """
        データが空かどうか、ジオメトリが存在するかどうかをlayer名ごとに確認する

        :param data: チェック対象データ
        :type data: geopandas.GeoDataFrame|pandas.DataFrame

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        if len(data) <= 0:
            self.validate_logger.write_log(ErrorCodeList.EC00002)
            return False
        grouped_data = self.grouping_data_name(data)

        if grouped_data:
            results = []
            for _, gdata in enumerate(grouped_data):
                result = self.validate_exist_geometry(gdata)
                if not result and self.mode_value == self.MODE_STOP:
                    return result
                results.append(result)
            return all(results)
        else:
            self.validate_logger.write_log(ErrorCodeList.EC00006)
            return False

    def validate_no_nan(self, geondary):
        """
        GeoNdarrayにnanが含まれるかチェックする

        :param geondary: GeoNdarray
        :type geondary: GeoNdarray

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        if np.any(np.isnan(geondary)):
            self.validate_logger.write_log(ErrorCodeList.EC00019)
            return False
        return True

    def validate_exist_geometry(self, data):
        """
        データが空かどうか、ジオメトリが存在するかどうか確認する

        :param data: チェック対象データ（GeoDataFrameまたはFieldSetFileをDataFrameに変換したもの）
        :type data: geopandas.GeoDataFrame|pandas.DataFrame

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        # GeoDataFrameの場合
        if isinstance(data, gpd.GeoDataFrame):
            if data.empty:
                self.validate_logger.write_log(ErrorCodeList.EC00001)
                return False
            if "geometry" not in data.columns:
                args = {}
                if "layer" in data.columns:
                    args = {"データ名": data["layer"].values[0]}
                self.validate_logger.write_log(ErrorCodeList.EC00002, **args)
                return False
            return True
        # FieldSetFile(DataFrameに変換したもの)の場合
        elif isinstance(data, pd.DataFrame):
            if len(data) <= 0:
                self.validate_logger.write_log(ErrorCodeList.EC00002)
                return False
            # Value列の値がgeondarrayの場合にnanの存在チェックを行う
            values_list = data["Value"].values.tolist()
            for value in values_list:
                if (isinstance(value, np.ndarray) and value.ndim == 2 and all(len(row) in [3, 4] for row in value)
                        and np.any(np.vectorize(lambda x: isinstance(x, float))(value))):
                    if not self.validate_no_nan(value):
                        return False
            if not self.validate_exist_attribute(data, ["geometry"]):
                return False
            return True
        # それ以外の場合
        else:
            self.validate_logger.write_log(ErrorCodeList.EC00006)
            return False

    def validate_exist_grouping_geometry_attribute(self, data, attribute_names):
        """
        FieldSetFileのDwh列に指定した属性名が存在するか確認する

        :param data: チェック対象データ（GeoDataFrameまたはFieldSetFileをDataFrameにしたもの）
        :type data: geopandas.GeoDataFrame|pandas.DataFrame
        :param attribute_names: 属性リスト
        :type attribute_names: list

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        if len(data) <= 0:
            self.validate_logger.write_log(ErrorCodeList.EC00002)
            return False
        group_data = self.grouping_data_name(data)

        if group_data:
            results = []
            for _, gdata in enumerate(group_data):
                result = self.validate_exist_attribute(gdata, attribute_names)
                if not result and self.mode_value == self.MODE_STOP:
                    return result
                results.append(result)
            return all(results)
        else:
            self.validate_logger.write_log(ErrorCodeList.EC00006)
            return False

    def validate_exist_attribute(self, data, attribute_names):
        """
        FieldSetFileのDwh列に指定した属性名が存在するか確認する

        :param data: チェック対象データ（GeoDataFrameまたはFieldSetFileをDataFrameにしたもの）
        :type data: geopandas.GeoDataFrame|pandas.DataFrame
        :param attribute_names: 属性リスト
        :type attribute_names: list

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        result = True
        layer = None
        if isinstance(data, gpd.GeoDataFrame):
            values = data.columns
            if "layer" in values:
                layer = data["layer"].values[0]
        else:
            main_keys = data["Dwh"].str.split("/").str[0]
            sub_keys = data["Dwh"].str.split("/").str[1]
            layer = main_keys.values[0]
            values = sub_keys.values
        # '/' で分割し、前半をカテゴリ、後半をキーとして新しいカラムに追加
        for name in attribute_names:
            if not name in values:
                args = {}
                if layer:
                    args = {"データ名": layer}
                args["列名称"] = name
                self.validate_logger.write_log(error_code=ErrorCodeList.EC00009, **args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return result
        return result

    def validate_fsf_match_record(self, fsf):
        """
        FieldSetFileのFIDマッチングの一致を確認する。

        :param fsf: チェック対象データのFieldSetFile
        :type fsf: bytes

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        result = True
        df = CU.field_set_file_to_dataframe(fsf)

        # dwhの右辺がFIDのものを取得(キーとなるデータ)
        dwh_fid_rows = [
            row for _, row in df.iterrows() if row["Dwh"].split("/")[1] == "FID"
        ]
        key_fid_df = pd.DataFrame(dwh_fid_rows)

        # 上記の結果からDwhの左辺が一致するレコードを取得
        for _, key_row in key_fid_df.iterrows():
            target_name_row = []
            for _, row in df.iterrows():
                if (
                    row["Dwh"].split("/")[0] == key_row["Dwh"].split("/")[0]
                    and not row["Dwh"].split("/")[1] == "geometry"
                    and not row["Dwh"].split("/")[1] == "FID"
                    and not row["Type"] == "geometry"
                ):
                    target_name_row.append(row)
            target_df = pd.DataFrame(target_name_row)

            # Valueの中身をチェックしキーとなるデータとの一致を確認する
            reference_fid_list = key_row["Value"]
            for idx, target_row in target_df.iterrows():
                match_flg_list = []
                for target_fid in target_row["Value"]:
                    # 一つでも一致すればTrue、しなければFalse
                    match_flg_list.append(
                        any([target_fid[0] == fid for _, fid in reference_fid_list])
                    )
                if not all(match_flg_list):
                    args = {
                        "error_code": ErrorCodeList.EC00005,
                        "column_name": target_row["Dwh"],
                        "record_number": idx,
                    }
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return result
        return result

    def validate_data_types(self, data, expected_type):
        """
        期待するデータ型が正しいかをチェックする。

        :param data: チェック対象データ
        :type data: Any
        :param expected_type: 期待するデータ型
        :type expected_type: type

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        if not isinstance(data, expected_type):
            self.validate_logger.write_log(
                ErrorCodeList.EC00008,
                期待値=expected_type.__name__,
                実測値=type(data).__name__,
            )
            return False
        return True

    def validate_fsf_format(self, fsf, is_pickle=True):
        """
        FieldSetFileの形式が正しいかをチェックする

        :param fsf: チェック対象データのFieldSetFile
        :type fsf: bytes
        :param is_pickle: Valueがpickleされているかどうか
        :type is_pickle: bool

        :return: チェック結果(正常=True、異常=False)、正常に変換されたDataFrame(正常=DataFrame、異常=None)
        :rtype: tuple[bool, DataFrame|None]
        """
        result = True
        try:
            df = CU.field_set_file_to_dataframe(fsf, is_decode=False)
        except:
            args = {"error_code": ErrorCodeList.EC00006, "対象": "FieldSetFile"}
            self.validate_logger.write_log(**args)
            result = False
            return result, None

        # 列名称の有無をチェック
        required_columns = ["Dwh", "Type", "Value"]
        if not all(col in df.columns for col in required_columns):
            args = {"error_code": ErrorCodeList.EC00009}
            self.validate_logger.write_log(**args)
            result = False
            return result, None

        # Dwhの形式チェック
        dwh_pattern = re.compile(r"^[^/]+/[^/]+$")  # 任意の文字列/任意の文字列 の形式
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

        # レコード数の一致チェック
        # group列としてDwhの左辺だけを取得する
        count_df = df.copy()
        count_df["group"] = df["Dwh"].str.split("/").str[0]
        # group列でデータを分割してリストに格納
        grouped_dfs = [
            group_df[group_df["Type"] != "results"]
            for _, group_df in count_df.groupby("group")
        ]

        for _, group_df in enumerate(grouped_dfs):
            value_list = group_df[group_df["Type"] != "results"]["Value"].tolist()
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
            # Valueの値のチェック
            for idx in range(len(df)):
                try:
                    if isinstance(df.loc[idx, "Value"], str):
                        df.loc[idx, "Value"] = CU.decode_value(df.loc[idx, "Value"])
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
        インプットがシリアライズされたデータかチェックする

        :param input_data: チェック対象データ
        :type input_data: bytes
        
        :return: チェック結果(正常=True、異常=False)、デシリアライズされたデータ or None(正常=デシリアライズされたデータ、異常=None)
        :rtype: tuple[bool, Any]
        """
        try:
            return True, pickle.loads(input_data)
        except:
            self.validate_logger.write_log(ErrorCodeList.EC00015)
            return False, None

    def validate_data_boundaries(self, data, max_val, min_val):
        """
        境界値チェック

        :param data: チェック対象データ
        :type data: list
        :param max_val: 最大値
        :type max_val: Any
        :param min_val: 最小値
        :type min_val: Any

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        result = True
        out_of_ranges = [x for x in data if not (min_val <= x <= max_val)]
        if out_of_ranges:
            args = {
                "error_code": ErrorCodeList.EC00012,
                "指定最小値": min_val,
                "指定最大値": max_val,
                "範囲外の値": ",".join(out_of_ranges),
            }
            self.validate_logger.write_log(**args)
            result = False
            if self.mode_value == self.MODE_STOP:
                return result
        return result

    def validate_data_uniqueness(self, data, check_side=0):
        """
        重複チェック：チェック後、必要であればログ出力

        :param data: チェック対象データ
        :type data: list
        :param check_side: left = 0 or right = 1 右辺or左辺どちらをチェックするか指定する
        :type check_side: int

        :return: チェック結果(正常=True、異常=False), チェック結果 重複値
        :rtype: tuple[bool, list|None]
        """
        result = True
        seen = set()
        duplicates = set()
        for val in data:
            check_val = val if not isinstance(val, tuple) else val[check_side]
            if not isinstance(check_val, list):
                if check_val in seen:
                    duplicates.add(check_val)
                else:
                    seen.add(check_val)
        if duplicates:
            result = False
            return result, duplicates
        return result, None

    def validate_point_record(self, point):
        """
        各レコードに有効な座標が含まれているかチェックする
        座標値がnanでないかを確認

        :param point: チェック対象のPoint
        :type point: shapely.geometry.Point

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        # Point型であることを確認
        if not isinstance(point, Point):
            self.validate_logger.write_log(ErrorCodeList.EC00017)
            return False

        # 座標にnanが含まれていないかチェック
        if np.isnan(point.x) or np.isnan(point.y):
            self.validate_logger.write_log(ErrorCodeList.EC00020)
            return False
        return True

    def validate_linestring_record(self, linestring):
        """
        各データの座標数が2点以上であるかチェック
        各座標がvalidate_point_recordの基準を満たしているかチェック

        :param linestring: チェック対象のLineString
        :type linestring: shapely.geometry.LineString

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        # LineString型であることを確認
        if not isinstance(linestring, LineString):
            self.validate_logger.write_log(ErrorCodeList.EC00017)
            return False

        # 2点以上の座標を持っているか確認
        if len(linestring.coords) < 2:
            self.validate_logger.write_log(ErrorCodeList.EC00021)
            return False

        # 各座標がNaNを含まないかチェック
        for coord in linestring.coords:
            if np.isnan(coord[0]) or np.isnan(coord[1]):
                self.validate_logger.write_log(ErrorCodeList.EC00022)
                return False
        return True

    def validate_polygon_record(self, polygon):
        """
        各データの座標数が4点以上であるかチェック
        閉じたポリゴンになっているかチェック
        各座標がvalidate_point_recordの基準を満たしているかチェック

        :param polygon: チェック対象のPolygon
        :type polygon: shapely.geometry.Polygon

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        # Polygon型であることを確認
        if not isinstance(polygon, Polygon):
            self.validate_logger.write_log(ErrorCodeList.EC00017)
            return False

        # 4点以上の座標を持っているか確認
        if len(polygon.exterior.coords) < 4:
            self.validate_logger.write_log(ErrorCodeList.EC00023)
            return False

        # 最初と最後の座標が一致し、閉じた形状であるか確認
        if polygon.exterior.coords[0] != polygon.exterior.coords[-1]:
            self.validate_logger.write_log(ErrorCodeList.EC00024)
            return False

        # 各座標がnanを含まないかチェック
        for coord in polygon.exterior.coords:
            if np.isnan(coord[0]) or np.isnan(coord[1]):
                self.validate_logger.write_log(ErrorCodeList.EC00025)
                return False

        return True

    def validate_geodataframe_geometry(self, gdf, expected_type=None):
        """
        GeoDataFrameのgeometry列の各要素を適切なバリデーション関数に渡してチェックする。

        :param gdf: チェック対象のGeoDataFrame
        :type gdf: geopandas.GeoDataFrame
        :param expected_type: 期待するジオメトリの型 (Point, LineString, Polygon のいずれか)
        :type expected_type: type|None

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        # 入力がGeoDataFrameであることを確認
        if not isinstance(gdf, gpd.GeoDataFrame):
            self.validate_logger.write_log(ErrorCodeList.EC00006)
            return False

        results = []
        for geom in gdf.geometry:
            # 期待する型が指定され、かつ一致しない場合エラー
            if expected_type and not isinstance(geom, expected_type):
                self.validate_logger.write_log(ErrorCodeList.EC00017)
                results.append(False)
            # 各ジオメトリ型に応じたバリデーションを実施
            elif isinstance(geom, Point):
                results.append(self.validate_point_record(geom))
            elif isinstance(geom, LineString):
                results.append(self.validate_linestring_record(geom))
            elif isinstance(geom, Polygon):
                results.append(self.validate_polygon_record(geom))
            else:
                # 期待されないジオメトリタイプが含まれていた場合エラー
                self.validate_logger.write_log(ErrorCodeList.EC00017)
                results.append(False)

        # 全ての要素がTrueであればTrueを返す
        return all(results)

