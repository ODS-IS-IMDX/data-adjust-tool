# MIT License
# 
# Copyright (c) 2026 NTT InfraNet
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
# Shapefileから管路と旗揚げ線を読み込み、旗揚げ線を延長して管路と交差判定。
# 交差に基づき管路を分割し、各分割線の座標配列・累積距離を算出。
# さらに交差した旗揚げ線の「土被り」値を割り当て、属性情報と共に「土被り.txt」「属性.txt」として出力する処理。
# --------------------------------------------------------------------------------------------
# XY地物ID： 分割前地物id
# 地物ID： 分割後地物id
# attributes_text_dataframe： "XY地物ID","地物ID","始点距離","始点位置からの距離","土被り"のカラムを持つ
# earth_covering_text_dataframe： "地物ID"+対象管路が持つ属性のカラムを持つ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
pd = import_module("pandas")
np = import_module("numpy")
gpd = import_module("geopandas")
LineString = import_module("shapely.geometry").LineString
MultiLineString = import_module("shapely.geometry").MultiLineString
split = import_module("shapely.ops").split
STRtree = import_module("shapely.strtree").STRtree


class GenerateEarthCoveringText(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        管路を旗揚げ線で分割し、距離と土被り値を「土被り.txt」に、管路属性を「属性.txt」に出力する。
                        ①input: GeoDataFrameを持った2行のFieldSetFile。
                        ②output: 管路のジオメトリ、土被り.txt、属性.txtを持った3行のFieldSetFile。
                      """
        tags = ["GeoDataFrame", "Shapefile", "Callout", "Split", "Cumulative", "Python"]

    TARGET_PIPE_DWH_NAME = PropertyDescriptor(
        name="Target Pipe DWH Name",
        description="入力データの処理対象管路（GeoDataFrame）のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    CALLOUT_DWH_NAME = PropertyDescriptor(
        name="Callout DWH Name",
        description="入力データの旗揚げ（GeoDataFrame）のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    PIPE_GEOMETRY_DWH = PropertyDescriptor(
        name="Pipe Geometry DWH Name",
        description="出力データの管路（geometry）のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    EARTH_COVERING_DWH = PropertyDescriptor(
        name="Earth Covering DWH Name",
        description="出力データの土被り.txtのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    ATTRIBUTES_DWH = PropertyDescriptor(
        name="Attributes DWH Name",
        description="出力データの属性.txtのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [TARGET_PIPE_DWH_NAME,
                            CALLOUT_DWH_NAME,
                            PIPE_GEOMETRY_DWH,
                            EARTH_COVERING_DWH,
                            ATTRIBUTES_DWH]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            target_pipe_dwh_name: 入力データの処理対象管路（GeoDataFrame）のDWH名
            target_pipe_dwh_name: 入力データの旗揚げ（GeoDataFrame）のDWH名
            target_pipe_dwh_name: 出力データの管路（geometry）のDWH名
            target_pipe_dwh_name: 出力データの.txtのDWH名
            target_pipe_dwh_name: 出力データの属性.txtのDWH名
        """

        target_pipe_dwh_name = context.getProperty(
            self.TARGET_PIPE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        callout_dwh_name = context.getProperty(
            self.CALLOUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        pipe_geometry_dwh = context.getProperty(
            self.PIPE_GEOMETRY_DWH).evaluateAttributeExpressions(flowfile).getValue()

        earth_covering_dwh = context.getProperty(
            self.EARTH_COVERING_DWH).evaluateAttributeExpressions(flowfile).getValue()

        attributes_dwh = context.getProperty(
            self.ATTRIBUTES_DWH).evaluateAttributeExpressions(flowfile).getValue()

        return target_pipe_dwh_name, callout_dwh_name, pipe_geometry_dwh, earth_covering_dwh, attributes_dwh

    def get_geodataframe_from_field_set_file_dataframe(self,
                                                       field_set_file_dataframe,
                                                       target_pipe_dwh_name,
                                                       callout_dwh_name):
        """
        FieldSetFileで作成したdataframeから、プロパティで指定したDWH名を持つ行のValue列を取得

        Parameters:
            field_set_file_dataframe: FieldSetFileをDataFrameに加工したデータ
            target_pipe_dwh_name: 入力データの処理対象管路（GeoDataFrame）のDWH名
            callout_dwh_name: 入力データの旗揚げ（GeoDataFrame）のDWH名

        Returns:
            target_geodataframe: 処理対象管路のGeoDataFrame
            callout_geodataframe: 旗揚げのGeoDataFrame
        """

        # field_set_dataframeから処理対象管路取得
        target_geodataframe = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, target_pipe_dwh_name)

        # field_set_dataframeから旗揚げ取得
        callout_geodataframe = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, callout_dwh_name)

        return target_geodataframe, callout_geodataframe


    def extend_point_along_line(self, p0, p1, extension):
        """
        点p1から点p0への方向に沿って、p0をextension分だけ延長した点を返す

        Parameters
            p0 (np.array): 延長する起点の座標 (x, y)
            p1 (np.array): 方向を示す基準点の座標 (x, y)
            extension (float): 延長距離

        Returns
            np.array (np.array): 延長後の座標 (x, y)
        """

        v = p0 - p1
        v_unit = v / np.linalg.norm(v)

        return p0 + v_unit * extension


    def extend_line_edge_points(self, line, extension=0.1):
        """
        3点以上からなるLineStringの両端をextension分だけ延長

        Parameters
            line (shapely.geometry.LineString): 延長対象のLineString。構成点は点は3点以上を想定
            extension (float) 延長距離（デフォルトは0.1）

        Returns
            shapely.geometry.LineString: 延長後のLineString
        """

        coords = list(line.coords)
        if len(coords) == 3:

            p0 = np.array(coords[0])  # 始点
            p1 = np.array(coords[1])  # 中点
            p2 = np.array(coords[2])  # 終点

            # 中点→始点ベクトル（方向）
            new_p0 = self.extend_point_along_line(p0, p1, extension)

            # 中点→終点ベクトル（方向）
            new_p2 = self.extend_point_along_line(p2, p1, extension)

            # 新たな3点ライン（延長済み）
            new_coords = [tuple(new_p0), tuple(p1), tuple(new_p2)]

        elif len(coords) > 3:

            # 先頭2点で延長する方向を計算
            p0 = np.array(coords[0])
            p1 = np.array(coords[1])
            new_p0 = self.extend_point_along_line(p0, p1, extension)

            # 末尾2点で延長する方向を計算
            p_last = np.array(coords[-1])
            p_before_last = np.array(coords[-2])
            new_p_last = self.extend_point_along_line(p_last, p_before_last, extension)

            # 途中の点はそのまま使う（1から-2まで）
            middle_points = coords[1:-1]

            # 新しい座標列を作成
            new_coords = [tuple(new_p0)] + middle_points + [tuple(new_p_last)]

        return LineString(new_coords)


    def build_callout_tree(self, geodataframe):
        """
        ジオメトリから空間インデックス（STRtree）を構築

        Parameters
            geodataframe (geopandas.GeoDataFrame): 処理対象のGeoDataFrame

        Returns
            geometry (geopandas.array.GeometryArray): ジオメトリ配列
            tree (shapely.strtree.STRtree): STRtree空間インデックス
        """

        geometry = geodataframe.geometry.values

        tree = STRtree(geometry)

        return geometry, tree


    def find_intersecting_callouts(self, geom, callout_geoms, callout_tree):
        """
        geomと交差するcallout_geomsを空間インデックスから検索、
        実際に交差しているcallout_geomsのインデックスリストを返す。

        Parameters
            geom (shapely.geometry.linestring.LineString): 交差判定対象のジオメトリ
            callout_geoms (geopandas.array.GeometryArrayg): 旗揚げ線のジオメトリ配列
            callout_tree (shapely.strtree.STRtree): 旗揚げ線の空間インデックス

        Returns
            intersect_indices_list (list): 交差している旗揚げ線のインデックスリスト
        """

        candidates_indices = callout_tree.query(geom)

        intersect_indices_list = []

        for idx_candidate in candidates_indices:
            candidate_geom = callout_geoms[idx_candidate]

            if geom.intersects(candidate_geom):
                intersect_indices_list.append(idx_candidate)

        return intersect_indices_list


    def get_split_parts(self, geom, callout_geoms, intersect_callout_indices_list):
        """
        geomと交差するcallout_geomsと、交差部分のジオメトリを取得

        Parameters
        geom (shapely.geometry.linestring.LineString): 分割対象ジオメトリ
        callout_geoms (geopandas.array.GeometryArray): 旗揚げ線のジオメトリ配列
        intersect_callout_indices_list (list): geom と交差する旗揚げ線のインデックスリスト

        Returns
            intersecting_lines (shapely.geometry.multilinestring.MultiLineString): 交差旗揚げ線の MultiLineString
            intersection (shapely.geometry): geom と交差する部分のジオメトリ
        """

        intersecting_lines = MultiLineString([callout_geoms[i] for i in intersect_callout_indices_list])

        intersection = geom.intersection(intersecting_lines)

        return intersecting_lines, intersection


    def replace_geometry_at_row(self, row, geometries):
        """
        元のGeoDataFrameの行のジオメトリを更新

        Parameters
            row (geopandas.Series): 元の行
            geometries (shapely.geometry.collection.GeometryCollection): 更新後のジオメトリ
            分割後の複数ジオメトリ

        Returns
            new_rows (list[geopandas.GeoSeries]): 更新後のSeriesを格納したリス
        """

        new_rows = []

        for geom in geometries.geoms:
            new_row = row.copy()
            new_row.geometry = geom
            new_rows.append(new_row)

        return new_rows


    def split_pipe_by_callout(self, target_geodataframe, callout_geoms, callout_tree):
        """
        管路ジオメトリを旗揚げ線で分割し、分割後のジオメトリ群と管路IDに対する旗揚げ線インデックスの辞書を返す。

        Parameters
            target_geodataframe (geopandas.GeoDataFrame): 分割対象の管路GeoDataFrame
            callout_geoms (geopandas.array.GeometryArray): 旗揚げ線のジオメトリ配列
            callout_tree (STRtreeshapely.strtree.STRtree): 旗揚げ線の空間インデックス

        Returns
            split_target_geodataframe (geopandas.GeoDataFrame): 分割後の管路GeoDataFrame
            intersection_dict (dict): 管路IDごとに交差する旗揚げ線インデックスの辞書
        """

        intersection_dict = {}  # 管路と交差する旗揚げ線の辞書
        split_records = []  # 分割後の管路情報を格納

        for idx, row in target_geodataframe.iterrows():

            geom = row.geometry

            intersect_callout_indices_list = self.find_intersecting_callouts(geom, callout_geoms, callout_tree)

            # 分割対象がなければそのまま追加
            if not intersect_callout_indices_list:

                split_records.append(row.copy())

                continue

            intersecting_callouts, intersection = self.get_split_parts(geom, callout_geoms, intersect_callout_indices_list)

            if not intersection.is_empty:

                splitted = split(geom, intersecting_callouts)

                # 分割された場合のみコピー・記録
                new_rows = self.replace_geometry_at_row(row, splitted)

                split_records.extend(new_rows)

                intersection_dict[idx] = intersect_callout_indices_list

            else:
                pass

        # 分割された管路のGeoDataFrameを作成
        split_target_geodataframe = gpd.GeoDataFrame(split_records, geometry='geometry', crs=target_geodataframe.crs)

        return split_target_geodataframe, intersection_dict


    def create_earth_covering_text_dataframe(self, target_geodataframe, coordinates_array):
        """
        管路の分割後座標配列と元GeoDataFrameから、「土被り.txt」情報を入れるためのデータフレームを生成

        Parameters
            target_geodataframe (geopandas.GeoDataFrame): 分割後の管路GeoDataFrame
            coordinates_array (np.ndarray): 分割後の管路座標配列[地物ID, x, y]

        Returns
            earth_covering_text_datafram (geopandas.GeoDataFrame): 初期化済みDataFrame ※"XY地物ID","地物ID"のみ値格納済み
            coordinates_array_with_id (np.ndarray): 座標配列に元地物IDを結合した配列

        """

        column_name_list = ["XY地物ID", "地物ID", "始点距離", "始点位置からの距離", "土被り"]

        earth_covering_text_dataframe = pd.DataFrame(columns = column_name_list)

        # XY地物ID：分割前地物id, 地物id：分割後地物id
        xy_id_array = target_geodataframe['id'].values[coordinates_array[:, 0].astype(int)]

        earth_covering_text_dataframe["XY地物ID"] = xy_id_array
        earth_covering_text_dataframe["地物ID"] = coordinates_array[:, 0]

        # 地物IDを追加
        coordinates_array_with_id = np.hstack([coordinates_array, xy_id_array.reshape(-1, 1)])

        return earth_covering_text_dataframe, coordinates_array_with_id


    def calculate_cumulative_distances(self, coordinates_array_with_id):
        """
        各地物（管路）ごとに構成点間の距離を計算し、累積距離をリストとして返す。

        Parameters
            coordinates_array_with_id (np.ndarray): (N, 4) 形式の配列[地物ID, x座標, y座標, 元の管路ID]

        Returns
            distance_list (list[float]): 各点に対応する累積距離のリスト
        """

        distance_list = [] # 各点の累積距離を格納
        previous_point = None  # 前の地物IDを記録
        start_point_distance = 0.0 # 始点からの初期距離

        coordinates_unique_id_array = np.unique(coordinates_array_with_id[:, 0])

        # 地物IDによるループ
        for i in coordinates_unique_id_array:
            target_array = coordinates_array_with_id[coordinates_array_with_id[:, 0] == i]

            # 現在の処理対象XY地物ID取得
            current_point = target_array[0, 3]

            # 各構成点間の距離を計算
            for j in range(len(target_array) - 1):

                # XY地物IDが前回と異なる場合 → 新しい地物に切り替わったタイミング
                if current_point != previous_point:

                    # 前回の地物IDが None（最初のループ時）
                    if previous_point is None:

                        distance_list.append(start_point_distance)
                        previous_distance = start_point_distance

                    # 2回目以降で新しい地物に切り替わった場合
                    else:

                        distance_list.append(start_point_distance)
                        previous_distance = start_point_distance

                # XY地物IDが前回と同じ、かつ始点 → 線分の分割部分
                if j==0 and current_point == previous_point:
                    distance_list.append(start_point_distance)
                    previous_distance = start_point_distance

                # 現在の点と次の点の距離を計算（点間距離）
                distance = DCP.get_distance_points(target_array[j, 1:3], target_array[j+1, 1:3])

                # 新しい点の距離を累積して追加
                # 累積距離を更新（次の点に向けて）
                # 前回の地物IDとして現在のIDを記録（次のループに引き継ぐ）
                distance_list.append(previous_distance + distance)
                previous_distance = previous_distance + distance
                previous_point = current_point

        return distance_list


    def assign_start_distances(self, dataframe):
        """
        各 XY地物ID ごとに、分割後の地物ID単位で "始点距離" を計算し、DataFrame 内に直接書き込む

        Parameters
            dataframe（pd.DataFrame） : 対象データフレーム
            以下の列を持つことを前提とする：
            - "XY地物ID"
            - "地物ID"
            - "始点位置からの距離"
        """
        # 初期化
        dataframe["始点距離"] = 0.0

        # XY地物ID ごとに処理（入力順保持）
        for xy_id, group in dataframe.groupby("XY地物ID", sort=False):

            cumulative_distance = 0.0

            for geom_id in group["地物ID"].unique():

                mask = (dataframe["XY地物ID"] == xy_id) & (dataframe["地物ID"] == geom_id)

                # 地物IDごとの"始点位置からの距離"の最長距離取得
                sub_max = dataframe.loc[mask, "始点位置からの距離"].fillna(0.0).max()

                # 始点距離を一括で代入
                dataframe.loc[mask, "始点距離"] = cumulative_distance

                # 累積距離を更新
                cumulative_distance += float(sub_max)


    def create_attribute_dataframe(self, geodataframe):
        """
        指定されたGeoDataFrameからgeometryを除いた属性情報の DataFrame を生成し、
        地物ID（indexベース）を振り直して付与。

        Parameters
            geodataframe (geopandas.GeoDataFrame): 属性抽出対象のGeoDataFrame

        Returns
            attributes_text_dataframe (pd.DataFrame): geometry列を除き、地物ID列を追加した属性情報のDataFrame
        """

        attributes_text_dataframe = geodataframe.drop(columns="geometry").copy()

        attributes_text_dataframe = attributes_text_dataframe.reset_index(drop=True)

        attributes_text_dataframe["id"] = attributes_text_dataframe.index

        attributes_text_dataframe = attributes_text_dataframe.rename(columns={"id": "地物ID"})

        return attributes_text_dataframe


    def assign_single_callout_depth(self, earth_covering_text_dataframe, target_id, callout_id_list, callout_geodataframe):
        """
        管路と交差する旗揚げ線が1本だけの場合、
        該当する XY地物ID を持つすべての行にその旗揚げ線の「土被り」値を一括設定

        Parameters
            earth_covering_text_dataframe (pd.DataFrame): 土被り情報を持つデータフレーム
            target_id (int): XY地物ID
            callout_id_list (list[int]): 対象と交差する旗揚げ線のインデックス（1つだけ入っている前提）
            callout_geodataframe (gpd.GeoDataFrame): "geometry" 列と "土被り" 列を持つ旗揚げ線のGeoDataFrame
        """

        depth_value = callout_geodataframe.loc[callout_id_list[0], "土被り"]

        earth_covering_text_dataframe.loc[earth_covering_text_dataframe["XY地物ID"] == target_id, "土被り"] = depth_value


    def get_min_distances_to_callout(self, callout_geom, start_point, end_point):
        """
        指定された旗揚げ線の座標点の中で、始点および終点との最短距離を計算する。

        Parameters
            callout_geom (shapely.geometry.LineString): 距離を計算する対象の旗揚げ線
            start_point (np.array): 始点の座標 [x, y]
            end_point (np.array): 終点の座標 [x, y]

        Returns
            min_start_dist (float): 始点との最小距離
            min_end_dist (float): 終点との最小距離
        """

        callout_coords_list = list(callout_geom.coords)

        # 距離の初期値
        min_start_dist = float("inf")
        min_end_dist = float("inf")

        # 旗揚げ線の各座標点に対してループ
        for pt in callout_coords_list:
            pt_array = np.array(pt)

            # 始点終点との距離を計算
            dist_to_start = DCP.get_distance_points(start_point, pt_array)
            dist_to_end = DCP.get_distance_points(end_point, pt_array)

            # 現在の点がこれまでの最小距離より近ければ更新
            if dist_to_start < min_start_dist:
                min_start_dist = dist_to_start

            if dist_to_end < min_end_dist:
                min_end_dist = dist_to_end

        return min_start_dist, min_end_dist


    def build_callout_distance_array(self, callout_id_list, callout_geodataframe, start_point, end_point):
        """
        各旗揚げ線に対して、管路の始点・終点との最小距離を計算し、距離情報の配列を作成

        Parameters
            callout_id_list (list[int]): 対象となる旗揚げ線のインデックスリスト
            callout_geodataframe (gpd.GeoDataFrame): 旗揚げ線のGeoDataFrame
            start_point (np.ndarray): 管路の始点座標 [x, y]
            end_point (np.ndarray): 管路の終点座標 [x, y]

        Returns
            np.ndarray: 形状 (M, 3) の配列 [callout_id, 最小始点距離, 最小終点距離]
        """
        distance_array = []

        for cid in callout_id_list:

            callout_geom = callout_geodataframe.loc[cid, "geometry"]

            min_start_dist, min_end_dist = self.get_min_distances_to_callout(callout_geom, start_point, end_point)

            # callout_id, 始点距離, 終点距離 を 1行の 2次元配列に
            distance_array.append([cid, min_start_dist, min_end_dist])

        return np.array(distance_array)


    def set_earth_covering_value(self, dataframe, callout_geodataframe, target_id, callout_id, geom_id):
        """
        指定された XY地物ID と 地物ID に一致する行に、
        指定された旗揚げ線の「土被り」値を設定

        Args:
            dataframe (pd.DataFrame): 土被り情報を更新する対象のDataFrame
            callout_geodataframe (gpd.GeoDataFrame): 土被り値を含む旗揚げ線のGeoDataFrame
            target_id (int): XY地物ID
            callout_id (int): 土被りを取得する旗揚げ線のインデックス
            geom_id (int): 地物ID
        """

        depth_value = callout_geodataframe.loc[callout_id, "土被り"]

        dataframe.loc[
            (dataframe["XY地物ID"] == target_id) & (dataframe["地物ID"] == geom_id),
            "土被り"
        ] = depth_value


    def assign_multiple_callouts_depth(self, earth_covering_text_dataframe, target_id, callout_id_list, callout_geodataframe, coordinates_array_with_id):
        """
        管路と複数の旗揚げ線が交差する場合、構成点ごとに対応する旗揚げ線の土被り値を設定。

        ・始点に一致する旗揚げ線が存在する場合は、その順序に従って構成点に対応。
        ・始点と一致しない場合、先頭2点には最も近い旗揚げ線の値を設定し、それ以降を1対1で対応。

        Parameters
            earth_covering_text_dataframe (pd.DataFrame): 土被り情報を持つデータフレーム
            target_id (int): XY地物ID
            callout_id_list (list[int]): 管路と交差する旗揚げ線のインデックスのリスト
            callout_geodataframe (gpd.GeoDataFrame): 旗揚げ線のGeoDataFrame（geometry, 土被り）
            coordinates_array_with_id (np.ndarray): 各構成点の配列。形状 (N, 4) [地物ID, x, y, XY地物ID]
        """
        # 対象 target の構成点（全点）取得
        target_coords = coordinates_array_with_id[coordinates_array_with_id[:, 3] == target_id][:, 1:3]

        # 始点・終点取得
        start_point = target_coords[0]
        end_point = target_coords[-1]

        # 始点距離に０があるかフラグ（初期化）
        has_zero_start_distance = False

        callout_distance_array = self.build_callout_distance_array(callout_id_list, callout_geodataframe, start_point, end_point)

        # 始点に一致する旗揚げがあるか確認
        has_zero_start_distance = np.any(callout_distance_array[:, 1] == 0.0)

        # 始点距離（2列目）で昇順にソート
        callout_distance_array = callout_distance_array[callout_distance_array[:, 1].argsort()]

        # earth_covering_text_dataframe からこの target_id に該当する行を抽出
        target_dataframe = earth_covering_text_dataframe[earth_covering_text_dataframe["XY地物ID"] == target_id]

        # 地物IDごとにループ（順番保持のため unique() を使う）
        unique_geom_ids = target_dataframe["地物ID"].unique()

        # 始点と旗揚げの交差がある場合
        if has_zero_start_distance:

            for i, geom_id in enumerate(unique_geom_ids):

                callout_id = int(callout_distance_array[i, 0])  # callout_distance_array から対応する callout_id を取得
                self.set_earth_covering_value(earth_covering_text_dataframe, callout_geodataframe, target_id, callout_id, geom_id)

        # 始点と旗揚げの交差がない場合
        else:

            # 最初の callout（始点に一番近い）を取得
            first_callout_id = int(callout_distance_array[0, 0])

            # 最初の2つの地物に同じ土被りを設定
            for geom_id in unique_geom_ids[:2]:

                self.set_earth_covering_value(earth_covering_text_dataframe, callout_geodataframe, target_id, first_callout_id, geom_id)

            if len(unique_geom_ids) >= 2:
                # それ以降は順番に 1対1 対応（インデックス調整注意）
                for i, geom_id in enumerate(unique_geom_ids[2:], start=1):

                    callout_id = int(callout_distance_array[i, 0])

                    self.set_earth_covering_value(earth_covering_text_dataframe, callout_geodataframe, target_id, callout_id, geom_id)


    def assign_earth_covering_to_dataframe(self, earth_covering_text_dataframe, coordinates_array_with_id, intersection_dict, callout_geodataframe):
        """
        管路が旗揚げ線（callout）と交差する場合、その交差情報に基づき、
        各構成点に適切な土被りを割り当てる。

        Parameters
            earth_covering_text_dataframe (pd.DataFrame): 各構成点ごとの情報を保持するDataFrame
            coordinates_array_with_id (np.ndarray): (N, 4) 配列[地物ID, x, y, XY地物ID]
            intersection_dict (dict[int, list[int]]): 管路ID（target_id）と、それに交差する旗揚げ線のインデックス（callout_id）のリストの辞書
            callout_geodataframe (gpd.GeoDataFrame): "geometry" 列に旗揚げ線、"土被り" 列に深さ情報を持つGeoDataFrame

        Returns
            earth_covering_text_dataframe (pd.DataFrame): 土被り情報が割り当てられたDataFrame
        """

        # 管路ごとに旗揚げIDリストと対応付けて土被りを設定
        for target_id, callout_id_list in intersection_dict.items():

            # 管路と交差する旗揚げ線が1本の場合: その土被り値を全ての点に適用
            if len(callout_id_list) ==1:

                self.assign_single_callout_depth(earth_covering_text_dataframe, target_id, callout_id_list, callout_geodataframe)

            # 管路と交差する旗揚げ線が2本以上の場合
            else:
                self.assign_multiple_callouts_depth(earth_covering_text_dataframe, target_id, callout_id_list, callout_geodataframe, coordinates_array_with_id)

        return earth_covering_text_dataframe


    def dataframe_to_string(self, dataframe):
        """
        DataFrame を CSV 形式の文字列に変換

        Parameters
            dataframe (pd.DataFrame): CSV 文字列変換対象

        Returns
            csv_string  (str): DataFrame を CSV 形式に変換した文字列
        """

        # バッファを作成
        csv_buffer = io.StringIO()

        # CSV 形式の文字列に変換してバッファに書き込む
        dataframe.to_csv(csv_buffer, index=False, encoding='utf-8')

        # バッファの内容を文字列として取得
        csv_string  = csv_buffer.getvalue()

        return csv_string


    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # --------------------------------------------------------------------------
            target_pipe_dwh_name, \
                callout_dwh_name, \
                pipe_geometry_dwh, \
                earth_covering_dwh, \
                attributes_dwh\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # --------------------------------------------------------------------------
            # FieldSetFileをDataFrameとして取得
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(NSP.convert_flowfile_to_field_set_file_dataframe)(flowfile)

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameから、プロパティで指定したDWH名を持つ行のValue列を取得
            # --------------------------------------------------------------------------
            target_geodataframe, \
                callout_geodataframe\
                = WM.calc_func_time(self.logger)(self.get_geodataframe_from_field_set_file_dataframe)(
                     target_field_set_file_dataframe,
                     target_pipe_dwh_name,
                     callout_dwh_name
                     )

            target_geodataframe["id"] = range(len(target_geodataframe))
            callout_geodataframe["id"] = range(len(callout_geodataframe))

            # --------------------------------------------------------------------------
            # 指定された旗揚げ線を前後に延長
            # --------------------------------------------------------------------------
            callout_geodataframe['geometry'] = callout_geodataframe['geometry'].apply(self.extend_line_edge_points)

            # --------------------------------------------------------------------------
            # 旗揚げをもとに管路を分割
            # --------------------------------------------------------------------------
            callout_geoms, callout_tree = WM.calc_func_time(self.logger)(self.build_callout_tree)(callout_geodataframe)

            split_target_geodataframe, \
                intersection_dict\
                = WM.calc_func_time(self.logger)(self.split_pipe_by_callout)(target_geodataframe,
                                                                             callout_geoms,
                                                                             callout_tree)

            # --------------------------------------------------------------------------
            # 座標配列作成
            # --------------------------------------------------------------------------
            geometry_list = list(split_target_geodataframe["geometry"])

            coordinates_array = (NSP.get_geometries_points_numpy)(geometry_list)

            # --------------------------------------------------------------------------
            # 土被りtxt用DataFrameの作成
            # --------------------------------------------------------------------------
            earth_covering_text_dataframe, \
                coordinates_array_with_id\
                = WM.calc_func_time(self.logger)(self.create_earth_covering_text_dataframe)(split_target_geodataframe,
                                                                                            coordinates_array)

            # --------------------------------------------------------------------------
            # 始点からの距離・始点距離の算出
            # --------------------------------------------------------------------------
            distance_list\
                = WM.calc_func_time(self.logger)(self.calculate_cumulative_distances)(coordinates_array_with_id)

            # 計算した距離をDataFrameに反映
            earth_covering_text_dataframe["始点位置からの距離"] = distance_list

            # 始点距離の算出
            WM.calc_func_time(self.logger)(self.assign_start_distances)(earth_covering_text_dataframe)

            # --------------------------------------------------------------------------
            # 土被りtxt作成
            # --------------------------------------------------------------------------
            earth_covering_text_dataframe\
                = WM.calc_func_time(self.logger)(self.assign_earth_covering_to_dataframe)(earth_covering_text_dataframe,
                                                                                          coordinates_array_with_id,
                                                                                          intersection_dict,
                                                                                          callout_geodataframe)

            earth_covering_text\
                = WM.calc_func_time(self.logger)(self.dataframe_to_string)(earth_covering_text_dataframe)

            # --------------------------------------------------------------------------
            # 属性txt作成
            # --------------------------------------------------------------------------
            attributes_text_dataframe\
                = WM.calc_func_time(self.logger)(self.create_attribute_dataframe)(split_target_geodataframe)

            attributes_text\
                = WM.calc_func_time(self.logger)(self.dataframe_to_string)(attributes_text_dataframe)

            # --------------------------------------------------------------------------
            # FieldSetFileの作成(geometry)
            # --------------------------------------------------------------------------
            dwh_list = [pipe_geometry_dwh, earth_covering_dwh, attributes_dwh]
            type_list = ["geometry", "object", "object"]
            value_list = [geometry_list, earth_covering_text, attributes_text]

            output_field_set_file = PBP.set_field_set_file(dwh_list,
                                                           type_list,
                                                           value_list)


            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
