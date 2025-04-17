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
from io import StringIO
import uuid
from collections import Counter
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
gpd = import_module("geopandas")
LineString = getattr(import_module("shapely.geometry"), "LineString")
Point = getattr(import_module("shapely.geometry"), "Point")
unary_union = getattr(import_module("shapely.ops"), "unary_union")
csr_matrix = getattr(import_module("scipy.sparse"), "csr_matrix")
connected_components = getattr(
    import_module("scipy.sparse.csgraph"), "connected_components"
)
KDTree = getattr(import_module("scipy.spatial"), "KDTree")

import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import cad.common.cad_utils as CU


class AdjustEndpointsLogic:

    def __init__(self):
        pass

    def extend_endpoint_to_intersect(
        self, geometries, intersect_distance, target_layer
    ):
        """
        指定された距離内に他のLineStringが存在する場合、端点を延長して交差させる。
        延長された端点は交差点に移動され、元の端点は保持される。
        両端が延長される場合は両方に延長線が引かれる。また、複数のLineStringと交差する場合は、
        最も近い交差点にのみ延長される。

        :param geometries: LineStringのジオメトリを含むGeoDataFrame。
        :type geometries: geopandas.GeoDataFrame
        :param intersect_distance: 他のLineStringまでの最大延長距離。この距離内にあるLineStringに対して延長を行う。
        :type intersect_distance: float
        :param target_layer: 処理対象のレイヤー名。このレイヤーに属するジオメトリのみが処理される。
        :type target_layer: str

        :return: 延長された端点が反映された後の更新済みGeoDataFrame。
        :rtype: geopandas.GeoDataFrame

        :raises ValueError:
            指定されたレイヤーが存在しない場合に発生する。
        :raises Exception:
            その他の処理エラーが発生した場合に発生する。
        """
        try:
            # 処理対象のレイヤーをフィルタリング
            target_geometries = geometries[geometries["layer"] == target_layer]
            if target_geometries.empty:
                raise ValueError(f"ERROR: {target_layer} doesn't exist.")

            # 他のリンクの端点を集めてリストを作成
            all_endpoints = []
            for row in target_geometries.itertuples():
                geom = row.geometry
                start_point, end_point = self.get_endpoints(geom)
                all_endpoints.append((start_point.x, start_point.y))
                all_endpoints.append((end_point.x, end_point.y))

            # 座標の出現回数をカウント
            endpoint_counts = Counter(all_endpoints)

            # 重複しない端点を抽出（出現回数が1回の端点のみを選択）
            unique_endpoints = [
                point for point, count in endpoint_counts.items() if count == 1
            ]

            # これで重複しない端点だけを対象に処理を行う
            extended_lines = []

            # 各LineStringの端点を取得し、延長線を作成
            for row in target_geometries.itertuples(index=True):
                geom = row.geometry
                fid = row.FID

                # `get_endpoints`関数を使用して端点を取得
                start_point, end_point = self.get_endpoints(geom)

                # 他のリンクの端点と接続していない場合にのみ延長
                for is_start, endpoint in [(True, start_point), (False, end_point)]:
                    # 重複しない端点のみ延長処理を行う
                    if (endpoint.x, endpoint.y) not in unique_endpoints:
                        continue  # 重複する端点なので延長しない

                    # 延長処理
                    if is_start:
                        # 始点を延長する場合、2つ目の座標の逆方向に延長
                        next_coord = geom.coords[1]
                    else:
                        # 終点を延長する場合、最後から2つ目の座標の方向に延長
                        next_coord = geom.coords[-2]

                    direction_vector = Point(
                        endpoint.x - next_coord[0], endpoint.y - next_coord[1]
                    )

                    # ベクトルの長さを計算
                    vector_length = (
                        direction_vector.x**2 + direction_vector.y**2
                    ) ** 0.5
                    if vector_length == 0:
                        continue  # ベクトルの長さが0ならスキップ

                    # ベクトルを正規化して延長点を計算
                    normalized_vector = Point(
                        direction_vector.x / vector_length,
                        direction_vector.y / vector_length,
                    )
                    extended_endpoint = Point(
                        endpoint.x + normalized_vector.x * intersect_distance,
                        endpoint.y + normalized_vector.y * intersect_distance,
                    )

                    # 延長線を作成し保存
                    extended_line = LineString([endpoint, extended_endpoint])
                    extended_lines.append(
                        {"FID": fid, "is_start": is_start, "geometry": extended_line}
                    )

            # 延長後に交差するジオメトリを取得（組み合わせB）
            extended_lines_gdf = gpd.GeoDataFrame(
                extended_lines, geometry="geometry", crs=geometries.crs
            )
            post_intersections = gpd.sjoin(
                extended_lines_gdf,
                target_geometries,
                how="inner",
                predicate="intersects",
            )
            post_intersections = post_intersections[
                post_intersections["FID_left"] != post_intersections["FID_right"]
            ]
            post_intersection_pairs = set(
                zip(post_intersections["FID_left"], post_intersections["FID_right"])
            )

            # 新たに交差する組み合わせを取得
            pre_intersections = gpd.sjoin(
                target_geometries,
                target_geometries,
                how="inner",
                predicate="intersects",
            )
            pre_intersections = pre_intersections[
                pre_intersections["FID_left"] != pre_intersections["FID_right"]
            ]
            pre_intersection_pairs = set(
                zip(pre_intersections["FID_left"], pre_intersections["FID_right"])
            )

            new_intersections = post_intersection_pairs - pre_intersection_pairs

            # 交差するジオメトリがない場合、元のデータを返す
            if not new_intersections:
                return geometries

            # 更新されたLineStringを保存する
            updated_geometries = geometries.copy()

            # new_intersectionsを総なめして、ext_line_indexに対するtarget_indexの辞書を作成
            intersections_dict = {}
            for ext_line_index, target_index in new_intersections:
                if ext_line_index not in intersections_dict:
                    intersections_dict[ext_line_index] = []
                intersections_dict[ext_line_index].append(target_index)

            # FIDをキーにして新しい座標を保持する辞書
            coords_dict = {}

            # 辞書に変換してループ内でのフィルタリングを削減
            extended_lines_dict = extended_lines_gdf.groupby("FID")
            target_geometries_dict = target_geometries.set_index("FID")[
                "geometry"
            ].to_dict()

            # 交差点に端点を移動させてGeometryを更新
            for ext_line_index, target_indices in intersections_dict.items():
                extended_lines = extended_lines_dict.get_group(ext_line_index)

                for row in extended_lines.itertuples():
                    intersection_distances = []

                    for target_index in target_indices:
                        target_geom = target_geometries_dict[target_index]
                        intersection_result = row.geometry.intersection(target_geom)

                        if isinstance(intersection_result, Point):
                            distance = Point(row.geometry.coords[0]).distance(
                                intersection_result
                            )
                            intersection_distances.append(
                                (distance, intersection_result)
                            )

                    if not intersection_distances:
                        continue

                    # 最も距離が近い交点を取得
                    closest_intersection = min(
                        intersection_distances, key=lambda x: x[0]
                    )
                    closest_point = closest_intersection[1]

                    # 元の座標をtarget_geometriesから取得
                    if row.FID not in coords_dict:
                        original_geometry = target_geometries_dict[row.FID]
                        coords_dict[row.FID] = list(original_geometry.coords)

                    # 交点を辞書に保存
                    if row.is_start:
                        coords_dict[row.FID].insert(
                            0, (closest_point.x, closest_point.y)
                        )
                    else:
                        coords_dict[row.FID].append((closest_point.x, closest_point.y))

            # 最後にupdated_geometriesを一度に更新
            for fid, new_coords in coords_dict.items():
                updated_geometries.loc[
                    (geometries["layer"] == target_layer)
                    & (updated_geometries["FID"] == fid),
                    "geometry",
                ] = LineString(new_coords)

            return updated_geometries
        except Exception as e:
            raise Exception(f"[ERROR extend_endpoint_to_intersect]: {str(e)}")

    def move_endpoint_to_intersection(self, geometries, ratio_threshold, target_layer):
        """
        LineStringの端点を指定された交差点に移動させる処理を行う。
        LineString全体の長さに対する端点から交点までの距離の割合が指定された閾値以下の場合、
        その端点を交差点に移動させる。

        :param geometries: LineStringのジオメトリが含まれるGeoDataFrame。
        :type geometries: geopandas.GeoDataFrame
        :param ratio_threshold: LineString全体の長さに対する端点から交点までの距離の割合の閾値。この値以下の端点が交差点に移動される。
        :type ratio_threshold: float
        :param target_layer: 処理対象のレイヤー名。このレイヤーに属するジオメトリが処理される。
        :type target_layer: str

        :return: 端点が移動された後の更新済みGeoDataFrame。
        :rtype: geopandas.GeoDataFrame

        :raises ValueError:
            指定されたレイヤーが存在しない場合に発生する。
        :raises Exception:
            その他の処理エラーが発生した場合に発生する。
        """

        try:
            if ratio_threshold >= 0.5:
                raise ValueError(
                    "ERROR: Close Thresholdは0.5未満の数値を入力してください。"
                )

            # 元のジオデータフレームを保持する
            original_geometries = geometries.copy()

            # 処理対象のレイヤーをフィルタリング
            geometries = geometries[geometries["layer"] == target_layer]

            if geometries.empty:
                raise ValueError(f"ERROR: {target_layer} doesn't exist.")

            # 空間インデックスを作成して、自己結合で交差するLineStringを見つける
            intersections = gpd.sjoin(
                geometries, geometries, how="inner", predicate="intersects"
            )
            intersections = intersections[
                intersections.index != intersections.index_right
            ]
            intersections["index_left"] = intersections.index
            moved_endpoints = []

            # 交差する組み合わせに対して処理を行う
            for row in intersections.itertuples():
                geom1 = geometries.loc[row.index_left, "geometry"]
                geom2 = geometries.loc[row.index_right, "geometry"]
                intersection = geom1.intersection(geom2)

                if not intersection.is_empty and isinstance(intersection, Point):
                    endpoints1 = [Point(geom1.coords[0]), Point(geom1.coords[-1])]
                    endpoints2 = [Point(geom2.coords[0]), Point(geom2.coords[-1])]

                    length1 = geom1.length
                    length2 = geom2.length

                    for endpoint in endpoints1:
                        distance_to_intersection = endpoint.distance(intersection)
                        distance_ratio = distance_to_intersection / length1
                        if distance_ratio <= ratio_threshold:
                            moved_endpoints.append(
                                (row.index_left, endpoint, intersection)
                            )

                    for endpoint in endpoints2:
                        distance_to_intersection = endpoint.distance(intersection)
                        distance_ratio = distance_to_intersection / length2
                        if distance_ratio <= ratio_threshold:
                            moved_endpoints.append(
                                (row.index_right, endpoint, intersection)
                            )

            # 端点の移動処理
            for idx, original_point, intersection_point in moved_endpoints:
                geom = geometries.loc[idx, "geometry"]
                new_coords = list(geom.coords)

                if original_point.equals(Point(geom.coords[0])):
                    new_coords[0] = (intersection_point.x, intersection_point.y)
                elif original_point.equals(Point(geom.coords[-1])):
                    new_coords[-1] = (intersection_point.x, intersection_point.y)

                geometries.at[idx, "geometry"] = LineString(new_coords)

            # 元のジオデータフレームに移動したジオメトリを統合
            original_geometries.update(geometries)

            return original_geometries

        except Exception as e:
            raise Exception(f"[ERROR move_endpoint_to_intersection]: {str(e)}")

    def connect_nearby_endpoints(self, geometries, distance_threshold, target_layer):
        """
        端点の周囲に他のLineStringの端点が存在する場合、その端点同士を新たなLineStringで接続する処理を行う。

        :param geometries: 処理対象となるLineStringを含むGeoDataFrame。
        :type geometries: geopandas.GeoDataFrame
        :param distance_threshold: 端点同士を結ぶための最大距離。これよりも近い端点が接続される。
        :type distance_threshold: float
        :param target_layer: 処理対象のレイヤー名。このレイヤーに属する端点のみが処理される。
        :type target_layer: str

        :return: 新しいLineStringが追加された後の更新されたGeoDataFrame。
        :rtype: geopandas.GeoDataFrame

        :raises ValueError:
            指定されたレイヤーが存在しない場合に発生する。
        :raises Exception:
            その他の処理エラーが発生した場合に発生する。
        """

        try:
            new_lines = []
            new_layer_ids = []
            new_fids = []

            # 処理対象のレイヤーをフィルタリング
            filtered_geometries = geometries[geometries["layer"] == target_layer]

            if filtered_geometries.empty:
                raise ValueError(f"ERROR: {target_layer} doesn't exist.")

            # 端点のグループ化を実行
            grouped_list = self.cluster_endpoints_by_distance(
                filtered_geometries, distance_threshold
            )

            # 端点リストをKDTreeに渡すために座標配列を作成
            points = []
            point_data = []
            for group in grouped_list:
                for item in group:
                    point = item[2]
                    points.append((point.x, point.y))
                    point_data.append(item)  # 端点情報を保持

            # KDTreeの作成
            kdtree = KDTree(points)

            created_pairs = set()
            for _, row in filtered_geometries.iterrows():
                if row["geometry"].geom_type == "LineString":
                    coords = list(row["geometry"].coords)
                    for i in range(len(coords) - 1):
                        pair = tuple(
                            sorted(
                                [
                                    (coords[i][0], coords[i][1]),
                                    (coords[i + 1][0], coords[i + 1][1]),
                                ]
                            )
                        )
                        created_pairs.add(pair)

            # 各端点ごとに近接点を全て取得して接続
            for i, point_data_item in enumerate(point_data):
                current_point = point_data_item[2]
                distances, indices = kdtree.query(
                    (current_point.x, current_point.y), k=len(point_data)
                )

                # 自分自身を除外した他の端点との接続を確認
                for j, dist in enumerate(distances[1:]):
                    if dist > distance_threshold:
                        break

                    # 対象点を取得
                    nearest_point = point_data[indices[j + 1]][2]

                    # 既に同じペアが作成されていないか確認
                    pair = tuple(
                        sorted(
                            [
                                (current_point.x, current_point.y),
                                (nearest_point.x, nearest_point.y),
                            ]
                        )
                    )

                    # 既に作成済みのペアはスキップ
                    if pair in created_pairs:
                        continue

                    # 新しいLineStringを作成
                    new_line = LineString([current_point, nearest_point])
                    new_lines.append(new_line)
                    new_layer_ids.append(target_layer)  # 元のレイヤーID
                    new_fids.append(uuid.uuid4())  # 新しいFIDを追加

                    # 作成したペアを記録
                    created_pairs.add(pair)

            # 新しいLineStringをGeoDataFrameに追加
            if new_lines:
                new_lines_gdf = gpd.GeoDataFrame(geometry=new_lines, crs=geometries.crs)
                new_lines_gdf["layer"] = new_layer_ids
                new_lines_gdf["FID"] = new_fids
                geometries = pd.concat([geometries, new_lines_gdf], ignore_index=True)

            return gpd.GeoDataFrame(geometries)

        except Exception as e:
            raise Exception(f"[ERROR connect_nearby_endpoints]: {str(e)}")

    def cluster_endpoints_by_distance(self, geometries, buffer_radius):
        """
        指定されたバッファ半径内にある端点同士をクラスタリングし、近接する端点のリストを返す関数。

        :param geometries: 処理対象となるGeoDataFrame。各行にはLineStringが含まれている必要がある。
        :type geometries: geopandas.GeoDataFrame
        :param buffer_radius: 端点同士の距離を判定するためのバッファ半径。この半径内にある端点がグループ化される。
        :type buffer_radius: float

        :return: グループ化された端点のリスト。各グループはタプルのリストで、タプルは以下の要素を持つ。\n
            - fid : 端点のリンクID\n
            - is_start : 端点が始点か終点かを示すフラグ（Trueが始点、Falseが終点）\n
            - geometry : 端点の座標を表すshapely.geometry.Pointオブジェクト
        :rtype: list[list[tuple(str, bool, shapely.geometry.Point)]]

        :raises Exception:
            端点のクラスタリング処理中にエラーが発生した場合。
        """
        try:
            # 各LineStringの端点を取得し、始点と終点をリストに格納
            endpoint_data = []
            for index, line in geometries.iterrows():
                # LineStringの端点を取得（始点と終点）
                endpoints = self.get_endpoints(line["geometry"])
                # 始点と終点の情報を保存（リンクID、始点/終点、座標）
                endpoint_data.append([line["FID"], True, endpoints[0]])
                endpoint_data.append([line["FID"], False, endpoints[1]])

            # 端点の情報をGeoDataFrameに変換
            endpoint_gdf = gpd.GeoDataFrame(
                endpoint_data,
                columns=["fid", "is_start", "geometry"],
                geometry="geometry",
            )

            # 端点の座標情報をnumpy配列に変換（KDTreeで使用するため）
            coords = np.array([(point.x, point.y) for point in endpoint_gdf.geometry])

            # KDTreeを使って、各端点の近接する端点（buffer_radius内）のインデックスを取得
            tree = KDTree(coords)

            pairs = tree.query_ball_tree(tree, buffer_radius)

            num_points = len(coords)
            
            # 配列の最大サイズを推定して初期化 (推定が難しい場合は、動的にリサイズする方法も検討できます)
            max_pair_count = sum(
                len(neighbors) - 1 for neighbors in pairs
            )  # 自分自身を除外するので-1
            row_indices = np.empty(max_pair_count, dtype=int)
            col_indices = np.empty(max_pair_count, dtype=int)

            # 接触する端点同士の関係を記録
            index = 0
            for i, neighbors in enumerate(pairs):
                neighbors = np.array(neighbors)
                valid_neighbors = neighbors[neighbors != i]  # 自分自身を除外
                num_neighbors = len(valid_neighbors)
                if num_neighbors > 0:
                    row_indices[index : index + num_neighbors] = i
                    col_indices[index : index + num_neighbors] = valid_neighbors
                    index += num_neighbors

            # 最後に無駄な領域を削除
            row_indices = row_indices[:index]
            col_indices = col_indices[:index]

            # 2次元のブール型のnumpy配列を作成
            intersections = np.zeros((num_points, num_points), dtype=bool)
            intersections[row_indices, col_indices] = True
            intersections[col_indices, row_indices] = True  # 対称的な関係を保持

            # find_connected_groupsを使って、連結された端点のグループを見つける
            grouped_nodes = self.find_connected_groups(intersections)

            # grouped_nodes内の遅延を解消するために、GeoDataFrameからnumpy配列に変換
            fid_array = endpoint_gdf["fid"].to_numpy()
            is_start_array = endpoint_gdf["is_start"].to_numpy()
            geometry_array = np.array(endpoint_gdf["geometry"].to_list())

            # 端点同士の接触関係をもとに、グループ化を行う
            grouped_list = []
            for group in grouped_nodes:
                node_group = []
                for node_index in group:
                    # 各端点の情報（リンクID、始点/終点、座標）を事前に用意した配列から取得
                    fid = fid_array[node_index]
                    is_start = is_start_array[node_index]
                    geometry = geometry_array[node_index]
                    node_group.append((fid, is_start, geometry))

                processed_group = self.select_closest_endpoint_with_kdtree(
                    node_group, tree
                )
                grouped_list.append(processed_group)

            return grouped_list
        except Exception as e:
            raise Exception(f"[ERROR cluster_endpoints_by_distance]: {str(e)}")

    def select_closest_endpoint_with_kdtree(self, group, tree):
        """
        同じリンクIDを持つ複数の端点の中から、他のリンクIDの端点に最も近いものを残す処理を行う関数。

        :param group: グループ化された端点情報のリスト。各タプルには以下の要素が含まれる\n
                    - fid: リンクID\n
                    - is_start: 端点が始点か終点かを示すフラグ\n
                    - geometry: 端点の座標を表すPointオブジェクト
        :type group: list[tuple(str, bool, shapely.geometry.Point)]
        :param tree: 他のリンクIDの端点の座標を保持するKDTreeオブジェクト。これを用いて、指定された端点と他の端点との距離を効率的に計算する。
        :type tree: scipy.spatial.KDTree

        :return: 他のリンクIDの端点に最も近い端点を残した後のグループ化された端点情報のリスト。
        :rtype: list[tuple(str, bool, shapely.geometry.Point)]

        :raises ValueError:
            グループ内のデータが不正な場合に発生。
        :raises Exception:
            その他の処理エラーが発生した場合に発生する。
        """
        try:
            group_by_fid_and_start = {}

            # リンクIDとis_startのペアごとに端点をグループ化
            for item in group:
                fid = item[0]
                is_start = item[1]
                group_by_fid_and_start.setdefault((fid, is_start), []).append(item)

            processed_group = []

            # 各リンクIDとis_startごとに端点を処理
            for (fid, is_start), items in group_by_fid_and_start.items():
                if len(items) == 1:
                    # 重複がない場合はそのまま追加
                    processed_group.append(items[0])
                    continue

                # 重複している場合、最も近い端点をKDTreeで探索して残す
                closest_item = None
                min_distance = float("inf")

                for item in items:
                    item_coord = np.array([item[2].x, item[2].y])  # 座標情報を抽出
                    # KDTreeを使って、他のリンクIDの端点との距離を計算
                    distances, indices = tree.query(
                        item_coord, k=2
                    )  # k=2は自身を含むため
                    distance = distances[1]  # 最も近い他のリンクIDの距離を取得

                    if distance < min_distance:
                        min_distance = distance
                        closest_item = item

                if closest_item:
                    processed_group.append(closest_item)

            return processed_group
        except Exception as e:
            raise Exception(f"[ERROR select_closest_endpoint_with_kdtree]: {str(e)}")

    def get_endpoints(self, line):
        """
        指定されたLineStringの始点と終点を取得する関数。

        :param line: 端点を取得する対象のLineStringオブジェクト。
        :type line: shapely.geometry.LineString

        :return: LineStringの始点と終点を表す2つのPointオブジェクトのリスト。リストの最初の要素が始点、最後の要素が終点を表す。
        :rtype: list[shapely.geometry.Point]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            return [Point(line.coords[0]), Point(line.coords[-1])]
        except Exception as e:
            raise Exception(f"[ERROR get_endpoints]: {str(e)}")

    def find_connected_groups(self, intersections):
        """
        ブール型の numpy 配列を使って、端点の接触関係を基にグループ化する。

        :param intersections: 接触関係を表すブール型の2次元配列。
        :type intersections: numpy.ndarray

        :return: 連結した端点のインデックスのグループリスト。
        :rtype: list[list[int]]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # numpy配列から疎行列を作成
            sparse_matrix = csr_matrix(intersections)

            # connected_componentsで連結成分を計算
            num_components, labels = connected_components(
                csgraph=sparse_matrix, directed=False, return_labels=True
            )

            # ラベルに基づいてグループを作成
            groups = [[] for _ in range(num_components)]
            for idx, label in enumerate(labels):
                groups[label].append(idx)

            return groups
        except Exception as e:
            raise Exception(f"[ERROR find_connected_groups]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        メインの処理の入り口部分。各プロパティに基づいて、要求1,2,3を判定し、適切な処理を実行する。

        :param byte_data: 入力データとしてバイト型のFieldSetFileを受け取る
        :type byte_data: bytes
        :param attribute: 属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: シリアライズされた簡略化後のGeoNdarray形式データと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティ値取得
            geometry_name = properties["GEOMETRY_NAME"]
            extend_to_intersect = properties["EXTEND_ENDPOINT_TO_INTERSECT"]
            move_to_intersect = properties["MOVE_ENDPOINT_TO_INTERSECT"]
            adjust_to_converge = properties["CONNECT_NEARDY_ENDPOINTS"]
            close_threshold = properties["CLOSE_THRESHOLD"]
            intersect_distance = properties["INTERSECT_DISTANCE"]
            distance_threshold = properties["DISTANCE_THRESHOLD"]
            crs = properties["CRS"]

            # FieldSetFileをデータフレームに変換
            gdf = CU.field_set_file_to_geodataframe(byte_data, str(crs))

            # 各フラグに基づいて異なる処理を実行
            if move_to_intersect == "true":
                gdf = self.move_endpoint_to_intersection(
                    gdf, float(close_threshold), geometry_name
                )

            if adjust_to_converge == "true":
                gdf = self.connect_nearby_endpoints(
                    gdf, float(distance_threshold), geometry_name
                )

            if extend_to_intersect == "true":
                gdf = self.extend_endpoint_to_intersect(
                    gdf, float(intersect_distance), geometry_name
                )

            # GeoDataFrame→FieldSetFileに変換
            data_definition = attribute.get("DataDefinition", "")
            data_definition_stream = StringIO(data_definition)

            # data_definition_streamからフィールド情報を取得
            (
                geometry_type_list,
                _,
                dwh_file_name_list,
                _,
                attribute_name_list,
                _,
                _,
                all_attribute_name_list,
                *_,
            ) = (NSP.get_data_definition_index)(
                data_definition_stream,
                data_definition_delimiter="Comma",
            )

            # ジオメトリ情報のカラムのみ取得
            __geometry_type_list__ = ["+1", "+2", "+3"]
            geometry_type_bool_array = np.array(
                [
                    geometry_type_list[i] in __geometry_type_list__
                    for i in range(len(geometry_type_list))
                ],
                dtype=np.bool_,
            )
            # ジオメトリデータがない場合はfailureを返す
            if np.all(geometry_type_bool_array):
                pass
            else:
                raise ValueError("Undefined geometry type exists")

            # ジオデータフレームからFieldSetFileを生成
            field_set_file = CU.geodataframe_to_fieldsetfile(
                gdf, all_attribute_name_list, "FID"
            )

            return field_set_file, attribute

        except Exception as e:
            raise Exception(f"[__call__　Exception]: {str(e)}")
