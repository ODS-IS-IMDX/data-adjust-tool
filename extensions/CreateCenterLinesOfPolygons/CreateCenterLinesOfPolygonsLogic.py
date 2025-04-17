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
import base64
import pickle
from importlib import import_module
from collections import defaultdict

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
Delaunay = getattr(import_module("scipy.spatial"), "Delaunay")
KDTree = getattr(import_module("scipy.spatial"), "KDTree")
distance = getattr(import_module("scipy.spatial"), "distance")
csr_matrix = getattr(import_module("scipy.sparse"), "csr_matrix")
dijkstra = getattr(import_module("scipy.sparse.csgraph"), "dijkstra")
Point = getattr(import_module("shapely.geometry"), "Point")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
LineString = getattr(import_module("shapely.geometry"), "LineString")

import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import cad.common.cad_utils as CU


class CreateCenterLinesOfPolygonsLogic:

    def __init__(self, **kwargs):
        pass

    def is_valid_polygon(self, vertices):
        """
        numpy配列が有効なポリゴンに変換できるかどうかをチェックする。

        :param vertices: ポリゴンの頂点座標
        :type vertices: numpy.ndarray

        :return: 有効なポリゴンに変換できる場合はTrue、そうでない場合はFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            polygon = Polygon(vertices)
            return polygon.is_valid

        except Exception as e:
            return False

    def create_centerline_of_polygon(self, polygon_coords):
        """
        ドロネー三角分割を用いてポリゴンの中心線を作成する。

        :param polygon_coords: ポリゴンの頂点座標(3D座標を含む)
        :type polygon_coords: numpy.ndarray

        :return: 中心線の3D LineStringリスト
        :rtype: list[shapely.geometry.LineString] | None

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """

        # 戻り値として使用するリスト
        result = None

        try:
            # 2D座標に変換（ドロネー分割に使用）
            points_2d = polygon_coords[:, :2]
            delaunay = Delaunay(points_2d)
            polygon = Polygon(points_2d)

            # エッジの出現回数を記録（ポリゴン内部の三角形のみカウント）
            edge_count = defaultdict(int)
            for simplex in delaunay.simplices:
                triangle = [tuple(points_2d[simplex[i]]) for i in range(3)]
                centroid = Point(np.mean(triangle, axis=0))  # 三角形の重心を計算

                # ポリゴンに内包される三角形のみを対象にエッジをカウント
                if polygon.contains(centroid):
                    for i in range(3):
                        edge = tuple(sorted([triangle[i], triangle[(i + 1) % 3]]))
                        edge_count[edge] += 1

            # 2回登場するエッジを抽出（ポリゴン内部のエッジ）
            inner_edges = [edge for edge, count in edge_count.items() if count == 2]

            # 内部エッジの中心点を取得
            center_points = [((e[0][0] + e[1][0]) / 2, (e[0][1] + e[1][1]) / 2) for e in inner_edges]

            # ノード数
            num_nodes = len(center_points)

            # 1本しかない場合は四角形
            if num_nodes == 1:
                return None

            # 距離行列を計算
            dist_matrix = distance.cdist(center_points, center_points)
            np.fill_diagonal(dist_matrix, np.inf)  # 自分自身との距離を無効化

            # 最も近いノードのみを接続
            closest_links = {}
            for i in range(num_nodes):
                nearest_idx = np.argmin(dist_matrix[i])  # 最も近いノードを取得
                if (nearest_idx, i) not in closest_links and (i, nearest_idx) not in closest_links:  # 双方向リンクの重複を防ぐ
                    closest_links[(i, nearest_idx)] = dist_matrix[i, nearest_idx]

            # グラフの隣接行列を作成
            graph_matrix = np.full((num_nodes, num_nodes), np.inf)
            for (i, j), dist in closest_links.items():
                graph_matrix[i, j] = dist
                graph_matrix[j, i] = dist  # 無向グラフなので双方向

            # ノードの接続数を確認（3本以上の接続がある場合エラー）
            connection_count = {i:0 for i in range(num_nodes)}
            for (i, j) in closest_links.keys():
                connection_count[i] += 1
                connection_count[j] += 1

            for node, count in connection_count.items():
                if count >= 3:
                    raise ValueError(f"中心点 {node} に3本以上のリンクが接続されています")

            # 端点（接続が1本しかないノード）を取得
            endpoints = [node for node, count in connection_count.items() if count == 1]
            if len(endpoints) != 2:
                raise ValueError(f"中心線候補の端点が2つではありません: {endpoints}")

            # グラフをCSR（Compressed Sparse Row）形式に変換
            graph_csr = csr_matrix(graph_matrix)

            # 端点間の最短経路を計算
            distances, predecessors = dijkstra(graph_csr, directed=False, return_predecessors=True,
                                               indices=endpoints[0])

            # 最短経路を復元
            ordered_centerline = []
            current = endpoints[1]
            while current != endpoints[0]:
                ordered_centerline.append(center_points[current])
                current = predecessors[current]
                if current == -9999:
                    raise ValueError("最短経路が見つかりませんでした")

            ordered_centerline.append(center_points[endpoints[0]])
            ordered_centerline.reverse()  # 逆順にしたため修正

            # 中心線の両端を延長
            extension_length = 10000  # 延長距離
            start_vector = np.array(ordered_centerline[1]) - np.array(ordered_centerline[0])
            end_vector = np.array(ordered_centerline[-2]) - np.array(ordered_centerline[-1])
            start_extension = np.array(ordered_centerline[0]) - start_vector / np.linalg.norm(start_vector) * extension_length
            end_extension = np.array(ordered_centerline[-1]) - end_vector / np.linalg.norm(end_vector) * extension_length

            # ポリゴンのエッジを取得
            polygon_edges = [LineString([polygon.exterior.coords[i], polygon.exterior.coords[i + 1]]) for i in
                range(len(polygon.exterior.coords) - 1)]

            # ポリゴンのエッジリストを取得
            polygon_edges_3D = [LineString([polygon_coords[i], polygon_coords[i + 1]]) for i in
                range(len(polygon_coords) - 1)]

            # 最初に交差するエッジを検索
            start_point, end_point = None, None
            min_start_dist, min_end_dist = float('inf'), float('inf')

            start_intersection_line = LineString([ordered_centerline[0], tuple(start_extension)])
            end_intersection_line = LineString([ordered_centerline[-1], tuple(end_extension)])

            start_edge_num, end_edge_num = -1, -1

            for i, edge_line in enumerate(polygon_edges):
                start_intersection = edge_line.intersection(start_intersection_line)
                end_intersection = edge_line.intersection(end_intersection_line)

                if not start_intersection.is_empty and isinstance(start_intersection, Point):
                    dist = start_intersection.distance(Point(ordered_centerline[0]))
                    if dist < min_start_dist:
                        min_start_dist = dist
                        start_point = start_intersection
                        start_edge_num = i

                if not end_intersection.is_empty and isinstance(end_intersection, Point):
                    dist = end_intersection.distance(Point(ordered_centerline[-1]))
                    if dist < min_end_dist:
                        min_end_dist = dist
                        end_point = end_intersection
                        end_edge_num = i

            # 中心線の両端に中点を追加し、Z値を線形補間
            if start_point and end_point:
                extended_centerline = LineString([start_point] + ordered_centerline + [end_point])

                if(polygon_coords.shape[1] == 3):

                    # 両端のZ座標を計算
                    start_z = (polygon_edges_3D[start_edge_num].coords[0][2] + polygon_edges_3D[start_edge_num].coords[-1][
                        2]) / 2
                    end_z = (polygon_edges_3D[end_edge_num].coords[0][2] + polygon_edges_3D[end_edge_num].coords[-1][2]) / 2

                    # Z値の線形補間
                    extended_coords = list(extended_centerline.coords)
                    total_length = sum(
                        np.linalg.norm(np.array(extended_coords[i + 1][:2]) - np.array(extended_coords[i][:2])) for i in
                        range(len(extended_coords) - 1))

                    cumulative_length = 0
                    res_extended_coords = []
                    for i, coord in enumerate(extended_coords):
                        if i == 0:
                            z = start_z
                        elif i == len(extended_coords) - 1:
                            z = end_z
                        else:
                            cumulative_length += np.linalg.norm(np.array(coord[:2]) - np.array(extended_coords[i - 1][:2]))
                            z = start_z + (end_z - start_z) * (cumulative_length / total_length)
                        res_extended_coords.append((coord[0], coord[1], z))

                    result = [LineString([res_extended_coords[i], res_extended_coords[i + 1]]) for i in
                        range(len(res_extended_coords) - 1)]
                else :
                    # LineStringの配列を作成
                    extended_coords = list(extended_centerline.coords)
                    result = [LineString([extended_coords[i], extended_coords[i + 1]]) for i in
                        range(len(extended_coords) - 1)]

            return result

        except Exception as e:
            raise Exception(f"[create_centerline_of_polygon]: {str(e)}")

    def calculate_distance(self, point1: tuple, point2: tuple):
        """
        ２つの座標から距離を計算(平面直角座標系の座標を対象とする)

        :param point1: 座標1つめ
        :type point1: tuple(float, float)
        :param point2: 座標2つめ
        :type point2: tuple(float, float)

        :return: 距離
        :rtype: float

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            return ((point1[0] - point2[0]) ** 2 + (point1[1] - point2[1]) ** 2) ** 0.5

        except Exception as e:
            raise Exception(f"[calculate_distance]: {str(e)}")

    def convert_from_geo_ndarray_to_ndarray_list(self, geo_ndarray):
        """
        データ形式の変換（3Dポリゴンでも2Dポリゴンでも対応可能）

        :param geo_ndarray: 変換対象のデータ
        :type geo_ndarray: numpy.ndarray

        :return: ndarray形式に変換したデータ（キーはインデックス, 値はnumpy.ndarray）
        :rtype: dict[int, numpy.ndarray]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            ndarray_dict = {}
            i = 0

            for geom_points in geo_ndarray:
                # Z値が存在するか確認し、適切に変換
                coords = np.array(geom_points.exterior.coords)
                if coords.shape[1] == 3:  # 3Dポリゴン
                    geom_array = coords
                else:  # 2Dポリゴン（Z値なし）
                    geom_array = coords[:, :2]

                # 辞書に追加
                ndarray_dict[i] = geom_array
                i += 1

            return ndarray_dict

        except Exception as e:
            raise Exception(f"[convert_from_geo_ndarray_to_ndarray_list]: {str(e)}")

    def create_centerline_for_rectangle(self, polygon):
        """
        1つのポリゴンの中心線の座標を取得

        :param polygon: 1つのポリゴン
        :type polygon: shapely.geometry.Polygon

        :return: 1つのポリゴンの中心線の座標のリスト
        :rtype: shapely.geometry.LineString

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # ポリゴンの頂点をリストとして取得
            # Tupleで経度(float)と緯度(float)の座標の入ったリスト 長方形なので5個の座標、1つは重複
            vertices = list(polygon)

            # 2つの座標とその距離のリスト
            edges = []

            # 長方形のポリゴンの座標の数 5
            num_vertices = len(vertices)

            # 座標が1つ重複するのでLoopの回数は-1
            for point_idx in range(num_vertices - 1):
                p1 = vertices[point_idx]
                p2 = vertices[point_idx + 1]
                distance = self.calculate_distance(p1, p2)
                edges.append((p1, p2, distance))

            # １辺目と同じ辺を抽出
            same_len = [ln for ln in edges if ln[2] == edges[0][2]]

            # 1辺目と同じ長さの辺の数がポリゴンの辺の数と一致しているか確認(一致していれば4辺であるという前提で正方形かひし形である。)
            if num_vertices - 1 == len(same_len):
                return None

            # 距離の長い順にソート
            edges_sorted = sorted(edges, key=lambda edge: edge[2], reverse=True)
            first_longest = edges_sorted[0]
            second_longest = edges_sorted[1]

            first_longest_p1 = first_longest[0]
            first_longest_p2 = first_longest[1]

            second_longest_p1 = second_longest[0]
            second_longest_p2 = second_longest[1]

            # 2つの線を10等分した点を取得
            split_num = 10
            split_array_first = np.linspace(first_longest_p1, first_longest_p2, split_num)
            split_array_second = np.linspace(second_longest_p2, second_longest_p1, split_num)
            # 中心点のリスト
            center_point_list = []
            # 等分した数分Loop
            for split_idx in range(split_num):
                center_point = np.linspace(split_array_first[split_idx], split_array_second[split_idx], 3)[1]
                center_point_list.append(center_point)

            return LineString(center_point_list)

        except Exception as e:
            raise Exception(f"[get_centerline_from_rectangle]: {str(e)}")

    def get_params(self, properties):
        """
        プロパティの値を取得し辞書にして返す

        :param properties: プロパティ
        :type properties: dict

        :return: プロパティの値を辞書にしたもの
        :rtype: dict

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:

            all_params = {}

            # プロパティの値を取得する
            all_params["margin"] = properties['MARGIN']
            all_params["geom_name"] = properties['GEOMETRY_NAME']

            return all_params

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def simplify_polygon(self, polygon, margin):
        """
        3Dまたは2Dポリゴンを簡略化し、必要に応じてZ値を復元する。

        :param polygon: 3Dまたは2Dポリゴンの頂点リスト（形状: (N, 2) または (N, 3)）
        :type polygon: numpy.ndarray
        :param margin: 簡略化（simplify）処理時の許容誤差
        :type margin: float

        :return: 簡略化後のポリゴン（入力が3Dなら3Dポリゴン、2Dなら2Dポリゴン）
        :rtype: shapely.geometry.Polygon

        :raises Exception: 入力データの形式が正しくない場合や、処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # Z値があるか判定
            is_3d = polygon.shape[1] == 3

            # 1. 2D座標を取得（3Dの場合はXYのみ抽出）
            polygon_2d = Polygon(polygon[:, :2])

            # 2. simplify処理 (2D)
            simplified_polygon_2d = polygon_2d.simplify(margin, preserve_topology=True)
            simplified_coords_2d = np.array(simplified_polygon_2d.exterior.coords)

            if not is_3d:
                # 入力が2Dなら、そのまま返す
                return Polygon(simplified_coords_2d)

            # 3. 元の3Dポリゴンから KDTree を作成（XY座標のみ）
            tree = KDTree(polygon[:, :2])  # KDTree 作成
            z_values = polygon[:, 2]  # Z値リスト

            # 4. simplify後の各頂点について、Z値を復元
            simplified_coords_3d = []
            for x, y in simplified_coords_2d:
                # 最近傍点のZ値を取得（複数あれば平均）
                dists, idxs = tree.query([x, y], k=5, distance_upper_bound=1e-3)

                # valid_idxs: 近傍点のうち無限大でないもの（有効な点）
                valid_idxs = idxs[dists != np.inf]
                if len(valid_idxs) > 0:
                    z = np.mean(z_values[valid_idxs])  # 平均Z値
                else:
                    z = 0  # Z値が見つからなかった場合のデフォルト値（地表面と仮定）

                simplified_coords_3d.append((x, y, z))

            # 5. 3Dポリゴンとして返す
            return Polygon(simplified_coords_3d)

        except Exception as e:
            raise Exception(f"[simplify_3d_polygon]: {str(e)}")

    def create_field_set_file(self, lines, fids, properties):
        """
        出力結果のFieldSetFileを生成する。

        :param lines: 生成されたPolygonの座標配列(GeoNdarray)。
        :type lines: numpy.ndarray

        :param fids: 生成されたPolygonのFID情報。
        :type fids: list[tuple(str, float)]

        :param properties: 属性の値とプロパティの値を格納した辞書。
        :type properties: dict

        :return: 処理結果としてのFieldSetFile。
        :rtype: str

        :raises Exception:
            FieldSetFile生成中にエラーが発生した場合に例外をスローする。
        """
        try:
            # FieldSetFile作成用のDataFrameを作成する
            fsf_df = pd.DataFrame({
                "Dwh": [f"{properties['geom_name']}/geometry", f"{properties['geom_name']}/FID"],
                "Type": ["geometry", "string"],
                "Value": [lines, fids]
            })

            # DataFrameからFieldSetFileを作成して返す
            return CU.dataframe_to_field_set_file(fsf_df)

        except Exception as e:
            raise Exception(f"[create_field_set_file_Exception]: {str(e)}")

    def create_center_lines_of_polygons(self, byte_data, properties):
        """
        与えられた点群に対してドロネー分割を行い、三角形の頂点インデックスを返す。

        :param byte_data: 入力となるFieldSetFileのバイトデータ
        :type byte_data: bytes
        :param properties: プロパティ
        :type properties: dict

        :return: 処理されたFieldSetFile。
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティの取得
            all_params = self.get_params(properties)
            geometry_name = all_params['geom_name']
            margin = all_params['margin']

            # FieldSetFileをGeoDataFrameに変換
            geodataframe = CU.field_set_file_to_geodataframe(byte_data)

            # FIDと座標に分割する
            geometry_list = list(geodataframe.geometry)
            ndarray_dict = self.convert_from_geo_ndarray_to_ndarray_list(geometry_list)
            fid_list = np.array(list(ndarray_dict.keys()), dtype=float)
            coords = list(ndarray_dict.values())

            # 座標列格納先
            list_geo_ndarray = []

            for i in range(len(coords)):
                try:

                    polygon = coords[i]

                    # Z値があるか判定
                    is_3d = polygon.shape[1] == 3

                    # 対象をポリゴンに変換できない場合、処理を行わない
                    if not self.is_valid_polygon(polygon[:, :2]):
                        continue

                    # FID値を設定
                    fid = float(fid_list[i])

                    # ドロネー処理のエラーを回避するために、ポリゴンをsimplyfyする
                    target_polygon = self.simplify_polygon(polygon, float(margin))

                    # 3Dポリゴンの座標をnumpy配列として取得
                    polygon_coords = np.array(target_polygon.exterior.coords)

                    # ポリゴン中心線を作成する
                    center_lines = self.create_centerline_of_polygon(polygon_coords)

                    # 作成できなかった場合は、長方形の可能性がある
                    if center_lines is None:
                        center_lines = []
                        center_line = self.create_centerline_for_rectangle(polygon_coords)
                        if center_line:
                            center_lines.append(center_line)
                        else:
                            raise ValueError(
                                f"長方形以外の四角形のため処理出来ません。 重心座標:" + str(polygon_coords.centroid))

                    # geo_ndarryの形になるようlistに格納
                    for i, link in enumerate(center_lines):
                        coords = list(link.coords)
                        start_idx = 0 if i == 0 else 1  # 最初のリンクは全座標、それ以降は1番目以降を追加
                        for coord in coords[start_idx:]:
                            if is_3d:
                                list_geo_ndarray.append([fid, coord[0], coord[1], coord[2]])
                            else:
                                list_geo_ndarray.append([fid, coord[0], coord[1]])

                except Exception as e:
                    raise Exception(f"[create_center_lines_of_polygons]  fid[{fid}] : {str(e)}")

            # GeoNdarrayの先頭の値をFIndexとしてFID情報を作成
            fids = [(str(int(i)), float(i)) for i in np.unique(np.array(list_geo_ndarray)[:, 0])]

            # Polygonのジオメトリ情報とFID情報からFieldSetFileを作成
            field_set_file = self.create_field_set_file(list_geo_ndarray, fids, all_params)

            return field_set_file

        except Exception as e:
            raise Exception(f"[create_center_lines_of_polygons] fid[{fid}] : {str(e)}")

    def __call__(self, contents, attribute, properties):
        """
        メインの処理の入り口部分。各プロパティに基づいて適切な処理を実行する。

        :param contents: 入力データとしてバイト型のFieldSetFileを受け取る
        :type contents: bytes
        :param attribute: 属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: シリアライズされた簡略化後のGeoNdarray形式データと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # メインの処理関数に渡して実行
            field_set_file = self.create_center_lines_of_polygons(contents, properties)
            return field_set_file, attribute

        except Exception as e:
            # 例外発生時、エラーメッセージを記録し、失敗を返す
            raise Exception(f"[transform]:{e}")
