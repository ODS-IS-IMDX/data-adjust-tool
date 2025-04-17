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
from collections import defaultdict
import io
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
LineString = getattr(import_module("shapely.geometry"), "LineString")
Point = getattr(import_module("shapely.geometry"), "Point")
transform = getattr(import_module("shapely.ops"), "transform")
polygonize = getattr(import_module("shapely.ops"), "polygonize")
KDTree = getattr(import_module("scipy.spatial"), "KDTree")
csr_matrix = getattr(import_module("scipy.sparse"), "csr_matrix")
dijkstra = getattr(import_module("scipy.sparse.csgraph"), "dijkstra")
pd = import_module("pandas")
linemerge = getattr(import_module("shapely.ops"), "linemerge")
jit = getattr(import_module("numba"), "jit")

import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


@jit(nopython=True, cache=True, nogil=True)
def process_data(integrated_geo_ndarray, original_geo_ndarray, org_fids, int_fids):
    """
    FIDのペアリストを生成する

    :param integrated_geo_ndarray: 座標のリスト。各要素はタプル形式で表された座標。
    :type integrated_geo_ndarray: numpy.ndarray
    :param int_fids: 統合後線分のFIDデータ。
    :type int_fids: list[tuple[str, float]]
    :param original_geo_ndarray: 統合前線分のジオメトリデータ。
    :type original_geo_ndarray: list[shapely.geometry.base.BaseGeometry]
    :param org_fids: 統合前線分のFIDデータ。
    :type org_fids: list[tuple[str, float]]

    :return: FIDのペアリスト。
    :rtype: list

    :raises Exception:
        座標の処理中にエラーが発生した場合に例外をスローする。
    """

    pairs_list = []
    org_segment = []

    for org_idx in range(len(original_geo_ndarray) - 1):
        if original_geo_ndarray[org_idx, 0] != original_geo_ndarray[org_idx + 1, 0]:
            continue

        org_segment.append(original_geo_ndarray[org_idx:org_idx + 2, 1:])

    for int_idx in range(len(integrated_geo_ndarray) - 1):
        if integrated_geo_ndarray[int_idx, 0] != integrated_geo_ndarray[int_idx + 1, 0]:
            continue

        int_segment = integrated_geo_ndarray[int_idx:int_idx + 2, 1:]

        match_fidx_list = []

        for i, B in enumerate(org_segment):
            if np.array_equal(int_segment, B) or np.array_equal(int_segment, B[::-1]):
                match_fidx_list.append((org_fids[int(original_geo_ndarray[i, 0])][1]))

        target_int_fidx = int_fids[int(integrated_geo_ndarray[int_idx, 0])][1]
        pairs_list.append((integrated_geo_ndarray[int_idx, 0], sorted(match_fidx_list)))
        pairs_list.append((target_int_fidx, sorted(match_fidx_list)))

    return pairs_list


def extract_cycles_and_cycle_nodes(polygon_coords, nodes_keys, nodes_values):
    """
    多角形の座標からサイクルノードを抽出。

    :param polygon_coords: ポリゴンのリスト。
    :type polygon_coords: List

    :param nodes_keys: ノードのキー。
    :type nodes_keys: List

    :param nodes_values: ノード情報。
    :type nodes_values: List

    :return: ポリゴンリスト、一致するノードのリスト。
    :rtype: tuple[List, List]

    :raises Exception:
        座標の処理中にエラーが発生した場合に例外をスローする。
    """

    def find_matching_key(point, nodes_keys, nodes_values):
        """ポイントに一致するノードキーを検索"""
        for i in range(len(nodes_keys)):
            if nodes_values[i, 0] == point[0] and nodes_values[i, 1] == point[1]:
                return nodes_keys[i]
        return -1  # 一致しない場合

    cycles = []
    cycle_node_list = []

    for poly_coords in polygon_coords:
        cycle_path = []
        for point in poly_coords:
            matching_key = find_matching_key(point, nodes_keys, nodes_values)
            if matching_key != -1:
                cycle_path.append(matching_key)
        if cycle_path:
            cycles.append(cycle_path)
            for i in range(len(cycle_path) - 1):
                cycle_node_list.append([cycle_path[i], cycle_path[i + 1]])

    return cycles, cycle_node_list


class IntegrateLineStringLogic:

    # 閾値
    THRESHOLD = 0.001

    def __init__(self):
        pass

    def process_geometry(self, unused_edges, nodes):
        """
        Shapelyを用いたpolygonize。

        :param unused_edges: 未使用エッジリスト。
        :type unused_edges: List
        :param nodes: ノード。
        :type nodes: List

        :return: ポリゴンリストと、一致するノードのリスト。
        :rtype: tuple[List, List]

        :raises Exception:
            座標の処理中にエラーが発生した場合に例外をスローする。
        """
        lines = [
            LineString([tuple(nodes[edge[0]]), tuple(nodes[edge[1]])])
            for edge in unused_edges
        ]
        polygon_list = list(polygonize(lines))

        # 多角形の外周座標を抽出
        polygon_coords = [
            np.array(polygon.exterior.coords) for polygon in polygon_list
        ]

        # ノード辞書をNumPy形式に変換
        nodes_keys = np.array(list(nodes.keys()))
        nodes_values = np.array(list(nodes.values()))

        # Numbaで高速化された処理を呼び出し
        cycles, cycle_node_list = extract_cycles_and_cycle_nodes(
            polygon_coords, nodes_keys, nodes_values
        )

        return cycles, cycle_node_list

    def reduce_points_within_threshold(self, points, threshold):
        """
        閾値内の座標を同一の座標としてまとめる。

        :param points: 座標のリスト。各要素はタプル形式で表された座標。
        :type points: numpy.ndarray
        :param threshold: 同一座標とみなす距離の閾値。
        :type threshold: float

        :return: 閾値内でまとめられた座標リストと、変更前後の座標情報をまとめた辞書のリスト。
        :rtype: tuple[numpy.ndarray, list[dict[str, Union[numpy.ndarray, numpy.ndarray]]]]

        :raises Exception:
            座標の処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 変更前後の座標情報格納先
            point_before_and_after = {}

            kdtree = KDTree(points)
            unique_points = []
            processed = set()

            for i, point in enumerate(points):
                if i in processed:
                    continue
                idxs = kdtree.query_ball_point(point, r=threshold)
                cluster = [points[idx] for idx in idxs]
                centroid = np.mean(cluster, axis=0)
                unique_points.append(centroid)
                processed.update(idxs)

                # 変化前と変化後の座標情報を辞書に格納
                for elem in np.unique(cluster, axis=0):
                    if not np.array_equal(elem, centroid):
                        point_before_and_after[tuple(elem)] = centroid

            return np.array(unique_points), point_before_and_after

        except Exception as e:
            raise Exception(f"[reduce_points_within_threshold_Exception]: {str(e)}")

    def remove_edge(self, sp_mtx, start, end, del_dup_flg):
        """
        疎行列から対象のエッジを削除する。

        :param sp_mtx: エッジを持つ疎行列。
        :type sp_mtx: scipy.sparse.csr_matrix
        :param start: 始点ノードのインデックス。
        :type start: int
        :param end: 終点ノードのインデックス。
        :type end: int
        :param del_dup_flg: エッジを完全に削除するか、重複エッジを1本だけ削除するかのフラグ。
        :type del_dup_flg: bool

        :return: 該当エッジが削除された疎行列。
        :rtype: scipy.sparse.csr_matrix

        :raises Exception:
            エッジの削除処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # Delete DuplicatesがTrueの場合
            if del_dup_flg:
                # 始点から終点へのエッジを削除
                sp_mtx[start, end] = 0
                # 終点から始点へのエッジを削除
                sp_mtx[end, start] = 0
            else:
                # 始点から終点へのエッジを1本分削除
                if sp_mtx[start, end] != 0:
                    sp_mtx[start, end] -= 1
                # 終点から始点へのエッジを1本分削除
                if sp_mtx[end, start] != 0:
                    sp_mtx[end, start] -= 1

            # 疎行列の再構成
            row, col = sp_mtx.nonzero()  # 非ゼロ要素の行と列のインデックスを取得
            data = sp_mtx.data[sp_mtx.data != 0]
            sp_mtx = csr_matrix((data, (row, col)), shape=sp_mtx.shape)

            return sp_mtx

        except Exception as e:
            raise Exception(f"[remove_edge_Exception]: {str(e)}")

    def build_sparse_matrix(self, line_segments, threshold):
        """
        線分の座標のリストから疎行列を作成する。

        :param line_segments: 線分の座標配列を持つリスト。
        :type line_segments: list[numpy.ndarray]
        :param threshold: 同一ノードとみなす距離の閾値。
        :type threshold: float

        :return: 疎行列、ノード別の座標配列を持つ辞書、座標の変化前後情報を持つ辞書のリスト。
        :rtype: tuple[scipy.sparse.csr_matrix, dict, list[dict[str, Union[numpy.ndarray, numpy.ndarray]]]]

        :raises Exception:
            疎行列の作成処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # すべての線分の端点を1つのリストにする
            points = np.array([point for segment in line_segments for point in segment])

            # 閾値内の座標を同一ノードとしてまとめる
            reduced_points, points_bef_aft = self.reduce_points_within_threshold(
                points, threshold
            )

            # KDTreeを再構築して新しいノードを生成
            kdtree = KDTree(reduced_points)

            unique_nodes = {}  # ノードのインデックスを保持する辞書
            node_counter = 0  # ノードのインデックスカウンター
            node_coordinates = (
                {}
            )  # ノードの座標を保持する辞書 key=node_counter value=node_counterの座標

            # 隣接行列データ格納用
            row = []
            col = []
            data = []

            # 全ての線分をLoop
            for start, end in line_segments:
                # KDTreeを使用して最近傍点を検索し、同一ノードとする
                start_dist, start_idx = kdtree.query(
                    start, k=1
                )  # start点に最も近い点を検索
                end_dist, end_idx = kdtree.query(end, k=1)  # end点に最も近い点を検索

                if start_idx not in unique_nodes:
                    unique_nodes[start_idx] = node_counter  # 新しいノードを追加
                    node_coordinates[node_counter] = reduced_points[
                        start_idx
                    ]  # ノードの座標を保存
                    node_counter += 1  # ノードカウンターをインクリメント

                if end_idx not in unique_nodes:
                    unique_nodes[end_idx] = node_counter  # 新しいノードを追加
                    node_coordinates[node_counter] = reduced_points[
                        end_idx
                    ]  # ノードの座標を保存
                    node_counter += 1  # ノードカウンターをインクリメント

                # 始点と終点が同じである場合は追加しない(点のような線分)
                if unique_nodes[start_idx] != unique_nodes[end_idx]:
                    start_idx, end_idx = sorted([start_idx, end_idx])
                    # 後の隣接ノード検出のために、双方向で隣接関係を登録
                    row.extend([start_idx, end_idx])
                    col.extend([end_idx, start_idx])
                    data.extend([1, 1])

            # 隣接行列からCSR形式の疎行列を作成
            sparse_matrix = csr_matrix((data, (row, col)), shape=(len(row), len(row)))

            return sparse_matrix, node_coordinates, points_bef_aft

        except Exception as e:
            raise Exception(f"[build_sparse_matrix_Exception]: {str(e)}")

    def create_polyline_list(self, sp_mtx, delete_duplicate_flg):
        """
        グラフから分岐のないポリライン別のリストを作成する。

        :param sp_mtx: ノード間の隣接情報を保持する疎行列。
        :type sp_mtx: scipy.sparse.csr_matrix
        :param delete_duplicate_flg: 重複するエッジを削除するかどうかを決定するフラグ。
        :type delete_duplicate_flg: bool

        :return: ポリラインにするノードのリスト、および検出したポリラインノードを削除した疎行列。
        :rtype: tuple[list[list[int]], scipy.sparse.csr_matrix]

        :raises Exception:
            処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 全ノードを取得
            # 非ゼロ要素の行と列のインデックスを取得
            rows, cols = sp_mtx.nonzero()
            # 全ノードのリストを取得（重複を削除するためにsetを使用）
            node_list = sorted(set(rows) | set(cols))

            # 接続ノード数が2以外の有効ノードを取得
            valid_nodes = [
                node
                for node in node_list
                if (sp_mtx[node].toarray().flatten() != 0).sum() != 2
            ]

            if not delete_duplicate_flg:
                # Delete DuplicatesがFalseのとき重さ（重複数）が2以上のエッジを取得し、構成ノードを有効ノードとする
                duplicates_node_list = np.array(
                    [
                        np.array([rows[i], cols[i]])
                        for i in range(len(sp_mtx.data))
                        if sp_mtx[rows[i], cols[i]] >= 2
                    ]
                )
                if len(duplicates_node_list) > 0:
                    nodes_of_duplicate_edges = np.unique(
                        np.sort(duplicates_node_list, axis=1), axis=0
                    )[0]
                    valid_nodes = list(
                        set(valid_nodes + list(nodes_of_duplicate_edges))
                    )

            # ポリラインにするnodeのリストのリスト
            node_list_of_list_for_polyline = []

            # 有効ノードをLoop
            for start_node in valid_nodes:
                # 隣接ノードリストを取得
                neighbor_node_list = sp_mtx[start_node].nonzero()[1]
                # 有効ノードである隣接ノードを抽出
                valid_neighbor_node_list = list(
                    filter(
                        lambda node: node in valid_nodes,
                        neighbor_node_list,
                    )
                )

                # 隣接ノードに有効ノードがある場合、経路として登録し、疎行列からエッジを削除する
                if len(valid_neighbor_node_list) > 0:
                    for neighbor in valid_neighbor_node_list:
                        # 経路としてポリラインリストに追加
                        node_list_of_list_for_polyline.append([start_node, neighbor])
                        # 検出した経路を疎行列から削除
                        sp_mtx = self.remove_edge(
                            sp_mtx,
                            start_node,
                            neighbor,
                            delete_duplicate_flg,
                        )

                # 隣接ノードから有効ノード以外を抽出
                invalid_neighbor_node_list = list(
                    filter(
                        lambda node: node not in valid_neighbor_node_list,
                        neighbor_node_list,
                    )
                )
                # 有効ノードではない隣接ノードがある場合、最短経路を探索し、疎行列からエッジを削除する
                if len(invalid_neighbor_node_list) > 0:
                    # 有効ノードから始点以外を抽出
                    end_nodes = list(
                        filter(
                            lambda node: node != start_node,
                            valid_nodes,
                        )
                    )
                    # 最短経路の計算
                    _distances, predecessors = dijkstra(
                        sp_mtx, return_predecessors=True, indices=start_node
                    )

                    # 経路の復元
                    for end_node in end_nodes:
                        no_path_flg = False  # パス無しフラグ
                        path = []  # 検出パス格納先
                        current_node = end_node  # 現在ノード
                        # 現在ノードが開始ノードではない限りループ
                        while current_node != start_node:
                            path.append(current_node)
                            current_node = predecessors[current_node]
                            # 前駆ノードが見つからない、もしくは、前駆ノードが始点終点を除く有効ノードの場合
                            if current_node == -9999 or (
                                current_node != start_node
                                and current_node != end_node
                                and current_node in valid_nodes
                            ):
                                no_path_flg = True
                                break
                        if not no_path_flg:
                            path.append(start_node)  # 始点をパスに追加
                            path.reverse()  # 始点から始まるパスに並び替え
                            # 経路としてポリラインリストに追加
                            node_list_of_list_for_polyline.append(path)
                            # パス登録したエッジを疎行列から削除
                            for idx in range(len(path) - 1):
                                sp_mtx = self.remove_edge(
                                    sp_mtx,
                                    path[idx],
                                    path[idx + 1],
                                    delete_duplicate_flg,
                                )

            return node_list_of_list_for_polyline, sp_mtx

        except Exception as e:
            raise Exception(f"[create_polyline_list_Exception]: {str(e)}")

    def get_unused_edges(
        self, remove_pl_list, sp_mtx, remove_pl_flg, delete_duplicate_flg
    ):
        """
        疎行列からポリラインのノードのリストに含まれていないエッジを抽出する。

        :param remove_pl_list: 削除対象のポリラインのノードのリスト。
        :type remove_pl_list: list[list[int]]
        :param sp_mtx: グラフのエッジ情報を保持する疎行列。
        :type sp_mtx: scipy.sparse.csr_matrix
        :param remove_pl_flg: 検出したポリラインを疎行列から削除するかどうかのフラグ。
        :type remove_pl_flg: bool
        :param delete_duplicate_flg: 重複するエッジを削除するかどうかのフラグ。
        :type delete_duplicate_flg: bool

        :return: 未使用のエッジのリスト。
        :rtype: list[list[int]]

        :raises Exception:
            エッジの抽出処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            if remove_pl_flg and len(remove_pl_list) > 0:
                # 疎行列から検出したポリラインのエッジを削除
                for polyline in remove_pl_list:
                    # 検出した経路を疎行列から削除
                    sp_mtx = self.remove_edge(
                        sp_mtx,
                        polyline[0],
                        polyline[1],
                        delete_duplicate_flg,
                    )

            # 非ゼロ要素の行と列のインデックスを取得
            row_indices, col_indices = sp_mtx.nonzero()

            # 未使用のエッジを取得
            unused_edges = list()
            for row, col in zip(row_indices, col_indices):
                if sp_mtx[row, col] > 0:
                    unused_edges.append(tuple((row, col)))

            # 結果をリストに変換して表示
            if delete_duplicate_flg:
                # 重複エッジは削除の場合
                unused_edge_list = list({tuple(sorted(t)) for t in unused_edges})
            else:
                unused_edge_list = [list(edge) for edge in unused_edges]

            return unused_edge_list

        except Exception as e:
            raise Exception(f"[get_unused_edges_Exception]: {str(e)}")

    def find_basis_cycles(self, unused_edges, nodes):
        """
        未使用エッジから閉路を探索する。

        :param unused_edges: 未使用エッジのリスト。各要素はノードのインデックスを持つ。
        :type unused_edges: list[list[int]]
        :param nodes: ノードの座標を格納した辞書。キーはノードのインデックス、値は座標のタプル。
        :type nodes: dict[int, tuple[float, float]]

        :return: 検出された閉路パスのリストと、閉路を構成するノードリスト。
        :rtype: tuple[list[list[int]], list[list[int]]]

        :raises Exception:
            閉路探索中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 未使用エッジとノード情報からラインを作成する
            lines = [
                LineString([tuple(nodes[edge[0]]), tuple(nodes[edge[1]])])
                for edge in unused_edges
            ]
            # ラインをポリゴン化
            polygon_list = list(polygonize(lines))

            cycles = []  # 検出サイクル格納用

            # 生成されたポリゴンの数分Loop
            for polygon in polygon_list:
                cycle_path = []  # サイクル経路格納用
                # ポリゴンの頂点の数分Loop
                for point in polygon.exterior.coords:
                    # ノード情報から座標が一致するキーを取得
                    matching_key = next(
                        (key for key, value in nodes.items() if tuple(value) == point),
                        None,
                    )
                    if matching_key is not None:
                        # サイクル経路に取得したノードのキーを格納
                        cycle_path.append(matching_key)

                # サイクル経路をリストに格納
                cycles.append(cycle_path)

            # サイクルのノードリストを作成
            cycle_node_list = [
                [cycle[idx], cycle[idx + 1]]
                for cycle in cycles
                for idx in range(len(cycle) - 1)
            ]

            return cycles, cycle_node_list

        except Exception as e:
            raise Exception(f"[find_basis_cycles_Exception]: {str(e)}")

    def decompose_linestrings(self, lines):
        """
        すべての線分の座標を2点で構成される線分に分解しソートする。

        :param lines: 線分座標と、その線分に対応するFIDを持つ辞書のリスト。
        :type lines: numpy.ndarray

        :return: 2点で構成されソートされた線分座標と、その線分に対応するFIDを持つ辞書のリスト。
        :rtype: list[numpy.ndarray]

        :raises Exception:
            線分の分解処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            line_segments = []
            for line in lines:
                for idx in range(len(line) - 1):
                    line_array = line[[idx, idx + 1]]
                    sorted_line = line_array[
                        np.lexsort((line_array[:, 1], line_array[:, 0]))
                    ]
                    line_segments.append(sorted_line)

            return line_segments

        except Exception as e:
            raise Exception(f"[decompose_linestrings_Exception]: {str(e)}")

    def ignore_z(self, x, y, z=None):
        """
        Z座標を削除した座標を返す。

        :param x: X座標。
        :type x: float
        :param y: Y座標。
        :type y: float
        :param z: Z座標。デフォルトはNone。
        :type z: float, optional

        :return: Z座標が含まれていない座標 (X, Y)。
        :rtype: tuple[float, float]
        """
        return x, y

    def interpolate_z(self, line, point_2d):
        """
        点の位置(Z座標を含まない)から線分のZ座標を推定する。

        :param line: Z座標を含む線分。
        :type line: shapely.geometry.LineString
        :param point_2d: Z座標を含まない点。
        :type point_2d: shapely.geometry.Point

        :return: 推定されたZ座標。Z座標がない場合はNone。
        :rtype: float or None

        :raises Exception:
            Z座標推定処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            proj_distance = line.project(point_2d)
            nearest_point = line.interpolate(proj_distance)
            if len(nearest_point.coords[0]) == 3:
                # Z座標がある場合
                return nearest_point.z
            else:
                # Z座標がない場合
                return None

        except Exception as e:
            raise Exception(f"[interpolate_z_Exception]: {str(e)}")

    def is_3d_geo(self, geometry):
        """
        3Dの座標であるかを確認する。

        :param geometry: 座標情報を持つジオメトリ。
        :type geometry: shapely.geometry.BaseGeometry

        :return: Z座標がすべての座標に含まれていればTrue、そうでなければFalse。
        :rtype: bool

        :raises Exception:
            Z座標の存在チェック処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            geo_coords = np.array(geometry.coords)

            # Z座標が含まれていない座標が1つでもあればZ座標はないとみなす(Z座標の有るものと無いものは混在しない事が想定されるため)
            for coord in geo_coords:
                if len(coord) < 3:
                    return False

            return True

        except Exception as e:
            raise Exception(f"[is_3d_geo_Exception]: {str(e)}")

    def check_3d_intersection_lines(self, line1, line2, threshold):
        """
        線分が交差しているか確認し、交差している場合はその交点の座標を返す。

        :param line1: 1つ目の線分。
        :type line1: shapely.geometry.LineString
        :param line2: 2つ目の線分。
        :type line2: shapely.geometry.LineString
        :param threshold: 交差判定のための閾値。
        :type threshold: float

        :return: 交差しているかどうかのフラグと、交差している場合は交点の座標。
        :rtype: tuple[bool, numpy.ndarray or None]

        :raises Exception:
            交差判定中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 3Dの座標か確認
            is_line1_3d = self.is_3d_geo(line1)
            is_line2_3d = self.is_3d_geo(line2)

            # 2DのLinestringに変換
            line1_2d = self.correct_linestring(line1, line2, threshold)
            line2_2d = self.correct_linestring(line2, line1, threshold)

            # 2Dで交差していなければ3Dでも交差していない
            if not line1_2d.intersects(line2_2d):
                return False, None

            # 2Dの交点を求める
            intersection_geom_2d = line1_2d.intersection(line2_2d)
            intersection_coordinates = np.array(intersection_geom_2d.coords)

            intersection_coordinate_3d_array = []

            if is_line1_3d and is_line2_3d:
                # 2Dで交差している箇所の座標でloop(点で重なっている場合は1周、線で重なっている場合は2周する)
                for coordinate in intersection_coordinates:
                    # Z座標を含まずに確認した交点
                    intersection_point = Point(coordinate)

                    # line1、line2の両方が3Dの座標であれば
                    # 交差点のZ座標を推定
                    intersection_z1 = self.interpolate_z(line1, intersection_point)
                    intersection_z2 = self.interpolate_z(line2, intersection_point)

                    # 交点のZ座標が浮動小数点の誤差の範囲であれば
                    if not (intersection_z1 is None or intersection_z2 is None):
                        if abs(intersection_z1 - intersection_z2) < 1e-6:
                            mean_z = (intersection_z1 + intersection_z2) / 2

                            intersection_coordinate_3d = np.append(coordinate, mean_z)
                            intersection_coordinate_3d_array.append(
                                intersection_coordinate_3d
                            )
            else:
                # 2Dの座標であれば2Dの座標で確認した結果を返す
                return True, intersection_coordinates

            if len(intersection_coordinate_3d_array) >= 1:
                # 交点が1つ以上あれば
                return True, np.array(intersection_coordinate_3d_array)
            else:
                # 交点がなければ
                return False, None

        except Exception as e:
            raise Exception(f"[check_3d_intersection_lines_Exception]: {str(e)}")

    def correct_linestring(self, line1, line2, threshold):
        """
        1本目の線分の端点から2本目の線分までの距離が閾値内であれば、2本目の線分上の座標に修正し、2Dの座標に変換する。

        :param line1: 1本目の線分。
        :type line1: shapely.geometry.LineString
        :param line2: 2本目の線分。
        :type line2: shapely.geometry.LineString
        :param threshold: 補正する際の距離の閾値。
        :type threshold: float

        :return: 補正されて2D座標に変換された1本目の線分。
        :rtype: shapely.geometry.LineString

        :raises Exception:
            線分補正中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 3Dの座標の場合は2Dの座標に変更
            line1_2d = transform(self.ignore_z, line1)
            line2_2d = transform(self.ignore_z, line2)

            # 1本目の線分の端点と2本目の線分までの距離が閾値内の距離であればその端点を2本目の線分上の座標に変更
            correct_point_line1_st = self.correct_point(
                Point(line1_2d.coords[0]), line2_2d, threshold
            )
            correct_point_line1_ed = self.correct_point(
                Point(line1_2d.coords[-1]), line2_2d, threshold
            )

            return LineString([correct_point_line1_st, correct_point_line1_ed])

        except Exception as e:
            raise Exception(f"[correct_linestring_Exception]: {str(e)}")

    def correct_point(self, point, line, threshold):
        """
        点と線分の距離が閾値以内なら、線分上の座標に修正する。

        :param point: Z座標を含まない点。
        :type point: shapely.geometry.Point
        :param line: Z座標を含まない線分。
        :type line: shapely.geometry.LineString
        :param threshold: 点を補正する際の距離の閾値。
        :type threshold: float

        :return: 座標が補正された点。
        :rtype: shapely.geometry.Point

        :raises Exception:
            点の補正中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 線分上で対象の点から最も近い点を取得
            nearest_point_on_line = line.interpolate(line.project(point))

            # 最近傍点の座標を取得
            nearest_coords = nearest_point_on_line.coords[0]

            # 最近傍点から点までの距離が閾値以下
            nearest_point = Point(nearest_coords)
            nearest_point_distance = nearest_point.distance(point)

            if (nearest_coords not in line.coords) and (
                nearest_point_distance <= threshold
            ):
                # 最近傍点が線分の端点ではなく、最近傍点から対象の点までの距離が閾値以下なら
                return nearest_point
            else:
                return point

        except Exception as e:
            raise Exception(f"[correct_point_Exception]: {str(e)}")

    def check_3d_intersection_point_line(self, point, line):
        """
        点が線分上にあるかを判定する。

        :param point: 判定対象の点。
        :type point: shapely.geometry.Point
        :param line: 判定対象の線分。
        :type line: shapely.geometry.LineString

        :return: 点が線分上にあるかどうかの判定結果。
        :rtype: bool

        :raises Exception:
            交差判定中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 3Dの座標か確認
            is_point_3d = self.is_3d_geo(point)
            is_line_3d = self.is_3d_geo(line)

            # 2Dの座標に変換
            point_2d = transform(self.ignore_z, point)
            line_2d = transform(self.ignore_z, line)

            if point_2d.buffer(1e-9).intersects(line_2d.buffer(1e-9)):
                # 2Dで交差している場合
                if is_point_3d and is_line_3d:
                    # 3Dの座標であれば
                    intersection_z = self.interpolate_z(line, point_2d)
                    point_z = point.coords[0][2]

                    # 交点のZ座標が浮動小数点の誤差の範囲であれば
                    if abs(intersection_z - point_z) < 1e-6:
                        return True
                    else:
                        return False
                else:
                    # 2Dの座標で交差している場合
                    return True

            else:
                # 2Dで交差していなければ3Dでも交差していない
                return False

        except Exception as e:
            raise Exception(f"[check_3d_intersection_point_line_Exception]: {str(e)}")

    def split_lines(self, lines, threshold):
        """
        線分を交点で分割する。

        :param lines: 線分座標と、その線分に対応するFIDを持つ辞書のリスト。
        :type lines: numpy.ndarray
        :param threshold: 交点とみなす距離の閾値。
        :type threshold: float

        :return: 交点で分割された線分座標。
        :rtype: numpy.ndarray

        :raises Exception:
            線分の分割中にエラーが発生した場合に例外をスローする。
        """

        def check(line1, line2):
            """
            線分の交差チェック（閾値範囲内を交差とみなす）を行い、交差する点を線分の構成点に追加する。

            :param line1: １本目の線分
            :type line1: shapely.geometry.LineString
            :param line2: ２本目の線分
            :type line2: shapely.geometry.LineString

            :return: 交差チェックによる構成点追加処理後の１本目の線分と２本目の線分。
            :rtype: numpy.ndarray
            """
            line1_coordinate = np.array(line1.coords)  # 1つ目のLineStringの座標
            line2_coordinate = np.array(line2.coords)  # 2つ目のLineStringの座標

            line_coordinate_list = [line1_coordinate, line2_coordinate]

            # 線分が交差しているか、交差している場合はその座標を確認
            is_intersects, intersection_coordinates = self.check_3d_intersection_lines(
                line1, line2, threshold
            )

            if is_intersects:
                # 交差している箇所の座標でloop(点で重なっている場合は1周、線で重なっている場合は2周する)
                for point_coordinate in intersection_coordinates:
                    # 2本の線をLoop
                    for line_idx in range(len(line_coordinate_list)):
                        # 追加する座標が既に存在するか
                        exist_coordinate = any(
                            np.array_equal(point_coordinate, coordinate)
                            for coordinate in line_coordinate_list[line_idx]
                        )

                        # 追加する座標が線分上にあるか
                        intersection_point = Point(point_coordinate)
                        point_on_line = None
                        if line_idx == 0:
                            point_on_line = self.check_3d_intersection_point_line(
                                intersection_point, line1
                            )
                        else:
                            point_on_line = self.check_3d_intersection_point_line(
                                intersection_point, line2
                            )

                        # 追加する条件であれば
                        if (not exist_coordinate) and point_on_line:
                            # 始点の次に交点の座標を各線分に追加
                            new_line_coordinates = np.insert(
                                line_coordinate_list[line_idx],
                                1,
                                point_coordinate,
                                axis=0,
                            )

                            sorted_new_line_coordinate = new_line_coordinates[
                                np.lexsort(
                                    (
                                        new_line_coordinates[:, 1],
                                        new_line_coordinates[:, 0],
                                    )
                                )
                            ]

                            line_coordinate_list[line_idx] = sorted_new_line_coordinate

            return line_coordinate_list

        try:
            # 線分の本数
            num_lines = len(lines)

            # 線分の本数分Loop
            for line_1_idx in range(num_lines):
                line_1 = lines[line_1_idx]
                new_line_1_segments = []  # 分割後の１本目の線分を格納

                # １本目のラインを2点で構成される線分に分解して交差チェック
                for seg_1_idx in range(len(line_1) - 1):
                    line_1_segment = LineString(line_1[[seg_1_idx, seg_1_idx + 1]])

                    # ２本目の線分は1本目以降の線分をあてる
                    for line_2_idx in range(line_1_idx + 1, num_lines):
                        line_2 = lines[line_2_idx]
                        new_line_2_segments = []

                        # ２本目のラインを2点で構成される線分に分解して交差チェック
                        for seg_2_idx in range(len(line_2) - 1):
                            line_2_segment = LineString(
                                line_2[[seg_2_idx, seg_2_idx + 1]]
                            )

                            # １本目の線分の一部と、２本目の線分の一部の接触判定を行い、接触点を線分の構成点に追加
                            new_seg_1, new_seg_2 = check(line_1_segment, line_2_segment)

                            # 判定中の線分を更新する
                            line_1_segment = LineString(new_seg_1)
                            new_line_2_segments.append(new_seg_2)

                        modified_line2 = linemerge(
                            [LineString(line_seg) for line_seg in new_line_2_segments]
                        )
                        lines[line_2_idx] = np.array(
                            [coord for coord in modified_line2.coords]
                        )

                    # 全線分との交点が追加された1本目の線分の一部を、リストに格納する
                    new_line_1_segments.append(line_1_segment)

                new_line_1 = linemerge(new_line_1_segments)
                lines[line_1_idx] = np.array([coord for coord in new_line_1.coords])

            return lines

        except Exception as e:
            raise Exception(f"[split_lines_Exception]: {str(e)}")

    def convert_field_set_file_to_dataframe(self, field_set_file):
        """
        FieldSetFileのValue列をデシリアライズし、DataFrame型に変換する。

        :param field_set_file: ジオメトリ情報を持つFieldSetFile。
        :type field_set_file: bytes

        :return: DataFrame。
        :rtype: pandas.DataFrame

        :raises Exception:
            FieldSetFileのデシリアライズまたは変換中にエラーが発生した場合に例外をスローする。
        """
        try:
            # FieldSetFileをデコード
            field_set_file_decode = field_set_file.decode("utf-8")

            # FieldSetFileをpathとして扱う
            field_set_file_stringio = io.StringIO(field_set_file_decode)

            # DataFrameに加工
            dataframe = pd.read_csv(field_set_file_stringio)

            # Value列の値をデシリアライズ
            for i in range(len(dataframe)):
                dataframe.loc[i, "Value"] = CU.decode_value(dataframe.loc[i, "Value"])

            return dataframe
        except Exception as e:
            raise Exception(
                f"[convert_field_set_file_to_dataframe_Exception]: {str(e)}"
            )

    def extract_geometry_and_fid(self, dataframe, geometry_name):
        """
        指定したDataFrameからジオメトリの座標リストを作成する。

        :param dataframe: DataFrame。
        :type dataframe: pandas.DataFrame
        :param geometry_name: 操作対象ジオメトリデータを持つレコードのデータ名。
        :type geometry_name: str

        :return: ジオメトリリスト（線分）とFIDリスト。
        :rtype: tuple[list[shapely.geometry.LineString], list[tuple[str, float]]]

        :raises Exception:
            座標リストの作成中にエラーが発生した場合に例外をスローする。
        """
        try:
            # ジオメトリを取得
            # 対象データの存在チェック
            if (dataframe["Dwh"] == f"{geometry_name}/geometry").any():
                geometry_list = dataframe.loc[
                    dataframe["Dwh"] == f"{geometry_name}/geometry", "Value"
                ].values[0]
            else:
                raise ValueError(f"{geometry_name}/geometry does not exist.")

            # 対象データの存在チェック
            if (dataframe["Dwh"] == f"{geometry_name}/FID").any():
                fid_list = dataframe.loc[
                    dataframe["Dwh"] == f"{geometry_name}/FID", "Value"
                ].values[0]
            else:
                raise ValueError(f"{geometry_name}/FID does not exist.")

            return [np.array(geo.coords) for geo in geometry_list], fid_list

        except Exception as e:
            raise Exception(f"[extract_geometry_and_fid_Exception]: {str(e)}")

    def create_fsf(self, integrated_linestrings, integrated_fids, pairs_results, props):
        """
        FieldSetFileを作成する。

        :param integrated_linestrings: 統合処理後のジオメトリデータ（LineString）。
        :type integrated_linestrings: list[shapely.geometry.LineString]
        :param integrated_fids: 統合処理後の線分のFIDデータ。
        :type integrated_fids: list[tuple[str, float]]
        :param pairs_results: 統合前線分のFindexと統合後線分のFindexの紐づけ結果。左が統合後、右が統合前。
        :type pairs_results: list[tuple[float, float]]
        :param props: パラメーターの設定値を格納した辞書。
        :type props: dict

        :return: 出力用のFieldSetFile。
        :rtype: bytes

        :raises Exception:
            FieldSetFileの作成中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 処理結果のFieldSetFile生成用DataFrameを作成
            results_df = pd.DataFrame(
                {
                    "Dwh": [
                        f'{props["OUTPUT_GEOMETRY_NAME"]}/geometry',
                        f'{props["OUTPUT_GEOMETRY_NAME"]}/FID',
                        f'{props["OUTPUT_GEOMETRY_NAME"]}/results',
                    ],
                    "Type": ["geometry", "string", "results"],
                    "Value": [
                        integrated_linestrings,
                        integrated_fids,
                        pairs_results,
                    ],
                }
            )

            # DataFrameをFieldSetFileに変換
            field_set_file = CU.dataframe_to_field_set_file(results_df)

            return field_set_file

        except Exception as e:
            raise Exception(f"[create_fsf_Exception]: {str(e)}")

    def delete_duplicate_edges(self, sp_mtx):
        """
        疎行列から、重複しているエッジを削除する。

        :param sp_mtx: 隣接関係を持つ疎行列。
        :type sp_mtx: scipy.sparse.csr_matrix

        :return: DataFrame。
        :rtype: pandas.DataFrame

        :raises Exception:
            処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # エッジを持つノードリストを取得
            row, col = sp_mtx.nonzero()
            data = sp_mtx.data

            # 重複エッジの削除（エッジの重みを合計する）
            # 行と列のペアをソートして、順番を統一する
            pairs = np.vstack([row, col]).T

            # 新しい行と列を作成（重複を排除して合計する）
            unique_pairs, unique_indices = np.unique(pairs, axis=0, return_inverse=True)

            # 各ペアの重みを合計する
            unique_data = np.zeros(len(unique_pairs))
            for idx in unique_indices:
                unique_data[idx] += data[idx]

            # 新しい疎行列を作成
            new_sparse_matrix = csr_matrix(
                (unique_data, (unique_pairs[:, 0], unique_pairs[:, 1])),
                shape=(len(unique_data), len(unique_data)),
            )

            return new_sparse_matrix

        except Exception as e:
            raise Exception(f"[delete_duplicate_edges_Exception]: {str(e)}")

    def create_findex_pairs(
        self, int_geoms, int_fids, org_geoms, org_fids, dup_del_flg
    ):
        """
        統合前と統合後の線分のFindex紐づけ処理を行う。

        :param int_geoms: 統合後線分のジオメトリデータ。
        :type int_geoms: list[shapely.geometry.base.BaseGeometry]
        :param int_fids: 統合後線分のFIDデータ。
        :type int_fids: list[tuple[str, float]]
        :param org_geoms: 統合前線分のジオメトリデータ。
        :type org_geoms: list[shapely.geometry.base.BaseGeometry]
        :param org_fids: 統合前線分のFIDデータ。
        :type org_fids: list[tuple[str, float]]
        :param dup_del_flg: 重複削除フラグ。
        :type dup_del_flg: bool

        :return: 統合前の線分のFindexと統合後の線分のFindexを紐づけた結果。
        :rtype: list[tuple[float, float]]

        :raises Exception:
            処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # ジオメトリリストをGeoNdarrayに変換
            integrated_geo_ndarray = NSP.get_geometries_points_numpy(int_geoms)

            # 統合前線分のジオメトリリストをGeoNdarrayに変換
            original_geo_ndarray = NSP.get_geometries_points_numpy(org_geoms)

            # FIDのペアリストを生成する
            pairs_list = process_data(integrated_geo_ndarray, original_geo_ndarray, org_fids, int_fids)

            # 重複削除フラグがFalseの場合
            if dup_del_flg:
                # ペアリストからペアデータを生成する
                pairs_results = set(
                    [
                        (float(int_fidx), float(org_fidx))
                        for int_fidx, org_fidx_list in pairs_list
                        for org_fidx in org_fidx_list
                    ]
                )
            else:
                # １つの統合後線分に対し紐づけられた統合元線分のFindexが複数かつ、組合わせも同じ場合、１対１のペアにする
                pairs_groups = defaultdict(list)
                # 右辺のリストをキーにしてグループ化
                for key, value in pairs_list:
                    pairs_groups[tuple(value)].append(float(key))
                # 紐づけられた統合前線分のFindexの組合せごとに、統合後線分のFindexをグルーピングする
                duplicates_pairs = [
                    (list(keys), list(values)) for values, keys in pairs_groups.items()
                ]
                # グルーピング結果に基づいてペアデータを生成
                pairs_data = []
                for pairs in duplicates_pairs:
                    if len(pairs[0]) == len(pairs[1]):
                        # 左右の個数が同じ場合
                        pair = zip(pairs[0], pairs[1])
                    elif len(pairs[0]) > len(pairs[1]):
                        # 統合後Findexの方が個数が多い場合（１つの線分が１つに分割されたときなど）
                        pair = [(int_fidx, pairs[1][0]) for int_fidx in pairs[0]]
                    elif len(pairs[1]) > len(pairs[0]):
                        # 統合前Findexの方が個数が多い場合（１つの線分が１つに統合されたときなど）
                        pair = [(pairs[0][0], org_fidx) for org_fidx in pairs[1]]
                    else:
                        continue
                    # ペアデータをリストに追加
                    pairs_data.append(pair)

                # ペアデータからペアリストを作成
                pairs_results = set([pair for pairs in pairs_data for pair in pairs])

            return sorted(pairs_results)

        except Exception as e:
            raise Exception(f"[create_findex_pairs_Exception]: {str(e)}")

    def create_fsf_values(self, fsf_df, props):
        """
        FieldSetFileのDataFrameから、出力用のFieldSetFileのValue列の値を作成する。

        :param fsf_df: FieldSetFileをDataFrameに変換したもの。
        :type fsf_df: pandas.DataFrame
        :param props: プロパティ情報。
        :type props: dict

        :return: 出力用FieldSetFileのValue列の値。
        :rtype: tuple[list[dict[str, Union[shapely.geometry.LineString, tuple[str, float]]], list[tuple[float, float]]]

        :raises Exception:
            FieldSetFileのValue列の作成中にエラーが発生した場合に例外をスローする。
        """

        def asBoolean(pros_val):
            """
            プロパティ設定値の「True」「False」を文字列型からbool型に変換する。

            :param pros_val: 変換対象の文字列
            :type pros_val: str

            :return: bool型に変換されたプロパティ設定値
            :rtype: bool or None
            """
            if pros_val is None:
                return None
            return pros_val.lower() == "true"

        try:
            # 真偽値を持つプロパティ値をbool型に変換
            split_flg = asBoolean(props["SPLIT"])
            delete_duplicates_flg = asBoolean(props["DELETE_DUPLICATES"])

            # 入力データから操作対象のジオメトリデータ（座標配列形式）とFID情報を取得
            coords_list, original_fid_list = self.extract_geometry_and_fid(
                fsf_df, props["INPUT_GEOMETRY_NAME"]
            )

            # プロパティで分割するよう設定されていれば交点で分割する
            if split_flg:
                # 分割点を含む座標配列で座標情報を更新
                coords_list = self.split_lines(coords_list, self.THRESHOLD)

            # 線分を２点で構成される線分に再構成
            line_segments = self.decompose_linestrings(coords_list)

            # 全ラインから疎行列を作成し、ノード情報を取得
            sparse_matrix, node_coordinates, point_before_after = (
                self.build_sparse_matrix(line_segments, self.THRESHOLD)
            )

            # 重複を許容しない場合、疎行列から重複するエッジを削除する
            if delete_duplicates_flg:
                sparse_matrix = self.delete_duplicate_edges(sparse_matrix)

            # 分岐のない線分を1つにまとめたリストにする(ポリラインのノードのリストを作成)
            polyline_list, sparse_matrix = self.create_polyline_list(
                sparse_matrix, delete_duplicates_flg
            )

            # 未使用エッジリストの抽出
            unused_edges = self.get_unused_edges(
                polyline_list, sparse_matrix, False, delete_duplicates_flg
            )

            # 未使用エッジからサイクルを形成するノードのリストを取得
            if len(unused_edges) != 0:
                cycle_path_list, cycle_node_list = self.process_geometry(unused_edges, node_coordinates)
            else:
                cycle_path_list = []
                cycle_node_list = []

            # サイクルのポリラインを追加
            polyline_list = polyline_list + cycle_path_list

            # サイクルを形成するエッジを疎行列から削除し、未使用エッジを取得
            unused_edges = self.get_unused_edges(
                cycle_node_list, sparse_matrix, True, delete_duplicates_flg
            )

            # 未使用エッジのポリラインを追加
            polyline_list = polyline_list + unused_edges

            # 統合後線分のジオメトリデータを作成
            integrated_geom_list = [
                LineString(
                    [Point(node_coordinates[nodes[idx]]) for idx in range(len(nodes))]
                )
                for nodes in polyline_list
            ]
            # 統合後線分のFIDデータを作成
            integrated_fid_list = [
                (str(fidx), float(fidx)) for fidx in range(len(integrated_geom_list))
            ]

            # 統合処理によって変更された座標を、統合前線分に対し差し替える
            original_geom_list = [
                LineString(
                    np.array(
                        [
                            point_before_after.get(tuple(coord), coord)
                            for coord in coords
                        ]
                    )
                )
                for coords in coords_list
            ]

            # 統合処理前後の線分のFindexペアを生成する
            findex_pairs = self.create_findex_pairs(
                integrated_geom_list,
                integrated_fid_list,
                original_geom_list,
                original_fid_list,
                delete_duplicates_flg,
            )

            return integrated_geom_list, integrated_fid_list, findex_pairs

        except Exception as e:
            raise Exception(f"[create_fsf_values_Exception]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: 入力としてのCSV形式のFieldSetFileのバイトデータ
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のFieldSetFileと、更新された属性情報を含むタプル。
        :rtype: tuple[bytes, dict]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # FieldSetFileのValue列をデシリアライズしDataFrame型に変換
            fsf_df = CU.field_set_file_to_dataframe(byte_data)

            # 線分の統合処理を行い、処理結果のFieldSetFileのValue列に格納するデータを生成する
            # 統合処理後線分のジオメトリデータ、統合処理後線分のFIDデータ、統合処理前後のFIDのペアリングデータ
            integrated_geoms, integrated_fids, integrated_pairs = (
                self.create_fsf_values(fsf_df, properties)
            )

            # 出力するFieldSetFileの作成
            out_fsf = self.create_fsf(
                integrated_geoms, integrated_fids, integrated_pairs, properties
            )

            return out_fsf, attribute

        except Exception as e:
            raise Exception(f"[__call___Exception]: {str(e)}")
