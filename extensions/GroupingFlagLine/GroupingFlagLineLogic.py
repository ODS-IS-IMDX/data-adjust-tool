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

from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
geometry = import_module("shapely.geometry")
LineString = geometry.LineString
Point = geometry.Point
Polygon = geometry.Polygon
MultiPoint = geometry.MultiPoint
sparse = import_module("scipy.sparse")
csr_matrix = getattr(sparse, "csr_matrix")
csgraph = import_module("scipy.sparse.csgraph")
connected_components = getattr(csgraph, "connected_components")

# Nifiライブラリ
import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class GroupingFlagLineLogic:
    def __init__(self):
        pass

    def get_params(self, properties):
        """
        プロパティの値を取得し、辞書にして返す

        :param properties: 各プロパティの値を含む辞書。
        :type properties: dict[str,str]

        :return: プロパティの属性値を辞書形式で返す。
        :rtype: dict[str,str]

        :raises ValueError: toleranceプロパティが負の値の場合に発生する。
        :raises Exception: プロパティの取得処理でエラーが発生した場合に発生する。
        """
        try:
            params = {}

            # プロパティの値を取得
            params["geo_name"] = properties["GEOMETRY_NAME"]
            params["main"] = properties["MAIN_RESULTS_NAME"]
            params["sub"] = properties["SUB_RESULTS_NAME"]
            params["grp"] = properties["GROUP_RESULTS_NAME"]
            params["suffix"] = properties["SUFFIX"]
            params["tolerance"] = CU.get_number_from_string(properties["TOLERANCE"])
            if params["tolerance"] < 0:
                raise ValueError(f"Unsupported tolerance: {params['tolerance']}")

            return params
        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def grouping_tuple_list(self, tuple_list):
        """
        タプルのリストを変換し、連結したノードをグループ化する

        :param tuple_list: タプルのリスト。各タプルは（ノード, 隣接ノードリスト）を表す。
        :type tuple_list: list[tuple[float, list[float]]]

        :return: 連結されたノードをグループ化したリスト。
        :rtype: list[list[float]]

        :raises Exception: グルーピング処理中にエラーが発生した場合に発生する。
        """
        try:
            # ノードリストの作成
            nodes = sorted({x[0] for x in tuple_list})
            node_index = {node: i for i, node in enumerate(nodes)}

            # 隣接行列を作成
            row = []
            col = []
            for src, neighbors in tuple_list:
                if neighbors is not None:
                    for neighbor in neighbors:
                        row.append(node_index[src])
                        col.append(node_index[neighbor])

            # グラフを疎行列（CSR）形式で表現
            n = len(nodes)
            adj_matrix = csr_matrix((np.ones(len(row)), (row, col)), shape=(n, n))

            # 連結成分を計算
            n_components, labels = connected_components(
                csgraph=adj_matrix, directed=False
            )

            # グループ化した結果
            fidxs_list = [[] for _ in range(n_components)]
            for node, label in zip(nodes, labels):
                fidxs_list[label].append(node)

            return fidxs_list
        except Exception as e:
            raise Exception(f"[grouping_tuple_list]: {str(e)}")

    def get_values_from_dataframe(self, dataframe, params):
        """
        DataFrameからDwh列の接尾語をキーに、Value列を値とした辞書を返す。

        :param dataframe: FieldSetFileのDataFrame。Dwh列とValue列を含む必要がある。
        :type dataframe: pandas.DataFrame
        :param params: プロパティの値を格納した辞書。geo_name、main、sub、grpなどの値を含む。
        :type params: dict[str,str|float]

        :return: Dwh列の接尾語をキーにして、対応するValue列の値を持つ辞書。
        :rtype: dict[str, list]

        :raises Exception: データ取得中にエラーが発生した場合に例外を送出する。
        """
        try:
            suffix_list = [
                params["main"],
                params["sub"],
                params["grp"],
                "FID",
                "geometry",
                f'{params["main"]}_Cross',
            ]

            # FieldSetFileのDwhの接尾語をkey、値をvalueとした辞書
            return {
                suffix: dataframe.loc[
                    dataframe["Dwh"] == f'{params["geo_name"]}/{suffix}', "Value"
                ].values[0]
                for suffix in suffix_list
            }

        except Exception as e:
            raise Exception(f"[get_values_from_dataframe]: {str(e)}")

    def convert_tuple_to_dict(self, tuple_list):
        """
        タプルのリストをキーと値の辞書形式に変換する。

        :param tuple_list: タプルのリスト。各タプルはキーと値のペアを表す。
        :type tuple_list: list[tuple[float, shapely.geometry.base.BaseGeometry]]

        :return: キーとそれに対応する値のリストを持つ辞書。
        :rtype: collections.defaultdict

        :raises Exception: 処理中にエラーが発生した場合に例外を送出する。
        """
        try:
            grouped_dict = defaultdict(list)

            # タプルの2つ目の値をグループ化
            for key, value in tuple_list:
                grouped_dict[key].append(value)

            return grouped_dict
        except Exception as e:
            raise Exception(f"[convert_tuple_to_dict]: {str(e)}")

    def add_pairs(self, original_pairs, pairs):
        """
        元の対になっている値のリストに追加の組み合わせを追加する。

        :param original_pairs: 元の対になっている値のリスト。各タプルはキーと隣接する値のリストを持つ。
        :type original_pairs: list[tuple[float, list[float]]]
        :param pairs: 追加するFindexの組み合わせ。
        :type pairs: list[tuple[float, float]]

        :return: 元のリストに追加の値が追加されたもの。
        :rtype: list[tuple[float, list[float]]]

        :raises Exception: 処理中にエラーが発生した場合に例外を送出する。
        """
        try:
            # 元の対になっている値のリストを辞書形式に変換
            org_pairs_dict = {
                node: (neighbors if neighbors is not None else [])
                for node, neighbors in original_pairs
            }

            # 追加の組み合わせを追加
            for main_fidx, sub_fidx in pairs:
                # main_fidxの隣接リストにsub_fidxを追加し、重複を避けるためにリスト内でソート
                if sub_fidx not in org_pairs_dict[main_fidx]:
                    org_pairs_dict[main_fidx].append(sub_fidx)
                    org_pairs_dict[main_fidx].sort()  # 数値順にソート

                # sub_fidxの隣接リストにmain_fidxを追加し、重複を避けるためにリスト内でソート
                if sub_fidx not in org_pairs_dict:
                    org_pairs_dict[sub_fidx] = [main_fidx]
                else:
                    if main_fidx not in org_pairs_dict[sub_fidx]:
                        org_pairs_dict[sub_fidx].append(main_fidx)
                        org_pairs_dict[sub_fidx].sort()  # 数値順にソート

            return [
                (fidx, None) if not neighbors else (fidx, neighbors)
                for fidx, neighbors in org_pairs_dict.items()
            ]

        except Exception as e:
            raise Exception(f"[add_pairs]: {str(e)}")

    def create_fsf(self, fsf_value, params):
        """
        FieldSetFileを作成する。

        :param fsf_value: FieldSetFileのValue。
        :type fsf_value: list[tuple[float, list[float]]]
        :param params: プロパティの値の辞書。
        :type params: dict

        :return: FieldSetFile
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外を送出する。
        """
        try:
            # FieldSetFileのDwh
            dwh_file_name_list = [f'{params["geo_name"]}/{params["suffix"]}']

            # FieldSetFileのType
            field_type_list = ["results"]

            # FieldSetFileのValue
            field_value_list = [CU.encode_value(fsf_value)]

            # FieldSetFileの形式に変換
            return NSP.get_field_set_file(
                field_type_list, field_value_list, dwh_file_name_list
            )

        except Exception as e:
            raise Exception(f"[create_fsf]: {str(e)}")

    def get_straightest_pairs(self, main_2endpoints, sub_2endpoints, tolerance):
        """
        2点のPointのリストを総当たりで繋げて最も直線となる組み合わせを返す。

        :param main_2endpoints: メインの2点のPointのリスト。
        :type main_2endpoints: list[tuple[float, shapely.geometry.Point]]
        :param sub_2endpoints: サブの2点のPointのリスト。
        :type sub_2endpoints: list[tuple[float, shapely.geometry.Point]]
        :param tolerance: メインとサブをグループ化する距離の閾値。
        :type tolerance: float

        :return: 2点のPointのリストを総当たりで繋げて最直線となる組み合わせ。
        :rtype: list[tuple[float, float]]

        :raises Exception: 処理中にエラーが発生した場合に例外を送出する。
        """
        try:

            def calc_vector(p1, p2):
                """
                2つのポイント間のベクトルを算出する。

                :param p1: 1つ目のポイント。
                :type p1: shapely.geometry.Point
                :param p2: 2つ目のポイント。
                :type p2: shapely.geometry.Point

                :return: 2つのポイント間のベクトル。
                :rtype: numpy.ndarray
                """
                return np.array([p2.x - p1.x, p2.y - p1.y])

            def angle_between(v1, v2):
                """
                2つのベクトル間の角度を算出する。

                :param v1: 1つ目のベクトル。
                :type v1: numpy.ndarray
                :param v2: 2つ目のベクトル。
                :type v2: numpy.ndarray

                :return: 2つのベクトル間の角度（ラジアン単位）。
                :rtype: float
                """
                norm_v1 = np.linalg.norm(v1)
                norm_v2 = np.linalg.norm(v2)

                # ノルムがゼロでないか確認
                if norm_v1 == 0 or norm_v2 == 0:
                    return float("nan")

                dot_product = np.dot(v1, v2)
                norms = norm_v1 * norm_v2
                cos_theta = dot_product / norms

                # cos_thetaを-1から1の範囲にクリップする
                cos_theta = np.clip(cos_theta, -1.0, 1.0)

                return np.arccos(cos_theta)

            pairs = []

            # メインの2点をループ
            for main_fidx, main_points in main_2endpoints:
                # メインの2点間のベクトル
                vector_main = calc_vector(main_points[1], main_points[0])

                # メインとメインとサブの間とサブを線分とした角度
                angles = []

                # サブの2点をループ
                for sub_fidx, sub_points in sub_2endpoints:
                    distance_between = main_points[0].distance(sub_points[0])

                    # メインとサブの距離が閾値以内であれば
                    if distance_between <= tolerance:
                        # メインとサブの間のベクトル
                        vector_main_sub = calc_vector(main_points[0], sub_points[0])

                        # サブの2点間のベクトル
                        vector_sub = calc_vector(sub_points[0], sub_points[1])

                        # メインとメインとサブの間を線分とした角度
                        angle_main_subend = angle_between(vector_main, vector_main_sub)

                        # メインとサブの間とサブを線分とした角度
                        angle_mainend_sub = angle_between(vector_main_sub, vector_sub)

                        angles.append(
                            (
                                sub_fidx,
                                (angle_main_subend + angle_mainend_sub),
                                distance_between,
                            )
                        )

                # 閾値内の距離にサブがあれば
                if angles:
                    # 最小の角度
                    min_value = min(t[1] for t in angles)

                    # 最小の角度のFindexとサブまでの距離
                    min_findex = [(t[0], t[2]) for t in angles if t[1] == min_value]

                    # 最小の角度の中で最も距離の近いもの
                    min_distance = min(t[1] for t in min_findex)

                    # 最小の角度の中で最も距離の近いもののFindex
                    pairs_fidx = [t[0] for t in min_findex if t[1] == min_distance][0]

                    pairs.append((main_fidx, pairs_fidx))

            return pairs
        except Exception as e:
            raise Exception(f"[get_straightest_pairs]: {str(e)}")

    def get_2points_of_line(self, line, point):
        """
        指定されたPointから最も遠いLineStringの端点と、その次の座標をPointオブジェクトとして返す。

        :param line: 端点と座標を取得する対象のLineString。
        :type line: shapely.geometry.LineString
        :param point: 距離を比較するための基準となるポイント。
        :type point: shapely.geometry.Point

        :return: 指定されたPointから遠い方のLineStringの端点と、その次の座標をPointとして返す。
        :rtype: tuple[shapely.geometry.Point, shapely.geometry.Point]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 始点
            start_point = Point(line.coords[0])
            # 終点
            end_point = Point(line.coords[-1])

            # pointからの距離が始点の方が近ければ
            if point.distance(start_point) < point.distance(end_point):
                # 始点と始点の次の点を返す
                return end_point, Point(line.coords[-2])
            else:
                # 終点と終点の1つ前を返す
                return start_point, Point(line.coords[1])
        except Exception as e:
            raise Exception(f"[get_2points_of_line]: {str(e)}")

    def get_endpoints(self, main_line, sub_line):
        """
        メインのラインの端点に対するサブのラインの端点の距離からサブの端点から最も離れているメインの端点とその次の座標をポイントにしたものを取得

        :param main_line: メインのライン
        :type main_line: shapely.geometry.LineString
        :param sub_line: サブのライン
        :type sub_line: shapely.geometry.LineString

        :return: メインの端点とその次の座標のポイント
        :rtype: tuple[shapely.geometry.Point, shapely.geometry.Point]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            min_distance = float("inf")
            min_main_line_idx = None
            for main_line_idx in [0, -1]:
                main_line_point = Point(main_line.coords[main_line_idx])
                for sub_line_idx in [0, -1]:
                    sub_line_point = Point(sub_line.coords[sub_line_idx])
                    points_distance = main_line_point.distance(sub_line_point)
                    if points_distance < min_distance:
                        min_distance = points_distance
                        min_main_line_idx = main_line_idx
            if min_main_line_idx == 0:
                return Point(main_line.coords[-1]), Point(main_line.coords[-2])
            else:
                return Point(main_line.coords[0]), Point(main_line.coords[1])
        except Exception as e:
            raise Exception(f"[get_endpoints]: {str(e)}")

    def get_additional_pairs(self, suffix_dict, params):
        """
        メインの交点ではない側の端部を延長し、サブの端部と最も直線的に繋がる組み合わせを返す。

        :param suffix_dict: FieldSetFileのDwhの接尾語をKey:、値をValueとした辞書。
        :type suffix_dict: dict[str, list]
        :param params: プロパティの値の辞書。
        :type params: dict[str, str|float]

        :return: メインのFindexとサブのFindexの組み合わせを返す。
        :rtype: list[tuple[float, float]]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # Main Results Nameの値のtupleの1つ目の値を一意にしたもの
            main_fidxs_all = list(set(fidx for fidx, _ in suffix_dict[params["main"]]))

            # Sub Results Nameの値を取得(tupleの1つ目の値を一意にしたもの)
            sub_fidxs_all = list(
                set(
                    fidx
                    for fidx, pair_fidx in suffix_dict[params["sub"]]
                    if not pair_fidx or not np.isnan(pair_fidx)
                )
            )

            # Key:FID Value:geometry
            fid_geo_dict = {
                key[1]: value
                for key, value in zip(suffix_dict["FID"], suffix_dict["geometry"])
            }

            # Group Results Nameの値をリストにまとめる
            grp_fidxs = self.grouping_tuple_list(suffix_dict[params["grp"]])

            # メインとサブが紐づかなかったもの
            unpairs_fidxs_all = [
                fidxs
                for fidxs in grp_fidxs
                if not (
                    any(x in main_fidxs_all for x in fidxs)
                    and any(x in sub_fidxs_all for x in fidxs)
                )
            ]

            # 端部のFindex
            end_fidxs = [
                fidx
                for fidx, fidxs in suffix_dict[params["grp"]]
                if (fidxs is None) or (len(fidxs) < 2)
            ]

            # Findexをキーにした辞書に変換
            cross_points_dict = self.convert_tuple_to_dict(
                suffix_dict[f'{params["main"]}_Cross']
            )
            # Group Results Nameの値を辞書に変換
            grp_dict = {key: value for key, value in suffix_dict[params["grp"]]}

            # メインとサブの端部2点のリストのリスト
            both_two_endpoints = []

            # メインとサブをループ
            for main_or_sub_fidxs_all in [main_fidxs_all, sub_fidxs_all]:
                # メインとサブが紐づかなかったもののループ中の側のうち端部
                unpairs_end_fidxs = [
                    fidx
                    for fidxs_list in unpairs_fidxs_all
                    if any(fidx in main_or_sub_fidxs_all for fidx in fidxs_list)
                    for fidx in fidxs_list
                    if fidx in end_fidxs
                ]

                # ループ中の側の端部2点のリスト
                two_endpoints = []

                # メインとサブが紐づかなかったもののループ中の側のうち端部をループ
                for end_fidx in unpairs_end_fidxs:
                    # 端部のジオメトリ
                    end_geo = fid_geo_dict[end_fidx]

                    # 端部と接しているFindex
                    connect_fidxs = grp_dict[end_fidx]

                    # ループ中のFindexと接しているものがなければ
                    if connect_fidxs is None:
                        # ループ中のFindexに交点があり、交点の座標が1つであれば
                        if (
                            end_fidx in cross_points_dict
                            and len(cross_points_dict[end_fidx]) == 1
                        ):
                            # 交点ではない側の端部2点を取得
                            endpoint, second_endpoint = self.get_2points_of_line(
                                end_geo, cross_points_dict[end_fidx][0]
                            )
                            two_endpoints.append(
                                (end_fidx, (endpoint, second_endpoint))
                            )
                        elif end_fidx not in cross_points_dict:
                            # 交点がなければ両側
                            two_endpoints.append(
                                (
                                    end_fidx,
                                    (
                                        Point(end_geo.coords[0]),
                                        Point(end_geo.coords[1]),
                                    ),
                                )
                            )
                            two_endpoints.append(
                                (
                                    end_fidx,
                                    (
                                        Point(end_geo.coords[-1]),
                                        Point(end_geo.coords[-2]),
                                    ),
                                )
                            )
                    else:
                        if end_fidx not in cross_points_dict:
                            # 交点がなければ
                            endpoint, second_endpoint = self.get_endpoints(
                                end_geo, fid_geo_dict[connect_fidxs[0]]
                            )
                            two_endpoints.append(
                                (end_fidx, (endpoint, second_endpoint))
                            )
                both_two_endpoints.append(two_endpoints)

            # メインとサブを総当たりで繋げて最も直線となる組み合わせ
            return self.get_straightest_pairs(
                both_two_endpoints[0], both_two_endpoints[1], params["tolerance"]
            )
        except Exception as e:
            raise Exception(f"[get_additional_pairs]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: 入力としてのCSV形式のFieldSetFileのバイトデータ
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict[str,str]
        :param properties: プロパティ情報を含む辞書
        :type properties: dict[str,str]

        :return: 出力用のFieldSetFileと、更新された属性情報を含むタプル
        :rtype: tuple[str, dict]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティの値の辞書
            params = self.get_params(properties)

            # CSV形式のFieldSetFileをDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(byte_data)

            # FieldSetFileのDwhの接尾語をkey、値をvalueとした辞書
            suffix_dict = self.get_values_from_dataframe(dataframe, params)

            # 離れているメインとサブの最も直線的に繋がる組み合わせ
            pairs = self.get_additional_pairs(suffix_dict, params)

            # Group Results Nameの値にpairsの値を追加
            fsf_output_value = self.add_pairs(suffix_dict[params["grp"]], pairs)

            # 出力用のFieldSetFileを作成
            output_fsf = self.create_fsf(fsf_output_value, params)

            return output_fsf, attribute

        except Exception as e:
            raise Exception(f"[__call__]: {str(e)}")
