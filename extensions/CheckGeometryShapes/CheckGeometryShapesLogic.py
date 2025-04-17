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
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
gpd = import_module("geopandas")
pd = import_module("pandas")
Point = getattr(import_module("shapely.geometry"), "Point")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
LineString = getattr(import_module("shapely.geometry"), "LineString")

import cad.common.cad_utils as CU


class CheckGeometryShapesLogic:

    def get_geometry_from_field_set_file(self, contents, filename):
        """
        FieldSetFileからGeoNdarrayを取得する。

        :param contents: FieldSetFileのバイトデータ
        :type contents: bytes
        :param filename: 処理対象のファイル名。ログ出力のために使用。
        :type filename: str

        :return: GeoNdarray
        :rtype: numpy.ndarray

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # FlowFileからFieldSetFileを取得し、DataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(contents)
            if not len(dataframe) > 0:
                raise ValueError(f"Invalid input data: {filename}")

            # DataFrameからジオメトリデータのみを取得し、元のデータ構造に復元
            if (dataframe["Type"] == "geometry").any():
                serialized_geo_ndarray = dataframe[dataframe["Type"] == "geometry"][
                    "Value"
                ].tolist()[0]
            else:
                return None

            return serialized_geo_ndarray

        except Exception as e:
            raise Exception(
                f"[get_geometry_and_dataframe_from_field_set_file]:{str(e)}"
            )


    def is_valid_polygon(self, vertices):
        """
        有効なポリゴンに変換できるかどうかをチェックする。

        :param vertices: ポリゴン
        :type vertices: shapely.geometry.Polygon

        :return: 有効なポリゴンに変換できる場合はTrue、そうでない場合はFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            polygon = Polygon(vertices)
            return polygon.is_valid
        except Exception as e:
            raise Exception(f"[is_valid_polygon_Exception]:{e}")

    def check_self_intersection(self, geometry):
        """
        自己交差の検出

        :param geometry: 対象ポリゴンまたはライン
        :type geometry: shapely.geometry.Polygon or shapely.geometry.LineString

        :return: 交差していればTrue、していなければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            if geometry.is_simple:
                return False
            else:
                return True
        except Exception as e:
            raise Exception(f"[check_self_intersection_Exception]:{e}")

    def check_duplicate_nodes(self, polygon):
        """
        ノードの重複チェック

        :param polygon: 対象ポリゴン
        :type polygon: shapely.geometry.Polygon

        :return: 重複していればTrue、していなければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            coords = list(polygon.exterior.coords)
            interior_coords = coords[:-1]
            return len(interior_coords) != len(set(interior_coords))
        except Exception as e:
            raise Exception(f"[check_duplicate_nodes_Exception]:{e}")

    def check_closed_polygon(self, polygon):
        """
        ポリゴンの閉合性の確認

        :param polygon: 対象ポリゴン
        :type polygon: shapely.geometry.Polygon

        :return: 閉じていなければTrue、閉じていればFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            return polygon.is_closed
        except Exception as e:
            raise Exception(f"[check_closed_polygon_Exception]:{e}")

    def check_non_zero_area(self, polygon):
        """
        ポリゴンの非ゼロ面積の確認

        :param polygon: 対象ポリゴン
        :type polygon: shapely.geometry.Polygon

        :return: 面積が0以下ならTrue、0より大きければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            return not polygon.area > 0
        except Exception as e:
            raise Exception(f"[check_non_zero_area_Exception]:{e}")

    def check_vertex_order(self, polygon):
        """
        ポリゴンの頂点順序の確認

        :param polygon: 対象ポリゴン
        :type polygon: shapely.geometry.Polygon

        :return: 時計回りでなければTrue、時計回りあればFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            return polygon.exterior.is_ccw
        except Exception as e:
            raise Exception(f"[check_vertex_order_Exception]:{e}")

    def check_convexity(self, polygon):
        """
        ポリゴンの凸性の確認

        :param polygon: 対象ポリゴン
        :type polygon: shapely.geometry.Polygon

        :return: 凸型ならTrue、凸型でなければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            coords = np.array(polygon.exterior.coords)

            # 頂点の数が8未満の場合、凸型ではない
            if len(coords) < 8:
                return False

            x_max = -float("inf")
            x_min = float("inf")
            y_max = -float("inf")
            y_min = float("inf")

            # ポリゴンの端点4つを取得し四角形のポリゴンを形成する
            for coord in coords:
                if x_max < coord[0]:
                    x_max = coord[0]
                    x_max_pt = coord
                elif x_max == coord[0]:
                    if x_max_pt[1] < coord[1]:
                        x_max_pt = coord
                if x_min > coord[0]:
                    x_min = coord[0]
                    x_min_pt = coord
                elif x_min == coord[0]:
                    if x_min_pt[1] > coord[1]:
                        x_min_pt = coord
                if y_max < coord[1]:
                    y_max = coord[1]
                    y_max_pt = coord
                elif y_max == coord[1]:
                    if y_max_pt[0] < coord[0]:
                        y_max_pt = coord
                if y_min > coord[1]:
                    y_min = coord[1]
                    y_min_pt = coord
                elif y_min == coord[1]:
                    if y_min_pt[0] > coord[0]:
                        y_min_pt = coord

            if np.array_equal(x_min_pt, y_min_pt):
                square = Polygon(
                    [(x_min, y_min), (x_max, y_min), (x_max, y_max), (x_min, y_max)]
                )
            else:
                square = Polygon([(x_max_pt), (y_max_pt), (x_min_pt), (y_min_pt)])

            # 生成したポリゴンと元のポリゴンの差分領域を取得
            remaining_area = square.difference(polygon)

            # 差分領域が単一のポリゴンでない場合、凸型ポリゴンと判定
            if remaining_area.geom_type == "MultiPolygon":
                return True

            return False
        except Exception as e:
            raise Exception(f"[check_convexity_Exception]:{e}")

    def is_valid_line(self, vertices):
        """
        有効なラインに変換できるかどうかをチェックする。

        :param vertices: ライン
        :type vertices: shapely.geometry.LineString

        :return: 有効なラインに変換できる場合はTrue、そうでない場合はFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            linestring = LineString(vertices)
            return linestring.is_valid
        except Exception as e:
            raise Exception(f"[is_valid_line_Exception]:{e}")

    def check_duplicate_line_nodes(self, line):
        """
        ノードの重複チェック

        :param line: 対象ライン
        :type line: shapely.geometry.LineString

        :return: 重複していればTrue、していなければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            coords = list(line.coords)
            return len(coords) != len(set(coords))
        except Exception as e:
            raise Exception(f"[check_duplicate_line_nodes_Exception]:{e}")

    def check_line_intersections(self, line, other_lines):
        """
        線分の交差チェック

        :param line: 対象ライン
        :type line: shapely.geometry.LineString
        :param other_lines: 全てのライン
        :type other_lines: list[shapely.geometry.LineString]

        :return: 交差していればTrue、していなければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            for other_line in other_lines:
                if line.crosses(other_line):
                    return True
            return False
        except Exception as e:
            raise Exception(f"[check_line_intersections_Exception]:{e}")

    def check_continuity(self, line):
        """
        ラインの連続性の確認

        :param line: 対象ライン
        :type line: shapely.geometry.LineString

        :return: 連続していればTrue、していなければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            coords = list(line.coords)
            for i in range(len(coords) - 1):
                if coords[i] == coords[i + 1]:
                    return True
            return False
        except Exception as e:
            raise Exception(f"[check_continuity_Exception]:{e}")

    def check_point_distances(self, line, permission_distance):
        """
        頂点間の距離のチェック

        :param line: 対象ライン
        :type line: shapely.geometry.LineString
        :param permission_distance: 指定距離
        :type permission_distance: float

        :return: 指定した距離以下であればTrue、以上であればFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            coords = list(line.coords)
            for i in range(len(coords) - 1):
                # ラインのポイント同士の距離が対象距離以下かどうか
                distance = np.linalg.norm(np.array(coords[i]) - np.array(coords[i + 1]))
                if not (0 <= distance <= permission_distance):
                    return False
            return True
        except Exception as e:
            raise Exception(f"[check_point_distances_Exception]:{e}")

    def check_duplicate_points(self, points):
        """
        ポイントの重複チェック

        :param points: 全ポイント
        :type points: numpy.ndarray

        :return: 重複していればTrue、していなければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            for point in points:
                for chk_point in points:
                    duplicate = 0
                    if point == chk_point:
                        duplicate += 1
                        if duplicate >= 2:
                            return True
            return False
        except Exception as e:
            raise Exception(f"[check_duplicate_points_Exception]:{e}")

    def check_geometry_shapes(
        self, geometries, geom_type, filename="", permission_distance=0.0, tolerance=0.0
    ):
        """
        Polygon、LineStringおよびPointの形状チェックを行う関数。

        :param geometries: Polygon、LineStringおよびPointの座標を含むndarrayのリスト
        :type geometries: numpy.ndarray
        :param geom_type: チェックするジオメトリのタイプ ('Polygon', 'LineString', 'Point')
        :type geom_type: str
        :param filename: 対象のファイルネーム
        :type filename: str
        :param permission_distance: LineStringの頂点間距離を求める際の許容距離
        :type permission_distance: float
        :param tolerance: 凸型ポリゴンを検出する際にポリゴンに対し座標の簡略化を行う数値。
        :type tolerance: float

        :return: エラーが1つでもあればTrue、なければFalse
        :rtype: bool

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            permission_distance = permission_distance
            tolerance = tolerance
            check_geometry = False
            if geom_type == "Polygon":
                polygons = [Polygon(coords) for coords in geometries]
                for polygon in polygons:
                    set_message = [
                        str(filename) + " ポリゴン重心座標:" + str(polygon.centroid)
                    ]
                    check_polygon = False
                    if not self.is_valid_polygon(polygon):
                        check_geometry = True
                        check_polygon = True
                        set_message.append("ポリゴンとして正しくありません。")
                    if self.check_self_intersection(polygon):
                        check_geometry = True
                        check_polygon = True
                        set_message.append("交差しています。")
                    if self.check_closed_polygon(polygon):
                        check_geometry = True
                        check_polygon = True
                        set_message.append("ポリゴンが閉じられていません。")
                    if self.check_non_zero_area(polygon):
                        check_geometry = True
                        check_polygon = True
                        set_message.append("ポリゴンの面積が0です。")
                    if self.check_vertex_order(polygon):
                        check_geometry = True
                        check_polygon = True
                        set_message.append("ポリゴンの頂点順序が正しくありません。")
                    if self.check_duplicate_nodes(polygon):
                        check_geometry = True
                        check_polygon = True
                        set_message.append("ノードが重複しています。")
                    if self.check_convexity(
                        polygon.simplify(tolerance, preserve_topology=True)
                    ):
                        check_geometry = True
                        check_polygon = True
                        set_message.append("ポリゴンが凸型です。")
                    if check_polygon:
                        raise Exception(str(set_message))
            elif geom_type == "LineString":
                lines = [LineString(coords) for coords in geometries]
                for line in lines:
                    check_line = False
                    set_message = [
                        str(filename) + " ライン重心座標:" + str(line.centroid)
                    ]
                    if not self.is_valid_line(line):
                        check_geometry = True
                        check_line = True
                        set_message.append("ラインとして正しくありません。")
                    if self.check_self_intersection(line):
                        check_geometry = True
                        check_line = True
                        set_message.append("交差しています。")
                    if self.check_line_intersections(line, lines):
                        check_geometry = True
                        check_line = True
                        set_message.append("交差しています。")
                    if self.check_continuity(line):
                        check_geometry = True
                        check_line = True
                        set_message.append("ラインが連続しています。")
                    if self.check_duplicate_line_nodes(line):
                        check_geometry = True
                        check_line = True
                        set_message.append("ノードが重複しています。")
                    if self.check_point_distances(line, permission_distance):
                        check_geometry = True
                        check_line = True
                        set_message.append("頂点間の距離が近すぎます。")

                    if check_line:
                        raise Exception(str(set_message))
            elif geom_type == "Point":
                if self.check_duplicate_points(geometries):
                    raise Exception(str(filename) + f" ポイントが重複しています。")

            return check_geometry
        except Exception as e:
            raise Exception(f"[check_geometry_shapes_Exception]:{e}")

    def convert_from_geo_ndarray_to_ndarray_dict(self, geo_ndarray):
        """
        GeoNdarrayから、IDをKeyとする辞書構造に変換

        :param geo_ndarray: NumPy配列形式のジオメトリ情報。
        :type geo_ndarray: numpy.ndarray

        :return: IDをKeyとする辞書構造のジオメトリ情報。
        :rtype: dict{int, numpy.ndarray}

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            ndarray_dict = {}

            # ジオメトリIDを取得
            unique_ids = np.unique(geo_ndarray[:, 0])

            for geom_id in unique_ids:
                # 同じIDの座標を取得
                geom_points = geo_ndarray[geo_ndarray[:, 0] == geom_id][
                    :, 1:
                ]  # IDを除いた座標部分を取得

                if geom_points.shape[1] == 2:  # 2次元（Zがない場合）
                    geom_array = np.array([np.array([x, y]) for x, y in geom_points])
                elif geom_points.shape[1] == 3:  # 3次元（Zがある場合）
                    geom_array = np.array(
                        [np.array([x, y, z]) for x, y, z in geom_points]
                    )

                # 辞書に追加
                ndarray_dict[int(geom_id)] = geom_array

            return ndarray_dict

        except Exception as e:
            raise Exception(f"[convert_from_geo_ndarray_to_ndarray_dict_Exception]:{e}")

    def check_geometry_data(self, properties, geo_ndarray, filename):
        """
        FlowFileから取得したGeoNdarrayを解析し、ジオメトリチェックや変換処理を行う。

        :param properties: プロセッサの実行コンテキスト。プロパティや環境設定にアクセスするために使用。
        :type properties: dict
        :param geo_ndarray: FlowFileから取得したジオメトリ情報を含むNumPy配列形式のデータ。
        :type geo_ndarray: numpy.ndarray
        :param filename: 処理対象のファイル名。ログ出力のために使用。
        :type filename: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # GeoNdarrayを辞書型に変換する
            ndarray_dict = self.convert_from_geo_ndarray_to_ndarray_dict(geo_ndarray)

            # Shapelyオブジェクトのリストを作成（Point, Polygon, LineStringを判断して追加）
            geometries = []
            for _id, shape in ndarray_dict.items():
                if len(shape) == 1:
                    # Pointオブジェクトとして追加
                    geometries.append(Point(shape[0]))
                elif len(shape) > 2 and np.array_equal(shape[0], shape[-1]):
                    # Polygonオブジェクトとして追加
                    geometries.append(Polygon(shape))
                else:
                    # LineStringオブジェクトとして追加
                    geometries.append(LineString(shape))

            # GeoDataFrameを作成（Shapelyオブジェクトのリストを使用）
            gdf = gpd.GeoDataFrame(geometry=geometries)

            # 各ジオメトリタイプごとにNumPy配列を作成
            polygons_ndarray = np.array(
                [
                    np.array(polygon.exterior.coords)
                    for polygon in gdf.geometry
                    if polygon.geom_type == "Polygon"
                ],
                dtype=object,
            )
            lines_ndarray = np.array(
                [
                    np.array(lineString.coords)
                    for lineString in gdf.geometry
                    if lineString.geom_type == "LineString"
                ],
                dtype=object,
            )
            points_ndarray = np.array(
                [
                    np.array(point)
                    for point in gdf.geometry
                    if point.geom_type == "Point"
                ],
                dtype=object,
            )

            error_flg = False
            # 許容される誤差と距離をプロパティから取得
            tolerance = float(properties.get('TOLERANCE', 0.0))
            permission_distance = float(
                properties.get('PERMISSIONDISTANCE', 0)
            )

            # ポリゴンの形状チェックを実行
            if self.check_geometry_shapes(
                polygons_ndarray, "Polygon", filename=filename, tolerance=tolerance
            ):
                error_flg = True

            # ラインストリングの形状チェックを実行
            if self.check_geometry_shapes(
                lines_ndarray,
                "LineString",
                filename=filename,
                permission_distance=permission_distance,
            ):
                error_flg = True

            # ポイントの形状チェックを実行
            if self.check_geometry_shapes(points_ndarray, "Point", filename=filename):
                error_flg = True

            # エラーフラグが立っている場合は失敗を返す
            if error_flg:
                raise Exception(f"[ERROR check_geometry_data]: {str(error_flg)}")

        except Exception as e:
            # 例外発生時、エラーメッセージを記録し、失敗を返す
            raise Exception(f"[check_geometry_data]:{e}")

    def __call__(self, contents, attribute, properties):
        """
        NiFiプロセッサのエントリーポイント関数。FlowFileからジオメトリデータを取得し、
        メイン処理関数に渡して処理を実行する。

        :param contents: バイナリ形式のFieldSetFile
        :type contents: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: バイナリ形式の入力データ、更新された属性情報
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 対象のファイル名をFlowFileの属性から取得
            filename = attribute.get("filename", "")

            # FlowFileからGeoNdarrayを取得
            geo_ndarray = self.get_geometry_from_field_set_file(contents, filename)
            if geo_ndarray is None:
                # ジオメトリデータが存在しない場合、エラーメッセージを記録し、失敗を返す
                raise Exception(f"geometry data does not exist: {filename}")

            # メインの処理関数に渡して実行
            self.check_geometry_data(properties, geo_ndarray, filename)
            return contents, attribute

        except Exception as e:
            # 例外発生時、エラーメッセージを記録し、失敗を返す
            raise Exception(f"[transform_Exception]:{e}")
