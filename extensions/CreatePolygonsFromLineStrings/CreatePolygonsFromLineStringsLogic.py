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

from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
CRS = getattr(import_module("pyproj"), "CRS")
pd = import_module("pandas")
LineString = getattr(import_module("shapely.geometry"), "LineString")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
polygonize = getattr(import_module("shapely.ops"), "polygonize")

import cad.common.cad_utils as CU


class CreatePolygonsFromLineStringsLogic:
    def __init__(self):
        """
        CreatePolygonsFromLineStringsLogic クラスのコンストラクタ。
        """
        self.kdtree = None
        self.z_values = None

    def check_params(self, attribute, properties):
        """
        プロパティの値を取得し、辞書にして返す。

        :param attribute: FlowFileの属性情報を格納する辞書。
        :type attribute: dict
        :param properties: プロパティ情報を格納した辞書。
        :type properties: dict

        :return: プロパティの値を格納した辞書。
        :rtype: dict

        :raises Exception:
            プロパティの取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 座標参照系(CRS)を取得（プロパティに無ければ属性から取得する）
            crs_value = properties["CRS"] or attribute.get("crs", None) or attribute.get("CRS", None)
            if crs_value is None:
                # CRSが設定されていない場合例外を発生させる
                raise Exception("座標参照系(CRS)が設定されていません")

            # CRSの有効チェック
            is_valid_crs = CRS.from_user_input(crs_value)
            if is_valid_crs:
                # 有効な場合はプロパティに格納
                properties["CRS"] = CU.get_number_from_string(crs_value, int)
            else:
                # 無効な場合例外を発生させる
                raise Exception(f"無効な座標参照系(CRS)が指定されています：{crs_value}")

            return properties

        except Exception as e:
            raise Exception(f"{str(e)}")

    def convert_from_polygons_to_geo_ndarray(self, polygons):
        """
        Polygonのジオメトリリストを座標配列(GeoNdarray)に変換する。
        ドーナツ型のPolygonは外周・内周を取得して座標配列化を行う。

        :param polygons: Polygonのジオメトリリスト。
        :type polygons: list[shapely.geometry.Polygon]

        :return: Polygonの座標配列(GeoNdarray)。
        :rtype: numpy.ndarray

        :raises Exception:
            座標配列化処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            max_count = len(polygons)

            max_count_range = range(max_count)
            if "Polygon" not in polygons[0].geom_type:
                return None
            result_coords_list = []
            for i in max_count_range:
                exterior_coords_list = [
                    coords for coords in polygons[i].exterior.coords
                ]
                interior_coords_list = [
                    coords
                    for interior in polygons[i].interiors
                    for coords in interior.coords
                ]
                polygon_coords_list = exterior_coords_list + interior_coords_list
                result_coords_list.append(polygon_coords_list)

            # ジオメトリごとにid（0からの連番）を列挿入する
            result_id_shapes_list = [
                np.insert(
                    np.array(result_coords_list[i], dtype=np.float64), 0, i, axis=1
                )
                .copy()
                .tolist()
                for i in max_count_range
            ]

            # ID付ジオメトリ座標情報格納リスト
            result_list = []

            # ジオメトリの構成点を一つずつ配列に格納する
            for result_id_shape_list in result_id_shapes_list:
                # ジオメトリごとの座標リスト
                list = [
                    result_id_shape_list[i] for i in range(len(result_id_shape_list))
                ]

                # リストに追加
                result_list.extend(list)

            # ID付ジオメトリ座標情報格納リストをnumpy配列に変換し、GeoNdarrayを返す
            return np.array(result_list, dtype=np.float64).copy()

        except Exception as e:
            raise Exception(f"[convert_from_polygons_to_geo_ndarray_Exception]: {str(e)}")

    def create_field_set_file(self, polygons, fids, properties):
        """
        出力結果のFieldSetFileを生成する。

        :param polygons: 生成されたPolygonの座標配列(GeoNdarray)。
        :type polygons: numpy.ndarray
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
                "Dwh": [f"{properties['GEOMETRY_NAME']}/{properties['SUFFIX']}", f"{properties['GEOMETRY_NAME']}/FID"],
                "Type": ["geometry", "string"],
                "Value": [polygons, fids]
            })

            # DataFrameからFieldSetFileを作成して返す
            return CU.dataframe_to_field_set_file(fsf_df)

        except Exception as e:
            raise Exception(f"[create_field_set_file_Exception]: {str(e)}")

    def unity_geom_types_into_linestring(self, geom):
        """
        ジオメトリタイプをLineStringに統一する。

        :param geom: 操作対象のジオメトリ
        :type geom: shapely.geometry.base.BaseGeometry

        :return: LineString形式に変換されたジオメトリ
        :rtype: shapely.geometry.LineString

        :raises Exception:
            LineString変換中にエラーが発生した場合に例外をスローする。
        """
        try:
            if isinstance(geom, LineString):
                # LineStringの場合そのまま返す
                return geom
            elif isinstance(geom, Polygon):
                # Polygonの場合外周をLineStringに変換して返す
                return LineString(geom.exterior.coords)
            else:
                # LineString、Polygon以外のジオメトリの場合例外を発生させる
                raise Exception(f"無効なジオメトリタイプが含まれています：{str(geom)}")

        except Exception as e:
            raise Exception(f"[unity_geom_types_into_linestring_Exception]: {str(e)}")
        
    def build_kdtree(self, lines_geo_series):
        """
        KDTree を構築し、LineString の頂点座標 (X, Y) をツリーに登録する。

        :param lines_geo_series: LineString のリストを含む GeoSeries。
        :type lines_geo_series: pandas.Series (shapely.geometry.LineString)

        :raises Exception: KDTree の構築に失敗した場合に例外をスローする。
        """
        all_coords = []
        all_z_values = []
        
        for line in lines_geo_series:
            if isinstance(line, LineString):
                coords = np.array(line.coords)
                all_coords.extend(coords[:, :2])  # X, Y
                all_z_values.extend(coords[:, 2])  # Z

        # KDTreeを構築
        self.kdtree = KDTree(np.array(all_coords))
        self.z_values = np.array(all_z_values)

    def apply_z_to_polygons_kdtree(self, polygons):
        """
        生成した2Dポリゴンに KDTree を用いて Z座標を適用し、3Dポリゴンを生成する。

        :param polygons: 2Dポリゴンのリスト。
        :type polygons: list[shapely.geometry.Polygon]

        :return: Z座標が適用された3Dポリゴンのリスト。
        :rtype: list[shapely.geometry.Polygon]

        :raises Exception: Z座標の適用処理中にエラーが発生した場合に例外をスローする。
        """
        updated_polygons = []

        for polygon in polygons:
            coords_2d = np.array(polygon.exterior.coords)[:, :2]

            # KDTree を利用して最近傍のZ座標を取得
            _, nearest_indices = self.kdtree.query(coords_2d)
            coords_3d = np.column_stack((coords_2d, self.z_values[nearest_indices]))

            updated_polygons.append(Polygon(coords_3d))

        return updated_polygons

    def create_polygons_from_linestrings(self, fsf, props):
        """
        CreatePolygonsFromLineStringsのメイン処理。

        :param fsf: 入力データのFieldSetFile（バイナリ形式）。
        :type fsf: bytes
        :param props: プロパティ情報を持つ辞書。
        :type props: dict

        :return: 処理結果としてのFieldSetFile。
        :rtype: str

        :raises Exception: ポリゴン生成中やFieldSetFile作成中にエラーが発生した場合に例外をスローする。
        """
        try:
            # FieldSetFileをGeoDataFrameに変換
            geo_df = CU.field_set_file_to_geodataframe(fsf, props["CRS"], props["GEOMETRY_NAME"])

            # GeoDataFrameから全てLineString形式でジオメトリデータを取得
            lines_geo_series = geo_df.geometry.apply(lambda geom: self.unity_geom_types_into_linestring(geom))

            # LineStringをPolygonに変換
            polygons = list(polygonize(lines_geo_series.tolist()))

            if len(polygons) < 1:
                # ポリゴンが生成されない場合、例外処理を発生させる
                raise Exception(f"ポリゴンが生成できません")
    
            # **KDTree を 1回だけ構築**
            self.build_kdtree(lines_geo_series)            
                
            # **KDTree を用いて Z座標を適用**
            polygons_3d = self.apply_z_to_polygons_kdtree(polygons)

            # Polygonの座標情報を座標配列(GeoNdarray)に変換（※ドーナツ型のPolygonを考慮に入れる）
            polygon_geo_ndarray = self.convert_from_polygons_to_geo_ndarray(
                polygons
            )

            # GeoNdarrayの先頭の値をFindexとしてFID情報を作成
            fids = [(str(int(i)), float(i)) for i in np.unique(polygon_geo_ndarray[:, 0])]

            # Polygonのジオメトリ情報とFID情報からFieldSetFileを作成
            return self.create_field_set_file(polygon_geo_ndarray, fids, props)

        except Exception as e:
            raise Exception(f"[create_polygons_from_linestrings_Exception]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: 入力データのFieldSetFile（バイナリ形式）
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 出力結果としてのFieldSetFileと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティ、属性情報をチェックする
            properties = self.check_params(attribute, properties)

            # メイン処理を実行
            results = self.create_polygons_from_linestrings(byte_data, properties)

            return results, attribute

        except Exception as e:
            raise Exception(f"[__call__Exception]: {str(e)}")
