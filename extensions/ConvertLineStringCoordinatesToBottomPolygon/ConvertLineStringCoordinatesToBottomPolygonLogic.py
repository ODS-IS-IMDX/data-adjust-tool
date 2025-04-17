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
import math
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
LineString = getattr(import_module("shapely.geometry"), "LineString")

import cad.common.cad_utils as CU


class ConvertLineStringCoordinatesToBottomPolygonLogic:

    def parallel_offset(self, line_coords, distance):
        """
        指定された線分に平行な新しい線分を作成する。

        :param line_coords: ２点で構成される線分の座標。[(x1, y1), (x2, y2)]または[(x1, y1, z1), (x2, y2, z2)]。
        :type line_coords: list[tuple(float)]
        :param distance: 平行線と線分の距離。
        :type distance: float

        :return: 新しい平行線の座標。[(x1', y1', z), (x2', y2', z)]。
        :rtype: list[tuple(float)]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            x1, y1, z1 = line_coords[0]
            x2, y2, z2 = line_coords[1]

            # 線分の方向ベクトル
            dx = x2 - x1
            dy = y2 - y1

            # 線分の長さ
            length = math.sqrt(dx**2 + dy**2)

            # 方向ベクトルの正規化
            dx /= length
            dy /= length

            # 垂直ベクトルを計算（90度回転）
            perp_dx = -dy
            perp_dy = dx

            # 平行線の座標を計算（指定された距離だけ平行移動）
            x1_offset = x1 + perp_dx * distance
            y1_offset = y1 + perp_dy * distance
            x2_offset = x2 + perp_dx * distance
            y2_offset = y2 + perp_dy * distance

            return [(x1_offset, y1_offset, z1), (x2_offset, y2_offset, z2)]

        except Exception as e:
            raise Exception(f"[parallel_offset_Exception]: {str(e)}")

    def create_parallel_lines(self, line, distance):
        """
        指定された距離で平行線を引く。

        :param line: 2点で構成される線分の座標情報
        :type line: list[tuple(float)]
        :param distance: 平行線と線分との距離。
        :type distance: float

        :return: 線分の右側の平行線の座標情報と、線分の左側の平行線の座標情報。
        :rtype: tuple(list[tuple(float)])

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            right_line = self.parallel_offset(line, distance)
            left_line = self.parallel_offset(line, distance * -1)

            return right_line, left_line

        except Exception as e:
            raise Exception(f"[create_parallel_lines_Exception]: {str(e)}")

    def line_intersection(self, p1, p2, p3, p4):
        """
        2本の線分が交差する場合、その交点を返す関数。

        :param p1: 線分1の端点。(x1, y1, z1)もしくは(x1, y1)。
        :type p1: tuple(float)
        :param p2: 線分1の端点。(x2, y2, z2)もしくは(x2, y2)。
        :type p2: tuple(float)
        :param p3: 線分2の端点。(x3, y3, z3)もしくは(x3, y3)。
        :type p3: tuple(float)
        :param p4: 線分2の端点。(x4, y4, z4)もしくは(x4, y4)。
        :type p4: tuple(float)

        :return: ２本の線分の交差判定結果。交差している場合は交差点の座標情報を、交差していない場合はNoneを返す。
        :rtype: tuple(float) or None

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 線分の端点から座標を取得 (z値があればz値も取得、なければNone)
            x1, y1, z1 = (p1[0], p1[1], p1[2] if len(p1) > 2 else None)
            x2, y2, z2 = (p2[0], p2[1], p2[2] if len(p2) > 2 else None)
            x3, y3, z3 = (p3[0], p3[1], p3[2] if len(p3) > 2 else None)
            x4, y4, z4 = (p4[0], p4[1], p4[2] if len(p4) > 2 else None)

            # 線分の方向ベクトル（z値がある場合はzの差分も計算）
            d1x, d1y, d1z = (
                x2 - x1,
                y2 - y1,
                (z2 - z1 if z1 is not None and z2 is not None else None),
            )  # 線分1のベクトル
            d2x, d2y, d2z = (
                x4 - x3,
                y4 - y3,
                (z4 - z3 if z3 is not None and z4 is not None else None),
            )  # 線分2のベクトル

            # 線分が平行かどうかの判定 (外積が0の場合平行)
            denominator = d1x * d2y - d1y * d2x
            if denominator == 0:
                return None  # 線分が平行で交差しない

            # パラメータtとsを求める
            t = ((x3 - x1) * d2y - (y3 - y1) * d2x) / denominator
            s = ((x3 - x1) * d1y - (y3 - y1) * d1x) / denominator

            # t, sが[0, 1]の範囲にあるかをチェックして交差点が線分上にあるかを確認
            if 0 <= t <= 1 and 0 <= s <= 1:
                # 交点を求める (x, y)
                intersect_x = x1 + t * d1x
                intersect_y = y1 + t * d1y

                # z値がある場合はz座標も求める
                if d1z is not None and d2z is not None:
                    intersect_z = z1 + t * d1z  # z値がある場合、交点のz座標も計算
                else:
                    intersect_z = None  # z値が無い場合はNone

                return (intersect_x, intersect_y, intersect_z)

            return None  # 交点が線分上にない場合

        except Exception as e:
            raise Exception(f"[line_intersection_Exception]: {str(e)}")

    def adjust_line_coord(self, line_coords):
        """
        1つ離れた線分同士を比較して交差した場合に交点に合わせて座標を調整する

        :param line_coords: 線分の座標リスト。
        :type line_coords: list[tuple(float)]

        :return: 座標調整後の線分の座標情報。
        :rtype: list[tuple(float)]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 1つ離れた線分同士を比較
            for i in range(len(line_coords) - 3):

                # 交差判定
                intersection_point = self.line_intersection(
                    line_coords[i],
                    line_coords[i + 1],
                    line_coords[i + 2],
                    line_coords[i + 3],
                )

                if intersection_point:
                    # 交差した場合、線分の終点と始点を交点に移動
                    # 手前の線分の終点を交点に変更
                    line_coords[i + 1] = intersection_point

                    # 後の線分の始点を交点に変更
                    line_coords[i + 2] = intersection_point

            return line_coords

        except Exception as e:
            raise Exception(f"[adjust_line_coord_Exception]: {str(e)}")

    def create_polygon_from_parallel_lines(self, right_lines, left_lines):
        """
        平行線リストを使ってポリゴンを作成

        :param right_lines: 線分の右側の平行線の座標情報
        :type right_lines: list[list[tuple(float)]]
        :param left_lines: 線分の左側の平行線の座標情報
        :type left_lines: list[list[tuple(float)]]

        :return: 正規化された底面ポリゴン。
        :rtype: shapely.geometry.Polygon

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 右側の平行線の座標を取得
            right_coords = []
            for line in right_lines:
                right_coords.extend(list(line))

            right_coords = self.adjust_line_coord(right_coords)

            # 左側の平行線の座標を逆順に取得
            left_coords = []
            for line in reversed(left_lines):
                left_coords.extend(list(line)[::-1])

            left_coords = self.adjust_line_coord(left_coords)

            # 全座標を結合してポリゴンを作成
            polygon_coords = right_coords + left_coords

            # Z 値が None なら 2D の Polygon、それ以外なら 3D の PolygonZ を作成
            if polygon_coords[0][2] is None:
                # Z 値がない場合は 2D の Polygon を作成
                polygon = Polygon([(coord[0], coord[1]) for coord in polygon_coords])
            else:
                # Z 値がある場合は 3D の PolygonZ を作成
                polygon = Polygon(polygon_coords)

            # 再作成したポリゴンを返却
            cleaned_polygon = polygon.simplify(0, preserve_topology=True)
            return cleaned_polygon

        except Exception as e:
            raise Exception(f"[create_polygon_from_parallel_lines_Exception]: {str(e)}")

    def exchange(self, coords, width):
        """
        LineStringから平行線を計算し、ポリゴンを生成する。

        :param coords: LineString または LineStringZ 型の座標。
        :type coords: shapely.geometry.LineString
        :param width: 線分に対応する底面ポリゴンの辺と線分の距離。
        :type width: float

        :return: 作成された底面ポリゴン。
        :rtype: shapely.geometry.Polygon

        :raises ValueError: 処理対象のジオメトリデータがLineStringではない場合に例外をスローする。
        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            right_lines = []  # リセット
            left_lines = []  # リセット

            # coordsがLineString型かどうかを確認して処理を分ける
            if isinstance(coords, LineString):
                # LineStringの場合、座標リストを取得（zがある場合は使用）
                line_coords = [
                    (
                        coord[0],
                        coord[1],
                        coord[2] if len(coord) > 2 else None,
                    )  # Zがある場合はその値を、ない場合はNoneを使用
                    for coord in coords.coords
                ]
            else:
                raise ValueError(
                    "coords must be a LineString, LineStringZ object, or a list of coordinates."
                )

            # 座標リストに対して平行線を作成
            for i in range(len(line_coords) - 1):
                # 平行線を作成
                right_line, left_line = self.create_parallel_lines(
                    [line_coords[i], line_coords[i + 1]], width
                )
                right_lines.append(right_line)
                left_lines.append(left_line)

            # ポリゴンを作成
            return self.create_polygon_from_parallel_lines(right_lines, left_lines)

        except ValueError as e:
            raise Exception(f"[exchange_Exception]: {str(e)}")

        except Exception as e:
            raise Exception(f"[exchange_Exception]: {str(e)}")

    def main_proc(self, geo_dataframe, width_name):
        """
        GeoDataFrameに格納された座標データを使ってポリゴンを生成する。

        :param geo_dataframe: 入力データをGeoDataFrameに変換したもの。
        :type geo_dataframe: geopandas.GeoDataFrame
        :param width_name: 外径データのカラム名。
        :type width_name: str

        :return: 生成された底面ポリゴンのリスト。
        :rtype: list[shapely.geometry.Polygon]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            pol_list = []  # 生成されたポリゴンを格納するリスト

            # GeoDataFrameの各行に対して処理を行う
            for _, row in geo_dataframe.iterrows():
                coord_list = row["geometry"]  # 'geometry' 列から座標リストを取得
                width = row[width_name]  # 'geometry' 列から座標リストを取得
                poly = self.exchange(
                    coord_list, width
                )  # 座標データを使ってポリゴンを作成
                pol_list.append(poly)  # 生成されたポリゴンをリストに追加

            return pol_list

        except Exception as e:
            raise Exception(f"[main_proc_Exception]: {str(e)}")

    def create_field_set_file_for_output(self, gdf, pollist):
        """
        出力結果のFieldSetFileを作成する。

        :param gdf: 入力データのFieldSetFileをGeoDataFrameに変換したもの。
        :type gdf: geopandas.GeoDataFrame
        :param pollist: ポリゴンのリスト
        :type pollist: list[shapely.geometry.Polygon]

        :return: 出力結果のFieldSetFile
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # レイヤ名をFieldSetFileのDwhの左側の値とする
            data_name = gdf["layer"].tolist()[0]

            # FieldSetFile生成用のDataFrameを作成
            result_df = pd.DataFrame(
                {
                    "Dwh": [f"{data_name}/geometry"],
                    "Type": ["geometry"],
                    "Value": [pollist],
                }
            )

            # DataFrameからFieldSetFileを作成
            output_fsf = CU.dataframe_to_field_set_file(result_df)

            return output_fsf

        except Exception as e:
            raise Exception(f"[create_field_set_file_for_output_Exception]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティ、属性情報を受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: FlowFileの中身（FieldSetFile）。
        :type byte_data: bytes
        :param attribute: FlowFileの属性。
        :type attribute: dict
        :param properties: プロセッサのプロパティ情報。
        :type properties: dict

        :return: 処理結果のFieldSetFileと、属性情報を含むタプル。
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # プロパティから必要な値を取得
            width_name = properties["WIDTH_NAME"]

            # GeoDataFrame作成
            gdf = CU.field_set_file_to_geodataframe(byte_data, attribute["crs"])

            # LineStringから底面ポリゴンを作成
            pollist = self.main_proc(gdf, width_name)

            # 出力結果のFieldSetFileを作成
            results = self.create_field_set_file_for_output(gdf, pollist)

            return results, attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {e}")
