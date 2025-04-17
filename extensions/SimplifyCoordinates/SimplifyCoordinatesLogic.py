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
Point = getattr(import_module("shapely.geometry"), "Point")
LineString = getattr(import_module("shapely.geometry"), "LineString")
BaseGeometry = getattr(import_module("shapely.geometry.base"), "BaseGeometry")

import cad.common.cad_utils as CU


class SimplifyCoordinatesLogic:

    def simplify_geometry(self, geometry, tolerance):
        """
        ジオメトリを簡略化する関数。

        :param geometry: 簡略化するジオメトリオブジェクト
        :type geometry: shapely.geometry.base.BaseGeometry
        :param tolerance: 簡略化の許容範囲
        :type tolerance: float

        :return: 簡略化されたジオメトリオブジェクト
        :rtype: shapely.geometry.base.BaseGeometry

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # ジオメトリの座標簡略化を行う
            return geometry.simplify(tolerance, preserve_topology=True)
        except Exception as e:
            raise Exception(str(e))

    def simplify_geo_ndarray(self, geondarray_dict, tolerance):
        """
        GeoNdarray形式の配列を簡略化する関数。

        :param geondarray_dict: GeoNdarrayを格納する辞書
        :type geondarray_dict: dict{int, numpy.ndarray}
        :param tolerance: 簡略化の許容範囲
        :type tolerance: float

        :return: 座標が簡略化されたGeoNdarray
        :rtype: numpy.ndarray

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            simplified_geo_ndarray = []  # 座標が簡略化されたGeoNdarray格納先

            # GeoNdarrayの座標情報を簡略化する
            for key, geom_array in geondarray_dict.items():
                if len(geom_array.shape) != 2:
                    # GeoNdarrayの形式で無い場合スキップ
                    continue

                # 座標配列からジオメトリを生成し、座標簡略化処理を実行
                line_or_polygon = (
                    LineString(geom_array)
                    if geom_array.shape[0] > 1
                    else Point(geom_array)
                )
                simplified_geometry = self.simplify_geometry(line_or_polygon, tolerance)

                # 簡略化された座標データをGeoNdarrayデータを再構築
                for coords in simplified_geometry.coords:
                    # 0列目にID情報を追加
                    simplified_geometry_insert = np.insert(coords, 0, key)
                    simplified_geo_ndarray.append(simplified_geometry_insert)

            return np.array(simplified_geo_ndarray)

        except Exception as e:
            raise Exception(str(e))

    def simpify_process(self, row, tolerance):
        """
        GeoNdarray形式の配列を簡略化する関数。

        :param row: 処理対象のDataFrameの列
        :type row: pandas.Series
        :param tolerance: 簡略化の許容範囲
        :type tolerance: float

        :return: 座標が簡略化されたジオメトリデータ
        :rtype: list[shapely.geometry.BaseGeometry] or numpy.ndarray

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            geometry = row["Value"]
            if isinstance(geometry[0], BaseGeometry):
                # 対象がジオメトリデータの場合
                # 簡略化された座標を返す
                return [self.simplify_geometry(geom, tolerance) for geom in geometry]
            elif isinstance(geometry, np.ndarray) and geometry.ndim == 2:
                # 対象データが座標配列（GeoNdarray）の場合
                # GeoNdarray形式に変換して辞書に格納
                ndarray_simplified_geometries_dict = CU.geo_ndarray_to_ndarray_dict(
                    geometry
                )
                # 簡略化された座標を返す
                return self.simplify_geo_ndarray(
                    ndarray_simplified_geometries_dict, tolerance
                )
            else:
                # それ以外の場合、例外発生
                raise Exception(
                    f"ジオメトリデータの配列、またはGeoNdarrayを渡してください"
                )

        except Exception as e:
            raise Exception(str(e))

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileのデータを返す。

        :param byte_data: 入力データとしてバイト型のFieldSetFileを受け取る
        :type byte_data: bytes
        :param attribute: 属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 簡略化後のFieldSetFileと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # FieldSetFileのValue列を復元しDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(byte_data)

            # DataFrameから対象のジオメトリデータのみを取得
            geometry_name = properties["GEOMETRY_NAME"]
            geometry_df = dataframe[
                (dataframe["Type"] == "geometry")
                & (dataframe["Dwh"].str.split("/").str[0] == geometry_name)
            ]

            # DataFrameにジオメトリデータが含まれていない場合
            if geometry_df.empty:
                raise Exception(f"ジオメトリデータが含まれていません。")

            # 許容値を取得
            tolerance = CU.get_number_from_string(properties["TOLERANCE"])

            # 座標の簡略化を実施
            geometry_df["Value"] = geometry_df.apply(
                lambda row: self.simpify_process(row, tolerance), axis=1
            )
            # 簡略化されたジオメトリデータを持つDataFrameで元のDataFrameを更新
            dataframe.update(geometry_df)

            # DataFrameからFieldSetFileを生成
            output_fsf = CU.dataframe_to_field_set_file(dataframe)

            return output_fsf, attribute

        except Exception as e:
            raise Exception(str(e))
