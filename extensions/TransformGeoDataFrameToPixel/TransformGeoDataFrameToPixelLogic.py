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
import ast
import pickle
from importlib import import_module

# 外部ライブラリの動的インポート
gpd = import_module("geopandas")
np = import_module("numpy")
pd = import_module("pandas")
shapely = import_module("shapely")
scale = getattr(import_module("shapely.affinity"), "scale")
translate = getattr(import_module("shapely.affinity"), "translate")
MultiLineString = getattr(import_module("shapely.geometry"), "MultiLineString")
MultiPolygon = getattr(import_module("shapely.geometry"), "MultiPolygon")
box = getattr(import_module("shapely.geometry"), "box")

import cad.common.cad_utils as CU


class TransformGeoDataFrameToPixelLogic:
    def __init__(self):
        pass

    def get_params(self, properties, attributes):
        """
        プロパティの値を取得し、辞書にして返す。

        :param properties: プロパティ情報を格納した辞書。
        :type properties: dict
        :param attributes: 属性情報を格納した辞書。
        :type attributes: dict

        :return: パラメーターを格納した辞書。
        :rtype: dict

        :raises Exception: プロパティの取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            all_params = {
                "layer_name": properties.get("LAYER_NAME", None),
                "scale_factor": properties.get("SCALE_FACTOR", None),
                "out_of_bounds": properties.get("OUT_OF_BOUNDS", None),
                "BoundingBox": attributes.get("BoundingBox", None),
                "clip_areas": attributes.get("ClipAreas", None),
                "max_clip_areas": int(attributes.get("MaxClipAreas", 1)),
            }

            # 文字列をリストに変換
            if all_params["clip_areas"]:
                all_params["clip_areas"] = ast.literal_eval(
                    all_params["clip_areas"])

            if properties["SCALE_FACTOR"]:
                # Scale Factorが指定されている場合Widthは適用しない。
                scale_factor = CU.get_number_from_string(
                    properties["SCALE_FACTOR"])
                if scale_factor <= 0:
                    raise Exception(
                        "Scale Factor property must be greater than 0.")
                all_params["scale_factor"] = scale_factor
                all_params["width"] = None
            elif properties["WIDTH"]:
                # Scale Factorが指定されていなくてWidthが指定されている場合
                width = CU.get_number_from_string(properties["WIDTH"])
                if width <= 0:
                    raise Exception("Width property must be greater than 0.")
                all_params["scale_factor"] = None
                all_params["width"] = width

            return all_params

        except Exception as e:
            raise Exception(f"[get_params Exception]: {str(e)}")

    def transform_geodataframe_to_pixel(self, gdf, all_params, clip_bounding_box):
        """
        GeoDataFrameの座標値を指定された横幅に基づきピクセル系座標に変換する。

        :param gdf: 入力データのGeoDataFrame。
        :type gdf: geopandas.GeoDataFrame
        :param all_params: プロパティ情報を格納した辞書。
        :type all_params: dict
        :param clip_bounding_box: クリップバウンディングボックス。
        :type clip_bounding_box: str or None

        :return: ピクセル系座標に変換されたGeoDataFrame。
        :rtype: geopandas.GeoDataFrame

        :raises Exception: 変換中にエラーが発生した場合に例外をスローする。
        """
        try:
            # プロパティの値取得
            width = all_params["width"]
            layer_name = all_params["layer_name"]

            # clip_bounding_boxが存在する場合は最優先で使用
            if clip_bounding_box:
                clip_bounding_box = clip_bounding_box.strip("()")
                min_coords, max_coords = clip_bounding_box.split("), (")
                min_x, min_y = map(float, min_coords.split(","))
                max_x, max_y = map(float, max_coords.split(","))
            elif all_params["clip_areas"]:
                clip_areas = all_params["clip_areas"].strip("()")
                min_coords, max_coords = clip_areas.split("), (")
                min_x, min_y = map(float, min_coords.split(","))
                max_x, max_y = map(float, max_coords.split(","))
            else:
                # Bounding Boxを取得
                filtered_gdf = gdf[
                    gdf["layer"].str.contains(
                        all_params["layer_name"], case=False, na=False
                    )
                ]
                min_x, min_y, max_x, max_y = filtered_gdf.total_bounds

            # Bounding Boxの横幅と高さを計算
            image_width = max_x - min_x

            # スケール係数の計算
            # Scale Factorがプロパティに指定されていればそれを適用、無ければ計算する
            scale_factor = (
                all_params["scale_factor"]
                if all_params["scale_factor"]
                else width / image_width
            )
            # translate_factorの計算
            translate_factor = (-min_x * scale_factor, max_y * scale_factor)

            # 座標をピクセル座標に変換
            def to_pixel_coords(geometry):
                """
                座標をピクセル座標に変換する。

                :param geometry: 入力データのジオメトリ。
                :type geometry: shapely.geometry.base.BaseGeometry

                :return: ピクセル系座標。
                :rtype: shapely.geometry.base.BaseGeometry

                :raises Exception: 変換中にエラーが発生した場合に例外をスローする。
                """
                if geometry.geom_type == "Point":
                    scaled_point = scale(
                        geometry,
                        xfact=scale_factor,
                        yfact=-scale_factor,
                        origin=(0, 0, 0),
                    )
                    translated_point = translate(
                        scaled_point, xoff=translate_factor[0], yoff=translate_factor[1]
                    )
                    pixel_x, pixel_y = translated_point.x, translated_point.y
                    return gpd.points_from_xy([np.floor(pixel_x)], [np.floor(pixel_y)])[
                        0
                    ]

                elif geometry.geom_type == "LineString":
                    scaled_line = scale(
                        geometry,
                        xfact=scale_factor,
                        yfact=-scale_factor,
                        origin=(0, 0, 0),
                    )
                    translated_line = translate(
                        scaled_line, xoff=translate_factor[0], yoff=translate_factor[1]
                    )
                    x, y = translated_line.coords.xy
                    pixel_coords = list(zip(np.floor(x), np.floor(y)))
                    return gpd.GeoSeries(
                        [shapely.geometry.LineString(pixel_coords)]
                    ).values[0]

                elif geometry.geom_type == "Polygon":
                    scaled_polygon = scale(
                        geometry,
                        xfact=scale_factor,
                        yfact=-scale_factor,
                        origin=(0, 0, 0),
                    )
                    translated_polygon = translate(
                        scaled_polygon,
                        xoff=translate_factor[0],
                        yoff=translate_factor[1],
                    )
                    x, y = translated_polygon.exterior.coords.xy
                    pixel_coords = list(zip(np.floor(x), np.floor(y)))
                    return gpd.GeoSeries(
                        [shapely.geometry.Polygon(pixel_coords)]
                    ).values[0]

                else:
                    raise Exception("Unsupported geometry type")

            # 対象のレイヤ名のみのGeoDataFrameを作成
            if layer_name:
                layer_name = layer_name.split(",")
                gdf = gdf[gdf["layer"].isin(layer_name)]

            # 変換処理を適用
            gdf["geometry"] = gdf["geometry"].apply(to_pixel_coords)

            return gdf

        except Exception as e:
            raise Exception(
                f"[transform_geodataframe_to_pixel Exception]: {str(e)}")

    def clip_geodataframe(
            self, gdf, lower_left_x, lower_left_y, upper_right_x, upper_right_y, out_of_bounds
    ):
        """
        指定された座標でGeoDataFrameをクリップし、「Multi」タイプを個別のジオメトリに分解します。

        :param gdf: 対象のGeoDataFrame
        :type gdf: geopandas.GeoDataFrame
        :param lower_left_x: クリップ範囲の左下のX座標
        :type lower_left_x: float
        :param lower_left_y: クリップ範囲の左下のY座標
        :type lower_left_y: float
        :param upper_right_x: クリップ範囲の右上のX座標
        :type upper_right_x: float
        :param upper_right_y: クリップ範囲の右上のY座標
        :type upper_right_y: float
        :param out_of_bounds: 範囲内のジオメトリのみを保持するかどうか
        :type out_of_bounds: str

        :return: クリップされたGeoDataFrame
        :rtype: geopandas.GeoDataFrame

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            clipping_box = box(lower_left_x, lower_left_y,
                               upper_right_x, upper_right_y)

            # 完全に範囲内のジオメトリのみを保持（範囲外のものは削除）
            if out_of_bounds == "True":
                gdf = gdf[gdf.geometry.within(clipping_box)]
            else:
                # GeoDataFrameをクリップ
                gdf = gdf.clip(clipping_box)

            # `Multi`タイプを個別のジオメトリに分解
            def explode_multi_geometry(geometry):
                if isinstance(geometry, (MultiPolygon, MultiLineString)):
                    return list(geometry.geoms)
                return [geometry]

            # 各ジオメトリを確認し、`Multi`タイプを分解
            exploded_geometries = gdf["geometry"].apply(
                explode_multi_geometry)

            # 分解後のジオメトリをフラット化
            exploded_gdf = gdf.loc[
                gdf.index.repeat(exploded_geometries.str.len())
            ]
            exploded_gdf["geometry"] = [
                geom for geoms in exploded_geometries for geom in geoms
            ]

            return exploded_gdf.reset_index(drop=True)
        except Exception as e:
            raise Exception(f"[clip_geodataframe Exception]: {str(e)}")

    def __call__(self, byte_data, attributes, properties):
        """
        GeoDataFrameの座標値をピクセル系座標に変換し、結果として返す。

        :param byte_data: バイト型の入力データ。GeoDataFrameをデシリアライズして取得する。
        :type byte_data: bytes
        :param attributes: FlowFileの属性情報を格納する辞書
        :type attributes: dict
        :param properties: プロパティ情報を格納した辞書。
        :type properties: dict
        :return: ピクセル座標に変換されたGeoDataFrameと、元の属性を含むタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: GeoDataFrameの変換中にエラーが発生した場合に例外をスローする。
        """
        try:
            # プロパティ値を取得
            all_params = self.get_params(properties, attributes)

            # バイトデータからGeoDataFrameをデシリアライズ
            gdf = pickle.loads(byte_data)

            # レイヤ絞り込み
            if all_params["layer_name"] and isinstance(all_params["layer_name"], str):
                # 文字列で渡されている場合はリストに変換
                layer_name = all_params["layer_name"].split(",")
                target_gdf = gdf[gdf["layer"].isin(layer_name)]
            else:
                target_gdf = gdf

            result_gdf = gpd.GeoDataFrame()

            for i in range(all_params["max_clip_areas"]):
                if all_params["max_clip_areas"] > 1:
                    (min_x, min_y), (max_x,
                                     max_y) = all_params["clip_areas"][i]
                    gdf = self.clip_geodataframe(
                        target_gdf, min_x, min_y, max_x, max_y, all_params["out_of_bounds"])

                clip_bounding_box = (
                    str(all_params["clip_areas"][i])
                    if all_params["clip_areas"]
                    else None
                )

                # ピクセル系座標に変換
                transformed_gdf = self.transform_geodataframe_to_pixel(
                    gdf, all_params, clip_bounding_box
                )
                result_gdf = gpd.GeoDataFrame(
                    pd.concat([result_gdf, transformed_gdf], ignore_index=True)
                )

            return pickle.dumps(result_gdf), attributes

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
