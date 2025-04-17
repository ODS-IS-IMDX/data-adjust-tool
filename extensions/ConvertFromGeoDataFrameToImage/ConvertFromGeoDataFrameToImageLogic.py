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
import math
import pickle
import uuid
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
Image = import_module("PIL.Image")
ImageDraw = import_module("PIL.ImageDraw")
ImageFont = import_module("PIL.ImageFont")
scale = getattr(import_module("shapely.affinity"), "scale")
translate = getattr(import_module("shapely.affinity"), "translate")
MultiPolygon = getattr(import_module("shapely.geometry"), "MultiPolygon")
MultiLineString = getattr(import_module("shapely.geometry"), "MultiLineString")
box = getattr(import_module("shapely.geometry"), "box")

import cad.common.cad_utils as CU


class ConvertFromGeoDataFrameToImageLogic:

    def draw_polygon(
            self, draw, polygon, scale_factor, translate_factor, color=(
                    0, 0, 0)
    ):
        """
        ポリゴンを描画する。

        :param draw: 描画を行うためのオブジェクト
        :type draw: PIL.ImageDraw.ImageDraw
        :param polygon: 描画するジオメトリポリゴンオブジェクト
        :type polygon: shapely.geometry.Polygon
        :param scale_factor: スケーリングの倍率
        :type scale_factor: float
        :param translate_factor: 移動のオフセット (x, y) のタプル
        :type translate_factor: tuple(float, float)
        :param color: 描画するポリゴンの色 (デフォルトは黒)
        :type color: tuple(int, int, int)

        :raises Exception: エラーが発生した場合に発生する。
        """
        try:
            scaled_polygon = scale(
                polygon, xfact=scale_factor, yfact=-scale_factor, origin=(0, 0, 0)
            )
            translated_polygon = translate(
                scaled_polygon, xoff=translate_factor[0], yoff=translate_factor[1]
            )
            x, y = translated_polygon.exterior.coords.xy
            coords = list(zip(x, y))
            draw.polygon(coords, outline=color)

        except Exception as e:
            raise Exception(f"[draw_polygon_Exception]:{e}")

    def draw_line(self, draw, line, scale_factor, translate_factor, color=(0, 0, 0)):
        """
        ラインを描画する。

        :param draw: 描画を行うためのオブジェクト
        :type draw: PIL.ImageDraw.ImageDraw
        :param line: 描画するジオメトリラインオブジェクト
        :type line: shapely.geometry.LineString
        :param scale_factor: スケーリングの倍率
        :type scale_factor: float
        :param translate_factor: 移動のオフセット (x, y) のタプル
        :type translate_factor: tuple(float, float)
        :param color: 描画するラインの色 (デフォルトは黒)
        :type color: tuple(int, int, int)

        :raises Exception: エラーが発生した場合に発生する。
        """
        try:
            scaled_line = scale(
                line, xfact=scale_factor, yfact=-scale_factor, origin=(0, 0, 0)
            )
            translated_line = translate(
                scaled_line, xoff=translate_factor[0], yoff=translate_factor[1]
            )
            x, y = translated_line.coords.xy
            coords = list(zip(x, y))
            draw.line(coords, fill=color)

        except Exception as e:
            raise Exception(f"[draw_line_Exception]:{e}")

    def draw_point(self, draw, point, scale_factor, translate_factor, color=(0, 0, 0)):
        """
        ポイントを描画する。

        :param draw: 描画を行うためのオブジェクト
        :type draw: PIL.ImageDraw.ImageDraw
        :param point: 描画するジオメトリポイントオブジェクト
        :type point: shapely.geometry.Point
        :param scale_factor: スケーリングの倍率
        :type scale_factor: float
        :param translate_factor: 移動のオフセット (x, y) のタプル
        :type translate_factor: tuple(float, float)
        :param color: 描画するポイントの色 (デフォルトは黒)
        :type color: tuple(int, int, int)

        :raise Exception: エラーが発生した場合に発生する。
        """
        try:
            scaled_point = scale(
                point, xfact=scale_factor, yfact=-scale_factor, origin=(0, 0, 0)
            )
            translated_point = translate(
                scaled_point, xoff=translate_factor[0], yoff=translate_factor[1]
            )
            x, y = translated_point.x, translated_point.y
            draw.point((x, y), fill=color)

        except Exception as e:
            raise Exception(f"[draw_point_Exception]:{e}")

    def draw_text(
            self,
            draw,
            point,
            text,
            scale_factor,
            translate_factor,
            font_size,
            rotation,
            font_path,
    ):
        """
        テキストを描画する。

        :param draw: 描画を行うためのオブジェクト
        :type draw: PIL.ImageDraw.ImageDraw
        :param point: テキストを描画する位置を示すジオメトリオブジェクト
        :type point: shapely.geometry.Point
        :param text: 描画するテキスト
        :type text: str
        :param scale_factor: スケーリングの倍率
        :type scale_factor: float
        :param translate_factor: 移動のオフセット (x, y) のタプル
        :type translate_factor: tuple(float, float)
        :param font_size: フォントサイズ
        :type font_size: float
        :param rotation: textの回転角度
        :type rotation: float
        :param font_path: フォトファイルパス
        :type font_path: str

        :raise Exception: エラーが発生した場合に発生する。
        """
        try:
            scaled_point = scale(
                point, xfact=scale_factor, yfact=-scale_factor, origin=(0, 0)
            )
            translated_point = translate(
                scaled_point, xoff=translate_factor[0], yoff=translate_factor[1]
            )
            x, y = translated_point.x, translated_point.y

            font = self.set_font_path(font_path, font_size, scale_factor)

            # テキストのバウンディングボックスを取得
            text_bbox = draw.textbbox((0, 0), text, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]

            # フォントのメトリクスを取得して余白を追加
            ascent, descent = font.getmetrics()
            extra_height = max(descent, 0)

            # テキストイメージのサイズを設定（バウンディングボックスサイズに余白を追加）
            text_image = Image.new(
                "RGBA",
                (text_width + 2 * extra_height, text_height + 2 * extra_height),
                (255, 255, 255, 0),
            )
            text_draw = ImageDraw.Draw(text_image)

            # テキストをイメージの左下から描画
            text_draw.text(
                (extra_height, extra_height - descent), text, font=font, fill=(0, 0, 0)
            )

            # 回転
            rotated_text_image = text_image.rotate(
                rotation, expand=1, resample=Image.BICUBIC
            )

            # 回転後のサイズを取得
            rotated_text_width, rotated_text_height = rotated_text_image.size

            # 回転後の位置調整（左下を基準）
            rotated_x = x - extra_height
            rotated_y = y - rotated_text_height + extra_height

            # 描画
            draw.bitmap((rotated_x, rotated_y),
                        rotated_text_image, fill=(0, 0, 0))
        except Exception as e:
            raise Exception(f"[draw_text_Exception]:{e}")

    def set_font_path(self, font_path, font_size, scale_factor):
        """
        フォントファイルパスを設定する。

        :param font_path: フォントファイルパス
        :type font_path: str
        :param font_size: フォントサイズ
        :type font_size: float
        :param scale_factor: スケーリングの倍率
        :type scale_factor: float

        :return: フォント
        :rtype: PIL.ImageFont.FreeTypeFont

        :raise Exception: エラーが発生した場合に発生する。
        """
        try:
            font = ImageFont.truetype(font_path, int(
                font_size * scale_factor), index=1)
            return font
        except Exception as e:
            raise Exception(f"無効なFont Pathです。:{e}")

    def create_image(
            self,
            gdf,
            scaled_image_width,
            scaled_image_height,
            scale_factor,
            translate_factor,
            draw_text_flag,
            font_path,
            unique_index,
            geometry_name,
            color_space,
    ):
        """
        画像を作成する。

        :param gdf: GeoDataFrame
        :type gdf: geopandas.GeoDataFrame
        :param scaled_image_width: 画像の横幅
        :type scaled_image_width: int
        :param scaled_image_height: 画像の縦幅
        :type scaled_image_height: int
        :param scale_factor: スケーリングの倍率
        :type scale_factor: float
        :param translate_factor: 座標移動量
        :type translate_factor: tuple(float, float)
        :param draw_text_flag: テキスト描画フラグ
        :type draw_text_flag: str
        :param font_path: フォントファイルパス
        :type font_path: str
        :param unique_index: Value列タプルの連番
        :type unique_index: int
        :param geometry_name: Dwhの/以前の値
        :type geometry_name: str
        :param color_space: 画像データフォーマット
        :type color_space: str

        :return: DataFrame
        :rtype: pandas.DataFrame

        :raise Exception: エラーが発生した場合に発生する。
        """
        try:
            # 出力用のリスト
            output_data = []

            # 画像作成
            image = Image.new(
                color_space, (scaled_image_width,
                              scaled_image_height), (255, 255, 255)
            )
            draw = ImageDraw.Draw(image)

            # gdfが空でない場合の処理
            if not gdf.empty:
                for idx, row in gdf.iterrows():
                    geom = row["geometry"]
                    text = row["text"] if "text" in gdf.columns else None
                    height = row["height"] if "height" in gdf.columns else None
                    rotation = row["rotation"] if "rotation" in gdf.columns else None

                    # テキスト描画
                    if pd.notna(text) and draw_text_flag == "True":
                        self.draw_text(
                            draw,
                            geom,
                            text,
                            scale_factor,
                            translate_factor,
                            height,
                            rotation,
                            font_path,
                        )

                    # ジオメトリのタイプに応じた描画
                    elif geom.geom_type == "Polygon":
                        self.draw_polygon(
                            draw, geom, scale_factor, translate_factor)
                    elif geom.geom_type == "LineString":
                        self.draw_line(draw, geom, scale_factor,
                                       translate_factor)
                    elif geom.geom_type == "Point":
                        self.draw_point(
                            draw, geom, scale_factor, translate_factor)

            # 画像をnumpy配列に変換
            image_np = np.array(image)
            image_np_dump = pickle.dumps(image_np)

            # 出力データに追加
            output_data.append(
                {
                    "Dwh": f"{geometry_name}/content",
                    "Type": "list",
                    "Value": [(unique_index, image_np_dump)],
                }
            )
            output_data.append(
                {
                    "Dwh": f"{geometry_name}/color_space",
                    "Type": "string",
                    "Value": [(unique_index, image.mode)],
                }
            )
            output_data.append(
                {
                    "Dwh": f"{geometry_name}/image_id",
                    "Type": "string",
                    "Value": [(unique_index, str(uuid.uuid4()))],
                }
            )

            # DataFrameを作成して返す
            output_df = pd.DataFrame(output_data)

            return output_df

        except Exception as e:
            raise Exception(f"[create_image]:{e}")

    def process_bounding_box(
            self,
            gdf,
            clip_bounding_box,
            individual_gdf,
            specified_image_width,
            whole_image_flag,
            scale_factor,
    ):
        """
        バウンディングボックスを設定する

        :param gdf: GeoDataFrame
        :type gdf: geopandas.GeoDataFrame
        :param clip_bounding_box: 切り取り範囲
        :type clip_bounding_box: str
        :param individual_gdf: 対象のGeoDataFrame
        :type individual_gdf: geopandas.GeoDataFrame
        :param specified_image_width: 画像の横サイズ(px)
        :type specified_image_width: str
        :param whole_image_flag: 画像出力時の外接矩形のフラグ
        :type whole_image_flag: str
        :param scale_factor: スケール係数
        :type scale_factor: str

        :return: 設定した値
        :rtype: tuple(float, float, int, int, float, tuple(float, float), float, float)

        :raise Exception: エラーが発生した場合に発生する。
        """
        try:
            if whole_image_flag == "True":
                gdf = individual_gdf

            if clip_bounding_box:
                # clip_bounding_boxが存在する場合はここで取得
                clip_bounding_box = clip_bounding_box.strip("()")
                min_coords, max_coords = clip_bounding_box.split("), (")
                min_x, min_y = map(float, min_coords.split(","))
                max_x, max_y = map(float, max_coords.split(","))
            else:
                # すべての条件が満たされない場合、gdfの総範囲を使用
                min_x, min_y, max_x, max_y = gdf.total_bounds

            # バウンディングボックスの横幅と高さを計算
            image_width = max_x - min_x
            image_height = max_y - min_y

            # スケール係数がプロパティで指定されていない場合で、指定された画像幅がある場合、スケール係数を計算
            if scale_factor:
                scale_factor = CU.get_number_from_string(scale_factor)
                specified_image_width = image_width * scale_factor
            elif specified_image_width:
                specified_image_width = int(specified_image_width)
            else:
                # Noneの場合は元の横幅を使用
                specified_image_width = image_width

            # スケール係数がプロパティで指定されていなければスケール係数を計算
            scale_factor = (
                scale_factor if scale_factor else specified_image_width / image_width
            )

            # translate_factorの計算
            translate_factor = (-min_x * scale_factor, max_y * scale_factor)

            # 縦方向のスケール（縦横比を維持）
            scaled_image_height = math.ceil(image_height * scale_factor)

            # ピクセルあたりの座標単位を計算
            pixel_per_unit_x = (max_x - min_x) / specified_image_width
            pixel_per_unit_y = (max_y - min_y) / scaled_image_height

            # スケールされた画像の幅と高さを設定（縦横比を維持）
            scaled_image_width = math.ceil(image_width * scale_factor) + 1
            # scaled_image_height = math.ceil(image_height * scale_factor)

            return (
                pixel_per_unit_x,
                pixel_per_unit_y,
                scaled_image_width,
                scaled_image_height,
                scale_factor,
                translate_factor,
                min_x,
                max_y,
            )

        except Exception as e:
            raise Exception(f"[process_bounding_box]:{e}")

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

        :raise Exception: エラーが発生した場合に発生する。
        """
        try:
            # クリップ範囲をボックスで定義
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

    def __call__(self, byte_data, attribute, properties):
        """
        GeoDataFrameを画像のNumPy配列に変換する。

        :param byte_data: シリアライズされたGeoDataFrame。
        :type byte_data: bytes
        :param attribute: 属性。
        :type attribute: dict
        :param properties: プロパティ。
        :type properties: dict

        :return: 処理結果のフィールドセットファイル、属性。
        :rtype: tuple(str, dict)

        :raises Exception: エラーが発生した場合に発生する。
        """
        try:
            # コンテンツ、属性、プロパティ読み込み
            gdf = pickle.loads(byte_data)
            clip_bounding_box = attribute.get("BoundingBox", None)
            clip_areas = attribute.get("ClipAreas", None)
            max_clip_areas = int(attribute.get("MaxClipAreas", 1))
            layer_name = properties["LAYER_RANGE"]
            draw_text_flag = properties["DRAW_TEXT_FLAG"]
            specified_image_width = properties["IMAGE_WIDTH"]
            font_path = properties["FONT_PATH"]
            geometry_name = properties["GEOMETRY_NAME"]
            whole_image_flag = properties["WHOLE_IMAGE_FLAG"]
            scale_factor = properties["SCALE_FACTOR"]
            out_of_bounds = properties["OUT_OF_BOUNDS"]

            # 文字列をリストに変換
            if clip_areas:
                clip_areas = ast.literal_eval(clip_areas)

            # レイヤー絞り込み
            if layer_name:
                if isinstance(layer_name, str):
                    # 文字列で渡されている場合はリストに変換
                    layer_name = layer_name.split(",")
                target_gdf = gdf[gdf["layer"].isin(layer_name)]
            else:
                target_gdf = gdf

            # 結果格納用DataFrame
            result_df = pd.DataFrame()

            # Attribute初期値
            color_space = "RGB"
            unit_per_pixel_x = 0
            unit_per_pixel_y = 0
            upper_x = 0
            upper_y = 0

            for i in range(max_clip_areas):
                if max_clip_areas > 1:
                    (min_x, min_y), (max_x, max_y) = clip_areas[i]
                    individual_gdf = self.clip_geodataframe(
                        target_gdf, min_x, min_y, max_x, max_y, out_of_bounds
                    )
                else:
                    individual_gdf = target_gdf

                # バウンディングボックス計算
                (
                    unit_per_pixel_x,
                    unit_per_pixel_y,
                    scaled_image_width,
                    scaled_image_height,
                    scale_factor,
                    translate_factor,
                    upper_x,
                    upper_y,
                ) = self.process_bounding_box(
                    gdf,
                    clip_bounding_box,
                    individual_gdf,
                    specified_image_width,
                    whole_image_flag,
                    scale_factor,
                )

                # 画像作成
                image_df = self.create_image(
                    individual_gdf,
                    scaled_image_width,
                    scaled_image_height,
                    scale_factor,
                    translate_factor,
                    draw_text_flag,
                    font_path,
                    i,
                    geometry_name,
                    color_space,
                )

                # 各image_dfを結合
                result_df = pd.concat([result_df, image_df], ignore_index=True)

            # DataFrameをマージ
            merged_df = result_df.groupby(
                ["Dwh", "Type"], as_index=False, sort=False
            ).agg({"Value": "sum"})

            # FieldSetFileに変換
            fsf = CU.dataframe_to_field_set_file(merged_df)

            attributes = {
                "ColorSpace": color_space,
                "UnitXPerPixel": str(unit_per_pixel_x),
                "UnitYPerPixel": str(unit_per_pixel_y),
                "UpperX": str(upper_x),
                "UpperY": str(upper_y),
                "ImageID": str(uuid.uuid4()),
            }

            return fsf, attributes

        except Exception as e:
            raise Exception(f"[transform_Exception]:{e}")
