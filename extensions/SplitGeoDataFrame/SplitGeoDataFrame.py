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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# GeoDataFrameをプロパティで指定した分割法で、分けるプロセッサ。
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback
import pickle

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP

# NiFiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
Polygon = import_module("shapely").geometry.Polygon
LineString = import_module("shapely").geometry.LineString
wkt = import_module("shapely.wkt")
box = import_module("shapely").geometry.box
MultiLineString = import_module("shapely").geometry.MultiLineString
MultiPolygon = import_module("shapely").geometry.MultiPolygon
MultiPoint = import_module("shapely").geometry.MultiPoint
Point = import_module("shapely").geometry.Point
gpd = import_module("geopandas")
np = import_module("numpy")


class SplitGeoDataFrame(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データサイズの大きいGeoDataFrameを指定の方法で分割する。
                        ①input: GeoDataFrame。
                        ②output: 分割した複数のGeoDataFrameと、タイル名のリストを1つ持った、複数行のFieldSetFile。分割できなかった場合は、入力データと同じFieldSetFile。
                      """
        tags = ['GeoDataFrame', 'Index', 'UnitCode', 'RegionalMesh',
                'ZoomLevel', 'JoinSplitGeoDataFrame', 'Python']

    # 分割方法を選択
    SPLIT_METHOD = PropertyDescriptor(
        name="Split Method",
        description="分割方法（index分割:入力データをインデックスの個数ごとに分割する）",
        allowable_values=[
            "国土基本図図郭(ジオメトリを分割する)", "国土基本図図郭(ジオメトリを分割しない)", "標準地域メッシュ", "ズームレベル", "index分割"],
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=True
    )

    # インデックス分割時に、どこで分割させるのか
    SPLIT_INDEX = PropertyDescriptor(
        name="Split Index Number",
        description="[分割方法:index分割選択時]1区切りあたりのインデックス数",
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=False
    )

    # 国土基本図図郭で分割時のレベル指定
    UNIT_CODE_LEVEL = PropertyDescriptor(
        name="Unit Code Level",
        description="[分割方法:国土基本図図郭選択時]地図情報レベル",
        allowable_values=["50000", "5000", "2500", "500"],
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=False
    )

    # 地域標準メッシュで分割時のレベル指定
    REGIONAL_MESH_LEVEL = PropertyDescriptor(
        name="Regional Mesh Level",
        description="[分割方法:標準地域メッシュ]分割時の標準地域メッシュのレベル",
        allowable_values=["1次", "2次", "3次"],
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=False
    )

    # ズームレベルで分割時のレベル指定
    SPLIT_ZOOM_LEVEL = PropertyDescriptor(
        name="Zoom Level",
        description="[分割方法:ズームレベル]分割時のズームレベル",
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=False
    )

    property_descriptors = [SPLIT_METHOD,
                            SPLIT_INDEX,
                            UNIT_CODE_LEVEL,
                            REGIONAL_MESH_LEVEL,
                            SPLIT_ZOOM_LEVEL]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context):
        """
        プロパティで入力した値を取得する関数

        引数
            context: プロセッサの設定値が格納されているデータ

        戻り値
            split_method: 分割方法
            split_index: 分割方法がindex分割時どのインデックスで分割するのか
            unit_code_level: 分割方法が国土基本図図郭時に、どのレベルで分割するのか
            regional_mesh_level: 分割方法が標準地域メッシュ時に、どのメッシュレベルで分割するのか
            split_zoom_level: 分割方法がズームレベル時に、どのズームレベルで分割するのか
        """
        split_method = context.getProperty(self.SPLIT_METHOD).getValue()

        split_index = context.getProperty(self.SPLIT_INDEX).getValue()

        if split_index is not None and split_index != "":
            split_index = int(split_index)

        unit_code_level = context.getProperty(self.UNIT_CODE_LEVEL).getValue()

        regional_mesh_level = context.getProperty(
            self.REGIONAL_MESH_LEVEL).getValue()

        split_zoom_level = context.getProperty(
            self.SPLIT_ZOOM_LEVEL).getValue()

        if split_zoom_level is not None and split_zoom_level != "":
            split_zoom_level = int(split_zoom_level)

        return split_method, split_index, unit_code_level, regional_mesh_level, split_zoom_level

    def deserialize_geodataframe(self, flowfile):
        """
        flowfileからシリアライズされたGeoDataFrameのバイトデータを取得し、バイトデータからGeoDataFrameを復元

        引数
            flowfile: プロセッサに入ってくるデータ
        戻り値
            geodataframe: デシリアライズされたGeoDataFrame
        """

        # FlowFileからシリアライズされたGeoDataFrameのバイトデータを取得する
        serialize_dataframe = flowfile.getContentsAsBytes()

        # バイトデータからGeoDataFrameをデシリアライズ
        geodataframe = pickle.loads(serialize_dataframe)

        return geodataframe

    def create_polygon(self, x_offset, y_offset, width_m, height_m, origin_x, origin_y):
        """
        レベル50000のポリゴンを作成する

        引数
            x_offset: ポリゴンのX軸方向のオフセット（メートル単位）
            y_offset: ポリゴンのY軸方向のオフセット（メートル単位）
            width_m: ポリゴンの幅（メートル単位）
            height_m: ポリゴンの高さ（メートル単位）
            origin_x: ポリゴンの原点のX座標
            origin_y: ポリゴンの原点のY座標

        戻り値
            Polygon: 指定された座標に基づいて生成されたポリゴンオブジェクト
        """
        # ポリゴンのX座標の最小値を計算
        x_min = origin_x + (x_offset * width_m)

        # ポリゴンのX座標の最大値を計算
        x_max = x_min + width_m

        # ポリゴンのy座標の最大値を計算
        y_max = origin_y - (y_offset * height_m)

        # ポリゴンのy座標の最小値を計算
        y_min = y_max - height_m

        return Polygon([(x_min, y_min), (x_max, y_min), (x_max, y_max), (x_min, y_max), (x_min, y_min)])

    def separate_polygon_100(self, level50000_polygon, grid_size):
        """
        ポリゴンの境界とグリッドサイズに基づいて、幅と高さを計算し、grid_size*grid_size分ポリゴンを作成する

        引数:
            level50000_polygon : level50000のポリゴン
            grid_size          : 分割数

        戻り値:
            level5000_polygons : level5000がgrid_size*grid_size分、分割されたポリゴン群
        """
        # ポリゴンのxyの最大値最小値を取得
        x_min, y_min, x_max, y_max = level50000_polygon.bounds

        # x_step と y_step を計算
        x_step = (x_max - x_min) / grid_size
        y_step = (y_max - y_min) / grid_size

        # レベル5000のポリゴンを格納用リスト
        level5000_polygons = []

        for level_5000_right_number in range(grid_size):

            for level_5000_left_number in range(grid_size):

                # レベル5000のポリゴンを作成
                level5000_subpolygon = box(x_min + level_5000_right_number * x_step,
                                           y_max -
                                           (level_5000_left_number + 1) * y_step,
                                           x_min +
                                           (level_5000_right_number + 1) * x_step,
                                           y_max - level_5000_left_number * y_step)

                # 図郭コードを作成
                level5000_unit_cord = \
                    str(level_5000_left_number) + str(level_5000_right_number)

                # 分割されたポリゴンと、図郭コードをtupleにまとめる。
                level5000_polygon = (level5000_subpolygon, level5000_unit_cord)

                # レベル5000のポリゴンを格納用リストにまとめる。
                level5000_polygons.append(level5000_polygon)

        return level5000_polygons

    def separate_polygon_4(self, level5000_polygon):
        """
        ポリゴンを4つに分ける、図郭コードを振る

        引数:
            level5000_polygon : level5000のポリゴン。

        戻り値:
            level2500_polygons : level5000が4つに分けられたポリゴン群
        """
        # ポリゴンのxyの最大値最小値を取得
        level5000_polygon_x_min, \
            level5000_polygon_y_min, \
            level5000_polygon_x_max, \
            level5000_polygon_y_max = level5000_polygon.bounds

        # ポリゴンを4つのサブポリゴンに分割
        half_x = (level5000_polygon_x_max - level5000_polygon_x_min) / 2
        half_y = (level5000_polygon_y_max - level5000_polygon_y_min) / 2

        # ポリゴンを作成し、番号を振る
        level2500_polygons = [
            (box(level5000_polygon_x_min, level5000_polygon_y_min + half_y,
             level5000_polygon_x_min + half_x, level5000_polygon_y_max), "1"),  # 左上
            (box(level5000_polygon_x_min + half_x, level5000_polygon_y_min +
             half_y, level5000_polygon_x_max, level5000_polygon_y_max), "2"),  # 右上
            (box(level5000_polygon_x_min, level5000_polygon_y_min,
             level5000_polygon_x_min + half_x, level5000_polygon_y_min + half_y), "3"),  # 左下
            (box(level5000_polygon_x_min + half_x, level5000_polygon_y_min,
             level5000_polygon_x_max, level5000_polygon_y_min + half_y), "4")   # 右下
        ]

        return level2500_polygons

    def search_level50000(self, level50000_polygon_code,
                          level50000_polygon,
                          temp_50000_dict,
                          ):
        """
        polygonの分割を行ってレベル5000のポリゴンを生成し、
        50000の図郭コードをキーに、分割されたポリゴンをvalueにし、
        level5000_dictに格納。

        対象の図郭の、indexと、それに触れあっている図郭のインデックスを更新する

        引数:
            level50000_polygon_code : レベル50000の図郭コード。
            level50000_polygon : 対象のレベル50000のポリゴン。
            temp_50000_dict : 図郭コードとポリゴンを格納する、辞書

        戻り値:
            temp_50000_dict : 対象の図郭コードとポリゴンを格納する、辞書
        """

        # ポリゴンをレベル5000に分割
        level5000_polygons = WM.calc_func_time(self.logger, False)(
            self.separate_polygon_100)(level50000_polygon, grid_size=10)

        # 対象の図郭を保持
        temp_50000_dict[level50000_polygon_code] = level5000_polygons

        return temp_50000_dict

    def search_level5000_and_separate_2500(self, level5000_polygon_code, level5000_polygon, temp_5000_dict, level50000_polygon_code):
        """
        ポリゴンの分割を行ってレベル2500のポリゴンを生成し、
        5000までのコードをキーに、分割されたポリゴンをvalueにし、
        level2500_dictに格納。

        対象の図郭の、indexと、それに触れあっている図郭のインデックスを更新する

        引数:
            level5000_polygon_code : レベル5000の図郭コード。
            level5000_polygon : 対象のレベル5000のポリゴン。
            temp_5000_dict : 図郭コードとポリゴンを格納する、初期化された辞書
            level50000_polygon_code : 対象となる平面直角座標系の系レベル50000の図郭コード。

        戻り値:
            temp_5000_dict : 図郭コードとポリゴンを格納する、辞書
        """

        level2500_polygons = WM.calc_func_time(self.logger, False)(
            self.separate_polygon_4)(level5000_polygon)

        # キーを図郭コード:valueをレベル2500のポリゴン群のdictを保持
        temp_5000_dict[level50000_polygon_code +
                       level5000_polygon_code] = level2500_polygons

        return temp_5000_dict

    def search_level5000_and_separate_500(self, level5000_polygon_code,
                                          level5000_polygon,
                                          level50000_polygon_code,
                                          temp_5000_dict,
                                          ):
        """
        ポリゴンの分割を行ってレベル500のポリゴンを生成し、
        5000までのコードをキーに、分割されたポリゴンをvalueにし、
        level500_dictに格納。

        対象の図郭の、indexと、それに触れあっている図郭のインデックスを更新する

        引数:
            level5000_polygon_code : レベル5000の図郭コード。
            level5000_polygon : 対象のレベル5000のポリゴン。
            level50000_polygon_code : 対象となる平面直角座標系の系レベル50000の図郭コード。
            temp_5000_dict : 図郭コードとポリゴンを格納する、初期化された辞書

        戻り値:
            temp_5000_dict : 1ジオメトリ分だけの図郭コードとpolygonを格納する辞書
        """
        level500_polygons \
            = WM.calc_func_time(self.logger)(self.separate_polygon_100)(level5000_polygon, grid_size=10)

        # キーを図郭コード:valueをレベル500のポリゴン群のdictを保持
        temp_5000_dict[level50000_polygon_code + level5000_polygon_code] \
            = level500_polygons

        return temp_5000_dict

    def separate_regional_mesh(self, level50000_polygon, grid_size):
        """
        ポリゴンの境界とグリッドサイズに基づいて、幅と高さを計算し、grid_size*grid_size分ポリゴンを作成する

        引数:
            level50000_polygon : level50000のポリゴン
            grid_size          : 分割数

        戻り値:
            level5000_polygons : level5000がgrid_size*grid_size分、分割されたポリゴン群
        """
        # ポリゴンのxyの最大値最小値を取得
        x_min, y_min, x_max, y_max = level50000_polygon.bounds

        # x_step と y_step を計算
        x_step = (x_max - x_min) / grid_size
        y_step = (y_max - y_min) / grid_size

        # レベル5000のポリゴンを格納用リスト
        level5000_polygons = []

        # 左下から (0, 0) になるように修正
        for level_5000_left_number in range(grid_size):
            for level_5000_right_number in range(grid_size):

                # レベル5000のポリゴンを作成
                level5000_subpolygon = box(
                    x_min + level_5000_right_number * x_step,
                    y_min + level_5000_left_number * y_step,  # y_minを使用
                    x_min + (level_5000_right_number + 1) * x_step,
                    y_min + (level_5000_left_number + 1) * y_step  # y_minを使用
                )

                # 図郭コードを作成
                level5000_unit_cord = str(
                    level_5000_left_number) + str(level_5000_right_number)

                # 分割されたポリゴンと、図郭コードをtupleにまとめる。
                level5000_polygon = (level5000_subpolygon, level5000_unit_cord)

                # レベル5000のポリゴンを格納用リストにまとめる。
                level5000_polygons.append(level5000_polygon)

        return level5000_polygons

    def separate_polygon(self, polygon_tile_dict):
        """
        概要: 与えられたポリゴンを4つのポリゴンに分割し、新しいポリゴンの辞書を返す
        引数:
            polygon_tile_dict: 分割対象のポリゴンを含む辞書(1要素のみ対応)。キーは"z-x-y"形式の文字列、値はポリゴンオブジェクト
        戻り値:
            dict: 分割後のポリゴンを含む辞書。新しいキーは"z+1-2x-2y"などの形式。
        """

        # 辞書の最初の要素を取得
        (key, polygon) = next(iter(polygon_tile_dict.items()))

        # キーを分割して数値に変換
        z, x, y = map(int, key.split('-'))

        # ポリゴンのxyの最大値最小値を取得
        polygon_x_min, polygon_y_min, polygon_x_max, polygon_y_max = polygon.bounds

        # ポリゴンを4つのサブポリゴンに分割
        half_x = (polygon_x_max - polygon_x_min) / 2
        half_y = (polygon_y_max - polygon_y_min) / 2

        # 新しいポリゴン辞書を作成
        new_polygons = {
            # 左上
            f"{z + 1}-{2 * x}-{2 * y}": box(polygon_x_min, polygon_y_min + half_y, polygon_x_min + half_x, polygon_y_max),
            # 右上
            f"{z + 1}-{2 * x + 1}-{2 * y}": box(polygon_x_min + half_x, polygon_y_min + half_y, polygon_x_max, polygon_y_max),
            # 左下
            f"{z + 1}-{2 * x}-{2 * y + 1}": box(polygon_x_min, polygon_y_min, polygon_x_min + half_x, polygon_y_min + half_y),
            # 右下
            f"{z + 1}-{2 * x + 1}-{2 * y + 1}": box(polygon_x_min + half_x, polygon_y_min, polygon_x_max, polygon_y_min + half_y)
        }

        return new_polygons

    def split_geodataframe(self, input_polygon_dict, dataframe_list, crs, level=0, split_zoom_level=0):
        """
        概要:
            与えられたDataFrameを指定されたポリゴンで分割し、新しいDataFrameリストを生成する
        引数:
            input_polygon_dict: 分割に使用するポリゴンの辞書
            dataframe_list: 処理するDataFrameのリスト
            crs: 対象のCRS
            level: 現在の分割レベル
            split_zoom_level: 最大分割レベル
        戻り値:
            after_split_df_dict: 次回処理に必要なポリゴンを含む辞書
            after_split_geodataframe_list: 分割後のDataFrameのリスト
            level + 1: 次のレベルの値
        """

        output_polygon_dict = {}
        after_split_geodataframe_list = []

        for key, polygon in input_polygon_dict.items():

            for dataframe in dataframe_list:

                # overlayを使用してポリゴンの交差部分を取得
                intersected = gpd.overlay(dataframe, gpd.GeoDataFrame(
                    geometry=[polygon], crs=crs), how='intersection')

                if not intersected.empty:

                    output_polygon_dict[key] = polygon

                    after_split_geodataframe_list.append(intersected)

        if level == split_zoom_level:

            return output_polygon_dict, after_split_geodataframe_list, level

        else:

            after_split_polygon_dict = {}

            for key, polygon in output_polygon_dict.items():

                polygons = WM.calc_func_time(self.logger, False)(
                    self.separate_polygon)({key: polygon})

                after_split_polygon_dict.update(polygons)

            return after_split_polygon_dict, after_split_geodataframe_list, level + 1

    def create_tile_list(self, output_dwh_list, output_type_list, after_split_geodataframe_list, output_dwh_name):
        """
        概要: TileListを作成する
        引数:
            output_dwh_list: FieldSetFile作成用のDWH名が格納されたリスト
            output_type_list: FieldSetFile作成用のTypeが格納されたリスト
            after_split_geodataframe_list: FieldSetFile作成用のGeoDataFrameが格納されたリスト
            output_dwh_name: Attributeのfilenameから拡張子以降を除いたファイル名部分

        戻り値:
            output_dwh_list: FieldSetFile作成用のDWH名が格納されたリストにTileListのDWH名が追加されたもの
            output_type_list: FieldSetFile作成用のTypeが格納されたリストにTileListのTypeが追加されたもの
            after_split_geodataframe_list: FieldSetFile作成用のGeoDataFrameが格納されたリストにTileListが追加されたもの
        """

        tile_name_list = [item.split('_')[-1] for item in output_dwh_list]

        output_dwh_list.append(f"{output_dwh_name}_TileList")
        output_type_list.append("object")
        after_split_geodataframe_list.append(tile_name_list)

        return output_dwh_list, output_type_list, after_split_geodataframe_list

    # -------------------------------------------------------------------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            no_split_flag = False

            split_method, \
                split_index, \
                unit_code_level, \
                regional_mesh_level, \
                split_zoom_level\
                = WM.calc_func_time(self.logger)(self.get_property)(context)

            geodataframe\
                = WM.calc_func_time(self.logger)(self.deserialize_geodataframe)(flowfile)

            geom_list = list(geodataframe["geometry"])

            try:
                # 対象のジオメトリがどの、エリアにいるのかを特定するために、マルチ化。
                # こうすることで、大幅な処理時間の短縮になる。
                if isinstance(geom_list[0], LineString):
                    multi_geometry = MultiLineString(geom_list)

                elif isinstance(geom_list[0], Polygon):
                    multi_geometry = MultiPolygon(geom_list)

                elif isinstance(geom_list[0], Point):
                    multi_geometry = MultiPoint(geom_list)
            except Exception:
                self.logger.error(traceback.format_exc())
                return FlowFileTransformResult(relationship="failure")

            try:
                crs = flowfile.getAttribute("crs")
                filename = flowfile.getAttribute("filename")
                output_dwh_name = filename.split('.')[0]
            except Exception:
                self.logger.error(traceback.format_exc())
                return FlowFileTransformResult(relationship="failure")

            # -----------------------------------------------------------------------------------------------------------
            # 国土基本図図郭
            # -----------------------------------------------------------------------------------------------------------
            # 国土基本図図郭の処理概要。
            # レベル指定されたエリアをポリゴンにし、特定(例:09LD181など)し、そのエリア内に存在するジオメトリとその属性で、GeoDataFrameにする。
            if split_method in ["国土基本図図郭(ジオメトリを分割する)", "国土基本図図郭(ジオメトリを分割しない)"]:

                # レベル50000ポリゴンを作成に必要な数字。
                origin_x = 0
                origin_y = 0
                width_m = 40000
                height_m = 30000
                num_x = 4
                num_y = 10

                try:
                    # crsでどのエリアなのかを特定。(例:6677 = 09)
                    target_level50000 = DDC.TARGET_LEVEL50000_DICT[crs]
                except Exception:
                    self.logger.error(traceback.format_exc())
                    return FlowFileTransformResult(relationship="failure")

                # 行名と列名の作成
                rows = [chr(char_code) for char_code in range(
                    ord("A"), ord("A") + num_y * 2)]
                columns = [chr(char_code) for char_code in range(
                    ord("A"), ord("A") + num_x * 2)]

                # レベル50000ポリゴンのデータを辞書に保存
                level50000_polygon_data = {}
                for x_offset in range(-num_x, num_x):
                    for y_offset in range(-num_y, num_y):
                        level50000_polygon = WM.calc_func_time(self.logger)(self.create_polygon)(
                            x_offset, y_offset, width_m, height_m, origin_x, origin_y)

                        row_name = rows[y_offset + num_y]
                        column_name = columns[x_offset + num_x]

                        level50000_polygon_data[f"{row_name}{column_name}"] = level50000_polygon

                # 対象のポリゴンを格納するためのset
                target_polygons_set = set()

                # 図郭コードを格納するset
                # 被らせたくない為、set型を採用
                unit_code_set = set()

                # ジオメトリがどの図郭にいるのかを一時的に保持。
                temp_50000_dict = {}
                temp_5000_dict = {}

                # 50000レベルの図郭にあるか１つずつ判定。
                for (level50000_polygon_code, level50000_polygon) in (level50000_polygon_data.items()):

                    # 図郭と交差しているか？
                    if multi_geometry.intersects(level50000_polygon):

                        # レベルが50000の場合はこれが対象のポリゴン
                        if unit_code_level == "50000":
                            target_polygons_set.add(level50000_polygon)
                            unit_code_set.add(
                                f"{target_level50000}{level50000_polygon_code}")

                        # レベルが50000より小さい場合は、分割する。
                        else:
                            temp_50000_dict = WM.calc_func_time(self.logger)(self.search_level50000)(level50000_polygon_code,
                                                                                                     level50000_polygon,
                                                                                                     temp_50000_dict)
                        continue

                # temp_50000_dictの各要素をループし、さらに細かく判定を掛ける。
                if not unit_code_level == "50000":

                    for level50000_polygon_code, level5000_polygons in temp_50000_dict.items():

                        # レベル5000の図郭にあるか1つずつ判定。
                        for (level5000_polygon, level5000_polygon_code) in (level5000_polygons):

                            # レベルが5000の場合に交差していた場合は対象のポリゴン
                            if unit_code_level == "5000":
                                if multi_geometry.intersects(level5000_polygon):
                                    target_polygons_set.add(level5000_polygon)
                                    unit_code_set.add(
                                        f"{target_level50000}{level50000_polygon_code}{level5000_polygon_code}")

                            # ユニットレベルが2500の場合は、5000のポリゴンを分割し、2500のポリゴンを作成する。
                            if unit_code_level == "2500":
                                # 図郭と交差しているか。
                                if multi_geometry.intersects(level5000_polygon):

                                    # ポリゴンを分割し、レベル2500のポリゴンに加工
                                    temp_5000_dict = WM.calc_func_time(self.logger)(self.search_level5000_and_separate_2500)(level5000_polygon_code,
                                                                                                                             level5000_polygon,
                                                                                                                             temp_5000_dict,
                                                                                                                             level50000_polygon_code,
                                                                                                                             )

                                    continue

                                else:
                                    continue

                            # ユニットレベルが500の場合は基本の処理は2500と変わらないが、分割の関数が変わる
                            elif unit_code_level == "500":

                                # 図郭と交差しているか
                                if multi_geometry.intersects(level5000_polygon):

                                    # ポリゴンを分割し、レベル500のポリゴンに加工
                                    temp_5000_dict \
                                        = WM.calc_func_time(self.logger)(self.search_level5000_and_separate_500)(level5000_polygon_code,
                                                                                                                 level5000_polygon,
                                                                                                                 level50000_polygon_code,
                                                                                                                 temp_5000_dict,
                                                                                                                 )

                                    continue

                # temp_50000_dictの各要素をループし、最後の判定を掛け、図郭コードをset型に格納
                if not unit_code_level == "50000" and not unit_code_level == "5000":
                    for level5000_polygon_code, level2500_or_500_polygons in temp_5000_dict.items():

                        # 以下処理は同じだが、変数名を変えたいため、条件分岐。
                        if unit_code_level == "2500":

                            # ポリゴンと一致した場合、unit_code_setに図郭コードを追加
                            # 対象のポリゴンもtarget_polygons_setに追加
                            for level2500_polygon, code in level2500_or_500_polygons:
                                if multi_geometry.intersects(level2500_polygon):
                                    target_polygons_set.add(level2500_polygon)
                                    unit_code_set.add(
                                        f"{target_level50000}{level5000_polygon_code}{code}")

                        elif unit_code_level == "500":

                            # ポリゴンと一致した場合、unit_code_setに図郭コードを追加
                            # 対象のポリゴンもtarget_polygons_setに追加
                            for (level500_polygon, code) in (level2500_or_500_polygons):
                                if multi_geometry.intersects(level500_polygon):
                                    target_polygons_set.add(level500_polygon)
                                    unit_code_set.add(
                                        f"{target_level50000}{level5000_polygon_code}{code}")

                # キーを図郭コード、valueをポリゴンに設定し、dictを作成
                dict_result = dict(zip(unit_code_set, target_polygons_set))

                # 作成したdictとGeoDataFrameのリスト、crsを引数に、GeoDataFrameを分割する。
                after_split_polygon_dict, after_split_geodataframe_list, rest_level = (
                    self.split_geodataframe)(dict_result, [geodataframe], crs)

                # DataFrameを、GeoDataFrameに変換。
                for i in range(len(after_split_geodataframe_list)):
                    after_split_geodataframe_list[i] = gpd.GeoDataFrame(
                        after_split_geodataframe_list[i], geometry='geometry')
                    after_split_geodataframe_list[i].to_crs(
                        epsg=crs, inplace=True)

                if split_method == "国土基本図図郭(ジオメトリを分割しない)":

                    coordinates_array = NSP.get_geometries_points_numpy(
                        geom_list)

                    # geometry_value_coordinates_array から重複した要素を取り除いた座標IDを取得
                    coordinates_id_array = np.unique(
                        coordinates_array[:, 0])

                    # coordinates_dict を生成
                    linestring_dict = {coordinates_id_array[i]: coordinates_array[list(np.where(
                        coordinates_array[:, 0] == coordinates_id_array[i])[0]), 1:4] for i in range(len(coordinates_id_array))}

                    # 分割されたGeoDataFrame格納用リストの設定
                    splitted_geodataframes_list = []

                    # 出荷用DWHのリストを設定
                    output_dwh_list = []

                    # 出荷用Typeのリストを設定
                    output_type_list = []
                    unit_code_list = list(unit_code_set)
                    for target_unit_code_list_index in range(len(unit_code_list)):

                        # -----------------------------------------------------------------------------------------------------------
                        # 【取得】出力対象図郭情報取得
                        # -----------------------------------------------------------------------------------------------------------
                        unit_origin_array, level_mesh_array = WM.calc_func_time(self.logger)(
                            NSP.get_unit_origin)(unit_code_list[target_unit_code_list_index])
                        # -----------------------------------------------------------------------------------------------------------

                        # -----------------------------------------------------------------------------------------------------------
                        # 【抽出】ジオメトリ指定図郭内に存在するかチェック
                        # -----------------------------------------------------------------------------------------------------------
                        feature_bool = [WM.calc_func_time(self.logger)(NSP.judge_citygml)(
                            linestring_dict[coordinates_id_array[i]][:, :2], unit_origin_array, level_mesh_array) for i in range(len(coordinates_id_array))]

                        # 出力対象がなければ次へ
                        if np.any(feature_bool) == True:
                            pass
                        else:
                            continue

                        # 図郭対象のIDを取得
                        target_id_array = coordinates_id_array[feature_bool].copy(
                        )

                        # 分割対象を抽出
                        splitted_geodataframe = geodataframe.loc[target_id_array]

                        # 分割されたGeoDataFrameをリストに格納
                        splitted_geodataframes_list.append(
                            splitted_geodataframe)

                        # 出荷DWHをリストに格納(ファイル名に、図郭コードを付与)
                        output_dwh_list.append(
                            f"{output_dwh_name}_{unit_code_list[target_unit_code_list_index]}")

                        # Type列用リストに種類を格納
                        output_type_list.append("GeoDataFrame")

                    split_count = str(len(splitted_geodataframes_list))

                    output_dwh_list, output_type_list, splitted_geodataframes_list = WM.calc_func_time(self.logger)(self.create_tile_list)(
                        output_dwh_list, output_type_list, splitted_geodataframes_list, output_dwh_name)

                    output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_list,
                                                                                                   output_type_list,
                                                                                                   splitted_geodataframes_list)

                    return FlowFileTransformResult(relationship="success",
                                                   contents=output_field_set_file,
                                                   attributes={"Split_Method": split_method, "split_count": split_count})

            # -----------------------------------------------------------------------------------------------------------
            # 標準地域メッシュ
            # -----------------------------------------------------------------------------------------------------------
            # 地域標準メッシュの処理概要。
            # レベル指定されたエリアまでポリゴンにし、特定(例:54394611など)し、そのエリア内に存在するジオメトリとその属性で、GeoDataFrameにする。
            elif split_method == "標準地域メッシュ":

                # 対象のポリゴンがまとめられるset
                target_polygons_set = set()

                # 対象のポリゴンのメッシュコードがまとめられるset
                target_polygons_code_set = set()

                # 後続処理用のdict
                temp_first_dict = {}
                temp_second_dict = {}

                # 日本全土の1次メッシュのWKTがまとめられたdictの中から、対象のポリゴンを探す。
                for first_code, first_wkt in DDC.first_mesh_dict.items():

                    # dictのvalueがwktなので、ポリゴンに変換
                    first_polygon = wkt.loads(first_wkt)

                    # 1次メッシュと交差しているか
                    if first_polygon.intersects(multi_geometry):

                        # 1次メッシュが対象の場合、対象のポリゴン、コードをsetに格納
                        if regional_mesh_level == "1次":
                            target_polygons_set.add(first_polygon)
                            target_polygons_code_set.add(first_code)

                        # ポリゴンを2次メッシュの形に分割
                        else:
                            second_polygons = WM.calc_func_time(self.logger)(
                                self.separate_regional_mesh)(first_polygon, 8)

                            # temp_first_dictにキーがコードで、valueに2次メッシュをdict化
                            temp_first_dict[first_code] = second_polygons

                # 2次メッシュのポリゴンから、対象のポリゴンを探す。
                for first_code, second_polygons in temp_first_dict.items():

                    # 分割されたポリゴンをひとつづつ判定。
                    for (second_polygon, second_code) in second_polygons:

                        # 交差しているか
                        if second_polygon.intersects(multi_geometry):

                            # 2次メッシュが対象の場合、対象のポリゴン、コードをsetに格納
                            if regional_mesh_level == "2次":
                                target_polygons_set.add(second_polygon)
                                target_polygons_code_set.add(
                                    f"{first_code}{second_code}")

                            # ポリゴンを３次メッシュの形に分割
                            else:
                                third_polygons = WM.calc_func_time(self.logger)(
                                    self.separate_regional_mesh)(second_polygon, 10)

                                # temp_first_dictにキーがコードで、valueに3次メッシュをdict化
                                temp_second_dict[f"{first_code}{second_code}"] = third_polygons

                # 3次メッシュのポリゴンから、対象のポリゴンを探す。
                for first_second_code, third_polygons in temp_second_dict.items():

                    # 分割されたポリゴンから1つずつ判定
                    for third_polygon, third_code in third_polygons:

                        # 交差しているか
                        if third_polygon.intersects(multi_geometry):

                            # 対象のポリゴン、コードをsetに格納
                            target_polygons_set.add(third_polygon)
                            target_polygons_code_set.add(
                                f"{first_second_code}{third_code}")

                # キーをメッシュコード、valueをポリゴンに設定し、dictを作成
                dict_result = dict(
                    zip(target_polygons_code_set, target_polygons_set))

                # 作成したdictとGeoDataFrameのリスト、crsを引数に、GeoDataFrameを分割する。
                after_split_polygon_dict, after_split_geodataframe_list, rest_level = WM.calc_func_time(
                    self.logger)(self.split_geodataframe)(dict_result, [geodataframe], crs)

                # DataFrameを、GeoDataFrameに変換。
                for i in range(len(after_split_geodataframe_list)):
                    after_split_geodataframe_list[i] = gpd.GeoDataFrame(
                        after_split_geodataframe_list[i], geometry='geometry')
                    after_split_geodataframe_list[i].to_crs(
                        epsg=crs, inplace=True)

                if after_split_geodataframe_list:
                    pass

                else:
                    self.logger.error(
                        f"地域標準メッシュは平面直角座標に対応していません。inputされたGeoDataFrameのEPSG: {crs}")

            # -----------------------------------------------------------------------------------------------------------
            # ズームレベル
            # -----------------------------------------------------------------------------------------------------------
            # ズームレベルの処理概要。
            # 地球を4等分し、2つ以上対象のポリゴンができるまでエリアを絞る。
            # 対象のレベルまでジオメトリが含まれているポリゴンを分割し、対象のレベルのポリゴンごとにGeoDataFrameを分割する。
            elif split_method == "ズームレベル":

                # DataFrameをウェブメルカトル座標系（EPSG:3857）に変換
                input_dataframe = geodataframe.to_crs(epsg=3857)

                # geometry列を抽出
                input_geometries_dataframe = input_dataframe['geometry']

                # 座標の最小値と最大値を求める
                min_x, min_y, max_x, max_y = float('inf'), float(
                    'inf'), float('-inf'), float('-inf')
                for geometry in input_geometries_dataframe:

                    if geometry.geom_type == 'Polygon':
                        x, y = geometry.exterior.xy
                    else:
                        x, y = geometry.xy

                    min_x = min(min_x, min(x))
                    max_x = max(max_x, max(x))
                    min_y = min(min_y, min(y))
                    max_y = max(max_y, max(y))

                # geometryを囲むポリゴンを作成
                geometry_polygon = Polygon([(min_x, min_y), (max_x, min_y),
                                            (max_x, max_y), (min_x, max_y),
                                            (min_x, min_y)])

                # 北西隅を基準にしたズームレベル0のポリゴンの座標（EPSG:4326）
                polygon_coords = [
                    (-180, 85.05112878),  # 北西隅
                    (180, 85.05112878),   # 北東隅
                    (180, -85.05112878),   # 南東隅
                    (-180, -85.05112878),  # 南西隅
                    (-180, 85.05112878)    # 北西隅に戻る
                ]

                # GeoDataFrameに変換
                polygon_coords_geodataframe = gpd.GeoDataFrame(
                    geometry=[Polygon(polygon_coords)], crs="EPSG:4326")

                # EPSG:3857に変換
                polygon_coords_geodataframe = polygon_coords_geodataframe.to_crs(
                    "EPSG:3857")

                # ズームレベル0のポリゴンを取得
                polygon = polygon_coords_geodataframe.geometry.iloc[0]

                # ズームレベル0の辞書を作成
                polygon_tile_dict = {"0-0-0": polygon}

                # タイルのポリゴンとgeometryのポリゴンの比較
                for level in range(split_zoom_level + 1):

                    # ズームレベル0の場合
                    if level == 0:

                        level_check = level

                        pass

                    # ズームレベル1の場合
                    elif level == 1:

                        # 与えられたタイルのポリゴンを4つのポリゴンに分割し、新しいポリゴンのdictを作成
                        separated_polygons_dict\
                            = WM.calc_func_time(self.logger)(self.separate_polygon)(polygon_tile_dict)

                        # タイルのポリゴンとgeometryのポリゴンを比較
                        try:

                            containing_polygons\
                                = {k: poly for k, poly in separated_polygons_dict.items() if poly.contains(geometry_polygon)}

                        except Exception:
                            self.logger.error(traceback.format_exc())
                            return FlowFileTransformResult(relationship="failure")

                        # geometryから作成したポリゴンが1つのタイルポリゴン上に存在する場合
                        if len(containing_polygons) == 1:
                            filtered_containing_polygons = containing_polygons
                            level_check = level

                        # geometryから作成したポリゴンが複数のタイルポリゴンに跨る場合
                        elif len(containing_polygons) > 1:
                            level_check = level
                            break

                    elif level >= 2:

                        # 測地系の変換ミスや異常なデータでズームレベル0のタイル上に地物が存在しない場合があり、
                        # 下記関数でUnboundLocalErrorが発生するためその対策のログを用意
                        try:

                            # タイルのポリゴンとgeometryのポリゴンを比較
                            separated_polygons_dict = WM.calc_func_time(self.logger)(
                                self.separate_polygon)(filtered_containing_polygons)

                        except Exception:
                            self.logger.error(
                                "図郭内に地物が存在していない、もしくは意図していない測地系の可能性があります。")
                            return FlowFileTransformResult(relationship="failure")

                        # タイルのポリゴンとgeometryのポリゴンを比較
                        try:

                            containing_polygons\
                                = {k: poly for k, poly in separated_polygons_dict.items() if poly.intersects(geometry_polygon)}

                        except Exception:
                            self.logger.error(traceback.format_exc())
                            return FlowFileTransformResult(relationship="failure")

                        # geometryから作成したポリゴンが1つのタイルポリゴン上に存在する場合
                        if len(containing_polygons) == 1:
                            filtered_containing_polygons = containing_polygons
                            level_check = level

                        # geometryから作成したポリゴンが複数のタイルポリゴンに跨る場合
                        elif len(containing_polygons) > 1:
                            level_check = level
                            break

                # タイルのポリゴンと各geometryの比較
                # level0指定の場合
                if level_check == 0 and level_check == split_zoom_level:

                    no_split_flag = True

                # level0以上の指定の場合
                else:

                    # 分割しないで目的のlevelに達した場合
                    if level_check == split_zoom_level:

                        no_split_flag = True

                    # 複数のポリゴンに跨ぎ、まだ分割処理が残っている場合(指定されたレベルに達していない場合)
                    elif level_check != split_zoom_level:

                        containing_polygons_dict = {}

                        for key, polygon in containing_polygons.items():

                            # 与えられたポリゴンを4つのポリゴンに分割し、新しいポリゴンの辞書を返す
                            polygons = WM.calc_func_time(self.logger)(
                                self.separate_polygon)({key: polygon})

                            # 新しいポリゴンを辞書に追加
                            containing_polygons_dict.update(polygons)

                        # タイルのポリゴンとgeometryのポリゴンを比較した続きのレベルから分割開始
                        for level in range(level_check + 1, split_zoom_level + 1):

                            # 初回はrest_levelが生成されていないため下記条件
                            if level == level_check + 1:
                                # 与えられたDataFrameを指定されたポリゴンで分割し、新しいDataFrameリストを生成する
                                after_split_polygon_dict, after_split_geodataframe_list, rest_level = WM.calc_func_time(self.logger)(
                                    self.split_geodataframe)(containing_polygons_dict, [input_dataframe], "3857", level, split_zoom_level)

                            # 2回目以降はrest_levelが生成されているため下記条件
                            else:
                                # 与えられたDataFrameを指定されたポリゴンで分割し、新しいDataFrameリストを生成する
                                after_split_polygon_dict, after_split_geodataframe_list, rest_level = WM.calc_func_time(self.logger)(
                                    self.split_geodataframe)(after_split_polygon_dict, after_split_geodataframe_list, "3857", rest_level, split_zoom_level)

                if no_split_flag:

                    if level_check == 0:
                        dwh_name = f"{output_dwh_name}_0-0-0"
                    else:
                        zoom_level_name = list(
                            filtered_containing_polygons.keys())[0]
                        dwh_name = f"{output_dwh_name}_{zoom_level_name}"

                    output_dwh_list = [dwh_name]
                    output_type_list = ["GeoDataFrame"]
                    after_split_geodataframe_list = [geodataframe]

                else:
                    # after_split_df_listの各GeoDataFrameにCRSを設定
                    for i in range(len(after_split_geodataframe_list)):
                        after_split_geodataframe_list[i] = gpd.GeoDataFrame(
                            after_split_geodataframe_list[i], geometry='geometry')
                        after_split_geodataframe_list[i].to_crs(
                            epsg=crs, inplace=True)

            # -----------------------------------------------------------------------------------------------------------
            # index分割
            # -----------------------------------------------------------------------------------------------------------
            elif split_method == "index分割":

                n_rows = len(geodataframe)

                # split_sizeでの商と余りを計算
                quotient, remainder = divmod(n_rows, split_index)

                # split_geodataframe_listの初期化
                after_split_geodataframe_list = []

                # 商の分だけループ
                for i in range(quotient):
                    after_split_geodataframe_list.append(
                        geodataframe.iloc[i * split_index: (i + 1) * split_index])

                # 余りがある場合は追加
                if remainder > 0:
                    after_split_geodataframe_list.append(
                        geodataframe.iloc[quotient * split_index:])

                # dwh_listを生成
                output_dwh_list = [f"{output_dwh_name}_{i}" for i in range(
                    len(after_split_geodataframe_list))]

                # type_listを生成
                output_type_list = ["GeoDataFrame"] * \
                    len(after_split_geodataframe_list)

            # -----------------------------------------------------------------------------------------------------------
            # 出力データの成形
            # -----------------------------------------------------------------------------------------------------------
            if no_split_flag:

                self.logger.info(
                    "指定されたズームレベルでは分割は発生しませんでした。")

            else:

                if not split_method == "index分割":
                    output_dwh_list = []
                    output_type_list = []
                    for code in after_split_polygon_dict:
                        output_dwh_list.append(f"{output_dwh_name}_{code}")
                        output_type_list.append("GeoDataFrame")

            split_count = str(len(after_split_geodataframe_list))

            output_dwh_list, output_type_list, after_split_geodataframe_list = WM.calc_func_time(self.logger)(self.create_tile_list)(
                output_dwh_list, output_type_list, after_split_geodataframe_list, output_dwh_name)

            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_list,
                                                                                           output_type_list,
                                                                                           after_split_geodataframe_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes={"Split_Method": split_method,
                                                       "split_count": split_count})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
