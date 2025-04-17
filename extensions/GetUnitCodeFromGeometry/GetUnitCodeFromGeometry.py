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
# ジオメトリが格納されているlistから、どの国土基本図図郭にあるのかを判定し、
# 図郭コードをAttributesのunit_codeに格納するプロセッサ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
Polygon = import_module("shapely").geometry.Polygon
LineString = import_module("shapely").geometry.LineString
box = import_module("shapely").geometry.box
MultiLineString = import_module("shapely").geometry.MultiLineString

MultiPolygon = import_module("shapely").geometry.MultiPolygon
MultiPoint = import_module("shapely").geometry.MultiPoint
Point = import_module("shapely").geometry.Point


class GetUnitCodeFromGeometry(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ジオメトリが属する図郭コードを取得する。
                        ①input: ジオメトリ1行のFieldSetFile。
                        ①output: ジオメトリ1行のFieldSetFile。
                      """
        tags = ['Geometry', 'Attributes', 'Python']

    # 地図情報レベル
    UNIT_LEVEL = PropertyDescriptor(
        name="Unit Level",
        description="地図情報レベル",
        default_value="500",
        allowable_values=["500", "2500"],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [UNIT_LEVEL]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context):
        """
        プロパティで入力した値を取得する関数

        引数:
            context : プロセッサの設定値が格納されているデータ

        戻り値:
            unit_level : 地図情報レベル
        """

        unit_code_level = context.getProperty(self.UNIT_LEVEL).getValue()

        return unit_code_level

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

        # レベル5000のポリゴンを格納用list
        level5000_polygons = []

        # 1列目
        for level_5000_right_number in range(grid_size):
            # 1行目
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

                # レベル5000のポリゴンを格納用listにまとめる。
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
        50000の図郭コードをkeyに、分割されたポリゴンをvalueにし、
        level5000_dictに格納。

        対象の図郭の、indexと、それに触れあっている図郭のインデックスを更新する

        引数:
            level50000_polygon_code : レベル50000の図郭コード。
            level50000_polygon : 対象のレベル50000のポリゴン。
            temp_50000_dict : 図郭コードとpolygonを格納する、dict

        戻り値:
            temp_50000_dict : 対象の図郭コードとポリゴンを格納する、dict
        """

        # polygonをレベル5000に分割
        level5000_polygons = WM.calc_func_time(self.logger, False)(
            self.separate_polygon_100)(level50000_polygon, grid_size=10)

        # 対象の図郭を保持
        temp_50000_dict[level50000_polygon_code] = level5000_polygons

        return temp_50000_dict

    def search_level5000_and_separate_2500(self, level5000_polygon_code, level5000_polygon, temp_5000_dict, level50000_polygon_code):
        """
        polygonの分割を行ってレベル2500のポリゴンを生成し、
        5000までのコードをkeyに、分割されたポリゴンをvalueにし、
        level2500_dictに格納。

        対象の図郭の、indexと、それに触れあっている図郭のインデックスを更新する

        引数:
            level5000_polygon_code : レベル5000の図郭コード。
            level5000_polygon : 対象のレベル5000のポリゴン。
            temp_5000_dict : 図郭コードとpolygonを格納する、初期化されたdict
            level50000_polygon_code : 対象となる平面直角座標系の系レベル50000の図郭コード。

        戻り値:
            temp_5000_dict : 図郭コードとポリゴンを格納する、dict
        """

        level2500_polygons = WM.calc_func_time(self.logger, False)(
            self.separate_polygon_4)(level5000_polygon)

        # keyを図郭コード:valueをレベル2500のポリゴン群のdictを保持
        temp_5000_dict[level50000_polygon_code +
                       level5000_polygon_code] = level2500_polygons

        return temp_5000_dict

    def search_level5000_and_separate_500(self, level5000_polygon_code,
                                          level5000_polygon,
                                          level50000_polygon_code,
                                          temp_5000_dict,
                                          ):
        """
        polygonの分割を行ってレベル500のポリゴンを生成し、
        5000までのコードをkeyに、分割されたポリゴンをvalueにし、
        level500_dictに格納。

        対象の図郭の、indexと、それに触れあっている図郭のインデックスを更新する

        引数:
            level5000_polygon_code : レベル5000の図郭コード。
            level5000_polygon : 対象のレベル5000のポリゴン。
            level50000_polygon_code : 対象となる平面直角座標系の系レベル50000の図郭コード。
            temp_5000_dict : 図郭コードとpolygonを格納する、初期化されたdict

        戻り値:
            temp_5000_dict : 1ジオメトリ分だけの図郭コードとpolygonを格納するdict
        """
        level500_polygons \
            = WM.calc_func_time(self.logger)(self.separate_polygon_100)(level5000_polygon, grid_size=10)

        # keyを図郭コード:valueをレベル500のポリゴン群のdictを保持
        temp_5000_dict[level50000_polygon_code + level5000_polygon_code] \
            = level500_polygons

        return temp_5000_dict

    def transform(self, context, flowfile):
        try:
            # プロパティで入力した値を取得
            unit_code_level = WM.calc_func_time(
                self.logger)(self.get_property)(context)

            field_set_file_dataframe, geometry_dwh, geometry_type, geometry_value_list = WM.calc_func_time(
                self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # CRSの取得
            try:
                crs = flowfile.getAttribute("crs")
            except Exception:
                self.logger.error(traceback.format_exc())

            # ターゲットレベルの取得
            try:
                target_level50000 = DDC.TARGET_LEVEL50000_DICT[crs]
            except Exception:
                self.logger.error(traceback.format_exc())

            try:
                # 対象のジオメトリがどの、エリアにいるのかを特定するために、マルチ化。
                # こうすることで、大幅な処理時間の短縮になる。
                if isinstance(geometry_value_list[0], LineString):
                    multi_geometry = MultiLineString(geometry_value_list)

                elif isinstance(geometry_value_list[0], Polygon):
                    multi_geometry = MultiPolygon(geometry_value_list)

                elif isinstance(geometry_value_list[0], Point):
                    multi_geometry = MultiPoint(geometry_value_list)
            except Exception:
                self.logger.error(traceback.format_exc())

            # ポリゴンのサイズと数の設定

            # xの原点
            origin_x = 0

            # yの原点
            origin_y = 0

            # polygonの幅 メートル
            width_m = 40000

            # polygonの高さ メートル
            height_m = 30000

            # polygonの個数(正の数)
            num_x = 4
            num_y = 10

            # レベル50000ポリゴンを作成に必要な数字。
            origin_x = 0
            origin_y = 0
            width_m = 40000
            height_m = 30000
            num_x = 4
            num_y = 10

            try:
                # CRSでどのエリアなのかを特定。(例:6677 = 09)
                target_level50000 = DDC.TARGET_LEVEL50000_DICT[crs]
            except Exception:
                self.logger.error(traceback.format_exc())
                return FlowFileTransformResult(relationship="failure")

            # 行名と列名の作成
            rows = [chr(char_code)
                    for char_code in range(ord("A"), ord("A") + num_y * 2)]
            columns = [chr(char_code)
                       for char_code in range(ord("A"), ord("A") + num_x * 2)]

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

            # ユニークな図郭コードをlist化
            unique_unit_code_list = list(unit_code_set)

            # 各要素の5文字目以降の数字を昇順で並び替える。
            sorted_unit_code_list = sorted(
                unique_unit_code_list, key=lambda x: int(x[4:]))

            # 各要素を@で区切り、文字列化
            unit_code = "@".join(sorted_unit_code_list)

            return FlowFileTransformResult(relationship="success", attributes={"unit_code": unit_code})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
