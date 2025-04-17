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
# ジオメトリを格納したリストにZ値(-9999)を追加する
# --------------------------------------------------------------------------------------------

# python 標準ライブラリ
import traceback

# 外部モジュールを動的インポート
from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

gpd = import_module("geopandas")
Polygon = import_module("shapely.geometry").Polygon
Point = import_module("shapely.geometry").Point
LineString = import_module("shapely.geometry").LineString


class ConvertGeometry2DTo3D(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ジオメトリを格納したリストにZ値(-9999)を追加する。
                        ①input: ジオメトリ1行のFieldSetFile。
                        ②output: 入力データと同じ形式のデータ。
                      """
        tags = ['Geometry', 'Python']

    property_descriptors = []

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def add_z_dimension(self, geom):
        """
        指定されたジオメトリにZ座標がない場合、Z座標を補完して返す関数。
        Z座標がすでに存在する場合は、そのまま返す。

        対応するジオメトリ型:
            Point
            LineString
            Polygon
        ※対応していない型に対してはエラーを発生させる。

        引数:
            geom: shapely.geometryオブジェクト（Point, LineString, Polygon）

        戻り値:
            Z座標を補完したジオメトリオブジェクト
        """
        # Z座標が既に存在する場合はそのまま返す
        if geom.has_z:
            return geom

        # Point型の処理
        if isinstance(geom, Point):
            # PointのX座標、Y座標、Z座標を取得し、Zがなければ0を補完
            return Point(geom.x, geom.y, geom.z if geom.has_z else -9999)

        # LineString型の処理
        elif isinstance(geom, LineString):
            # LineStringの各頂点座標を処理
            # 各座標が3次元 (x, y, z) かどうかを確認し、2次元なら z = -9999 を補完
            coords = [
                (x, y, z if len(coord) > 2 else -9999)  # Z座標があれば使い、なければ0
                for coord in geom.coords           # LineStringの全頂点座標を取得
                for x, y, *z in [coord]            # 各頂点のx, y, zを分解
            ]
            # Z座標補完後の座標リストを使用して新しいLineStringを生成
            return LineString(coords)

        # Polygon型の処理
        elif isinstance(geom, Polygon):
            # Polygonの外周リング (exterior) の各座標を処理
            # Z座標がなければ z = -9999 を補完
            exterior = [
                (x, y, z if len(coord) > 2 else -9999)  # Z座標があれば使い、なければ0
                for coord in geom.exterior.coords   # Polygonの外周リングの全頂点座標を取得
                for x, y, *z in [coord]             # 各頂点のx, y, zを分解
            ]

            # Polygonの内周リング (interiors: 穴) の各リングを処理
            # 各リングの座標についてZ座標を補完
            interiors = [
                [
                    (x, y, z if len(coord) > 2 else -9999)  # Z座標があれば使い、なければ0
                    for coord in ring.coords           # 内周リングの全頂点座標を取得
                    for x, y, *z in [coord]            # 各頂点のx, y, zを分解
                ]
                for ring in geom.interiors             # Polygonの内周リング（複数）を順に処理
            ]

            # Z座標補完後の外周リングと内周リングを使用して新しいPolygonを生成
            return Polygon(exterior, interiors)

        else:
            # 対応していないジオメトリ型（MultiPoint, MultiPolygonなど）の場合はエラーを発生
            raise ValueError(
                f"MultiPoint, MultiPolygonなどは出来ません。: {type(geom)}")

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # 取得したFieldSetFile(csv)をDataFrameに変換、Value列だけを抜きだし、base64でデコード、pickleでデシリアライズを行う。
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # GeoDataFrame に変換
            geodataframe = gpd.GeoDataFrame(geometry=geometry_value_list)

            geodataframe['geometry'] = geodataframe['geometry'].apply(
                WM.calc_func_time(self.logger)(self.add_z_dimension))

            geometry_list = geodataframe.geometry.tolist()

            output_value = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                [geometry_dwh], [geometry_type], [geometry_list])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_value)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
