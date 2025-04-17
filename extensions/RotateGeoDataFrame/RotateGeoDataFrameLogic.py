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
import pickle
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
LineString = getattr(import_module("shapely.geometry"), "LineString")
rotate = getattr(import_module("shapely.affinity"), "rotate")
pd = import_module("geopandas")


class RotateGeoDataFrameLogic:

    def __init__(self, **kwargs):
        pass

    def calculate_centroid(self, gdf):
        """
        指定されたGeoDataFrameのジオメトリリストから外接矩形の中心点を計算する関数。

        :param gdf: 中心点を計算する対象のGeoDataFrame。
        :type gdf: geopandas.GeoDataFrame

        :return: 計算された中心点の座標 (centroid_x, centroid_y) を返す。
        :rtype: tuple(numpy.float64, numpy.float64)

        :raises Exception: 計算中にエラーが発生した場合、エラーメッセージと共に例外を発生させる。
        """
        try:
            total_bounds = gdf.total_bounds
            centroid_x = (total_bounds[0] + total_bounds[2]) / 2
            centroid_y = (total_bounds[1] + total_bounds[3]) / 2
            return centroid_x, centroid_y

        except Exception as e:
            raise Exception(f'calculate_centroid_Exception]:{e}')

    def rotate_geodataframe(self, gdf, angle, origin, direction):
        """
        指定された角度でGeoDataFrame内のジオメトリを回転させる関数。
        「TEXT」型の要素に対しては、「rotation」カラムが存在する場合そのカラムの値に回転角度を加算する。

        :param gdf: 回転対象のジオメトリを含むGeoDataFrame。
        :type gdf: geopandas.GeoDataFrame
        :param angle: 回転角度（度単位）。正の値で左回転、負の値で右回転します（directionによって反転）。
        :type angle: float
        :param origin: 回転の原点を指定する (x, y) のタプル。これを中心に回転します。
        :type origin: tuple(int, int)
        :param direction: 回転方向（leftまたはright）を指定します。
        :type direction: str

        :return: 回転後のジオメトリを含むGeoDataFrame。
        :rtype: geopandas.GeoDataFrame
        """
        if direction == 'right':
            angle = -angle

        gdf['geometry'] = gdf['geometry'].apply(lambda geom: rotate(geom, angle, origin=origin, use_radians=False))

        if 'rotation' in gdf.columns:
            gdf.loc[(gdf['type'] == 'TEXT') & gdf['rotation'].notnull(), 'rotation'] += angle

        return gdf

    def __call__(self, serialized_coords, attribute, properties):
        """
        座標データをデシリアライズし、指定された条件に基づいてジオメトリを回転させる関数。

        :param serialized_coords: 座標データを含むシリアライズされたバイト列（GeoDataFrameをpickle形式でシリアライズしたもの）。
        :type serialized_coords: bytes
        :param attribute: 処理対象となるフローファイルの属性。
        :type attribute: dict
        :param properties: 処理に使用するプロパティ情報。
        :type properties: dict

        :return: 回転後のシリアライズされたGeoDataFrame（pickle形式）と、元の属性を含むタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合、エラーメッセージと共に例外を発生させる。
        """
        try:
            # 座標データをデシリアライズ
            geodataframe = pickle.loads(serialized_coords)
            roll_direction = properties['ROLL_DIRECTION']
            roll_angle = float(properties['ROLL_ANGLE'])
            center_mode = properties['CENTER_MODE']

            # 回転の中心を選択する
            if center_mode == 'centroid':
                origin = self.calculate_centroid(geodataframe)  # 外接矩形の重心を中心にする
            else:
                origin = (0, 0)  # 原点 (0,0) を中心にする

            rotated_gdf = self.rotate_geodataframe(geodataframe, roll_angle, origin, roll_direction)

            serialized_gdf = pickle.dumps(rotated_gdf)

            return serialized_gdf, attribute

        except Exception as e:
            raise Exception(f'transform_Exception]:{e}')

