# MIT License
#
# Copyright (c) 2026 NTT InfraNet
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
from shapely.geometry import Point


class ReplacePixelToCoordinateLogic:

    def extract_xy(self, gdf):
        """
        GeoDataFrameからX,Y(実座標)を取り出す。

        :param gdf: X,Y(実座標)を持つGeoDataFrame
        :type gdf: GeoPandas.GeoDataFrame

        :return [(X, Y), (X, Y), ...] のリスト
        """
        try:
            xy_list = []

            for i in range(len(gdf)):
                row = gdf.iloc[i]
                xy_list.append((row["X"], row["Y"]))
            return xy_list

        except Exception as e:
            raise Exception(f'[extract_xy Exception]: {str(e)}')

    def replace_geometry_with_xy(self, gdf, xy_list):
        """

        """
        try:
            if len(gdf) != len(xy_list):
                raise ValueError("gdf の行数と xy_list の要素数が一致していません。")

            # geometry を実座標で置き換える
            gdf["geometry"] = [Point(x, y) for x, y in xy_list]

            return gdf

        except Exception as e:
            raise Exception(f'[replace_geometry_with_xy Exception]: {str(e)}')

    def __call__(self, gdf_bytes, attribute, properties):
        """

        """
        try:
            # FlowFile Contents取得
            gdf = pickle.loads(gdf_bytes)

            # 実座標をプロパティから取得
            real_coordinates = self.extract_xy(gdf)

            # Coordinatesに実座標を代入
            new_gdf = self.replace_geometry_with_xy(gdf, real_coordinates)

            return pickle.dumps(new_gdf), attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
