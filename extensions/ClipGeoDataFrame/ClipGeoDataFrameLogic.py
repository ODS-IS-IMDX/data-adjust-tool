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
box = getattr(import_module("shapely.geometry"), "box")

import cad.common.cad_utils as CU


class ClipGeoDataFrameLogic:
    def __init__(self, **kwargs):
        pass

    def clip_geodataframe(self, gdf, lower_left_x, lower_left_y, upper_right_x, upper_right_y):
        """
        指定された座標でGeoDataFrameをクリップする。

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

        :return: クリップされたGeoDataFrame
        :rtype: geopandas.GeoDataFrame
        """
        # クリップ範囲に矩形のジオメトリ（ポリゴン）を設定
        clipping_box = box(lower_left_x, lower_left_y, upper_right_x, upper_right_y)

        # GeoDataFrameをクリップ
        clipped_gdf = gdf.clip(clipping_box)
        return clipped_gdf

    def __call__(self, byte_data, attributes, properties):
        """
        FlowFile内のGeoDataFrameを指定された範囲でクリップし、結果をシリアライズして出力する関数。

        :param byte_data: 入力となるGeoDataFrameのバイトデータ
        :type byte_data: bytes
        :param attributes: FlowFileの属性情報を格納する辞書
        :type attributes: dict
        :param properties: 処理に必要なプロパティ情報を含む辞書
        :type properties: dict

        :return: シリアライズされたGeoDataFrameと追加属性
        :rtype: tuple(bytes, dict)
        
        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # GeoDataFrameを取得しデシリアライズ
            gdf = pickle.loads(byte_data)

            # プロパティと属性情報からクリップ範囲の値を取得
            lower_left_x = CU.get_number_from_string(attributes.get("Lower Left X Coordinate") or properties['LOWER_LEFT_X'])
            lower_left_y = CU.get_number_from_string(attributes.get("Lower Left Y Coordinate") or properties['LOWER_LEFT_Y'])
            upper_right_x = CU.get_number_from_string(attributes.get("Upper Right X Coordinate") or properties['UPPER_RIGHT_X'])
            upper_right_y = CU.get_number_from_string(attributes.get("Upper Right Y Coordinate") or properties['UPPER_RIGHT_Y'])

            # GeoDataFrameをクリップ
            clipped_gdf = self.clip_geodataframe(gdf, lower_left_x, lower_left_y, upper_right_x, upper_right_y)

            # クリップされたGeoDataFrameをシリアライズ
            serialized_clipped_gdf = pickle.dumps(clipped_gdf)

            return serialized_clipped_gdf, attributes

        except Exception as e:
            raise Exception(f"[transform_Exception]: {str(e)}")


