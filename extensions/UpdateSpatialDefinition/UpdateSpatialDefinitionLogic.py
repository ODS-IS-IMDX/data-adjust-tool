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
gpd = import_module("geopandas")
translate = getattr(import_module("shapely.affinity"), "translate")
scale = getattr(import_module("shapely.affinity"), "scale")

import cad.common.cad_utils as CU


class UpdateSpatialDefinitionLogic:
    def calculate_origin(self, gdf, position):
        """
        指定した位置に基づいてGeoDataFrameの外接矩形の原点を計算する。

        :param gdf: GeoDataFrameオブジェクト
        :type gdf: geopandas.GeoDataFrame
        :param position: 原点の位置を指定する文字列。'center', 'top-left', 'top-right', 'bottom-left', 'bottom-right' のいずれか。
        :type position: str

        :return: 指定した位置に基づく原点の座標 (x, y)
        :rtype: tuple(numpy.float64, numpy.float64)
        """
        min_x, min_y, max_x, max_y = gdf.total_bounds

        origin_map = {
            'center': ((min_x + max_x) / 2, (min_y + max_y) / 2),
            'top-left': (min_x, max_y),
            'top-right': (max_x, max_y),
            'bottom-left': (min_x, min_y),
            'bottom-right': (max_x, min_y)
        }

        if position not in origin_map:
            raise ValueError(
                "Invalid origin position. Use 'center', 'top-left', 'top-right', 'bottom-left', 'bottom-right'.")

        return origin_map[position]

    def georeference_geodataframe(self, gdf, all_params):
        """
        GeoDataFrameの座標を指定された緯度・経度の原点とスケール、移動量に基づいて調整する。

        :param gdf: GeoDataFrame
        :type gdf: geopandas.GeoDataFrame
        :param all_params: 変換に使用するプロパティ情報を含む辞書
        :type all_params: dict

        :return: オフセットおよびスケールが適用されたGeoDataFrame
        :rtype: geopandas.GeoDataFrame

        :raises Exception: 調整中にエラーが発生した場合に例外をスローする。
        """
        try:
            # GeoDataFrameの原点位置を (Origin Longitude, Origin Latitude)の位置に移動させる
            if all_params["Origin Latitude"] != 0.0 or all_params["Origin Longitude"] != 0.0:
                # GeoDataFrameの原点（外接矩形における指定された基準位置の座標）を取得
                origin_x, origin_y = self.calculate_origin(gdf, all_params["Origin Position"])
                # (Origin Longitude, Origin Latitude)を原点として、全てのジオメトリを移動
                gdf['geometry'] = gdf['geometry'].apply(
                    lambda geom: translate(geom, xoff=-origin_x + all_params["Origin Longitude"], yoff=-origin_y + all_params["Origin Latitude"])
                )

            # X軸・Y軸の移動を適用（任意の座標系での移動）
            gdf["geometry"] = gdf["geometry"].apply(
                lambda geom: translate(geom, xoff=all_params["Shift X"], yoff=all_params["Shift Y"])
            )

            # スケールを適用（原点は(0, 0)でスケーリング）
            gdf['geometry'] = gdf['geometry'].apply(
                lambda geom: scale(geom, xfact=all_params["Coordinate Scale"], yfact=all_params["Coordinate Scale"], origin=(0, 0))
            )

            return gdf

        except Exception as e:
            raise Exception(f"[georeference_geodataframe]: {str(e)}")

    def get_params(self, properties):
        """
        指定されたプロパティからパラメータを取得し、辞書として返す。

        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: パラメータを格納した辞書
        :rtype: dict

        :raises Exception: プロパティの取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            all_params = {}

            # プロパティからオフセット値とスケール値、CRSを取得
            all_params["Origin Latitude"] = CU.get_number_from_string(
                properties['ORIGIN_LATITUDE'])
            all_params["Origin Longitude"] = CU.get_number_from_string(
                properties['ORIGIN_LONGITUDE'])
            all_params["Shift X"] = CU.get_number_from_string(
                properties['SHIFT_X'])
            all_params["Shift Y"] = CU.get_number_from_string(
                properties['SHIFT_Y'])
            all_params["Coordinate Scale"] = CU.get_number_from_string(
                properties['COORDINATE_SCALE'])
            # coordinate_scale が0以下の場合、例外をスローする
            if all_params["Coordinate Scale"] <= 0:
                raise Exception(f"Error: coordinate_scale must be greater than 0")
            all_params["Origin Position"] = properties['ORIGIN_POSITION']
            all_params["CRS"] = CU.get_number_from_string(
                properties['GEO_DATA_FRAME_CRS'], num_type=int)

            return all_params

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def __call__(self, gdf_bytes, attribute, properties):
        """
        バイトデータとして受け取ったGeoDataFrameに基づき、位置・スケールを調整するエントリーポイント。

        :param gdf_bytes: 入力としてのGeoDataFrameのバイトデータ
        :type gdf_bytes: bytes
        :param attribute: 属性情報を格納した辞書
        :type attribute: dict
        :param properties: 変換に必要なプロパティ情報を含む辞書
        :type properties: dict

        :return: 処理済みGeoDataFrameのバイトデータと更新済み属性情報
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:

            # デシリアライズ
            gdf = pickle.loads(gdf_bytes)

            # プロパティ値を取得
            all_params = self.get_params(properties)

            # GeoDataFrameにCRSを設定
            gdf = gdf.set_crs(epsg=all_params["CRS"])

            # GeoDataFrameをオフセットとスケール適用
            aligned_gdf = self.georeference_geodataframe(gdf, all_params)

            # 結果をシリアライズしてFlowFileに書き込む
            serialize_gdf = pickle.dumps(aligned_gdf)
            return serialize_gdf, attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
