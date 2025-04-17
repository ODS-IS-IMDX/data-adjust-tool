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

# NiFi自作ライブラリ
import cad.common.cad_utils as CU

gpd = import_module("geopandas")
pd = import_module("pandas")
MultiPoint = getattr(import_module("shapely.geometry"), "MultiPoint")

class TransformPixelsToMultiPointLogic:
    def convert_to_cad_coordinates(
        self,
        pixel_multi_points,
        upper_left_x,
        upper_left_y,
        pixel_to_coord_x,
        pixel_to_coord_y,
    ):
        """
        ピクセル座標をジオメトリ座標に変換する。
        ピクセルサイズに基づき、X方向とY方向のそれぞれのピクセルサイズを考慮した計算を行う。

        :param pixel_multi_points: ピクセルのポイント
        :type pixel_multi_points: list[tuple(numpy.float64, numpy.float64)]
        :param upper_left_x: 左上隅のX座標
        :type upper_left_x: float
        :param upper_left_y: 左上隅のY座標
        :type upper_left_y: float
        :param pixel_to_coord_x: X方向のピクセルからジオメトリ座標への変換係数
        :type pixel_to_coord_x: float
        :param pixel_to_coord_y: Y方向のピクセルからジオメトリ座標への変換係数
        :type pixel_to_coord_y: float

        :return: ジオメトリ座標で表現されたMultiPoint
        :rtype: shapely.geometry.MultiPoint
        """
        # ピクセルの中心を基準にジオメトリ座標へ変換
        # X, Y それぞれに 0.5 ピクセルを加えて中心を考慮
        multi_point_list = []
        for pixel_point in pixel_multi_points:
            cad_x = upper_left_x + (pixel_point[0] + 0.5) * pixel_to_coord_x
            cad_y = (
                upper_left_y - (pixel_point[1] + 0.5) * pixel_to_coord_y
            )  # Y座標は逆方向なので引く
            multi_point_list.append((cad_x, cad_y))

        return MultiPoint(multi_point_list)

    def scale_pixel_coordinates(self, pixel_points, pixel_size_x, pixel_size_y):
        """
        ピクセル座標をワールド座標にスケーリング変換する。

        :param pixel_points: ピクセル座標
        :type pixel_points: list[tuple(numpy.int32, numpy.int32)]
        :param pixel_size_x: X方向のピクセルサイズ
        :type pixel_size_x: float
        :param pixel_size_y: Y方向のピクセルサイズ
        :type pixel_size_y: float

        :return: スケーリングされたワールド座標
        :rtype: list[tuple(numpy.float64, numpy.float64)]
        """
        for i, pixel in enumerate(pixel_points):
            pixel_points[i] = pixel[0] * abs(pixel_size_x), pixel[1] * abs(pixel_size_y)
        return pixel_points

    def process_pixels_to_multipoint(
        self,
        pixel_multi_points,
        pixel_size_x,
        pixel_size_y,
        upper_left_x,
        upper_left_y,
        pixel_to_coord_x,
        pixel_to_coord_y,
        geo_name,
    ):
        """
        ピクセル座標リストをジオメトリ座標のMultiPointに変換し、GeoDataFrame形式で出力する。

        :param pixel_multi_points: ピクセル座標のリスト(MultiPoint)
        :type pixel_multi_points: list[list[tuple(numpy.int32, numpy.int32)]]
        :param pixel_size_x: X方向のピクセルサイズ
        :type pixel_size_x: float
        :param pixel_size_y: Y方向のピクセルサイズ
        :type pixel_size_y: float
        :param upper_left_x: 左上隅のX座標
        :type upper_left_x: float
        :param upper_left_y: 左上隅のY座標
        :type upper_left_y: float
        :param pixel_to_coord_x: X方向の座標変換係数
        :type pixel_to_coord_x: float
        :param pixel_to_coord_y: Y方向の座標変換係数
        :type pixel_to_coord_y: float
        :param geo_name: レイヤー名
        :type geo_name: str

        :return: 変換されたGeoDataFrame
        :rtype: geopandas.GeoDataFrame
        """

        # 各ピクセル線をジオメトリ座標に変換し、MultiPointに変換
        multi_points = []
        for pixel_points in pixel_multi_points:
            # スケーリング変換
            scaled_pixel_points = self.scale_pixel_coordinates(
                pixel_points, pixel_size_x, pixel_size_y
            )

            # ジオメトリ座標に変換
            cad_points = self.convert_to_cad_coordinates(
                scaled_pixel_points,
                upper_left_x,
                upper_left_y,
                pixel_to_coord_x,
                pixel_to_coord_y,
            )
            multi_points.append(cad_points)

        # GeoDataFrameに変換
        gdf = gpd.GeoDataFrame(geometry=multi_points)

        # layerカラムを追加
        gdf["layer"] = geo_name

        return gdf

    def create_dataframe(self, df):
        """
        DataFrameのValue列をデシリアライズしたDataFrameとしていないDataFrameに分割する。

        :param df: DataFrame
        :type df: pandas.DataFrame

        :return: デシリアライズされたDataFrame
        :rtype: pandas.DataFrame
        """
        try:
            # デシリアライズ可能/不可能な行を分けるためのリストを準備
            deserialized_rows = []

            # 各行の'Value'列のデータを確認し、デシリアライズ可能かチェック
            for index, row in df.iterrows():
                value_data = row["Value"]
                deserialized_values = []

                if isinstance(value_data, list):
                    deserializable = False  # デシリアライズ可能かどうかのフラグ
                    for item in value_data:
                        if (
                            isinstance(item, tuple)
                            and len(item) > 1
                            and isinstance(item[1], bytes)
                        ):
                            deserialized_value = pickle.loads(item[1])
                            deserialized_values.append(deserialized_value)
                            deserializable = True  # デシリアライズ可能
                        else:
                            deserialized_values.append(
                                item
                            )  # デシリアライズせずそのまま

                    # デシリアライズ可能かで行を分ける
                    if deserializable:
                        deserialized_row = row.copy()
                        deserialized_row["Value"] = deserialized_values
                        deserialized_rows.append(deserialized_row)

            # デシリアライズ済みの行のDataFrame
            deserialized_df = pd.DataFrame(deserialized_rows)

            return deserialized_df

        except Exception as e:
            raise Exception(f"[create_dataframe_Exception]: {e}")

    def __call__(self, pixel_multipoints_bytes, attribute, properties):
        """
        ピクセルデータをジオメトリ座標に変換し、GeoDataFrame形式で出力する。

        :param pixel_multipoints_bytes: ピクセルデータ
        :type pixel_multipoints_bytes: bytes
        :param attribute: 属性値
        :type attribute: dict
        :param properties: 変換に必要なプロパティ
        :type properties: dict

        :return: シリアライズされたGeoDataFrameと追加属性
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティから必要な値を取得
            pixel_size_x = float(properties["PIXEL_SIZE_X"])
            pixel_size_y = float(properties["PIXEL_SIZE_Y"])
            upper_left_x = float(properties["UPPER_LEFT_X"])
            upper_left_y = float(properties["UPPER_LEFT_Y"])
            pixel_to_coord_x = float(properties["PIXEL_TO_COORDINATE_X"])
            pixel_to_coord_y = float(properties["PIXEL_TO_COORDINATE_Y"])
            gdf_crs = int(properties["CRS"])
            layer_name = properties["LAYER_NAME"]
            target_attribute = properties['TARGET_DWH_ATTRIBUTE']

            # データフレーム作成
            df = CU.field_set_file_to_dataframe(pixel_multipoints_bytes)

            # 対象のレコードのみ抽出
            fsf_value = df[df['Dwh'].str.split('/').str[1] == target_attribute].iloc[0]["Value"]
            point_list = [sublist for _, sublist in fsf_value if isinstance(sublist, list)]

            # Value列をリストに変換
            flattened_pixel_points = []
            if isinstance(point_list, list):
                for data in point_list:
                    if not all(
                        [isinstance(val, tuple) for item in data for val in item]
                    ):
                        # list[list[tuple[int,int], int]]の場合（中心点と半径の情報を持つデータの場合）
                        flattened_pixel_points.append([item[0] for item in data])
                    else:
                        # list[list[tuple[int,int]]]の場合
                        flattened_pixel_points = [item for item in data]

            # メイン処理を呼び出して、GeoDataFrameを生成
            gdf = self.process_pixels_to_multipoint(
                flattened_pixel_points,
                pixel_size_x,
                pixel_size_y,
                upper_left_x,
                upper_left_y,
                pixel_to_coord_x,
                pixel_to_coord_y,
                layer_name,
            )
            gdf = gdf.set_crs(epsg=gdf_crs)
            # 結果をシリアライズ
            serialized_gdf = pickle.dumps(gdf)

            return serialized_gdf, attribute

        except Exception as e:
            raise Exception(f"[__call__　Exception]: {e}")
