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
LineString = getattr(import_module("shapely.geometry"), "LineString")
pd = import_module("pandas")
pygltflib = import_module("pygltflib")

import cad.common.cad_utils as CU


class TransformPixelsToLineStringsLogic:

    def check_pixel_duplicates(self, pixel_lines):
        """
        ピクセルの線リストから重複するピクセルを検出し、セットとして返す。

        :param pixel_lines: ピクセル座標のリスト
        :type pixel_lines: list[list[tuple(numpy.int32, numpy.int32), tuple(numpy.int32, numpy.int32)]]

        :return: 重複しているピクセル座標のセット
        :rtype: set
        """
        try:
            pixel_count = {}

            # 各ピクセルを数える
            for pixel_line in pixel_lines:
                for point in pixel_line:
                    if point in pixel_count:
                        pixel_count[point] += 1
                    else:
                        pixel_count[point] = 1

            # 2回以上出現しているピクセルを重複とみなす
            duplicates = {pixel for pixel, count in pixel_count.items() if count > 1}
            return duplicates
        except Exception as e:
            raise Exception(f'[check_pixel_duplicates]: {e}')

    def calculate_endpoint(self, pixel_line, pixel_size_x, pixel_size_y, start_or_end):
        """
        ピクセル線の状態に基づき、始点または終点を延長するための端点を計算する。

        :param pixel_line: ピクセル線の始点および終点を表すタプル
        :type pixel_line: list[tuple(numpy.int32, numpy.int32), tuple(numpy.int32, numpy.int32)]
        :param pixel_size_x: X方向のピクセルサイズ
        :type pixel_size_x: float
        :param pixel_size_y: Y方向のピクセルサイズ
        :type pixel_size_y: float
        :param start_or_end: 'start'の場合は始点を延長し、'end'の場合は終点を延長する
        :type start_or_end: str

        :return: 延長された新しいピクセル座標
        :rtype: tuple(numpy.float64, numpy.float64)
        """
        try:
            # ピクセル線の始点と終点を取得
            start_pixel = pixel_line[0]
            end_pixel = pixel_line[1]

            # X方向とY方向の変位を計算
            delta_x = end_pixel[0] - start_pixel[0]  # X方向の変位
            delta_y = end_pixel[1] - start_pixel[1]  # Y方向の変位

            # 始点または終点に応じて、変位の符号を反転させる
            # 始点の場合、正の変位を負にする
            if start_or_end == "start" and delta_x > 0:
                delta_x *= -1
            if start_or_end == "start" and delta_y > 0:
                delta_y *= -1
            # 終点の場合、負の変位を正にする
            if start_or_end == "end" and delta_x < 0:
                delta_x *= -1
            if start_or_end == "end" and delta_y < 0:
                delta_y *= -1

            # X方向とY方向のオフセット計算（変位の符号に応じてフラグを設定）
            x_flag = -1 if delta_x < 0 else 1
            y_flag = -1 if delta_y < 0 else 1

            # 始点か終点に基づき、対象の点を決定
            pixel = start_pixel if start_or_end == "start" else end_pixel

            # ピクセルの座標をスケーリングして、ワールド座標系に変換
            x, y = self.scale_pixel_coordinates(pixel, pixel_size_x, pixel_size_y)

            # 各方向のオフセットを計算（ピクセルサイズの半分の距離で拡張）
            offset_x = abs(pixel_size_x) * 0.5 * x_flag
            if x < 0:
                offset_x *= -1

            offset_y = abs(pixel_size_y) * 0.5 * y_flag
            if y < 0:
                offset_y *= -1

            # 線分の方向に応じて延長
            if delta_x != 0 and delta_y != 0:
                # 斜め方向の延長（X方向とY方向の変位がある場合）
                extended_point = (x + offset_x, y + offset_y)
            elif delta_x != 0:
                # X方向のみの延長
                extended_point = (x + offset_x, y)
            elif delta_y != 0:
                # Y方向のみの延長
                extended_point = (x, y + offset_y)
            else:
                # 同一点の場合
                extended_point = (x, y)

            # 延長された新しい座標を返す
            return extended_point
        except Exception as e:
            raise Exception(f'[calculate_endpoint]: {e}')

    def convert_to_cad_coordinates(self, pixel_line, upper_left_x, upper_left_y, pixel_to_coord_x, pixel_to_coord_y):
        """
        ピクセル座標をジオメトリ座標に変換する。
        ピクセルサイズに基づき、X方向とY方向のそれぞれのピクセルサイズを考慮した計算を行う。

        :param pixel_line: ピクセルの線 (2点のタプル)
        :type pixel_line: tuple(tuple(numpy.float64, numpy.float64), tuple(numpy.float64, numpy.float64))
        :param upper_left_x: 左上隅のX座標
        :type upper_left_x: float
        :param upper_left_y: 左上隅のY座標
        :type upper_left_y: float
        :param pixel_to_coord_x: X方向のピクセルからジオメトリ座標への変換係数
        :type pixel_to_coord_x: float
        :param pixel_to_coord_y: Y方向のピクセルからジオメトリ座標への変換係数
        :type pixel_to_coord_y: float

        :return: ジオメトリ座標で表現されたLineString
        :rtype: shapely.geometry.LineString
        """
        try:
            # ピクセルの中心を基準にジオメトリ座標へ変換
            # X, Yそれぞれに0.5ピクセルを加えて中心を考慮
            cad_start_x = upper_left_x + (pixel_line[0][0] + 0.5) * pixel_to_coord_x
            cad_start_y = upper_left_y - (pixel_line[0][1] + 0.5) * pixel_to_coord_y  # Y座標は逆方向なので引く
            cad_end_x = upper_left_x + (pixel_line[1][0] + 0.5) * pixel_to_coord_x
            cad_end_y = upper_left_y - (pixel_line[1][1] + 0.5) * pixel_to_coord_y  # Y座標は逆方向なので引く

            return LineString([(cad_start_x, cad_start_y), (cad_end_x, cad_end_y)])
        except Exception as e:
            raise Exception(f'[convert_to_cad_coordinates]: {e}')

    def scale_pixel_coordinates(self, pixel_point, pixel_size_x, pixel_size_y):
        """
        ピクセル座標をワールド座標にスケーリング変換する。

        :param pixel_point: ピクセル座標
        :type pixel_point: tuple(numpy.int32, numpy.int32)
        :param pixel_size_x: X方向のピクセルサイズ
        :type pixel_size_x: float
        :param pixel_size_y: Y方向のピクセルサイズ
        :type pixel_size_y: float

        :return: スケーリングされたワールド座標
        :rtype: tuple(numpy.float64, numpy.float64)
        """
        try:
            return pixel_point[0] * abs(pixel_size_x), pixel_point[1] * abs(pixel_size_y)
        except Exception as e:
            raise Exception(f'[scale_pixel_coordinates]: {e}')

    def process_pixels_to_linestrings(self, pixel_lines, pixel_size_x, pixel_size_y, upper_left_x, upper_left_y,
                                      pixel_to_coord_x, pixel_to_coord_y, geo_name):
        """
        ピクセル座標リストをジオメトリ座標のLineStringに変換し、GeoDataFrame形式で出力する。

        :param pixel_lines: ピクセル座標のリスト
        :type pixel_lines: list[list[tuple(numpy.int32, numpy.int32), tuple(numpy.int32, numpy.int32)]]
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
        try:
            # 重複するピクセルをチェック
            duplicate_pixels = self.check_pixel_duplicates(pixel_lines)

            # 各ピクセル線をジオメトリ座標に変換し、LineStringに変換
            linestrings = []
            for pixel_line in pixel_lines:
                # 始点と終点のそれぞれについて重複チェック
                if pixel_line[0] in duplicate_pixels:
                    # 始点が重複している場合、中心点を使用
                    start_point = self.scale_pixel_coordinates(pixel_line[0], pixel_size_x, pixel_size_y)
                else:
                    # 始点が重複していない場合は端点を計算
                    start_point = self.calculate_endpoint(pixel_line, pixel_size_x, pixel_size_y, 'start')

                if pixel_line[1] in duplicate_pixels:
                    # 終点が重複している場合、中心点を使用
                    end_point = self.scale_pixel_coordinates(pixel_line[1], pixel_size_x, pixel_size_y)
                else:
                    # 終点が重複していない場合は端点を計算
                    end_point = self.calculate_endpoint(pixel_line, pixel_size_x, pixel_size_y, 'end')

                # ジオメトリ座標に変換
                cad_line = self.convert_to_cad_coordinates((start_point, end_point), upper_left_x, upper_left_y,
                                                           pixel_to_coord_x, pixel_to_coord_y)
                linestrings.append(cad_line)

            # GeoDataFrameに変換して返す
            gdf = gpd.GeoDataFrame(geometry=linestrings)

            # 'layer'カラムを追加
            gdf['layer'] = geo_name

            return gdf
        except Exception as e:
            raise Exception(f'[process_pixels_to_linestrings]: {e}')

    def __call__(self, pixel_lines_bytes, attribute, properties):
        """
        ピクセルデータをジオメトリ座標に変換し、GeoDataFrame形式で出力する。

        :param pixel_lines_bytes: ピクセルデータ
        :type pixel_lines_bytes: bytes
        :param attribute: 属性値
        :type attribute: dict
        :param properties: 変換に必要なプロパティ
        :type properties: dict

        :return: シリアライズされたGeoDataFrameと追加属性
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # プロパティから必要な値を取得
            pixel_size_x = float(properties['PIXEL_SIZE_X'])
            pixel_size_y = float(properties['PIXEL_SIZE_Y'])
            upper_left_x = float(properties['UPPER_LEFT_X'])
            upper_left_y = float(properties['UPPER_LEFT_Y'])
            pixel_to_coord_x = float(properties['PIXEL_TO_COORDINATE_X'])
            pixel_to_coord_y = float(properties['PIXEL_TO_COORDINATE_Y'])
            gdf_crs = int(properties['CRS'])
            layer_name = properties['LAYER_NAME']
            target_attribute = properties['TARGET_DWH_ATTRIBUTE']

            # フィールドセットファイルからDataFrame作成
            df = CU.field_set_file_to_dataframe(pixel_lines_bytes)

            # 対象のレコードのみ抽出
            fsf_value = df[df['Dwh'].str.split('/').str[1] == target_attribute].iloc[0]["Value"]

            # Value列をリストに変換
            flattened_pixel_lines = [item for _, sublist in fsf_value if isinstance(sublist, list) for item in sublist]

            # メイン処理を呼び出して、GeoDataFrameを生成
            gdf = self.process_pixels_to_linestrings(flattened_pixel_lines, pixel_size_x, pixel_size_y, upper_left_x,
                                                     upper_left_y, pixel_to_coord_x, pixel_to_coord_y, layer_name)
            gdf = gdf.set_crs(epsg=gdf_crs)
            # 結果をシリアライズしてFlowFileに書き込む
            serialized_gdf = pickle.dumps(gdf)
            max_clip_areas = len(gdf) - 1

            attribute = {'LayerName': str(layer_name), 'crs': str(gdf_crs), 'MaxClipAreas': str(max_clip_areas)}

            return serialized_gdf, attribute

        except Exception as e:
            raise Exception(f'[__call__]: {e}')
