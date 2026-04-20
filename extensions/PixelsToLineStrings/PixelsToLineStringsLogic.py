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
from importlib import import_module

# 外部ライブラリの動的インポート
gpd = import_module("geopandas")
LineString = getattr(import_module("shapely.geometry"), "LineString")

import cad.common.cad_utils as CU


class PixelsToLineStringsLogic:
    """
    ピクセル座標(線分=2点)を、ピクセル座標のまま LineString にするロジック。
    座標変換やCRS付与は行わない。
    """

    def check_pixel_duplicates(self, pixel_lines):
        """
        ピクセルの線リストから重複するピクセル(端点共有など)を検出し、セットとして返す。

        :param pixel_lines: ピクセル座標の線分リスト
        :type pixel_lines: list[list[tuple(int,int), tuple(int,int)]]

        :return: 重複しているピクセル座標のセット
        :rtype: set
        """
        try:
            pixel_count = {}

            # 各ピクセル(端点)の出現回数を数える
            for pixel_line in pixel_lines:
                for point in pixel_line:
                    if point in pixel_count:
                        pixel_count[point] += 1
                    else:
                        pixel_count[point] = 1

            # 2回以上出現している端点を「共有端点」とみなす
            duplicates = {pixel for pixel, count in pixel_count.items() if count > 1}
            return duplicates
        except Exception as e:
            raise Exception(f'[check_pixel_duplicates]: {e}')

    def calculate_endpoint_pixel(self, pixel_line, start_or_end, use_pixel_center):
        """
        ピクセル座標系のまま、始点または終点を0.5ピクセルだけ延長する端点を計算する。

        注意:
          - この処理は「ピクセル座標を変更する」ので、EXTEND_ENDPOINTS=True のときだけ使う想定。
          - use_pixel_center=True のときは基準点が(x+0.5,y+0.5)になる。

        :param pixel_line: 2点の線分
        :type pixel_line: list[tuple(int,int), tuple(int,int)]
        :param start_or_end: 'start' or 'end'
        :type start_or_end: str
        :param use_pixel_center: Trueならピクセル中心を基準にする
        :type use_pixel_center: bool

        :return: 延長後の端点(小数になる可能性あり)
        :rtype: tuple(float, float)
        """
        try:
            start_pixel = pixel_line[0]
            end_pixel = pixel_line[1]

            sx, sy = float(start_pixel[0]), float(start_pixel[1])
            ex, ey = float(end_pixel[0]), float(end_pixel[1])

            # 中心座標に寄せたい場合は、基準点に +0.5 しておく
            base_offset = 0.5 if use_pixel_center else 0.0

            dx = ex - sx
            dy = ey - sy

            # 始点を延長する場合は逆方向へ、終点は正方向へ
            if start_or_end == "start":
                base_x = sx + base_offset
                base_y = sy + base_offset
                dir_x = -dx
                dir_y = -dy
            else:
                base_x = ex + base_offset
                base_y = ey + base_offset
                dir_x = dx
                dir_y = dy

            # 方向の符号だけ見て、0.5ピクセル分外へ出す
            off_x = 0.5 if dir_x > 0 else (-0.5 if dir_x < 0 else 0.0)
            off_y = 0.5 if dir_y > 0 else (-0.5 if dir_y < 0 else 0.0)

            return (base_x + off_x, base_y + off_y)
        except Exception as e:
            raise Exception(f'[calculate_endpoint_pixel]: {e}')

    def process_pixels_to_linestrings(self, pixel_lines, geo_name, use_pixel_center, extend_endpoints):
        """
        ピクセル線分リストをピクセル座標のままLineStringに変換し、GeoDataFrameを作成する。

        :param pixel_lines: ピクセル座標の線分リスト
        :type pixel_lines: list[list[tuple(int,int), tuple(int,int)]]
        :param geo_name: レイヤー名
        :type geo_name: str
        :param use_pixel_center: Trueなら(x+0.5,y+0.5)にする
        :type use_pixel_center: bool
        :param extend_endpoints: Trueなら非共有端点のみ0.5px延長する
        :type extend_endpoints: bool

        :return: GeoDataFrame
        :rtype: geopandas.GeoDataFrame
        """
        try:
            duplicate_pixels = self.check_pixel_duplicates(pixel_lines) if extend_endpoints else set()

            linestrings = []
            for pixel_line in pixel_lines:
                p0 = pixel_line[0]
                p1 = pixel_line[1]

                if extend_endpoints:
                    # 共有端点はズレると接続関係が壊れるので「そのまま」
                    if p0 in duplicate_pixels:
                        start_point = (float(p0[0]) + (0.5 if use_pixel_center else 0.0),
                                       float(p0[1]) + (0.5 if use_pixel_center else 0.0))
                    else:
                        start_point = self.calculate_endpoint_pixel(pixel_line, "start", use_pixel_center)

                    if p1 in duplicate_pixels:
                        end_point = (float(p1[0]) + (0.5 if use_pixel_center else 0.0),
                                     float(p1[1]) + (0.5 if use_pixel_center else 0.0))
                    else:
                        end_point = self.calculate_endpoint_pixel(pixel_line, "end", use_pixel_center)
                else:
                    # 端点延長なし=入力をそのまま(中心に寄せたい場合だけ+0.5)
                    start_point = (float(p0[0]) + (0.5 if use_pixel_center else 0.0),
                                   float(p0[1]) + (0.5 if use_pixel_center else 0.0))
                    end_point = (float(p1[0]) + (0.5 if use_pixel_center else 0.0),
                                 float(p1[1]) + (0.5 if use_pixel_center else 0.0))

                linestrings.append(LineString([start_point, end_point]))

            gdf = gpd.GeoDataFrame(geometry=linestrings)

            # layer列は既存プロセッサに合わせて付与(用途があるなら使える)
            gdf['layer'] = geo_name

            return gdf
        except Exception as e:
            raise Exception(f'[process_pixels_to_linestrings]: {e}')

    def __call__(self, pixel_lines_bytes, attribute, properties):
        """
        FieldSetFile(bytes)から対象DWH属性のValueを取り出し、
        ピクセル座標のままGeoDataFrame(LineString)にしてpickle化して返す。

        :param pixel_lines_bytes: FieldSetFile相当のbytes
        :type pixel_lines_bytes: bytes
        :param attribute: 既存属性
        :type attribute: dict
        :param properties: NiFiプロパティ
        :type properties: dict

        :return: (pickle化GeoDataFrame, 追加属性)
        :rtype: tuple(bytes, dict)
        """
        try:
            # プロパティ取得
            layer_name = properties['LAYER_NAME']
            target_attribute = properties['TARGET_DWH_ATTRIBUTE']
            use_pixel_center = str(properties.get('USE_PIXEL_CENTER', 'false')).lower() == 'true'
            extend_endpoints = str(properties.get('EXTEND_ENDPOINTS', 'false')).lower() == 'true'

            # FieldSetFile -> DataFrame
            df = CU.field_set_file_to_dataframe(pixel_lines_bytes)

            # 対象のレコードのみ抽出
            fsf_value = df[df['Dwh'].str.split('/').str[1] == target_attribute].iloc[0]["Value"]

            # Value列を線分リストへ(既存データ構造に合わせたフラット化)
            # 想定: fsf_value は「[(..., [[(x,y),(x,y)], ...]), (..., [[...], ...]), ...]」みたいな構造
            flattened_pixel_lines = [item for _, sublist in fsf_value if isinstance(sublist, list) for item in sublist]

            # ピクセル座標のままLineString化
            gdf = self.process_pixels_to_linestrings(flattened_pixel_lines, layer_name, use_pixel_center, extend_endpoints)

            # pickle化して返す
            serialized_gdf = pickle.dumps(gdf)
            max_clip_areas = len(gdf) - 1

            out_attribute = dict(attribute) if isinstance(attribute, dict) else {}
            out_attribute['LayerName'] = str(layer_name)
            out_attribute['MaxClipAreas'] = str(max_clip_areas)
            out_attribute['CoordinateMode'] = 'PIXEL'

            return serialized_gdf, out_attribute

        except Exception as e:
            raise Exception(f'[__call__]: {e}')
