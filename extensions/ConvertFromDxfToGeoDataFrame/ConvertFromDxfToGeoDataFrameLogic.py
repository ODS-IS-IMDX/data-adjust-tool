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
import os
from math import radians, cos, sin, pi
from importlib import import_module

# 外部ライブラリの動的インポート
ezdxf = import_module("ezdxf")
gpd = import_module("geopandas")
pd = import_module("pandas")
Point = getattr(import_module("shapely.geometry"), "Point")
LineString = getattr(import_module("shapely.geometry"), "LineString")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")

import cad.common.cad_utils as CU


class ConvertFromDxfToGeoDataFrameLogic:

    def normalize_angle(self, angle):
        """
        角度を0~2πの範囲に正規化する。

        :param angle: 円弧の開始角、終了角
        :type angle: float

        :return: 正規化後の角度
        :rtype: float
        """
        while angle < 0:
            angle += 2 * pi
        while angle >= 2 * pi:
            angle -= 2 * pi
        return angle

    def adjust_angle_range(self, start_angle, end_angle):
        """
        開始角度が終了角度より大きい場合に終了角度を調整する。

        :param start_angle: 円弧の開始角
        :type start_angle: float
        :param end_angle: 円弧の終了角
        :type end_angle: float

        :return: 調整後の開始角度と終了角度
        :rtype: tuple(float, float)
        """
        if start_angle > end_angle:
            end_angle += 2 * pi
        return start_angle, end_angle

    def is_clockwise(self, points):
        """
        ポリゴンの座標が時計回りかどうかを判断する。

        :param points: ポリゴンの座標リスト（(x, y, z) タプルのリスト）
        :type points: list[tuple]

        :return: 時計回りならTrue、反時計回りならFalse
        :rtype: bool

        :raises Exception: 座標リストの処理中にエラーが発生した場合
        """
        try:
            sum = 0
            for i in range(len(points)):
                # x, y座標を取得
                x1, y1 = points[i][:2]
                # x, y座標を取得
                x2, y2 = points[(i + 1) % len(points)][:2]
                sum += (x2 - x1) * (y2 + y1)
            return sum > 0

        except Exception as e:
            raise Exception(f'[is_clockwise_Exception]:{e}')

    def process_block(self, block, data, origin, scale):
        """
        DXFブロック内のすべてのエンティティを処理し、ジオメトリデータを収集する。

        :param block: 処理するDXFブロック
        :type block: Block
        :param data: エンティティ情報を格納するリスト
        :type data: list[dict]
        :param origin: 平面直角座標系の原点（(x, y)）
        :type origin: tuple
        :param scale: CADの1座標単位が現実世界で何メートルに相当するかのスケール
        :type scale: float
        :raises Exception: エンティティ処理中にエラーが発生した場合
        """
        try:
            for entity in block:
                entity_dict = self.process_entity(entity, origin, scale)
                if entity_dict:
                    data.append(entity_dict)

        except Exception as e:
            raise Exception(f'[process_block_Exception]:{e}')

    def process_insert(self, insert_entity, doc, data, origin, scale):

        """
        INSERTエンティティを処理し、参照されるブロック内のエンティティを再帰的に処理する。

        :param insert_entity: 処理するINSERTエンティティ
        :type insert_entity: Insert
        :param doc: DXFドキュメントオブジェクト
        :type doc: ezdxf.document.DXFDocument
        :param data: エンティティ情報を格納するリスト
        :type data: list[dict]
        :param origin: 平面直角座標系の原点（(x, y)）
        :type origin: tuple
        :param scale: CADの1座標単位が現実世界で何メートルに相当するかのスケール
        :type scale: float

        :raises Exception: INSERTエンティティ処理中にエラーが発生した場合
        """
        try:
            block_name = insert_entity.dxf.name
            block = doc.blocks.get(block_name)
            if block:
                self.process_block(block, data, origin, scale)
                for nested_insert in block:
                    if nested_insert.dxftype() == 'INSERT':
                        self.process_insert(nested_insert, doc, data, origin, scale)

        except Exception as e:
            raise Exception(f'[process_insert_Exception]:{e}')

    def process_entity(self, entity, origin, scale):
        """
        指定されたDXFエンティティを処理し、ジオメトリ情報を抽出する。

        :param entity: 処理するDXFエンティティオブジェクト
        :type entity: Entity
        :param origin: 平面直角座標系の原点（(x, y)）
        :type origin: tuple
        :param scale: CADの1座標単位が現実世界で何メートルに相当するかのスケール
        :type scale: float

        :return: エンティティのタイプ、レイヤー、属性、およびジオメトリを含む辞書
        :rtype: dict or None

        :raises Exception: エンティティ処理中にエラーが発生した場合
        """
        try:
            entity_dict = {
                "type": entity.dxftype(),
                "layer": entity.dxf.layer,
                "attributes": {},
                "geometry": None
            }

            def transform_point(point):
                """
                CADの座標を平面直角座標系に変換する。

                :param point: CADの座標タプル (x, y)
                :type point: tuple

                :return: 平面直角座標系での座標タプル (x, y)
                :rtype: tuple
                """
                x = origin[0] + (point[0] * scale)
                y = origin[1] + (point[1] * scale)
                return x, y

            # 各エンティティタイプに応じたジオメトリの処理
            if entity.dxftype() == 'LINE':
                start = transform_point((entity.dxf.start.x, entity.dxf.start.y))
                end = transform_point((entity.dxf.end.x, entity.dxf.end.y))
                entity_dict["geometry"] = LineString([start, end])

            elif entity.dxftype() == 'POLYLINE':
                points = [transform_point((vertex.dxf.location.x, vertex.dxf.location.y)) for vertex in entity.vertices]

                if entity.is_closed:
                    # 座標順序が時計回りでない場合、時計周りに並び替える
                    if not self.is_clockwise(points):
                        middle_points = points[1:]
                        middle_points.reverse()
                        # 開始座標以外の座標を取得し順番を逆にする
                        clockwise_points = [points[0]] + middle_points if points[0] != points[-1] else middle_points
                        entity_dict["geometry"] = Polygon(clockwise_points)
                    else:
                        entity_dict["geometry"] = Polygon(points)
                else:
                    entity_dict["geometry"] = LineString(points)

            elif entity.dxftype() == 'ARC':
                center = transform_point((entity.dxf.center.x, entity.dxf.center.y))
                radius = entity.dxf.radius * scale
                start_angle = radians(entity.dxf.start_angle)
                end_angle = radians(entity.dxf.end_angle)
                start_angle = self.normalize_angle(start_angle)
                end_angle = self.normalize_angle(end_angle)
                start_angle, end_angle = self.adjust_angle_range(start_angle, end_angle)

                points = [
                    transform_point((
                        center[0] + radius * cos(angle),
                        center[1] + radius * sin(angle)
                    )) for angle in [start_angle + (end_angle - start_angle) * i / 100 for i in range(101)]
                ]
                entity_dict["geometry"] = LineString(points)
                entity_dict["attributes"] = {
                "center": center,
                "radius": radius,
                "start_angle": start_angle,
                "end_angle": end_angle
            }

            elif entity.dxftype() == 'CIRCLE':
                center = (entity.dxf.center.x, entity.dxf.center.y)
                radius = entity.dxf.radius
                circle = Point(center).buffer(radius).exterior
                entity_dict["geometry"] = circle
                entity_dict["attributes"] = {
                    "center": center,
                    "radius": radius
                }

            elif entity.dxftype() == 'TEXT':
                point = transform_point((entity.dxf.insert.x, entity.dxf.insert.y))
                entity_dict["geometry"] = Point(point)
                entity_dict["attributes"] = {
                    "text": entity.dxf.text,
                    "rotation": entity.dxf.rotation,
                    "height": entity.dxf.height * scale,
                    "halign": entity.dxf.get('halign', 0),
                    "valign": entity.dxf.get('valign', 0)
                }

            elif entity.dxftype() == 'POINT':
                point = transform_point((entity.dxf.location.x, entity.dxf.location.y))
                entity_dict["geometry"] = Point(point)

            elif entity.dxftype() == 'LWPOLYLINE':
                points = list(entity.get_points())
                points = [transform_point((point[0], point[1])) for point in points]
                if entity.is_closed:
                    entity_dict["geometry"] = LineString(points)
                else:
                    entity_dict["geometry"] = LineString(points)
            
            else:
                return None

            return entity_dict

        except Exception as e:
            raise Exception(f'[process_entity_Exception]:{e}')

    def process_entities(self, msp, doc, data, origin, scale):

        """
        モデルスペース内のすべてのエンティティを処理し、ジオメトリデータを収集する。

        :param msp: モデルスペースオブジェクト（DXFドキュメント内の図面データ）
        :type msp: ezdxf.entities.Modelspace
        :param doc: DXFドキュメントオブジェクト
        :type doc: ezdxf.document.DXFDocument
        :param data: エンティティ情報を格納するリスト（辞書形式）
        :type data: list[dict]
        :param origin: 平面直角座標系の原点（(x, y)）
        :type origin: tuple
        :param scale: CADの1座標単位が現実世界で何メートルに相当するかのスケール
        :type scale: float

        :raises Exception: 処理中にエラーが発生した場合
        """
        try:
            processed_blocks = set()

            # INSERTタイプであれば、参照しているblockを再帰的に取得
            for entity in msp:
                if entity.dxftype() == 'INSERT':
                    block_name = entity.dxf.name
                    if block_name not in processed_blocks:
                        processed_blocks.add(block_name)
                        self.process_insert(entity, doc, data, origin, scale)
                else:
                    entity_dict = self.process_entity(entity, origin, scale)
                    if entity_dict:
                        data.append(entity_dict)

        except Exception as e:
            raise Exception(f'[process_entities_Exception]:{e}')

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてシリアライズされたGeoDataFrameと属性情報を返す。

        :param byte_data: 入力としてのDxfファイルのバイトデータ
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: シリアライズされたGeoDataFrameと、更新された属性情報を含むタプル
        :rtype: tuple

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 入力ファイルのパスを取得
            filename = attribute.get('filename', '')
            dir_path = attribute.get('absolute.path', '')
            dxf_file_path = os.path.join(dir_path, filename)

            # 拡張子がDXFでない場合は失敗とする
            if os.path.splitext(filename)[1].lower() != '.dxf':
                raise Exception(f'DXFファイルではありません。({filename})')

            # プロパティから必要な値を取得
            gdf_crs = CU.get_number_from_string(properties['GEO_DATA_FRAME_CRS'], num_type=int)
            origin_x = CU.get_number_from_string(properties['ORIGIN_X'])
            origin_y = CU.get_number_from_string(properties['ORIGIN_Y'])
            coordinate_scale = CU.get_number_from_string(properties['COORDINATE_SCALE'])

            # coordinate_scaleが0以下の場合は失敗とする
            if coordinate_scale <= 0:
                raise Exception(f"Error coordinate_scale is less than 0")
            coordinate_unit = properties['COORDINATE_UNIT']

            origin = (origin_x, origin_y)

            doc = ezdxf.readfile(dxf_file_path, encoding='cp932')
            msp = doc.modelspace()

            data = []
            self.process_entities(msp, doc, data, origin, coordinate_scale)

            gdf = gpd.GeoDataFrame(data, geometry='geometry')
            gdf = gdf.set_crs(epsg=gdf_crs)

            # レイヤ最大インデックス番号を取得
            layer_list = gdf['layer']
            unique_layer_list = layer_list.unique()
            max_layer_index = len(unique_layer_list) - 1

            # attributes列を分解して新しいカラムに展開
            attributes_df = pd.json_normalize(gdf['attributes'])
            
            # attributesをGeoDataFrameに結合
            gdf = pd.concat([gdf, attributes_df], axis=1)

            # attributes列は不要になったら削除
            gdf = gdf.drop(columns=['attributes'])

            # string型のデータの型を変更
            if 'type' in gdf.columns:
                gdf['type'] = gdf.get('type', '').astype('string')
            if 'layer' in gdf.columns:
                gdf['layer'] = gdf.get('layer', '').astype('string')
            if 'text' in gdf.columns:
                gdf['text'] = gdf.get('text', '').astype('string')

            output_content = pickle.dumps(gdf)

            attribute = {'MaxLayerIndex': str(max_layer_index),
                         'CoordinateUnit': coordinate_unit
                         }

            return output_content, attribute

        except Exception as e:
            raise Exception(f'[transform_Exception]:{e}')
