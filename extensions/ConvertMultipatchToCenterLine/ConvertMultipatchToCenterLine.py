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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 共通のIDを所持しているgeometryを1つにまとめた"マルチパッチ"を作成するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import zipfile
import pickle
import base64
import traceback
import math

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート

from shapely.geometry import Point, LineString, Polygon, MultiLineString
from shapely import MultiPoint, Polygon, concave_hull


np = import_module("numpy")
pd = import_module("pandas")
pygeoops = import_module("pygeoops")
CREATE = "作成する"
NOT_CREATE = "作成しない"
ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class ConvertMultipatchToCenterLine(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        マルチパッチを座標配列に変換する。
                        但し、管路かつ、マルチパッチがカーブをしていないもの限定とする。
                        ①input: マルチパッチを持った1行のFieldSetFile。
                        ②output: 2Dの座標配列を持った1行のFieldSetFile。
                      """
        tags = ["Coordinates", "Multipatch", "ZIP", "Unpack", "Python"]

    # 新しくgeometryのDWH名を指定する。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            start_multipatch_flag: 角柱の入口にマルチパッチを作成するかのフラグ
            end_multipatch_flag: 角柱の出口にマルチパッチを作成するかのフラグ
            width_dwh_name: 幅のDWHファイル名
            depth_dwh_name: 深さのDWHファイル名
            output_dwh_name: output_field_set_file用のgeometryのDWHファイル名
            output_zip_flag: ZIP圧縮するかどうかのフラグ
        """

        # output_field_set_file用のgeometryのDWHファイル名
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name

    def extend_line_to_polygon(self, center_line, mbr, extension_length=10000.0):
        """
        概要:
            LineStringの両端を、ポリゴン境界まで延長する

        引数:
            center_line: 回転外接矩形の中心線
            mbr: minimum bounding rectangle 2Dに加工したマルチパッチの回転外接矩形
            extension_length: 中心線を延長する長さ ポリゴンに絶対に当たる数字
        """
        center_line_coords = list(center_line.coords)

        # 中心線の両端を延長
        start_vector = np.array(center_line_coords[1]) - np.array(center_line_coords[0])
        end_vector = np.array(center_line_coords[-2]) - np.array(center_line_coords[-1])
        start_extension = np.array(center_line_coords[0]) - start_vector / np.linalg.norm(start_vector) * extension_length
        end_extension = np.array(center_line_coords[-1]) - end_vector / np.linalg.norm(end_vector) * extension_length

        # ポリゴンのエッジを取得
        polygon_edges = [LineString([mbr.exterior.coords[i], mbr.exterior.coords[i + 1]]) for i in
            range(len(mbr.exterior.coords) - 1)]

        # 最初に交差するエッジを検索
        start_point, end_point = None, None
        min_start_dist, min_end_dist = float('inf'), float('inf')

        start_intersection_line = LineString([center_line_coords[0], tuple(start_extension)])
        end_intersection_line = LineString([center_line_coords[-1], tuple(end_extension)])

        for i, edge_line in enumerate(polygon_edges):
            start_intersection = edge_line.intersection(start_intersection_line)
            end_intersection = edge_line.intersection(end_intersection_line)

            if not start_intersection.is_empty and isinstance(start_intersection, Point):
                dist = start_intersection.distance(Point(center_line_coords[0]))
                if dist < min_start_dist:
                    min_start_dist = dist
                    start_point = start_intersection

            if not end_intersection.is_empty and isinstance(end_intersection, Point):
                dist = end_intersection.distance(Point(center_line_coords[-1]))
                if dist < min_end_dist:
                    min_end_dist = dist
                    end_point = end_intersection

        # 中心線の両端に中点を追加し、Z値を線形補間
        if start_point and end_point:
            extended_centerline = LineString([start_point] + center_line_coords + [end_point])

        return extended_centerline
    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------

    def transform(self, context, flowfile):
        try:

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            filename = flowfile.getAttribute("filename")

            # Nifiのプロパティから入力したものを呼び出す。
            output_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # 取得したFieldSetFile(csv)をDataFrameに変換、Value列だけを抜きだし、base64でデコード、pickleでデシリアライズを行う。
            field_set_file_dataframe, \
                input_fieldset_dwh, \
                input_fieldset_type, \
                multipatch_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # マルチパッチの１地物の開始終了indexを取得
            si_array,ei_array=NCP.get_start_index_and_end_index(multipatch_array[:,:4])

            result_list=[]

            for i, (si, ei) in enumerate(zip(si_array, ei_array)):
                temp_2D_unique_array = np.unique(multipatch_array[si:ei+1, 1:3], axis=0)
                if len(temp_2D_unique_array) <= 2:
                    print(f"temp_2D_unique_arrayが一点しかない:{i}番目")

                    continue

                # 各頂点をマルチポイントに変換し、平面のポリゴン化
                multi_points = MultiPoint(temp_2D_unique_array)
                concave_hull_shapely = concave_hull(multi_points, ratio=1)

                # 平面ポリゴンの回転外接矩形に変換
                mbr = concave_hull_shapely.minimum_rotated_rectangle

                if concave_hull_shapely.geom_type != 'Polygon':
                    self.logger.warn(f"concave_hull_shapelyの形がポリゴンになっていない:{i}番目")
                    continue

                # 回転外接矩形の中心線を取得
                center_line = pygeoops.centerline(mbr)
                extended_centerline = self.extend_line_to_polygon(center_line)
                result_list.append(extended_centerline)

            # FieldSetFileに変換する。

            # マルチパッチをフィールドセットファイルに加工
            output_field_set_file = (PBP.set_field_set_file)(
                [output_dwh_name], ["geometry"], [result_list])

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file, attributes={"DWH": output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
