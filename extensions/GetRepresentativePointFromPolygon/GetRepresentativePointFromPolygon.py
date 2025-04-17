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
# Polygonから内部保証点を取得するプロセッサ。
# 入ってくるデータがPolygonのgeometry_listの場合、Pointのgeometry_listに変換し、
# 座標配列の場合、同じIDを１ジオメトリとし、IDごとの内部保証点の配列を作成。
# --------------------------------------------------------------------------------------------

# python 標準ライブラリ
from collections import defaultdict
import traceback

# 外部モジュールを動的インポート
from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

np = import_module("numpy")
Polygon = import_module("shapely.geometry").Polygon
Point = import_module("shapely.geometry").Point


class GetRepresentativePointFromPolygon(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ポリゴンから内部保証点を取得する。
                        ①input: ポリゴンのジオメトリを格納したリストか、ポリゴンの座標配列を持った1行のFieldSetFile。
                        ②output: ポイントのジオメトリを格納したリストか、ポイントの座標配列を持った1行のFieldSetFile。
                      """
        tags = ["Geometry", "Coordinates", "Python"]

    # 新しくgeometryのDWH名を指定する。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )

    property_descriptors = [OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要
            プロパティで入力した値を取得する関数

        引数
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ
        戻り値
            geometry_type: 変換後のジオメトリの型
        """
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
        return output_dwh_name

    def polygon_to_representative_point(self, target_value):
        """
        概要
            ポリゴンのリストを受け取って、各ポリゴンの内部保証点を計算する。
        引数
            target_value: ポリゴンのリスト
        戻り値
            output_object: 代表点のリスト
        """

        if isinstance(target_value, list):
            point_list = []
            for polygon_3d in target_value:
                if isinstance(polygon_3d, Polygon):
                    # 内部保証点を計算
                    rep_point = polygon_3d.representative_point()
                    # z 座標がない場合は 0.0 を設定
                    if len(rep_point.coords) > 2:
                        z = rep_point.z
                    else:
                        z = 0.0
                    point_list.append(Point(rep_point.x, rep_point.y, z))
                else:
                    raise ValueError("これはPolygonではありません:", type(polygon_3d))
            output_object = point_list

        elif isinstance(target_value, np.ndarray):
            coordinates_dict = defaultdict(list)
            for entry in target_value:
                # IDと座標を取り出して辞書に追加
                line_id, x, y, z = entry
                coordinates_dict[line_id].append((x, y, z))

            # 結果をnumpy配列に直接変換
            coordinates_list = []
            for line_id, points in coordinates_dict.items():
                # 最初と最後の点が異なる場合、最初の点を末尾に追加
                if points[0] != points[-1]:
                    raise ValueError(
                        f"始点と終点が違う値になっている為、ポリゴンではない。{line_id}:{points}")

                polygon = Polygon(points)
                # 代表点を計算
                rep_point = polygon.representative_point()
                coordinates_list.append((line_id, rep_point.x, rep_point.y, rep_point.z if len(
                    rep_point.coords) > 2 else 0.0))

            output_object = np.array(coordinates_list)

        return output_object

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:
            # Nifiのプロパティで入力したfilenameの呼び出し。
            output_dwh_name = WM.calc_func_time(self.logger)(
                self.get_property)(context, flowfile)

            # 1行のFieldSetFileのValue列を抜きだす。
            field_set_file_dataframe, geometry_dwh_name, target_type, target_value = \
                WM.calc_func_time(self.logger)(
                    PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 内部保証点を計算
            output_object = WM.calc_func_time(self.logger)(
                self.polygon_to_representative_point)(target_value)

            if output_dwh_name is None or output_dwh_name == "":
                field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                    [geometry_dwh_name], [target_type], [output_object])
                dwh_attribute = {"DWH": geometry_dwh_name}
            else:
                field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                    [output_dwh_name], [target_type], [output_object])
                dwh_attribute = {"DWH": output_dwh_name}

            return FlowFileTransformResult(relationship="success", contents=field_set_file, attributes=dwh_attribute)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
