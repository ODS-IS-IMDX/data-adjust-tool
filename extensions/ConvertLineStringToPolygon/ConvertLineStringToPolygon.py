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
# LineStringをPolygonに変換するプロセッサ。
# 入ってくるデータがLineStringのgeometry_listの場合、polygonのgeometry_listに変換し、
# coordinates_arrayの場合、始点と終点が同じでない場合は、終点を始点と同じ値に追加する。
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
LineString = import_module("shapely.geometry").LineString
Polygon = import_module("shapely.geometry").Polygon

TRUE_GEOMETRY_ERROR_PASS_FLAG = "継続"
FALSE_GEOMETRY_ERROR_PASS_FLAG = "停止"


class ConvertLineStringToPolygon(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        LineStringをPolygonに変換する。
                        ①input: LineStringのジオメトリが格納されたリストか、座標配列のどちらかを持った1行のFieldSetFile。
                        ②output: Polygonのジオメトリが格納されたリストか、座標配列のどちらかを持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Geometry', 'Linestring', 'Polygon', 'Python']

    # 新しくgeometryのDWH名を指定する。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )

    # ポリゴンに変換後、無効なジオメトリだった場合、後続の処理を行うのかのフラグ
    GEOMETRY_ERROR_PASS_FLAG = PropertyDescriptor(
        name="Geometry Error Pass Flag",
        description="ポリゴンにした際に、無効なジオメトリがあった場合に処理を継続するか停止するかを決定するフラグ",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=[TRUE_GEOMETRY_ERROR_PASS_FLAG,
                          FALSE_GEOMETRY_ERROR_PASS_FLAG],
        sensitive=False,
        required=False
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            GEOMETRY_ERROR_PASS_FLAG]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        プロパティで入力した値を取得する関数

        引数
            context - プロセッサの設定値が格納されているデータ
            flowfile - プロセッサに入ってくるデータ
        戻り値
            geometry_type - 変換後のジオメトリの型
            output_dwh_name - 処理後のdwh名
        """

        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        geometry_error_pass_flag = context.getProperty(
            self.GEOMETRY_ERROR_PASS_FLAG).evaluateAttributeExpressions(flowfile).getValue()
        return output_dwh_name, geometry_error_pass_flag

    def linestring_to_polygon(self, target_value, geometry_error_pass_flag):
        """
        概要:
            LineStringのジオメトリのリストまたは座標配列をPolygonに変換する関数
        引数:
            target_value: LineStringのジオメトリのリストまたは座標配列
            geometry_error_pass_flag: ジオメトリが無効だった場合に処理を続けるか（"継続"）、エラーを発生させるか（"停止"）を制御するフラグ。

        戻り値:
            output_object: LineStringのリストから変換されたPolygonのリスト、または座標が修正されて閉じたポリゴンに変換されたnumpy配列。

        例外:
            ValueError: geometry_error_pass_flagが"停止"の場合に無効なジオメトリが検出された際に発生。
        """

        # デコードデシリアライズした形がlistの場合、LineStringのlistからpolygonのlistに変更する。
        if type(target_value) is list:
            poly_list = []
            for geom in target_value:

                # ポリゴンにする場合は、閉じられている必要がある。
                # 閉じられていない場合は、始点と同じ値をを新しい終点とする。
                if not geom.is_closed:
                    geom = LineString(list(geom.coords) + [geom.coords[0]])
                poly_list.append(Polygon(geom))
            output_object = poly_list

        # デコードデシリアライズした形が配列の場合は、coordinates_arrayをIDをkey,IDごとに線の構成点のlistをValueのdictに変換し、
        # 各ジオメトリのlistの始点と終点が同じ出ない場合に、始点と同じ値をを新しい終点とする。
        elif isinstance(target_value, np.ndarray):
            coordinates_dict = defaultdict(list)
            for entry in target_value:
                # IDと座標を取り出して辞書に追加
                line_id, x, y, z = entry
                coordinates_dict[line_id].append((x, y, z))

            # 結果をnumpy配列に直接変換
            coordinates_list = []
            for line_id, points in coordinates_dict.items():

                polygon = []
                # 最初と最後の点が異なる場合、最初の点を末尾に追加
                if points[0] != points[-1]:
                    polygon = points
                    polygon.append(points[0])
                else:
                    polygon = points

                if Polygon(polygon).is_valid:
                    # line_idと座標をリストに追加
                    coordinates_list.extend(
                        [[line_id, *point] for point in polygon])
                else:
                    if geometry_error_pass_flag == TRUE_GEOMETRY_ERROR_PASS_FLAG:
                        self.logger.warn(
                            f"無効なジオメトリ発生。Findex:{line_id} coordinates:{points}")
                        coordinates_list.extend(
                            [[line_id, *point] for point in polygon])

                    else:
                        raise ValueError(
                            f"無効なジオメトリ発生。Findex:{line_id} coordinates:{points}")

            output_object = np.array(coordinates_list)

        return output_object

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # Nifiのプロパティで入力したfilenameの呼び出し。
            output_dwh_name, \
                geometry_error_pass_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # 1行のFieldSetFileのValue列を抜きだす。
            field_set_file_dataframe, \
                geometry_dwh_name, \
                target_type, \
                target_value\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            output_object \
                = WM.calc_func_time(self.logger)(self.linestring_to_polygon)(target_value, geometry_error_pass_flag)

            if output_dwh_name is None or output_dwh_name == "":
                field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                    [geometry_dwh_name], [target_type], [output_object])
                dwh_attribute = {"DWH": geometry_dwh_name}

            else:
                field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                    [output_dwh_name], [target_type], [output_object])
                dwh_attribute = {"DWH": output_dwh_name}

            return FlowFileTransformResult(relationship="success",
                                           contents=field_set_file,
                                           attributes=dwh_attribute)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
