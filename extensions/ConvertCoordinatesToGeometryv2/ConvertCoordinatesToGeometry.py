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
# coordinates_arrayをgeometryのlistに変換するプロセッサ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback
import io
import pickle
import base64

from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部モジュールを動的インポート
np = import_module("numpy")
pd = import_module("pandas")

class ConvertCoordinatesToGeometry(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "2.0.0"
        description = """
                        座標配列からジオメトリを作成する。
                        ①input: 座標配列を持った1行以上のFieldSetFile。
                        ②output: ジオメトリ1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Geometry', 'Python']

    # 変換後のジオメトリのタイプ
    GEOMETRY_TYPE = PropertyDescriptor(
        name="Geometry Type",
        description="出力データのジオメトリ",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=["Point", "LineString", "Polygon"],
        sensitive=False
    )

    # 内周の座標配列のDWH名
    INPUT_POLYGON_INTERIOR_DWH_NAME = PropertyDescriptor(
        name="Input Polygon Interior DWH Name",
        description="内周の座標配列のDWH名(内周が存在しない場合は未入力)",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )

    # 内周のindex配列のDWH名
    INPUT_POLYGON_INTERIOR_INDEX_DWH_NAME = PropertyDescriptor(
        name="Input Polygon Interior Index DWH Name",
        description="内周のindex配列のDWH名(内周が存在しない場合は未入力)",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )


    # 新しくgeometryのDWH名を指定する。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )

    property_descriptors = [GEOMETRY_TYPE, 
                            INPUT_POLYGON_INTERIOR_DWH_NAME,
                            INPUT_POLYGON_INTERIOR_INDEX_DWH_NAME,
                            OUTPUT_DWH_NAME]

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
            output_dwh_name - 処理後のDWH名
        """

        geometry_type = context.getProperty(
            self.GEOMETRY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        input_polygon_interior_dwh_name = context.getProperty(
            self.INPUT_POLYGON_INTERIOR_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        input_polygon_interior_index_dwh_name = context.getProperty(
            self.INPUT_POLYGON_INTERIOR_INDEX_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return geometry_type, input_polygon_interior_dwh_name, input_polygon_interior_index_dwh_name, output_dwh_name

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # Nifiのプロパティで入力した値を取得
            geometry_type, \
                input_polygon_interior_dwh_name, \
                input_polygon_interior_index_dwh_name, \
                output_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileから、CSV形式のFieldSetFileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # FieldSetFileをDataFrameに加工
            field_set_file_dataframe = pd.read_csv(io.StringIO(input_field_set_file))

            # プロパティで入力した、内周座標配列のDWH、内周index配列のDWHがない場合は、内周がないものとして扱う
            if input_polygon_interior_dwh_name in (None, "") or input_polygon_interior_index_dwh_name in (None, ""):

                # 1行のFieldSetFileのValue列を抜きだす。
                field_set_file_dataframe, \
                    geometry_dwh_name, \
                    target_type, \
                    coordinates_array\
                    = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

                # idがkey 構成点がvalueのdictに変更
                coordinates_dict = WM.calc_func_time(self.logger)(
                    NSP.split_geometry_points_dict)(coordinates_array[:, :4])

                # タイプ別で、geometry_listに加工
                if geometry_type == "Point":
                    geometry_list = list(np.array(WM.calc_func_time(self.logger)(
                        NSP.convert_dict_to_point_geometries)(coordinates_dict))[:, 1])

                elif geometry_type == "LineString":
                    geometry_list = list(np.array(WM.calc_func_time(self.logger)(
                        NSP.convert_dict_to_linestring_geometries)(coordinates_dict))[:, 1])
                else:
                    geometry_list = list(np.array(WM.calc_func_time(self.logger)(
                        NSP.convert_dict_to_polygon_geometries)(coordinates_dict))[:, 1])

            # 内周座標配列のDWH、内周index配列のDWHがある場合は、ポリゴンとして扱う
            else:
                for _, row in field_set_file_dataframe.iterrows():

                    # 内周の座標配列を取得する
                    if row["Dwh"] == input_polygon_interior_dwh_name:
                        polygon_interior_coordinates_array = pickle.loads(base64.b64decode(row["Value"]))

                    # 内周index配列を取得する
                    elif row["Dwh"] == input_polygon_interior_index_dwh_name:
                        polygon_interior_index_array = pickle.loads(base64.b64decode(row["Value"]))

                    # 指定されていないものを、外周の座標配列とし、取得する
                    else:
                        polygon_exterior_coordinates_array = pickle.loads(base64.b64decode(row["Value"]))
                        
                        # output_dwh_nameが入力されていない場合に使う
                        geometry_dwh_name = row["Dwh"]

                # 内周座標配列の構成点を地物IDごとにまとめる
                interiors_dict = NSP.convert_interiors_dict(polygon_interior_coordinates_array, polygon_interior_index_array)

                # 外周座標配列の構成点を地物IDごとにまとめる
                exterior_dict = NSP.split_geometry_points_dict(polygon_exterior_coordinates_array[:, :4])

                # 地物IDごとにまとめられた構成点をShapelyに変換する
                geometry_list = list(np.array(NSP.convert_dict_to_hole_polygon_geometries(exterior_dict, interiors_dict))[:, 1])

            # output_dwh名が、プロパティで入力されていない場合、元のDWH名から/で区切った１つ目の値に、/geometryをつける
            # 例
            if output_dwh_name is None or output_dwh_name == "":
                output_dwh_name = geometry_dwh_name.split(
                    '/')[0] + str("/geometry")

            # 出力されるFieldSetFileのType列を取得
            output_type = WM.calc_func_time(self.logger)(NSP.get_field_set_file_geometry_type)(geometry_list)

            # 出力用FieldSetFileを作成
            field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                [output_dwh_name], [output_type], [geometry_list])

            return FlowFileTransformResult(relationship="success",
                                           contents=field_set_file,
                                           attributes={"DWH": output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
