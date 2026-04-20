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
# geometryのValue列を座標データに変換するプロセッサ。
# --------------------------------------------------------------------------------------------

# python 標準ライブラリ
import traceback

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope


class ConvertGeometryToCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "2.0.0"
        description = """
                        ジオメトリを座標配列に変換する。
                        ①input: ジオメトリ1行のFieldSetFile。
                        ②output: 座標配列を持った1行以上のFieldSetFile。
                      """
        tags = ['Geometry', 'Python']

    # 出力される、ポイント、ライン、ポリゴンの外周座標配列のDWH名を指定する
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力される、ポイント、ライン、ポリゴン(外周)座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力される、ポリゴンの内周座標配列のDWH名
    OUTPUT_POLYGON_INTERIOR_DWH_NAME = PropertyDescriptor(
        name="Output Polygon Interior DWH Name",
        description="出力される、ポリゴンの内周座標配列のDWH名",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力される、ポリゴンの内周のDWH名
    OUTPUT_POLYGON_INTERIOR_INDEX_DWH_NAME = PropertyDescriptor(
        name="Output Polygon Interior Index DWH Name",
        description="出力される、ポリゴンの内周IndexのDWH名",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )
    property_descriptors = [OUTPUT_DWH_NAME,
                            OUTPUT_POLYGON_INTERIOR_DWH_NAME,
                            OUTPUT_POLYGON_INTERIOR_INDEX_DWH_NAME]

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
            output_dwh_name: 出力される、ポイント、ライン、ポリゴン(外周)座標配列ののDWH名
            output_polygon_interior_dwh_name: 出力される、ポリゴンの内周座標配列のDWH名
            output_polygon_interior_index_dwh_name: 出力される、ポリゴンの内周IndexのDWH名
        """

        # 出力される、ポイント、ライン、ポリゴン(外周)座標配列のDWH名を取得
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 出力される、ポリゴンの内周のDWH名
        output_polygon_interior_dwh_name = context.getProperty(
            self.OUTPUT_POLYGON_INTERIOR_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 出力される、ポリゴンの内周IndexのDWH名
        output_polygon_interior_index_dwh_name = context.getProperty(
            self.OUTPUT_POLYGON_INTERIOR_INDEX_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name, output_polygon_interior_dwh_name, output_polygon_interior_index_dwh_name

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # Nifiのプロパティで取得したDWH名を取得
            output_dwh_name,\
                output_polygon_interior_dwh_name, \
                output_polygon_interior_index_dwh_name=\
                WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # 取得したFieldSetFile(csv)をDataFrameに変換、Value列だけを抜きだし、base64でデコード、pickleでデシリアライズを行う。
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # プロパティで入力した、内周座標配列のDWH、内周index配列のDWHがない場合は、内周がないものとして扱う
            if output_polygon_interior_dwh_name in (None, "") or output_polygon_interior_index_dwh_name in (None, ""):

                # DataFrameに変換されたValueの値(geometry_value_list)を座標データ(配列)に変換する。
                coordinates_array = WM.calc_func_time(self.logger)(
                    NSP.get_geometries_points_numpy)(geometry_value_list)

                # 出力されるFieldSetFileのType列を取得
                output_type = WM.calc_func_time(self.logger)(NSP.get_field_set_file_geometry_type)(coordinates_array)

                # set_field_set_fileで使用するため戻り値の要素をリスト化
                output_dwh_name_list = [output_dwh_name]
                geometry_type_list = [output_type]
                coordinates_array_list = [coordinates_array]

                # 出力されるDWHをflowfileのattributeに設定する
                attributes_dict = {"DWH": output_dwh_name}

            # ポリゴン内周座標配列と、そのindexを持ったDWH名DWH名が入力された場合、入力されるジオメトリは絶対にポリゴン
            else:
                # ポリゴンのジオメトリから、外周の座標配列、内周の座標配列、内周のindex配列を取得する。
                outer_coordinates_array,\
                    interior_coordinates_array, \
                    interior_index_array =\
                    WM.calc_func_time(self.logger)(
                        NSP.get_geometry_points_numpy_polygon)(geometry_value_list)

                # 内周が存在しない場合はwarningを出力し、FieldSetFileにも出力しない。
                if len(interior_coordinates_array) == 0:
                    self.logger.warn("内周が存在しませんでした。外周のみ出力します。")

                    # 出力されるFieldSetFileのType列を取得
                    output_type = WM.calc_func_time(self.logger)(NSP.get_field_set_file_geometry_type)(outer_coordinates_array)

                    # 出力用のFieldSetFileを作成する為に、出力対象と、そのDWH、Typeを各listに格納
                    output_dwh_name_list = [output_dwh_name]
                    geometry_type_list = [output_type]
                    coordinates_array_list = [outer_coordinates_array]
                    attributes_dict = {"DWH": output_dwh_name}

                # 内周が存在する場合は、内周座標配列、内周index配列もFieldSetFileに加える
                else:

                    # output_field_set_fileを作成する為に、出力対象をlistにまとめる。
                    output_dwh_name_list = [output_dwh_name, output_polygon_interior_dwh_name, output_polygon_interior_index_dwh_name]
                    geometry_type_list = [WM.calc_func_time(self.logger)(NSP.get_field_set_file_geometry_type)(outer_coordinates_array), WM.calc_func_time(self.logger)(NSP.get_field_set_file_geometry_type)(interior_coordinates_array), "interior_index"]
                    coordinates_array_list = [outer_coordinates_array, interior_coordinates_array, interior_index_array]

                    # 3行のFieldSetFileとして出力される為、attributeの更新はしない。
                    attributes_dict = {}

            # 出力用FieldSetFileを作成する
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_name_list,
                                                                         geometry_type_list,
                                                                         coordinates_array_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
