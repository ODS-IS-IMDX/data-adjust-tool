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
# GeoDataFrameから座標配列を取得しFieldSetFileとして設定
# 入力はcontentsか１行のFieldSetFileを想定している
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")


class GetCoordinatesInGeoDataFrame(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "2.0.0"
        description = """GeoDataFrameから座標配列を抽出する。"""
        tags = ["python", "pandas", "Coordinates"]

    # 出力される、ポイント、ライン、ポリゴンの外周座標配列のDWH名を指定する
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力される、ポイント、ライン、ポリゴン(外周)座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力される、ポリゴンの内周のDWH名
    OUTPUT_POLYGON_INTERIOR_DWH_NAME = PropertyDescriptor(
        name="Output Polygon Interior DWH Name",
        description="出力される、ポリゴンの内周のDWH名(内周が存在しない場合は、未入力)",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力される、ポリゴンの内周IndexのDWH名
    OUTPUT_POLYGON_INTERIOR_INDEX_DWH_NAME = PropertyDescriptor(
        name="Output Polygon Interior Index DWH Name",
        description="出力される、ポリゴンの内周IndexのDWH名(内周が存在しない場合は、未入力)",
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

    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # 処理後のDWH名
            output_dwh_name\
                = context.getProperty(self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 出力される、ポリゴンの内周のDWH名
            output_polygon_interior_dwh_name = context.getProperty(
                self.OUTPUT_POLYGON_INTERIOR_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 出力される、ポリゴンの内周IndexのDWH名
            output_polygon_interior_index_dwh_name = context.getProperty(
                self.OUTPUT_POLYGON_INTERIOR_INDEX_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # ---------------------------------------------------------------------------
            # flowfileからGeoDataFrame取得
            # ---------------------------------------------------------------------------
            geodataframe\
                = WM.calc_func_time(self.logger)(NSP.get_geodataframe_from_contents_or_field_set_file)(flowfile)
            # ---------------------------------------------------------------------------

            # 出力用のFieldSetFileの要素格納用listを定義
            target_dwh_list = []
            target_type_list = []
            target_value_list = []

            # プロパティで入力された内周の情報が空文字、もしくはNoneだった場合
            if output_polygon_interior_dwh_name in (None, "") or output_polygon_interior_index_dwh_name in (None, ""):

                # ---------------------------------------------------------------------------
                # GeoDataFrameから座標配列取得
                # ---------------------------------------------------------------------------
                result_array, \
                    _, \
                    _\
                    = WM.calc_func_time(self.logger)(NSP.get_coordinates_array_from_geodataframe)(geodataframe)

                # ---------------------------------------------------------------------------
                target_dwh_list, \
                    target_type_list, \
                    target_value_list\
                    = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                            target_type_list,
                                                                                            target_value_list,
                                                                                            output_dwh_name,
                                                                                            'geometry',
                                                                                            result_array)

                # --------------------------------------------------------------------------
                # FieldSetFile構成要素の各リストをFieldSetFileに変換
                # --------------------------------------------------------------------------
                output_field_set_file\
                    = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(target_dwh_list,
                                                                                    target_type_list,
                                                                                    target_value_list)
                # --------------------------------------------------------------------------

            # プロパティで入力された内周の情報がある場合は、ポリゴンとして扱う
            else:
                # GeoDataFrameから、ジオメトリ列をの値を抽出し、list化
                geometry_list = list(geodataframe["geometry"])

                # ジオメトリから、外周の座標配列、内周の座標配列、内周のindex配列を取得
                outer_coordinates_array,\
                    interior_coordinates_array, \
                    interior_index_array =\
                    WM.calc_func_time(self.logger)(
                        NSP.get_geometry_points_numpy_polygon)(geometry_list)

                # 内周が存在しない場合はwarningを出力し、FieldSetFileにも出力しない。
                if len(interior_coordinates_array) == 0:
                    self.logger.warn("内周が存在しませんでした。外周のみ出力します。")

                    # 出力用のFieldSetFileを作成する為に、出力対象と、そのDWH、Typeを各listに格納
                    target_dwh_list = [output_dwh_name]
                    target_type_list = ["geometry"]
                    target_value_list = [outer_coordinates_array]

                # 内周が存在する場合は、内周座標配列、内周index配列もFieldSetFileに加える
                else:
                    # 出力用のFieldSetFileを作成する為に、出力対象と、そのDWH、Typeを各listに格納
                    target_dwh_list = [output_dwh_name, output_polygon_interior_dwh_name, output_polygon_interior_index_dwh_name]
                    target_type_list = ["geometry", "geometry", "interior_index"]
                    target_value_list = [outer_coordinates_array, interior_coordinates_array, interior_index_array]

                output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(target_dwh_list,
                                                                                               target_type_list,
                                                                                               target_value_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
