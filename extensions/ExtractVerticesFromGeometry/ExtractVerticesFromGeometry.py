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

# ---------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# LineString または Polygon のジオメトリから構成点(Point)を抽出するプロセッサ。
# 入出力はFieldSetFile形式で、value列にpickle+base64されたジオメトリリストが入っている。
# ---------------------------------------------------------------------------------------------------
# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

Point = import_module("shapely.geometry").Point
LineString = import_module("shapely.geometry").LineString
Polygon = import_module("shapely.geometry").Polygon


class ExtractVerticesFromGeometry(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        Polygon または LineString から構成点（Point）を抽出する。
                        ①input: Polygon または LineStringジオメトリ1行のFieldSetFile。
                        ②output: Pointジオメトリ1行のFieldSetFile。
                      """
        tags = ['Geometry', 'Extract', 'Point']

    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    property_descriptors = [OUTPUT_DWH_NAME]


    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors


    def get_property(self, context, flowfile):
        """
            プロパティで入力した値を取得

        Parameters:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        Returns:
            output_dwh_name: 出力データのDWH名
        """

        # プロパティで設定した値を取得
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name

    def extract_vertices(self, geom):
        """
        Polygon または LineString の構成点(Point)を抽出

        Parameters:
            geom: shapely.geometry.Polygon または LineString オブジェクト

        Returns:
            list[Point]: 構成点のリスト

        Raises:
            TypeError: 対応していないジオメトリ型の場合
        """

        if isinstance(geom, Polygon):

            points = [Point(coord) for coord in list(geom.exterior.coords)]

            if geom.interiors:
                for interior in geom.interiors:
                    points.extend([Point(coord) for coord in list(interior.coords)])

            return points

        elif isinstance(geom, LineString):

            return [Point(coord) for coord in list(geom.coords)]

        else:

            raise TypeError(f"対応していないジオメトリタイプです: {type(geom).__name__}")

    def transform(self, context, flowfile):
        try:

            # -----------------------------------------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # -----------------------------------------------------------------------------------------------------------
            output_dwh_name = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # flowfileの値を取得しFieldSetFileをDataFrameに加工
            # -----------------------------------------------------------------------------------------------------------
            field_set_file_dataframe, geometry_dwh, geometry_type, geometry_value = \
                WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # DEM作成
            # -----------------------------------------------------------------------------------------------------------
            extracted_points = []

            for geom in geometry_value:
                extracted_points.extend(WM.calc_func_time(self.logger)(self.extract_vertices)(geom))

            # -----------------------------------------------------------------------------------------------------------
            # DEM作成
            # -----------------------------------------------------------------------------------------------------------
            if output_dwh_name is None or output_dwh_name == "":
                output_field_set_file = WM.calc_func_time(self.logger)(
                    PBP.set_field_set_file)([geometry_dwh], ["geometry"], [extracted_points])
                attributes_dict = {"DWH": geometry_dwh}

            else:
                output_field_set_file = WM.calc_func_time(self.logger)(
                    PBP.set_field_set_file)([output_dwh_name], ["geometry"], [extracted_points])
                attributes_dict = {"DWH": output_dwh_name}

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")