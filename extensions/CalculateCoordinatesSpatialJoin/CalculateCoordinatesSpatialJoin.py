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

# ----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# coordinatesを取り込み空間結合を行う。results形式で出力。
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
gpd = import_module("geopandas")
Point = import_module("shapely.geometry").Point
LineString = import_module("shapely.geometry").LineString
Polygon = import_module("shapely.geometry").Polygon


class CalculateCoordinatesSpatialJoin(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        任意のジオメトリと、それを覆うPolygonで空間結合を行う。
                        ①input: Polygonの座標配列と、空間結合を行いたいジオメトリの座標配列の、2行のFieldSetFile。
                        ②output: 空間結合を行った、前後のインデックスデータ1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Results', 'Python']

    # 情報を抜き出す側のデータのDWH名
    TARGET_DWH_NAME = PropertyDescriptor(
        name="Target DWH Name",
        description="情報を抜き出す側のデータのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # 情報を抜き出す側のジオメトリタイプ
    TARGET_GEOMETRY_TYPE = PropertyDescriptor(
        name="Target Geometry Type",
        description="情報を抜き出す側のジオメトリタイプ",
        allowable_values=["Point", "LineString", "Polygon"],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 出力時のデータのDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [TARGET_DWH_NAME,
                            TARGET_GEOMETRY_TYPE,
                            OUTPUT_DWH_NAME]

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            results_dwh_name: 座標データ(データフレーム)のDwh
        """

        # プロパティで設定した値を取得
        target_dwh_name = context.getProperty(
            self.TARGET_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        target_geometry_type = context.getProperty(
            self.TARGET_GEOMETRY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return target_dwh_name, target_geometry_type, output_dwh_name

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):

        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # プロパティで設定した値を取得
            target_dwh_name, \
                target_geometry_type, \
                output_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileから、csv形式のfield_set_fileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            field_set_file_data_frame = pd.read_csv(
                io.StringIO(input_field_set_file))

            for __, row in field_set_file_data_frame.iterrows():

                target_dict = {}
                target_geometry_dict = {}

                # target処理(point or linestring or polygon)
                if row["Dwh"] == target_dwh_name:

                    if target_geometry_type == "Point":

                        point_array = pickle.loads(
                            base64.b64decode(row["Value"]))

                        point_array_id_list = [int(i)
                                               for i in point_array[:, 0]]
                        point_list = [Point(point_row[1], point_row[2])
                                      for point_row in point_array]

                        target_df = pd.DataFrame({
                            'Findex': point_array_id_list,
                            'Geometry': point_list
                        })

                    if target_geometry_type == "LineString":

                        line_array = pickle.loads(
                            base64.b64decode(row["Value"]))

                        for row in line_array:
                            id_value = int(row[0])
                            if id_value not in target_dict:
                                target_dict[id_value] = []
                            target_dict[id_value].append(row[1:3])

                        for id_value, coords in target_dict.items():
                            coords_array = np.array(coords)[:, :2]
                            target_geometry_dict[id_value] = LineString(
                                coords_array)

                        target_df = pd.DataFrame({
                            'Findex': list(target_geometry_dict.keys()),
                            'Geometry': list(target_geometry_dict.values())
                        })

                    if target_geometry_type == "Polygon":

                        polygon_array = pickle.loads(
                            base64.b64decode(row["Value"]))

                        for row in polygon_array:
                            id_value = int(row[0])
                            if id_value not in target_dict:
                                target_dict[id_value] = []
                            target_dict[id_value].append(row[1:3])

                        for id_value, coords in target_dict.items():
                            coords_array = np.array(coords)[:, :2]
                            target_geometry_dict[id_value] = Polygon(
                                coords_array)

                        target_df = pd.DataFrame({
                            'Findex': list(target_geometry_dict.keys()),
                            'Geometry': list(target_geometry_dict.values())
                        })

                # base処理(polygon)
                else:

                    polygon_array = pickle.loads(
                        base64.b64decode(row["Value"]))

                    base_dict = {}
                    base_geometry_dict = {}

                    for row in polygon_array:
                        id_value = int(row[0])
                        if id_value not in base_dict:
                            base_dict[id_value] = []
                        base_dict[id_value].append(row[1:3])

                    for id_value, coords in base_dict.items():
                        coords_array = np.array(coords)[:, :2]
                        base_geometry_dict[id_value] = Polygon(coords_array)

                    base_df = pd.DataFrame({
                        'Findex': list(base_geometry_dict.keys()),
                        'Geometry': list(base_geometry_dict.values())
                    })

            target_gdf = gpd.GeoDataFrame(target_df, geometry='Geometry')
            base_gdf = gpd.GeoDataFrame(base_df, geometry='Geometry')

            # 空間結合
            joined_gdf = gpd.sjoin(target_gdf, base_gdf,
                                   how="left", predicate="within")

            value_list = [sorted(list(zip(
                joined_gdf['Findex_right'], joined_gdf['Findex_left'])), key=lambda x: (x[0], x[1]))]

            dwh_list = [output_dwh_name]
            type_list = ["results"]

            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                                           type_list,
                                                                                           value_list
                                                                                           )

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
