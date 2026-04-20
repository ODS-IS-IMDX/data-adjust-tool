# MIT License
# 
# Copyright (c) 2025,2026 NTT InfraNet
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
# coordinates_arrayと属性値をデータ定義に則りGeoDataFrameに変換する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
import traceback
from io import StringIO

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
gpd = import_module("geopandas")
Polygon = import_module("shapely").geometry.Polygon
MultiPolygon = import_module("shapely").geometry.MultiPolygon


class ConvertFieldSetFileToGeoDataFrame(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データ定義記載項目に従い、FieldSetFileをGeoDataFrameに変換する。
                        ①input: 座標配列と、GeoDataFrameにするのに用いたいフィールドを持った複数行のFieldSetFile。
                        ②output: シリアライズされたGeoDataFrame。
                      """
        tags = ['Coordinates', 'GeoDataFrame', 'Python']

    # シェープファイルのCRSを指定
    CRS = PropertyDescriptor(
        name="CRS",
        description="CRS(epsgコード)",
        default_value="${crs}",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # データ定義ファイルの区切り文字を指定
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    POLYGON_INTERIOR_DISTRIBUTION_NAME = PropertyDescriptor(
        name="Polygon Interior Distribution Name",
        description="ポリゴンの内周座標配列の流通項目名",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=False,
        sensitive=False
    )

    POLYGON_INTERIOR_INDEX_DISTRIBUTION_NAME = PropertyDescriptor(
        name="Polygon Interior Index Distribution Name",
        description="ポリゴンの内周Index配列の流通項目名",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=False,
        sensitive=False
    )

    property_descriptors = [CRS,
                            DATA_DEFINITION_DELIMITER,
                            POLYGON_INTERIOR_DISTRIBUTION_NAME,
                            POLYGON_INTERIOR_INDEX_DISTRIBUTION_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数
        引数:
            context: プロパティで入力した値を取得する関数
            flowfile: プロセッサに入ってくるデータ
        戻り値:
            output_dwh_name: 出荷用のDWH名
            crs: GeoDataFrameのCRS
            data_definition_delimiter: データ定義ファイルの区切り文字(例:Comma)
        """
        # GeoDataFrameのCRSを指定
        crs = int(context.getProperty(
            self.CRS).evaluateAttributeExpressions(flowfile).getValue())

        # データ定義ファイルの区切り文字を指定
        data_definition_delimiter = context.getProperty(
            self.DATA_DEFINITION_DELIMITER).getValue()

        # 内周座標配列取得用流通項目名
        polygon_interior_distribution_name= context.getProperty(
            self.POLYGON_INTERIOR_DISTRIBUTION_NAME).getValue()

        # 内周index配列取得用流通項目名
        polygon_interior_index_distribution_name = context.getProperty(
            self.POLYGON_INTERIOR_INDEX_DISTRIBUTION_NAME).getValue()

        return crs, data_definition_delimiter, polygon_interior_distribution_name, polygon_interior_index_distribution_name

    def get_flowfile(self, flowfile):
        """
        概要:
            flowfileから、DataDefinitionというattributeのキーから値を取得し、(データ定義ファイル)
            input_field_set_fileをデータフレームに加工する関数
        引数:
            flowfile: プロセッサに入ってくるデータ
        戻り値:
            data_definition_stream: データ定義ファイル(csv形式)
            field_set_file_data_frame: field_set_fileをデータフレームに加工した物
        """
        # データ定義ファイルをflowfileのattribute、DataDefinitionから取得
        data_definition = flowfile.getAttribute("DataDefinition")

        # data_definitionを、pathとして扱う。
        data_definition_stream = StringIO(data_definition)

        # flowfileから、csv形式のfield_set_fileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # データフレームに加工
        field_set_file_data_frame = pd.read_csv(StringIO(input_field_set_file))

        return data_definition_stream, field_set_file_data_frame

    def convert_target_data_to_geodataframe(self,
                                            all_attribute_dataframe,
                                            geometry_value_coordinates_array,
                                            crs):
        """
        概要
            属性のDataFrameと座標のnumpy配列からGeoDataFrameを作成する関数
        引数:
            all_attribute_dataframe: 出力対象属性をすべて含むデータフレーム
            geometry_value_coordinates_array: Shapelyオブジェクト配列
            crs: 出力するGeoDataFrameのCRS
        戻り値:
            GeoDataFrame
        """

        geodataframe = gpd.GeoDataFrame(all_attribute_dataframe,
                                        geometry=geometry_value_coordinates_array,
                                        crs=crs)
        return geodataframe

    def get_coordinates_multipolygon_shapely_array(self, coordinates_array):
        
        coordinates_shapely_list=[]
        
        #si,ei取得
        si,ei=NCP.get_start_index_and_end_index(coordinates_array)

        #地物ごとにループ
        for i in range(len(si)):
            
            
            polygon_list=[]
            #ポリゴンを構成する座標数
            POLYGON_COORDINATES_COUNTS=4
            
            #地物取り出す
            temp_array=coordinates_array[si[i]:ei[i]+1,1:][:, :3]
            
            #三角形の枚数＞４点＞xyz座標に変換
            temp_array=temp_array.reshape((int(temp_array.shape[0]/4),
                                        POLYGON_COORDINATES_COUNTS,
                                        temp_array.shape[1]))
            
            
            polygon_list=[Polygon(temp_array[ti]) for ti in range(len(temp_array))]
            
            coordinates_shapely_list.append(MultiPolygon(polygon_list))

        return coordinates_shapely_list

    def transform(self, context, flowfile):
        try:
            # -----------------------------------------------------------------------------------------------------------
            # 【取得】フローファイルからFieldSetFileとプロパティの設定値と製品データ定義ファイルを取得
            # -----------------------------------------------------------------------------------------------------------
            crs, \
                data_definition_delimiter, \
                polygon_interior_distribution_name, \
                polygon_interior_index_distribution_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            data_definition_stream, field_set_file_data_frame = WM.calc_func_time(
                self.logger)(self.get_flowfile)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】製品データ定義ファイルから出力時ジオメトリ項目ファイル名List、出力時属性項目ファイル名List、入力ファイル元の属性名List取得を取得
            # -----------------------------------------------------------------------------------------------------------
            geometry_type_list, \
                geometry_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list\
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index)(data_definition_stream,
                                                                                data_definition_delimiter=data_definition_delimiter
                                                                                )

            # 座標配列が1行の場合
            if len(geometry_distribution_name_list) == 1:

                # -----------------------------------------------------------------------------------------------------------
                # 【取得】field_set_file_data_frameから、座標配列を抽出。
                # -----------------------------------------------------------------------------------------------------------
                geometry_value_coordinates_array = WM.calc_func_time(self.logger)(
                    NSP.get_value_from_field_set_file_dataframe)(field_set_file_data_frame, geometry_file_name_list[0])

                # -----------------------------------------------------------------------------------------------------------
                # 【取得】ジオメトリファイルを読み込みnumpy配列を取得
                # -----------------------------------------------------------------------------------------------------------

                # マルチパッチの場合
                if geometry_value_coordinates_array.shape[1] == 8:
                    coordinates_shapely_array = self.get_coordinates_multipolygon_shapely_array(geometry_value_coordinates_array)

                # マルチパッチ以外の場合
                else:
                    coordinates_shapely_array = WM.calc_func_time(self.logger)(NSP.get_attribute_coordinates_by_geometry_items)(geometry_value_coordinates_array,
                                                                                                                                geometry_type_list[0]
                                                                                                                                )

            # 内周座標配列、内周index配列も入力されていた場合。
            else:
                # データ定義ファイルの流通項目名列の中で、プロパティで指定した値を内周座標配列又は、内周index配列とする
                for i in range(len(geometry_distribution_name_list)):

                    # 内周座標配列のをFieldSetFileから抽出する
                    if geometry_distribution_name_list[i] == polygon_interior_distribution_name:
                        polygon_interior_coordinates_array = WM.calc_func_time(self.logger)(
                            NSP.get_value_from_field_set_file_dataframe)(field_set_file_data_frame,
                                                                                geometry_file_name_list[i])

                    # 内周index配列のをFieldSetFileから抽出する
                    elif geometry_distribution_name_list[i] == polygon_interior_index_distribution_name:
                        polygon_interior_index_array = WM.calc_func_time(self.logger)(
                            NSP.get_value_from_field_set_file_dataframe)(field_set_file_data_frame,
                                                                                geometry_file_name_list[i])

                    # 内周座標配列でも、内周index配列でもなければ、外周座標配列とする
                    else:
                        geometry_value_coordinates_array = WM.calc_func_time(self.logger)(
                            NSP.get_value_from_field_set_file_dataframe)(field_set_file_data_frame,
                                                                                geometry_file_name_list[i])

                # 内周座標配列の構成点を地物IDごとにまとめる
                interiors_dict =  WM.calc_func_time(self.logger)(
                    NSP.convert_interiors_dict)(polygon_interior_coordinates_array, polygon_interior_index_array)

                # 外周座標配列の構成点を地物IDごとにまとめる
                exterior_dict =  WM.calc_func_time(self.logger)(
                    NSP.split_geometry_points_dict)(geometry_value_coordinates_array[:, :4])

                # 地物IDごとにまとめられた構成点をShapelyに変換する
                geometry_list =  WM.calc_func_time(self.logger)(
                    NSP.convert_dict_to_hole_polygon_geometries)(exterior_dict, interiors_dict)

                # 各listの1つ目の要素の地物IDを除く為に、配列に変換する
                coordinates_shapely_array = np.array(geometry_list)[:, 1]

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】製品データ定義ファイルに指定された属性項目ファイルをすべて読み込み一つのDataFrameとする
            # -----------------------------------------------------------------------------------------------------------
            all_attribute_dataframe = WM.calc_func_time(self.logger)(NSP.create_attribute_dataframe)(field_set_file_data_frame,
                                                                                                     dwh_file_name_list,
                                                                                                     attribute_name_list,
                                                                                                     attribute_const_value_list,
                                                                                                     attribute_file_type_list,
                                                                                                     len(np.unique(geometry_value_coordinates_array[:, 0])),
                                                                                                     encoding="UTF-8",
                                                                                                     input_file_type=1
                                                                                                     )

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】属性のDataFrameと座標のnumpy配列からGeoDataFrameを作成する
            # -----------------------------------------------------------------------------------------------------------

            geodataframe = gpd.GeoDataFrame(all_attribute_dataframe,
                                            geometry=coordinates_shapely_array,
                                            crs=crs)

            return FlowFileTransformResult(relationship="success",
                                           contents=pickle.dumps(geodataframe))

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
