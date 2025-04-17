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
# coordinates_arrayと属性値をデータ定義に則りGeoDataFrameに変換する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
import traceback
import base64
from io import StringIO

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
gpd = import_module("geopandas")


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

    property_descriptors = [CRS,
                            DATA_DEFINITION_DELIMITER]

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

        return crs, data_definition_delimiter

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

    def extract_coordinates_array_from_field_set_file(self, field_set_file_data_frame, geometry_file_name_list):
        """
        概要:
            field_set_file_data_frameのDwh列で、geometry_file_name_listの0番目(geometryのDWH)の値の行とValue列の値を抽出し
            base64でデコード、デシリアライズし配列に戻す関数
        引数:
            field_set_file_data_frame: field_set_fileをデータフレームに加工した物
            geometry_file_name_list: データ定義ファイル内のgeometryのDwhファイル名が格納されているリスト(基本要素は1つ)
        戻り値:
            geometry_value_coordinates_array: 座標配列
        """
        # Value列且つ、Dwh列の値がgeometry_file_name_listのインデックスが0番目
        geometry_value_coordinates_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_file_name_list[0], "Value"].values[0]))

        return geometry_value_coordinates_array

    def convert_target_data_to_geodataframe(self,
                                            all_attribute_dataframe,
                                            geometry_value_coordinates_array,
                                            crs):
        """
        概要
            all_attribute_dataframeとgeometry_value_coordinates_arrayからGeoDataFrameを作成する関数
        引数:
            all_attribute_dataframe: 出力対象属性をすべて含むデータフレーム
            geometry_value_coordinates_array: 座標配列
            crs: 出力するシェープファイルのCRS
        戻り値:
            GeoDataFrame
        """

        geodataframe = gpd.GeoDataFrame(all_attribute_dataframe,
                                        geometry=geometry_value_coordinates_array,
                                        crs=crs)
        return geodataframe

    def transform(self, context, flowfile):
        try:
            # -----------------------------------------------------------------------------------------------------------
            # 【取得】フローファイルからFieldSetFileとプロパティの設定値と製品データ定義ファイルを取得
            # -----------------------------------------------------------------------------------------------------------
            crs, data_definition_delimiter\
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
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index)(data_definition_stream, data_definition_delimiter=data_definition_delimiter
                                                                                )

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】field_set_file_data_frameから、座標配列を抽出。
            # -----------------------------------------------------------------------------------------------------------
            geometry_value_coordinates_array = WM.calc_func_time(self.logger)(
                self.extract_coordinates_array_from_field_set_file)(field_set_file_data_frame, geometry_file_name_list)

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】製品データ定義ファイルに指定された属性項目ファイルをすべて読み込み一つのDataFrameとする
            # -----------------------------------------------------------------------------------------------------------
            all_attribute_dataframe = WM.calc_func_time(self.logger)(NSP.create_attribute_dataframe)(field_set_file_data_frame, dwh_file_name_list, attribute_name_list, attribute_const_value_list, attribute_file_type_list, len(np.unique(geometry_value_coordinates_array[:, 0])), encoding="UTF-8", input_file_type=1
                                                                                                     )

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】ジオメトリファイルを読み込みnumpy配列を取得
            # -----------------------------------------------------------------------------------------------------------
            coordinates_shapely_array = WM.calc_func_time(self.logger)(NSP.get_attribute_coordinates_by_geometry_items)(geometry_value_coordinates_array, geometry_type_list[0]
                                                                                                                        )

            # -----------------------------------------------------------------------------------------------------------
            # 【取得】属性のDataFrameと座標のnumpy配列からGeoJSONの文字列データを作成し、整形
            # -----------------------------------------------------------------------------------------------------------
            geodataframe \
                = WM.calc_func_time(self.logger)(self.convert_target_data_to_geodataframe)(all_attribute_dataframe,
                                                                                           coordinates_shapely_array,
                                                                                           crs)

            return FlowFileTransformResult(relationship="success",
                                           contents=pickle.dumps(geodataframe))

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
