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
# FieldSetFileの全行をGeoDataFrameに変換する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
import traceback
from io import StringIO
import base64

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
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

class ConvertFieldSetFileFromFieldToGeoDataFrame(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        FieldSetFileの全行をGeoDataFrameに変換する。
                        ①input: GeoDataFrame又は、DataFrameに変換する、GeoNdArray ジオメトリリスト、フィールドを持った複数行のFieldSetFile。
                        ②output: GeoDataFrame又は、DataFrameを持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'GeoDataFrame', 'Python']

    # CRSを指定
    CRS = PropertyDescriptor(
        name="CRS",
        description="CRS(epsgコード) ※データフレームを出力する場合は 「Set empty string」にチェックを入れてください。",
        default_value="${crs}",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )

    # ポリゴンの内周配列のDWH名
    POLYGON_INTERIOR_DWH_NAME = PropertyDescriptor(
        name="Polygon Interior DWH Name",
        description="ポリゴンの内周配列のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    # ポリゴンの内周Index配列のDWH名
    POLYGON_INTERIOR_INDEX_DWH_NAME = PropertyDescriptor(
        name="Polygon Interior Index DWH Name",
        description="ポリゴンの内周Index配列のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    # GeoNdArrayをジオメトリに変換する際の型
    GEONDARRAY_GEOMETRY_TYPE = PropertyDescriptor(
        name='GeoNdArray Geometry Type',
        description='GeoNdArrayをジオメトリに変換する際の型',
        allowable_values=[DDC.POINT, DDC.LINESTRING, DDC.POLYGON],
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力されるGeoDataFrameのDWH名
    OUTPUT_GEODATAFRAME_DWH_NAME = PropertyDescriptor(
        name='Output GeoDataFrame DWH Name',
        description='出力されるGeoDataFrameのDWH名',
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [CRS,
                            POLYGON_INTERIOR_DWH_NAME,
                            POLYGON_INTERIOR_INDEX_DWH_NAME,
                            GEONDARRAY_GEOMETRY_TYPE,
                            OUTPUT_GEODATAFRAME_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        # GeoDataFrameのCRSを指定
        crs = context.getProperty(
            self.CRS).evaluateAttributeExpressions(flowfile).getValue()

        # 内周配列取得用DWH名
        polygon_interior_dwh_name \
            = context.getProperty(self.POLYGON_INTERIOR_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 内周index配列取得用DWH名
        polygon_interior_index_dwh_name \
            = context.getProperty(self.POLYGON_INTERIOR_INDEX_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
        
        # GeoNdarrayをジオメトリに変換する際の型を取得
        geondarray_geometry_type\
            = context.getProperty(self.GEONDARRAY_GEOMETRY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        # 出力されるGeoDataFrameのDWH名
        output_geodataframe_dwh_name\
            = context.getProperty(self.OUTPUT_GEODATAFRAME_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return crs, polygon_interior_dwh_name, polygon_interior_index_dwh_name, geondarray_geometry_type, output_geodataframe_dwh_name

    def get_flowfile(self, flowfile):
        """
        input_field_set_fileをデータフレームに加工する関数

        flowfile: プロセッサに入ってくるデータ

        field_set_file_data_frame: field_set_fileをデータフレームに加工した物
        """
        # flowfileから、csv形式のfield_set_fileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # データフレームに加工
        field_set_file_data_frame = pd.read_csv(StringIO(input_field_set_file))

        return field_set_file_data_frame

    def get_coordinates_multipolygon_shapely_array(self, coordinates_array):
        """
        GeoNdarrayをマルチポリゴンに変換する
        
        coordinates_array: マルチパッチのGeoNdarray

        return: マルチポリゴンに変換されたジオメトリ
        """

        coordinates_shapely_list=[]
        
        #si,ei取得
        si,ei=NCP.get_start_index_and_end_index(coordinates_array)

        #地物ごとにループ
        for i in range(len(si)):

            polygon_list=[]

            #地物取り出す
            temp_array=coordinates_array[si[i]:ei[i]+1,1:][:, :3]
            
            #三角形の枚数＞４点＞xyz座標に変換
            temp_array=temp_array.reshape((int(temp_array.shape[0]/DDC.POLYGON_COORDINATES_COUNTS),
                                        DDC.POLYGON_COORDINATES_COUNTS,
                                        temp_array.shape[1]))

            polygon_list=[Polygon(temp_array[ti]) for ti in range(len(temp_array))]

            coordinates_shapely_list.append(MultiPolygon(polygon_list))

        return coordinates_shapely_list

    def get_interior_polygon_flom_field_set_file(self,
                                                 coordinates_field_set_file_data_frame,
                                                 polygon_interior_dwh_name,
                                                 interior_index_field_set_file_data_frame,
                                                 polygon_interior_index_dwh_name):
        """
        内周のジオメトリを取得する
        
        coordinates_field_set_file_data_frame: 座標のみのFieldSetFileのデータフレーム
        polygon_interior_dwh_name: 内周arrayのDWH名
        interior_index_field_set_file_data_frame: 内周indexのみのFieldSetFileのデータフレーム
        polygon_interior_index_dwh_name: 内周indexのDWH名

        return: 内周込みのポリゴン
        """
        # データ定義ファイルの流通項目名列の中で、プロパティで指定した値を内周配列又は、内周index配列とする
        # 内周配列のをFieldSetFileから抽出する
        polygon_interior_coordinates_array = WM.calc_func_time(self.logger)(
            NSP.get_value_from_field_set_file_dataframe)(coordinates_field_set_file_data_frame,
                                                         polygon_interior_dwh_name)

        # 内周index配列のをFieldSetFileから抽出する
        polygon_interior_index_array = WM.calc_func_time(self.logger)(
            NSP.get_value_from_field_set_file_dataframe)(interior_index_field_set_file_data_frame,
                                                         polygon_interior_index_dwh_name)

        # 座標のFieldSetFileから内周ではないDWH（外周）を取得。
        polygon_outer_dwh_name = coordinates_field_set_file_data_frame.loc[
            coordinates_field_set_file_data_frame["Dwh"] != polygon_interior_dwh_name,"Dwh"][0]

        # 内周配列でも、内周index配列でもなければ、外周配列とする
        geometry_value_coordinates_array = WM.calc_func_time(self.logger)(
            NSP.get_value_from_field_set_file_dataframe)(coordinates_field_set_file_data_frame,
                                                         polygon_outer_dwh_name)

        # 内周配列の構成点を地物IDごとにまとめる
        interiors_dict =  WM.calc_func_time(self.logger)(
            NSP.convert_interiors_dict)(polygon_interior_coordinates_array, polygon_interior_index_array)

        # 外周配列の構成点を地物IDごとにまとめる
        exterior_dict =  WM.calc_func_time(self.logger)(
            NSP.split_geometry_points_dict)(geometry_value_coordinates_array[:, :4])

        # 地物IDごとにまとめられた構成点をShapelyに変換する
        geometry_list =  WM.calc_func_time(self.logger)(
            NSP.convert_dict_to_hole_polygon_geometries)(exterior_dict, interiors_dict)

        # 各listの1つ目の要素の地物IDを除く為に、配列に変換する
        coordinates_shapely_array = np.array(geometry_list)[:, 1]

        return coordinates_shapely_array

    def create_all_attribute_dataframe_from_field_set(self, only_field_field_set_file_dataframe):
        """
        フィールドのみのFieldSetFileのデータフレームをすべてデータフレームに変換する
        
        only_field_field_set_file_dataframe: フィールドのみのFieldSetFileのデータフレーム

        return: データフレームに変換されたもの or None
        """

        # 出力するフィールドがあるか。
        if only_field_field_set_file_dataframe is not None:
            attribute_series_list = []
            feature_id_index = None

            # フィールドのみのFieldSetFileからデータフレームに変換
            for _, row in only_field_field_set_file_dataframe.iterrows():
                dwh_name = row["Dwh"]

                # Value をデコード 例: [(0, 5), (1, 2)]
                value_list = pickle.loads(base64.b64decode(row["Value"]))

                # DataFrame 化
                temp_df = pd.DataFrame(
                    value_list,
                    columns=["feature_id", dwh_name]
                )

                # 初回の場合の地物IDを属性項目の追加対象にしておく
                # 全ファイルが同じ順序で地物IDを持つとは限らないという前提
                if feature_id_index is None:
                    feature_id_index = temp_df["feature_id"]

                attribute_series_list.append(
                    temp_df[dwh_name]
                )

            # 横に結合し、１つのデータフレームにする。
            attribute_dataframe = pd.concat(
                attribute_series_list,
                axis=1
            ).fillna('None')

        # なかった場合はNoneを返す。
        else:
            attribute_dataframe = None

        return attribute_dataframe

    def get_geometry_value(self,
                           value_list,
                           geometry_index_array,
                           interior_geometry_index_array,
                           interior_index_array):
        """
        FieldSetFileのValueから内周array、内周index、外周、型を出力
        
        value_list: FieldSetFileのValue
        geometry_index_array: 外周arrayのindex
        interior_geometry_index_array: 内周arrayのindex
        interior_index_array: 内周indexarrayのindex
        """

        # ジオメトリのValueのみ取得
        if (interior_geometry_index_array is None) and (interior_index_array is None):
            # 内周ないときは外周のジオメトリ
            exterior_geometry_value=value_list[geometry_index_array[0]]
            exterior_geometry_value_type\
                =NSP.get_field_set_file_geometry_type(exterior_geometry_value)
            
            # 内周のデータはNone
            interior_geometry_value=None
            interior_geometry_value_type=None
            interior_index_value=None

        else:
            # 内周がある場合は内周ではない方を外周とする
            exterior_idxs = np.where(
                geometry_index_array != interior_geometry_index_array[0]
            )[0]

            # 外周（1つ前提）取得
            exterior_geometry_value = value_list[exterior_idxs[0]]
            exterior_geometry_value_type = \
                NSP.get_field_set_file_geometry_type(exterior_geometry_value)

            # 内周取得
            interior_idxs = np.where(
                geometry_index_array == interior_geometry_index_array[0]
            )[0]

            interior_geometry_value = value_list[interior_idxs[0]]
            interior_geometry_value_type = \
                NSP.get_field_set_file_geometry_type(interior_geometry_value)

            # 内周インデックスの値取得
            interior_index_value = value_list[interior_index_array[0]]


        return exterior_geometry_value,\
            interior_geometry_value,\
            exterior_geometry_value_type,\
            interior_geometry_value_type,\
            interior_index_value

    def get_coordinates_shapely_array_from_geondarray(self, 
                                                      geondarray,
                                                      geondarray_geometry_type):
        """
        FieldSetFileのジオメトリがGeoNdarrayの時に、ジオメトリに変換する
        
        geondarray: FieldSetFile内のGeoNdarray
        geondarray_geometry_type: GeoNdarrayを変換する型

        return: ジオメトリ
        """
        # GeoNdarrayの場合
        # マルチパッチの場合はマルチポリゴンに変換する
        if geondarray.shape[1] == 8:
            coordinates_shapely_array\
                = self.get_coordinates_multipolygon_shapely_array(geondarray)
        else:

            geometry_file_type_dict = {DDC.POINT:DDC.POINT_GEOMETRY_TYPE, 
                                        DDC.LINESTRING:DDC.LINESTRING_GEOMETRY_TYPE,
                                        DDC.POLYGON:DDC.POLYGON_GEOMETRY_TYPE}

            coordinates_shapely_array\
                    = NSP.get_attribute_coordinates_by_geometry_items(geondarray,
                                                                    geometry_file_type_dict.get(geondarray_geometry_type))
        return coordinates_shapely_array

    def get_interior_polygon_from_value(self,
                                        polygon_exterior_geondarray,
                                        polygon_interior_geondarray,
                                        polygon_interior_index_array):
        """
        外周、内周、内周indexからポリゴンに変換する
        
        polygon_exterior_geondarray: 外周のGeoNdarrayを
        polygon_interior_geondarray: 内周array
        polygon_interior_index_array: 内周indexarray

        return: ポリゴン
        """

        # 外周座標配列の構成点を地物IDごとにまとめる
        exterior_dict\
            = NSP.split_geometry_points_dict(polygon_exterior_geondarray[:, :4])

        # 内周座標配列の構成点を地物IDごとにまとめる
        interiors_dict\
            = NSP.convert_interiors_dict(polygon_interior_geondarray,
                                        polygon_interior_index_array)
        
        # 地物IDごとにまとめられた構成点をShapelyに変換する
        geometry_list\
            = NSP.convert_dict_to_hole_polygon_geometries(exterior_dict,
                                                        interiors_dict)

        # 各listの1つ目の要素の地物IDを除く為に、配列に変換する
        coordinates_shapely_array = np.array(geometry_list)[:, 1]

        return coordinates_shapely_array

    def get_coordinates_shapely_array_from_field_set_file(self,
                                                          dwh_list,
                                                          type_list,
                                                          value_list,
                                                          geometry_index_array,
                                                          interior_geometry_index_array,
                                                          interior_index_array,
                                                          geondarray_geometry_type):
        """
        FieldSetFileからジオメトリを取得
        
        dwh_list: FieldSetFileのDwh列のlist
        type_list: FieldSetFileのType列のlist
        value_list: FieldSetFileのValue列のlist
        geometry_index_array: FieldSetFileのジオメトリ列のindex
        interior_geometry_index_array: FieldSetFileの内周arrayのindex
        interior_index_array: FieldSetFileの内周indexarrayのindex
        geondarray_geometry_type: GeoNdarrayを変換する型
        """
        # ジオメトリのValueのみ取得
        exterior_geometry_value,\
        interior_geometry_value,\
        exterior_geometry_value_type,\
        interior_geometry_value_type,\
        interior_index_value\
            =self.get_geometry_value(value_list,
                                    geometry_index_array,
                                    interior_geometry_index_array,
                                    interior_index_array)

        # ジオメトリ行なし
        if len(geometry_index_array)==0:
            coordinates_shapely_array=None

        # ジオメトリ一行
        elif len(geometry_index_array)==1:

            # ジオメトリがlistの場合
            if exterior_geometry_value_type == DDC.GEOMETRY:
                coordinates_shapely_array=exterior_geometry_value

            # ジオメトリがGeoNdArrayの場合
            else:
                # GeoNDArrayをジオメトリリストに変換
                coordinates_shapely_array\
                    =self.get_coordinates_shapely_array_from_geondarray(exterior_geometry_value,
                                                                        geondarray_geometry_type)

        # 内周ありなのでPolygonのジオメトリリスト確定
        elif len(geometry_index_array)==2:
            coordinates_shapely_array\
                =self.get_interior_polygon_from_value(exterior_geometry_value,
                                                    interior_geometry_value,
                                                    interior_index_value)

        else:
            raise ValueError("ジオメトリが3列以上あります。FieldSetFileを確認してください。")

        return coordinates_shapely_array

    def get_attribute_dataframe_field_set_file(self, 
                                               dwh_list,
                                               type_list,
                                               value_list,
                                               geometry_index_array,
                                               polygon_interior_dwh_name,
                                               polygon_interior_index_dwh_name,
                                               coordinates_shapely_array,
                                               field_set_file_data_frame):
        """
        FieldSetFileからフィールドを取得
        
        dwh_list: FieldSetFileのDwh列のlist
        type_list: FieldSetFileのType列のlist
        value_list: FieldSetFileのValue列のlist
        geometry_index_array: FieldSetFileのジオメトリ列のindex
        polygon_interior_dwh_name: 内周arrayのDWH名
        polygon_interior_index_dwh_name: 内周indexarrayのDWH名
        coordinates_shapely_array: ジオメトリ
        field_set_file_data_frame: FieldSetFileのデータフレーム

        return: データフレームに変換したフィールド
        """
        # ジオメトリに関連するDWH名取得
        related_geometry_dwh_list = [dwh_list[i] for i in geometry_index_array]

        # 内周がないのであればジオメトリ行のDWH名取得
        if polygon_interior_dwh_name is None:
        # 内周があれば内周インデックスのDWH名追加
            pass
        else:
            related_geometry_dwh_list.append(polygon_interior_index_dwh_name)

        # フィールドのFieldSetFileインデックス取得
        # ジオメトリに関連するDWH名以外を取得
        field_index_array\
            =NSP.get_dataframe_index_by_list_condition(field_set_file_data_frame["Dwh"],\
                                                        related_geometry_dwh_list,
                                                        except_flag=True)
        
        # フィールドのDataFrame作成
        # フィールド0件の場合 ジオメトリの数連番 カラム名は「id」
        if len(field_index_array)==0:
            all_attribute_dataframe\
                =pd.DataFrame({'id': np.arange(len(coordinates_shapely_array))})

        # フィールドがある場合はデータフレームに変換
        else:
            all_attribute_dataframe\
                =self.create_all_attribute_dataframe_from_field_set(field_set_file_data_frame.iloc[field_index_array, :])
        return all_attribute_dataframe

    def get_value_no_data_definition(self,
                                    field_set_file_data_frame,
                                    polygon_interior_dwh_name,
                                    polygon_interior_index_dwh_name,
                                    geondarray_geometry_type):
        """
        データ定義がないときのジオメトリとフィールドデータフレームの取得
        
        field_set_file_data_frame: FieldSetFileをデータフレームに変換したもの
        polygon_interior_dwh_name: 内周arrayのDWH名
        polygon_interior_index_dwh_name: 内周indexのDWH名
        geondarray_geometry_type: GeoNdarrayを変換する型
        """

        # ジオメトリのFieldSetFileインデックス取得
        geometry_index_array\
            =NSP.get_dataframe_index_by_list_condition(field_set_file_data_frame['Type'],
                                                        [DDC.GEOMETRY,DDC.GEONDARRAY,DDC.GEODATAFRAME])

        # 内周がない場合None
        if polygon_interior_dwh_name is None:
            interior_geometry_index_array=None

        # 内周ジオメトリFieldSetFileインデックス取得
        else:
            interior_geometry_index_array\
                =NSP.get_dataframe_index_by_list_condition(field_set_file_data_frame['Dwh'],
                                                            [polygon_interior_dwh_name])
            
            # 内周のDWHを指定しているが取得できない場合
            if len(interior_geometry_index_array)==0:
                raise ValueError("内周配列のDWHが見つかりません。")

        # 内周インデックスのFieldSetFileインデックス取得
        if polygon_interior_index_dwh_name is None:
            interior_index_array=None
        else:
            interior_index_array\
                =NSP.get_dataframe_index_by_list_condition(field_set_file_data_frame['Dwh'],
                                                            [polygon_interior_index_dwh_name])

            # 内周インデックスのDWHを指定しているが取得できない場合
            if len(interior_index_array)==0:
                raise ValueError("内周index配列のDWHが見つかりません。")

        # フィールドセットファイル全行取得
        dwh_list,\
        type_list,\
        value_list\
            =NSP.get_field_set_file_data(field_set_file_data_frame)

        #--------------------------------------------------------------
        # ジオメトリ取得処理
        #--------------------------------------------------------------
        if len(geometry_index_array) == 0:
            coordinates_shapely_array = None
        
        else:
            # ジオメトリリスト取得
            coordinates_shapely_array\
                =self.get_coordinates_shapely_array_from_field_set_file(dwh_list,
                                                                        type_list,
                                                                        value_list,
                                                                        geometry_index_array,
                                                                        interior_geometry_index_array,
                                                                        interior_index_array,
                                                                        geondarray_geometry_type)

        #--------------------------------------------------------------
        # フィールド取得処理
        #--------------------------------------------------------------
        all_attribute_dataframe\
            =self.get_attribute_dataframe_field_set_file(dwh_list,
                                                        type_list,
                                                        value_list,
                                                        geometry_index_array,
                                                        polygon_interior_dwh_name,
                                                        polygon_interior_index_dwh_name,
                                                        coordinates_shapely_array,
                                                        field_set_file_data_frame)

        return coordinates_shapely_array, all_attribute_dataframe

    def convert_field_set_file_from_fields(self, coordinates_shapely_array, all_attribute_dataframe, crs, output_geodataframe_dwh_name):
        """
        各要素を出力用FieldSetFileに変換
        
        coordinates_shapely_array: ジオメトリリスト
        all_attribute_dataframe: フィールドが格納されたDataFrame
        crs: EPSGコード
        output_geodataframe_dwh_name: 出力用GeoDataFrameのDWH名
        """
        # 座標がある場合GeoDataFrameに変換する
        if coordinates_shapely_array is not None:
            output_dataframe = gpd.GeoDataFrame(all_attribute_dataframe,
                                                geometry=coordinates_shapely_array,
                                                crs=crs)
            
            # Type列の値を取得
            output_type = WM.calc_func_time(self.logger)(NSP.get_field_set_file_geometry_type)(output_dataframe)

        # 座標がない場合、フィールドのDataFrameのまま。
        else:
            output_dataframe = all_attribute_dataframe

            # Type列の値を指定
            output_type = DDC.DATAFRAME

        # FieldSetFileに格納し、出力
        output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)([output_geodataframe_dwh_name],
                                                                                        [output_type],
                                                                                        [output_dataframe])

        return output_field_set_file

    def transform(self, context, flowfile):
        try:
            # -----------------------------------------------------------------------------------------------------------
            # 【取得】フローファイルからFieldSetFileとプロパティの設定値と製品データ定義ファイルを取得
            # -----------------------------------------------------------------------------------------------------------
            crs, \
                polygon_interior_dwh_name, \
                polygon_interior_index_dwh_name, \
                geondarray_geometry_type, \
                output_geodataframe_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            field_set_file_data_frame = WM.calc_func_time(
                self.logger)(self.get_flowfile)(flowfile)

            # FieldSetFileをフィールドのデータフレームとジオメトリリストに変換する
            coordinates_shapely_array, all_attribute_dataframe \
                = WM.calc_func_time(self.logger)(
                    self.get_value_no_data_definition)(field_set_file_data_frame,
                                                        polygon_interior_dwh_name,
                                                        polygon_interior_index_dwh_name,
                                                        geondarray_geometry_type)

            # 出力用FieldSetFileに変換する
            output_field_set_file = WM.calc_func_time(self.logger)(
                self.convert_field_set_file_from_fields)(coordinates_shapely_array,
                                                         all_attribute_dataframe,
                                                         crs,
                                                         output_geodataframe_dwh_name)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
