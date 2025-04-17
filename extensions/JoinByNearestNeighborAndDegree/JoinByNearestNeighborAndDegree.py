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

import traceback

from importlib import import_module

# 使用パッケージimport
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
np = import_module("numpy")
gpd = import_module("geopandas")
Point = import_module("shapely").geometry.Point


class JoinByNearestNeighborAndDegree(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:

        version = "1.0.0"
        description = """JoinByNearestNeighborAndDegree"""
        tags = ["python", "digiline", "opencv"]

    SUBJECTIVE_DWH_NAME = PropertyDescriptor(
        name="SUBJECTIVE_DWH_NAME",
        description="スナップ先となる線の地物(位置基準側)",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    OBJECTIVE_DWH_NAME = PropertyDescriptor(
        name="OBJECTIVE_DWH_NAME",
        description="スナップ先となる線の地物(補正対象側)",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    SUBJECTIVE_GCP_DWH_NAME = PropertyDescriptor(
        name="SUBJECTIVE_GCP_DWH_NAME",
        description="位置基準側の地物から作成した特徴点",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    OBJECTIVE_GCP_DWH_NAME = PropertyDescriptor(
        name="OBJECTIVE_GCP_DWH_NAME",
        description="補正対象側の地物から作成した特徴点",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    TARGET_CRS = PropertyDescriptor(
        name="TARGET_CRS",
        description="地物のCRS",
        default_value=6677,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    VOXEL_CRS = PropertyDescriptor(
        name="VOXEL_CRS",
        description="グループ化用VoxelCRS",
        default_value=6668,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # グループ化用VoxelCRS
    VOXEL_ZOOM_LEVEL = PropertyDescriptor(
        name="VOXEL_ZOOM_LEVEL",
        description="グループ化用Voxel生成用ZoomLevel",
        default_value=15,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    NEAREST_NEIGHBOR_THRESHOLD = PropertyDescriptor(
        name="NEAREST_NEIGHBOR_THRESHOLD",
        description="ペアリングするときの特徴点間距離の閾値",
        default_value=3,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    ANGLE_THRESHOLD = PropertyDescriptor(
        name="ANGLE_THRESHOLD",
        description="スナップ対象となる、構成点がなす角度の閾値",
        default_value=150,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    DEGREES_DIFFERENCE_THRESHOLD = PropertyDescriptor(
        name="DEGREES_DIFFERENCE_THRESHOLD",
        description="位置基準側と補正対象側の構成点間角度の閾値",
        default_value=15,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    QUERY_GCP_DWH_NAME = PropertyDescriptor(
        name="QUERY_GCP_DWH_NAME",
        description="位置基準側GCPのDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    TRAIN_GCP_DWH_NAME = PropertyDescriptor(
        name="TRAIN_GCP_DWH_NAME",
        description="補正対象側GCPのDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # ★プロパティを設定した後にlistへ格納
    property_descriptors = [SUBJECTIVE_DWH_NAME,
                            SUBJECTIVE_GCP_DWH_NAME,
                            OBJECTIVE_DWH_NAME,
                            OBJECTIVE_GCP_DWH_NAME,
                            TARGET_CRS,
                            VOXEL_CRS,
                            VOXEL_ZOOM_LEVEL,
                            NEAREST_NEIGHBOR_THRESHOLD,
                            ANGLE_THRESHOLD,
                            DEGREES_DIFFERENCE_THRESHOLD,
                            QUERY_GCP_DWH_NAME,
                            TRAIN_GCP_DWH_NAME]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    # 座標配列からGeoDataFrameを生成 属性は連番のみ

    def get_geodateframe_from_point_coordinates(self, coordinates_array):

        shapely_list = []
        [shapely_list.append(Point(coordinates_array[i, 1:]))
         for i in range(len(coordinates_array))]
        attribute_id_array = np.arange(len(coordinates_array))

        geopandas_dict = {'id': attribute_id_array,
                          'geometry': shapely_list}

        result_geodataframe = gpd.GeoDataFrame(geopandas_dict)

        return result_geodataframe

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # --------------------------------------------------------------------------
            subjective_dwh_name\
                = context.getProperty(self.SUBJECTIVE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            subjective_gcp_dwh_name\
                = context.getProperty(self.SUBJECTIVE_GCP_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            objective_dwh_name\
                = context.getProperty(self.OBJECTIVE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            objective_gcp_dwh_name\
                = context.getProperty(self.OBJECTIVE_GCP_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            target_crs\
                = int(context.getProperty(self.TARGET_CRS).evaluateAttributeExpressions(flowfile).getValue())
            voxel_crs\
                = int(context.getProperty(self.VOXEL_CRS).evaluateAttributeExpressions(flowfile).getValue())
            voxel_zoom_level\
                = int(context.getProperty(self.VOXEL_ZOOM_LEVEL).evaluateAttributeExpressions(flowfile).getValue())
            nearest_neighbor_threshold\
                = float(context.getProperty(self.NEAREST_NEIGHBOR_THRESHOLD).evaluateAttributeExpressions(flowfile).getValue())
            angle_threshold\
                = float(context.getProperty(self.ANGLE_THRESHOLD).evaluateAttributeExpressions(flowfile).getValue())
            degrees_difference_threshold\
                = float(context.getProperty(self.DEGREES_DIFFERENCE_THRESHOLD).evaluateAttributeExpressions(flowfile).getValue())
            query_gcp_dwh_name\
                = context.getProperty(self.QUERY_GCP_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            train_gcp_dwh_name\
                = context.getProperty(self.TRAIN_GCP_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # --------------------------------------------------------------------------
            # flowfile→FieldSetFileのDataFrame
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(NSP.convert_flowfile_to_field_set_file_dataframe)(flowfile)

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameからValue列、Dwh列のListを取得
            # --------------------------------------------------------------------------
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameから座標取り出し
            # --------------------------------------------------------------------------
            subjective_linestring_array\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list, target_dwh_list, subjective_dwh_name
                                                                                          )[:, :3]

            subjective_coordinates_array\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list, target_dwh_list, subjective_gcp_dwh_name
                                                                                          )[:, :3]

            objective_linestring_array\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list, target_dwh_list, objective_dwh_name
                                                                                          )[:, :3]

            objective_coordinates_array\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list, target_dwh_list, objective_gcp_dwh_name
                                                                                          )[:, :3]

            # ------------------------------------------------------------------------
            # Voxelの範囲を特定するために座標系変換--
            # xyを空間IDへ
            # [XID]@[YID]をKey,xy座標をValueとした辞書型配列作成
            # まだ１対１で紐づいていないから両方のグリッドIDを作り出す必要がある
            # --------------------------------------------------------------------------
            subjective_xy_array, \
                unique_subjective_xy_array, \
                subjective_default_dict\
                = WM.calc_func_time(self.logger)(DCP._get_xyid_object_array)(subjective_coordinates_array,
                                                                             target_crs,
                                                                             voxel_crs,
                                                                             voxel_zoom_level)

            objective_xy_array, \
                unique_objective_xy_array, \
                objective_default_dict\
                = WM.calc_func_time(self.logger)(DCP._get_xyid_object_array)(objective_coordinates_array,
                                                                             target_crs,
                                                                             voxel_crs,
                                                                             voxel_zoom_level)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # スナップ先の座標を連続3点間の配列に変換する
            # --------------------------------------------------------------------------
            # 主たる地物側前点、中点、次点
            subjective_linestring_before_index_array, \
                subjective_linestring_center_index_array, \
                subjective_linestring_after_index_array\
                = WM.calc_func_time(self.logger)(NCP.get_serial_3points_index_array)(subjective_linestring_array)

            # 従たる地物側前点、中点、次点
            objective_linestring_before_index_array, \
                objective_linestring_center_index_array, \
                objective_linestring_after_index_array\
                = WM.calc_func_time(self.logger)(NCP.get_serial_3points_index_array)(objective_linestring_array)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 中点抽出 xy座標のみ
            # --------------------------------------------------------------------------
            # 主たる地物側前点、中点、次点
            subjective_linestring_before_array\
                = subjective_linestring_array[subjective_linestring_before_index_array]

            subjective_linestring_center_array\
                = subjective_linestring_array[subjective_linestring_center_index_array]

            subjective_linestring_after_array\
                = subjective_linestring_array[subjective_linestring_after_index_array]

            # 従たる地物側前点、中点、次点
            objective_linestring_before_array\
                = objective_linestring_array[objective_linestring_before_index_array]

            objective_linestring_center_array\
                = objective_linestring_array[objective_linestring_center_index_array]

            objective_linestring_after_array\
                = objective_linestring_array[objective_linestring_after_index_array]

            # --------------------------------------------------------------------------
            # 3点のなす角度 複数算出版
            # --------------------------------------------------------------------------
            subjective_degree_array\
                = WM.calc_func_time(self.logger)(NCP.get_degree_3points_array)(subjective_linestring_before_array[:, 1:],
                                                                               subjective_linestring_center_array[:, 1:],
                                                                               subjective_linestring_after_array[:, 1:])

            objective_degree_array\
                = WM.calc_func_time(self.logger)(NCP.get_degree_3points_array)(objective_linestring_before_array[:, 1:],
                                                                               objective_linestring_center_array[:, 1:],
                                                                               objective_linestring_after_array[:, 1:])
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 閾値未満の角度を形成する構成点のみ抽出
            # --------------------------------------------------------------------------
            subjective_degree_bool = subjective_degree_array < angle_threshold + \
                degrees_difference_threshold
            subjective_linestring_before_array = subjective_linestring_before_array[
                subjective_degree_bool]
            subjective_linestring_center_array = subjective_linestring_center_array[
                subjective_degree_bool]
            subjective_linestring_after_array = subjective_linestring_after_array[
                subjective_degree_bool]

            objective_degree_bool = objective_degree_array < angle_threshold
            objective_linestring_before_array = objective_linestring_before_array[
                objective_degree_bool]
            objective_linestring_center_array = objective_linestring_center_array[
                objective_degree_bool]
            objective_linestring_after_array = objective_linestring_after_array[
                objective_degree_bool]
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 中点からみた前点、次点の角度取得（x,yの差の正負で判定）
            # --------------------------------------------------------------------------
            # 主たる地物の角度配列
            subjective_degrees_array1\
                = WM.calc_func_time(self.logger)(NCP.get_2points_degree_array_x_axis)(subjective_linestring_center_array[:, 1:],
                                                                                      subjective_linestring_before_array[:, 1:])

            subjective_degrees_array2\
                = WM.calc_func_time(self.logger)(NCP.get_2points_degree_array_x_axis)(subjective_linestring_center_array[:, 1:],
                                                                                      subjective_linestring_after_array[:, 1:])

            # 従たる地物の角度配列
            objective_degrees_array1\
                = WM.calc_func_time(self.logger)(NCP.get_2points_degree_array_x_axis)(objective_linestring_center_array[:, 1:],
                                                                                      objective_linestring_before_array[:, 1:])

            objective_degrees_array2\
                = WM.calc_func_time(self.logger)(NCP.get_2points_degree_array_x_axis)(objective_linestring_center_array[:, 1:],
                                                                                      objective_linestring_after_array[:, 1:])

            # --------------------------------------------------------------------------
            # 中点をvoxelのidごとに辞書型配列へ分ける
            # 前点、中点、次点、象限配列すべて件数が同じ
            # 中点のインデックスを取得すればそれに付随する情報も取得可能
            # --------------------------------------------------------------------------
            _, \
                _, \
                subjective_center_default_dict\
                = WM.calc_func_time(self.logger)(DCP._get_xyid_object_array)(subjective_linestring_center_array,
                                                                             target_crs,
                                                                             voxel_crs,
                                                                             voxel_zoom_level)

            _, \
                _, \
                objective_center_default_dict\
                = WM.calc_func_time(self.logger)(DCP._get_xyid_object_array)(objective_linestring_center_array,
                                                                             target_crs,
                                                                             voxel_crs,
                                                                             voxel_zoom_level)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # VoxelIDごとに近傍処理を行う
            # --------------------------------------------------------------------------
            result_subjective_array, result_objective_array\
                = WM.calc_func_time(self.logger)(NCP._get_nearest_degrees_neighbors_array_by_voxel)(unique_subjective_xy_array,
                                                                                                    subjective_default_dict,
                                                                                                    objective_default_dict,
                                                                                                    subjective_coordinates_array,
                                                                                                    objective_coordinates_array,
                                                                                                    subjective_center_default_dict,
                                                                                                    objective_center_default_dict,
                                                                                                    subjective_linestring_center_array,
                                                                                                    subjective_degrees_array1,
                                                                                                    subjective_degrees_array2,
                                                                                                    objective_linestring_center_array,
                                                                                                    objective_degrees_array1,
                                                                                                    objective_degrees_array2,
                                                                                                    nearest_neighbor_threshold,
                                                                                                    degrees_difference_threshold)
            result_subjective_array = DCP.reset_coordinate_index(
                result_subjective_array)
            result_objective_array = DCP.reset_coordinate_index(
                result_objective_array)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # スナップ後の座標情報をFieldSetFileListに追加
            # --------------------------------------------------------------------------
            result_query_geodataframe = self.get_geodateframe_from_point_coordinates(
                result_subjective_array)
            result_train_geodataframe = self.get_geodateframe_from_point_coordinates(
                result_objective_array)

            target_dwh_list = []
            target_type_list = []
            target_value_list = []

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        query_gcp_dwh_name,
                                                                                        'GeoDataFrame',
                                                                                        result_query_geodataframe)

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        train_gcp_dwh_name,
                                                                                        'GeoDataFrame',
                                                                                        result_train_geodataframe)
            # --------------------------------------------------------------------------
            # FieldSetFile構成要素の各リストをFieldSetFileに変換
            # --------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(target_dwh_list, target_type_list, target_value_list
                                                                                 )

            # 結果を返す
            return FlowFileTransformResult(relationship="success", contents=output_field_set_file
                                           )

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
