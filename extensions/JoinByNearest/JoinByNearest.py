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
# pointとlineで属性の最近傍結合を行う
# ※※※※※※※※※※※※※※※※※※※※※※※【注意】※※※※※※※※※※※※※※※※※※※※※※※※※※
# DWHの内容は以下のみとすること
# １，検索する側のポイント座標（１行）
# ２，検索される側のライン座標（１行）
# ※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※※
# ---------------------------------------------------------------------------------------------------------
# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

np = import_module("numpy")


class JoinByNearest(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        属性の最近傍結合を行う。
                        ①input: 検索する側のポイント座標配列と検索される側のライン座標配列の2行のFieldSetFile。
                        ②output: 最近傍結合を行った、前後のインデックスデータ1行のFieldSetFile。
                      """
        tags = ["Point", "Linestring", "Python"]

    POINT_DWH_NAME = PropertyDescriptor(
        name="Point DWH Name",
        description="ポイントの座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    LINESTRING_DWH_NAME = PropertyDescriptor(
        name="LineString DWH Name",
        description="ラインの座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    INPUT_CRS = PropertyDescriptor(
        name="Input CRS",
        description="ポイントとラインのCRS(epsgコード)",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    VOXEL_CRS = PropertyDescriptor(
        name="Voxel CRS",
        description="空間IDのCRS(epsgコード)",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    ZOOM_LEVEL = PropertyDescriptor(
        name="Zoom Level",
        description="分割用空間IDのズームレベル",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    BUFFER_DISTANCE = PropertyDescriptor(
        name="Buffer Distance",
        description="""
図郭を覆う範囲を作成する際に、図郭の1辺を延長する長さ(メートル)。
※ 図郭外に最近傍線がある場合に備えて、図郭より広い範囲を指定しておく必要がある
                    """,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    RESULTS_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [POINT_DWH_NAME,
                            LINESTRING_DWH_NAME,
                            INPUT_CRS,
                            VOXEL_CRS,
                            ZOOM_LEVEL,
                            BUFFER_DISTANCE,
                            RESULTS_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # --------------------------------------------------------------------------
            point_dwh_name\
                = context.getProperty(self.POINT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            linestring_dwh_name\
                = context.getProperty(self.LINESTRING_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            input_crs\
                = int(context.getProperty(self.INPUT_CRS).evaluateAttributeExpressions(flowfile).getValue())
            voxel_crs\
                = int(context.getProperty(self.VOXEL_CRS).evaluateAttributeExpressions(flowfile).getValue())
            zoom_level\
                = int(context.getProperty(self.ZOOM_LEVEL).evaluateAttributeExpressions(flowfile).getValue())
            buffer_distance\
                = float(context.getProperty(self.BUFFER_DISTANCE).evaluateAttributeExpressions(flowfile).getValue())
            results_dwh_name\
                = context.getProperty(self.RESULTS_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # flowfile→FieldSetFileのDataFrame
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(DCP.convert_flowfile_to_field_set_file_dataframe)(flowfile)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameからValue列、Dwh列のListを取得
            # --------------------------------------------------------------------------
            target_value_list, target_type_list, target_dwh_list\
                = WM.calc_func_time(self.logger)(DCP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 検索する側のポイントの座標取得（MH側）
            # --------------------------------------------------------------------------
            point_array\
                = WM.calc_func_time(self.logger)(DCP.get_value_field_from_value_dwh_list)(target_value_list, target_dwh_list, point_dwh_name
                                                                                          )

            point_id_array = point_array[:, 0].astype(np.int64)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 検索される側のラインの座標取得（ポリゴンの座標でも可）
            # --------------------------------------------------------------------------
            line_array\
                = WM.calc_func_time(self.logger)(DCP.get_value_field_from_value_dwh_list)(target_value_list, target_dwh_list, linestring_dwh_name
                                                                                          )

            line_id_array = line_array[:, 0].astype(np.int64)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 始点、終点のインデックス取得
            # --------------------------------------------------------------------------
            start_index_array, end_index_array\
                = WM.calc_func_time(self.logger)(DCP.get_start_index_and_end_index)(line_array)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # ポイントを座標系の変換
            # --------------------------------------------------------------------------
            point_lon_array, point_lat_array\
                = WM.calc_func_time(self.logger)(DCP.convert_xy_coordinates_array)(point_array[:, 1], point_array[:, 2], input_crs, voxel_crs
                                                                                   )
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 一意の空間IDへ変換 経緯度の2次元配列 point_lon_lat_array
            # --------------------------------------------------------------------------
            _, _, _, _, _, _, _, _, xy_id_array, unique_xy_id_array, unique_xy_index, tile_counts\
                = WM.calc_func_time(self.logger)(DCP._get_voxel_id_array)(np.stack([point_lon_array, point_lat_array], axis=1), zoom_level
                                                                          )

            # --------------------------------------------------------------------------
            # 空間IDの4隅取得
            # --------------------------------------------------------------------------
            unique_xy_string_array, xy_id_default_dict\
                = WM.calc_func_time(self.logger)(DCP._get_xy_string_id_dict)(xy_id_array, unique_xy_index
                                                                             )
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 空間IDの4隅の座標取得
            # --------------------------------------------------------------------------
            left_up_lon, right_bottom_lon, right_bottom_lat, left_up_lat\
                = WM.calc_func_time(self.logger)(DCP.get_min_max_longitude_latitude_from_id)(unique_xy_id_array[:, 0], unique_xy_id_array[:, 1], tile_counts
                                                                                             )
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 4隅を平面直角座標系へ変換
            # --------------------------------------------------------------------------
            left_up_x, left_up_y\
                = WM.calc_func_time(self.logger)(DCP.convert_xy_coordinates_array)(left_up_lon, left_up_lat, voxel_crs, input_crs
                                                                                   )

            right_bottom_x, right_bottom_y\
                = WM.calc_func_time(self.logger)(DCP.convert_xy_coordinates_array)(right_bottom_lon, right_bottom_lat, voxel_crs, input_crs
                                                                                   )
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # m単位でバッファを持たせる（ラインのID分け用）
            # --------------------------------------------------------------------------
            left_up_x_line = left_up_x-buffer_distance
            left_up_y_line = left_up_y+buffer_distance
            right_bottom_x_line = right_bottom_x+buffer_distance
            right_bottom_y_line = right_bottom_y-buffer_distance
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 各IDごとに空間ID付近に存在する地物IDを取得する
            # --------------------------------------------------------------------------
            linestring_id_dict\
                = WM.calc_func_time(self.logger)(DCP._get_index_array_dict_around_unit)(line_array, start_index_array, end_index_array, unique_xy_string_array, left_up_x_line, right_bottom_x_line, right_bottom_y_line, left_up_y_line
                                                                                        )

            # --------------------------------------------------------------------------
            # 空間IDごとに最近傍結合を行い、検索側のインデックス配列と検索される側のインデックス配列を返却する
            # --------------------------------------------------------------------------
            result_point_index_array, result_linestring_index_array\
                = WM.calc_func_time(self.logger)(DCP._get_index_array_by_unit)(point_array[:, :3], line_array[:, :3], unique_xy_string_array, start_index_array, end_index_array, linestring_id_dict, point_id_array, line_id_array, xy_id_default_dict
                                                                               )
            # --------------------------------------------------------------------------

            # zipでタプルを作成し、リストに変換
            results_data_list = list(
                zip(result_point_index_array, result_linestring_index_array))

            # outputで使用
            dwh_list = []
            type_list = []
            output_value_list = []

            dwh_list.append(results_dwh_name)
            type_list.append("results")
            output_value_list.append(results_data_list)

            # set_field_set_file : pickle によるシリアライズ、base64 でのデコード、utf-8 でのエンコード処理
            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                                           type_list,
                                                                                           output_value_list
                                                                                           )

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
