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

# ------------------------------------------------------------------------------
# shapefileから画像用numpy配列生成
# ------------------------------------------------------------------------------
# shapefileをgrayscaleの画像配列に変換する。
# 1pixelの大きさは空間IDを指定して決める
# ------------------------------------------------------------------------------
# 外部ライブラリ
import traceback

from importlib import import_module

# 使用パッケージimport
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

np = import_module("numpy")


class ConvertGeoDataframeToImageArray(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """shpファイルから画像用numpy配列生成"""
        tags = ["python", "digiline", "opencv"]

    # 座標のDwh名を指定する。
    INPUT_COORDINATES_DWH_NAME = PropertyDescriptor(
        name="COORDINATES_DWH_NAME",
        description="画像化対象座標Dwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # flowfileのCRS
    INPUT_FLOWFILE_CRS = PropertyDescriptor(
        name="INPUT_FLOWFILE_CRS",
        description="画像化対象座標CRS",
        default_value=6668,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # pixelの1辺の単位を定める空間IDvoxelのzoomlevel
    VOXEL_ZOOM_LEVEL = PropertyDescriptor(
        name="VOXEL_ZOOM_LEVEL",
        description="pixelの1辺の単位を定める空間IDのzoomlevel",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # pixelの1辺の単位を定める空間IDvoxelのCRS
    VOXEL_CRS = PropertyDescriptor(
        name="VOXEL_CRS",
        description="pixelの1辺の単位を定める空間IDのCRS",
        default_value=6668,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 角度の閾値 これより小さい角度を形成すると白色とする
    ANGLE_THRESHOLD = PropertyDescriptor(
        name="ANGLE_THRESHOLD",
        description="角度の閾値",
        default_value=150,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力する画像配列
    OUTPUT_IMAGE_ARRAY_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_IMAGE_ARRAY_DWH_NAME",
        description="出力する画像配列のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力する画像の原点
    OUTPUT_ORIGIN_LIST_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_ORIGIN_LIST_DWH_NAME",
        description="出力する画像の原点のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [INPUT_COORDINATES_DWH_NAME,
                            INPUT_FLOWFILE_CRS,
                            VOXEL_ZOOM_LEVEL,
                            VOXEL_CRS,
                            ANGLE_THRESHOLD,
                            OUTPUT_IMAGE_ARRAY_DWH_NAME,
                            OUTPUT_ORIGIN_LIST_DWH_NAME]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def convert_xid2longitude(self, x_index, max_tile
                              ):
        return x_index / max_tile * 360.0 - 180.0

    def convert_yid2latitude(self, y_index, max_tile
                             ):
        latitude_rad = np.arctan(np.sinh(np.pi * (1 - 2 * y_index / max_tile)))
        return np.degrees(latitude_rad)

    # 【空間ID】
    # XID,YIDからx,y座標のmin,maxを取得する
    # 引数1:範囲抽出対象XID
    # 引数2:範囲抽出対象YID
    # 戻り値1:最小値経度
    # 戻り値2:最大値経度
    # 戻り値3:最小値緯度
    # 戻り値4:最大値緯度
    def get_min_max_longitude_latitude_from_id(self, x_id, y_id, tiles_counts
                                               ):

        # 左上の座標
        left_up_lon = self.convert_xid2longitude(x_id, tiles_counts)
        left_up_lat = self.convert_yid2latitude(y_id, tiles_counts)

        # 右下の座標
        right_bottom_lon = self.convert_xid2longitude(x_id + 1, tiles_counts)
        right_bottom_lat = self.convert_yid2latitude(y_id + 1, tiles_counts)

        return left_up_lon, right_bottom_lon, right_bottom_lat, left_up_lat

    # 空間ID内の地物を抽出し閾値未満の角度の場合白色255で更新　それ以外の場合は50で更新する
    # 該当のpixelの上下左右も白色で更新
    def _update_image_array_by_gradient_in_mesh(self, pixel_x_index_array, pixel_y_index_array, image_array
                                                ):

        # 該当のvoxel更新
        image_array[pixel_y_index_array, pixel_x_index_array] = 255
        image_array[pixel_y_index_array+1, pixel_x_index_array] = 255
        image_array[pixel_y_index_array-1, pixel_x_index_array] = 255
        image_array[pixel_y_index_array, pixel_x_index_array+1] = 255
        image_array[pixel_y_index_array, pixel_x_index_array-1] = 255

        return image_array

    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            target_input_dwh_name\
                = context.getProperty(self.INPUT_COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            target_input_flowfile_crs\
                = int(context.getProperty(self.INPUT_FLOWFILE_CRS).evaluateAttributeExpressions(flowfile).getValue())
            target_voxel_zoom_level\
                = int(context.getProperty(self.VOXEL_ZOOM_LEVEL).evaluateAttributeExpressions(flowfile).getValue())
            target_voxel_crs\
                = int(context.getProperty(self.VOXEL_CRS).evaluateAttributeExpressions(flowfile).getValue())
            target_angle_threshold\
                = float(context.getProperty(self.ANGLE_THRESHOLD).evaluateAttributeExpressions(flowfile).getValue())
            output_image_array_dwh_name\
                = context.getProperty(self.OUTPUT_IMAGE_ARRAY_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            output_origin_list_dwh_name\
                = context.getProperty(self.OUTPUT_ORIGIN_LIST_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

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
            target_geodataframe\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          target_input_dwh_name)

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameから座標取り出し
            # --------------------------------------------------------------------------
            target_coordinates_array, \
                _, \
                _\
                = WM.calc_func_time(self.logger)(NSP.get_coordinates_array_from_geodataframe)(target_geodataframe)

            # --------------------------------------------------------------------------
            # voxelid算出用座標を取得
            # --------------------------------------------------------------------------
            target_lon_lat_array\
                = WM.calc_func_time(self.logger)(DCP._convert_geometry_crs)(target_coordinates_array[:, :3],
                                                                            target_input_flowfile_crs,
                                                                            target_voxel_crs)

            # --------------------------------------------------------------------------
            # voxelの情報取得
            # --------------------------------------------------------------------------
            min_x_id, \
                max_x_id, \
                min_y_id, \
                max_y_id, \
                x_pixel_counts, \
                y_pixel_counts, \
                pixel_x_index_array, \
                pixel_y_index_array, \
                xy_id_array, \
                unique_xy_id_array, \
                unique_xy_index, \
                tile_counts\
                = WM.calc_func_time(self.logger)(DCP._get_voxel_id_array)(target_lon_lat_array[:, 1:],
                                                                          target_voxel_zoom_level)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 左上のVoxelIDから4隅の座標取得
            # --------------------------------------------------------------------------
            left_up_min_lon, \
                left_up_max_lon, \
                left_up_min_lat, \
                left_up_max_lat\
                = WM.calc_func_time(self.logger)(self.get_min_max_longitude_latitude_from_id)(int(min_x_id),
                                                                                              int(
                                                                                                  min_y_id),
                                                                                              tile_counts)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 右下のVoxelID4隅の座標取得
            # --------------------------------------------------------------------------
            right_down_min_lon, \
                right_down_max_lon, \
                right_down_min_lat, \
                right_down_max_lat\
                = WM.calc_func_time(self.logger)(self.get_min_max_longitude_latitude_from_id)(int(max_x_id),
                                                                                              int(
                                                                                                  max_y_id),
                                                                                              tile_counts)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 構成点を増やす用の1pixelの大きさ（単位：角度）
            # --------------------------------------------------------------------------
            x_unit = (right_down_max_lon-left_up_min_lon)/x_pixel_counts
            y_unit = (left_up_max_lat-right_down_min_lat)/y_pixel_counts
            interval = np.sqrt(x_unit**2+y_unit**2)/2

            # 画像の初期配列作成
            image_array = np.zeros(
                (y_pixel_counts+1, x_pixel_counts+1), dtype=np.float64)
            # --------------------------------------------------------------------------

            temp_lon_lat = target_lon_lat_array.copy()
            temp_coordinates = target_coordinates_array.copy()

            before_index_array, \
                center_index_array, \
                after_index_array\
                = WM.calc_func_time(self.logger)(NCP.get_serial_3points_index_array)(temp_coordinates)

            # 平面直角の座標
            before_coordinates_array = temp_coordinates[before_index_array, 1:3]
            center_coordinates_array = temp_coordinates[center_index_array, 1:3]
            after_coordinates_array = temp_coordinates[after_index_array, 1:3]

            # 最終的に色を付けるメソッドに渡す緯度経度の座標
            before_lon_lat_array = temp_lon_lat[before_index_array, 1:3]
            center_lon_lat_array = temp_lon_lat[center_index_array, 1:3]
            after_lon_latarray = temp_lon_lat[after_index_array, 1:3]
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 3点のなす角度 複数算出版
            # --------------------------------------------------------------------------
            degree_array\
                = WM.calc_func_time(self.logger)(NCP.get_degree_3points_array)(before_coordinates_array,
                                                                               center_coordinates_array,
                                                                               after_coordinates_array)
            # --------------------------------------------------------------------------
            # --------------------------------------------------------------------------
            # 閾値未満の角度を形成する構成点のみ抽出
            # --------------------------------------------------------------------------
            degree_bool = degree_array < target_angle_threshold

            # ピクセル位置求める用
            center_lon_lat_array = center_lon_lat_array[degree_bool]

            # RGB角度計算用
            degree_array = degree_array[degree_bool]
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 3点のなす角度降順に並び変えて順番に設定
            # --------------------------------------------------------------------------
            degree_sort_index = np.argsort(degree_array)[::-1]

            # ピクセル位置求める用
            center_lon_lat_array = center_lon_lat_array[degree_sort_index]
            # --------------------------------------------------------------------------

            pixel_x_index_array = np.floor(
                (center_lon_lat_array[:, 0]-left_up_min_lon)/x_unit).astype(np.int64)
            pixel_y_index_array = np.floor(
                (center_lon_lat_array[:, 1]-left_up_max_lat)/-y_unit).astype(np.int64)

            # -----------------------------------------------------------------------------------------------------------
            # 構成点間のpixelに色を付ける
            # -----------------------------------------------------------------------------------------------------------
            image_array\
                = WM.calc_func_time(self.logger)(DCP._update_image_array_by_densified_geometries)(target_lon_lat_array,
                                                                                                  x_unit,
                                                                                                  y_unit,
                                                                                                  interval,
                                                                                                  left_up_min_lon,
                                                                                                  left_up_max_lat,
                                                                                                  image_array)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 複数の地物の端点部分に白色を付ける
            # -----------------------------------------------------------------------------------------------------------
            image_array\
                = WM.calc_func_time(self.logger)(DCP._update_image_array_by_duplicated_points)(image_array, target_lon_lat_array, x_unit, y_unit, left_up_min_lon, left_up_max_lat
                                                                                               )

            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 角度のついた部分に白色を付ける
            # -----------------------------------------------------------------------------------------------------------
            image_array\
                = WM.calc_func_time(self.logger)(self._update_image_array_by_gradient_in_mesh)(pixel_x_index_array, pixel_y_index_array, image_array
                                                                                               )
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 画像の原点情報
            # -----------------------------------------------------------------------------------------------------------
            origin_list = [x_unit,
                           0.0,
                           0.0,
                           -y_unit,
                           left_up_min_lon,
                           left_up_max_lat]

            # -----------------------------------------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 画像のnumpy配列と原点の情報、pixelの情報をFieldSetFileListに追加
            # --------------------------------------------------------------------------
            target_dwh_list = []
            target_type_list = []
            target_value_list = []

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        target_input_dwh_name,
                                                                                        'float64',
                                                                                        target_coordinates_array)

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        output_image_array_dwh_name,
                                                                                        'uint8',
                                                                                        image_array)

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        output_origin_list_dwh_name,
                                                                                        'float64',
                                                                                        origin_list)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileListをFieldSetFileに変換
            # --------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(target_dwh_list,
                                                                                 target_type_list,
                                                                                 target_value_list)
            # --------------------------------------------------------------------------

            # 結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
