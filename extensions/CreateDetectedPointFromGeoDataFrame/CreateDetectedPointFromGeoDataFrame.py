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

# 画像配列から特徴点を生成する
import traceback

from importlib import import_module

# 使用パッケージimport
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
np = import_module("numpy")
cv2 = import_module("cv2")


class CreateDetectedPointFromGeoDataFrame(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """GeoDataFrameから、メッシュ単位の画像を生成し、画像処理ベースで特徴点の検出・座標化を行う。
                        ①input: GeoDataFrameか、GeoDataFrameを持った1行のFieldSetFile。
                        ②output: 2つの座標配列（GeoDataFrameから抽出した座標配列・検出された特徴点の座標配列）と
                                 2つのフィールド（各特徴点に対応する属性ID・各特徴点に対応するFID）を持った4行のFieldSetFile。
                      """
        tags = ["python", "digiline", "opencv"]


    # flowfileのCRS
    INPUT_FLOWFILE_CRS = PropertyDescriptor(
        name="INPUT_FLOWFILE_CRS",
        description="入力データのCRS",
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

    # GeoDataFrameから生成した座標配列の出力DWH名
    OUTPUT_BASE_COORDINATES_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_BASE_COORDINATES_DWH_NAME",
        description="出力データの座標配列(入力データのGeoDataFrameから生成)のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 特徴点座標配列のDWH名
    OUTPUT_DETECTED_POINT_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_DETECTED_POINT_DWH_NAME",
        description="出力データの特徴点座標配列のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 属性IDのDWH名
    OUTPUT_ATTRIBUTE_ID_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_ATTRIBUTE_ID_DWH_NAME",
        description="出力データの属性IDのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # FIDのDWH名
    OUTPUT_FID_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_FID_DWH_NAME",
        description="出力データのFIDのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [INPUT_FLOWFILE_CRS,
                            VOXEL_ZOOM_LEVEL,
                            VOXEL_CRS,
                            ANGLE_THRESHOLD,
                            OUTPUT_BASE_COORDINATES_DWH_NAME,
                            OUTPUT_DETECTED_POINT_DWH_NAME,
                            OUTPUT_ATTRIBUTE_ID_DWH_NAME,
                            OUTPUT_FID_DWH_NAME]

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

    # 検知器を生成 キーポイントと記述子算出
    def get_key_point_and_descriptor(self,
                                     image_array,
                                     descriptor_type=cv2.AKAZE_DESCRIPTOR_MLDB,
                                     descriptor_size=0,
                                     descriptor_channels=3,
                                     nOctaves=4,
                                     nOctaveLayers=4,
                                     diffusivity=cv2.KAZE_DIFF_PM_G2):

        # Akazeの検知器生成
        akaze = cv2.AKAZE_create(descriptor_type=descriptor_type,
                                 descriptor_size=descriptor_size,
                                 descriptor_channels=descriptor_channels,
                                 nOctaves=nOctaves,
                                 nOctaveLayers=nOctaveLayers,
                                 diffusivity=diffusivity)

        # キーポイントと記述子算出
        kp_query, des_query = akaze.detectAndCompute(image_array, None)

        return kp_query, \
            des_query

    # 画像の配列から特徴点のpixel2次元配列を取得
    def get_key_points_pixel_array(self, image_array):

        # 特徴点抽出
        key_points, descriptors = self.get_key_point_and_descriptor(
            image_array.astype(np.uint8))

        # 特徴点からpixelの配列取得
        key_points_pixel_array = np.array(
            [key_points[i].pt for i in range(len(key_points))])

        return key_points, \
            descriptors, \
            key_points_pixel_array

    def transform(self, context, flowfile):

        try:
            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            target_input_flowfile_crs\
                = int(context.getProperty(self.INPUT_FLOWFILE_CRS).evaluateAttributeExpressions(flowfile).getValue())
            target_voxel_zoom_level\
                = int(context.getProperty(self.VOXEL_ZOOM_LEVEL).evaluateAttributeExpressions(flowfile).getValue())
            target_voxel_crs\
                = int(context.getProperty(self.VOXEL_CRS).evaluateAttributeExpressions(flowfile).getValue())
            target_angle_threshold\
                = float(context.getProperty(self.ANGLE_THRESHOLD).evaluateAttributeExpressions(flowfile).getValue())
            output_base_coordinates_dwh_name\
                = context.getProperty(self.OUTPUT_BASE_COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            output_detected_point_dwh_name\
                = context.getProperty(self.OUTPUT_DETECTED_POINT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            output_attribute_id_dwh_name\
                = context.getProperty(self.OUTPUT_ATTRIBUTE_ID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            output_fid_dwh_name\
                = context.getProperty(self.OUTPUT_FID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # ---------------------------------------------------------------------------
            # flowfileからGeoDataFrame取得
            # ---------------------------------------------------------------------------
            target_geodataframe\
                = WM.calc_func_time(self.logger)(NSP.get_geodataframe_from_contents_or_field_set_file)(flowfile)
            # ---------------------------------------------------------------------------
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
            # --------------------------s------------------------------------------------
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
                                                                                              int(min_y_id),
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
                                                                                              int(max_y_id),
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
                = WM.calc_func_time(self.logger)(DCP._update_image_array_by_duplicated_points)(image_array,
                                                                                               target_lon_lat_array,
                                                                                               x_unit, y_unit,
                                                                                               left_up_min_lon,
                                                                                               left_up_max_lat)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 角度のついた部分に白色を付ける
            # -----------------------------------------------------------------------------------------------------------
            image_array\
                = WM.calc_func_time(self.logger)(self._update_image_array_by_gradient_in_mesh)(pixel_x_index_array,
                                                                                               pixel_y_index_array,
                                                                                               image_array)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # 画像の原点情報
            # -----------------------------------------------------------------------------------------------------------
            world_file_params_list = [x_unit,
                                      0.0,
                                      0.0,
                                      -y_unit,
                                      left_up_min_lon,
                                      left_up_max_lat]

            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # Geotiff読み込み+A-kazeによる特徴点取得 キーポイントと記述子取得
            # -----------------------------------------------------------------------------------------------------------
            key_points, \
                descriptors, \
                key_points_pixel_array\
                = WM.calc_func_time(self.logger)(self.get_key_points_pixel_array)(image_array)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # ワールドファイル情報からpixelの情報を地理座標系へ
            # id+xyの２次元配列になっている
            # -----------------------------------------------------------------------------------------------------------
            coordinates_array\
                = WM.calc_func_time(self.logger)(DCP._get_coordinates_by_tfw_array)(key_points_pixel_array,
                                                                                    world_file_params_list)

            # -----------------------------------------------------------------------------------------------------------
            # 距離計算用に経緯度から平面直角座標系へ変換
            # -----------------------------------------------------------------------------------------------------------
            converted_x_coordinates_array, \
                converted_y_coordinates_array\
                = WM.calc_func_time(self.logger)(DCP.convert_xy_corrdinates_array)(coordinates_array[:, 1],
                                                                                   coordinates_array[:, 2],
                                                                                   target_voxel_crs,
                                                                                   target_input_flowfile_crs)

            # xy更新
            coordinates_array[:, 1] = converted_x_coordinates_array.copy()
            coordinates_array[:, 2] = converted_y_coordinates_array.copy()
            # -----------------------------------------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 特徴点座標をFieldSetFileリストに追加
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
                                                                                        output_base_coordinates_dwh_name,
                                                                                        'float64',
                                                                                        target_coordinates_array)

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_coordinates_record_to_field_set_file_list)(target_dwh_list,
                                                                                                    target_type_list,
                                                                                                    target_value_list,
                                                                                                    output_detected_point_dwh_name,
                                                                                                    'float64',
                                                                                                    coordinates_array,
                                                                                                    add_target_attribute_id_name=output_attribute_id_dwh_name,
                                                                                                    add_target_fid_name=output_fid_dwh_name)

            # --------------------------------------------------------------------------
            # FieldSetFile構成要素の各リストをFieldSetFileに変換
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
