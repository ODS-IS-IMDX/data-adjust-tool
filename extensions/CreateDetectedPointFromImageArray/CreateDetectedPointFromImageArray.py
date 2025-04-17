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

# 画像配列から特徴点を生成する
import traceback

from importlib import import_module

# 使用パッケージimport
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
np = import_module("numpy")
cv2 = import_module("cv2")

# ★クラス名


class CreateDetectedPointFromImageArray(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """画像配列から特徴点を生成する"""
        tags = ["python", "digiline", "opencv"]

    # 画像配列のDWH名
    INPUT_IMAGE_DWH_NAME = PropertyDescriptor(
        name="INPUT_IMAGE_DWH_NAME",
        description="画像配列のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 画像原点情報のDWH名
    INPUT_ORIGIN_LIST_DWH_NAME = PropertyDescriptor(
        name="INPUT_ORIGIN_LIST_DWH_NAME",
        description="画像原点情報のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 画像の原点のCRS
    INPUT_CRS = PropertyDescriptor(
        name="INPUT_CRS",
        description="画像の原点のCRS",
        default_value=6668,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 特徴点座標配列のDWH名
    OUTPUT_DETECTED_POINT_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_DETECTED_POINT_DWH_NAME",
        description="特徴点座標配列のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 属性IDのDWH名
    OUTPUT_ATTRIBUTE_ID_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_ATTRIBUTE_ID_DWH_NAME",
        description="属性IDのDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # FIDのDWH名
    OUTPUT_FID_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_FID_DWH_NAME",
        description="FIDのDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力する特徴点のCRS
    OUTPUT_DETECTED_POINT_CRS = PropertyDescriptor(
        name="OUTPUT_DETECTED_POINT_CRS",
        description="出力する特徴点のCRS",
        default_value=6677,
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [INPUT_IMAGE_DWH_NAME,
                            INPUT_ORIGIN_LIST_DWH_NAME,
                            INPUT_CRS,
                            OUTPUT_DETECTED_POINT_DWH_NAME,
                            OUTPUT_ATTRIBUTE_ID_DWH_NAME,
                            OUTPUT_FID_DWH_NAME,
                            OUTPUT_DETECTED_POINT_CRS]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

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

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # DWHの名前だけ正規表現で取得する
            # --------------------------------------------------------------------------
            input_image_dwh_name\
                = context.getProperty(self.INPUT_IMAGE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            input_origin_list_dwh_name\
                = context.getProperty(self.INPUT_ORIGIN_LIST_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            input_crs\
                = int(context.getProperty(self.INPUT_CRS).evaluateAttributeExpressions(flowfile).getValue())
            output_detected_point_dwh_name\
                = context.getProperty(self.OUTPUT_DETECTED_POINT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            output_attribute_id_dwh_name\
                = context.getProperty(self.OUTPUT_ATTRIBUTE_ID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            output_fid_dwh_name\
                = context.getProperty(self.OUTPUT_FID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            output_crs\
                = int(context.getProperty(self.OUTPUT_DETECTED_POINT_CRS).evaluateAttributeExpressions(flowfile).getValue())
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # flowfile→FieldSetFileのDataFrame
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(NSP.convert_flowfile_to_field_set_file_dataframe)(flowfile)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameからValue列、Dwh列のListを取得
            # --------------------------------------------------------------------------
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameから画像numpy配列取り出し
            # --------------------------------------------------------------------------
            target_image_array\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          input_image_dwh_name)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameから原点情報取り出し
            # --------------------------------------------------------------------------
            tfw_file_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          input_origin_list_dwh_name)
            # --------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # Geotiff読み込み+A-kazeによる特徴点取得 キーポイントと記述子取得
            # -----------------------------------------------------------------------------------------------------------
            key_points, \
                descriptors, \
                key_points_pixel_array\
                = WM.calc_func_time(self.logger)(self.get_key_points_pixel_array)(target_image_array)
            # -----------------------------------------------------------------------------------------------------------

            # -----------------------------------------------------------------------------------------------------------
            # ワールドファイル情報からpixelの情報を地理座標系へ
            # id+xyの２次元配列になっている
            # -----------------------------------------------------------------------------------------------------------
            coordinates_array\
                = WM.calc_func_time(self.logger)(DCP._get_coordinates_by_tfw_array)(key_points_pixel_array,
                                                                                    tfw_file_list)

            # -----------------------------------------------------------------------------------------------------------
            # 距離計算用に経緯度から平面直角座標系へ変換
            # -----------------------------------------------------------------------------------------------------------
            converted_x_coordinates_array, \
                converted_y_coordinates_array\
                = WM.calc_func_time(self.logger)(DCP.convert_xy_corrdinates_array)(coordinates_array[:, 1],
                                                                                   coordinates_array[:, 2],
                                                                                   input_crs,
                                                                                   output_crs)

            # xy更新
            coordinates_array[:, 1] = converted_x_coordinates_array.copy()
            coordinates_array[:, 2] = converted_y_coordinates_array.copy()
            # -----------------------------------------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 特徴点座標をFieldSetFileリストに追加
            # --------------------------------------------------------------------------
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
