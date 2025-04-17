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
# FieldSetFileからGeoDataFrameと座標配列を取得
# その後GeoDataFrameのgeometry列を取得した座標配列で更新
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
np = import_module("numpy")


class UpdateCoordinatesInDataFrame(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """geodataframeのgeometry列をCoordinates配列で更新"""
        tags = ["python", "pandas", "Coordinates", "Affine", "VectorAffine"]

    # 更新する座標配列のCRS
    INPUT_COORDINATES_CRS = PropertyDescriptor(
        name="Input_Coordinates_CRS",
        description="更新する座標配列のCRS（未入力の場合はGeoDataFrameのCRSを維持）",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 処理後のDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="処理後のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [INPUT_COORDINATES_CRS, OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # 処理後のDWH名
            output_dwh_name\
                = context.getProperty(self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            input_coordinates_crs\
                = context.getProperty(self.INPUT_COORDINATES_CRS).evaluateAttributeExpressions(flowfile).getValue()
            # ---------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # flowfile→FieldSetFileのDataFrame
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(NSP.convert_flowfile_to_field_set_file_dataframe)(flowfile)

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameからValue列、Dwh列のリストを取得
            # --------------------------------------------------------------------------
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)

            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileの1行目、2行目をオブジェクトに戻す
            # --------------------------------------------------------------------------
            line1_object\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          target_dwh_list[0])

            line2_object\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          target_dwh_list[1])
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # クラスを比較してGeoDataFrameと座標配列に分ける
            # --------------------------------------------------------------------------
            if isinstance(line1_object, np.ndarray):

                coordinates_array = line1_object.copy()
                geodataframe = line2_object.copy()

            else:
                coordinates_array = line2_object.copy()
                geodataframe = line1_object.copy()

            # --------------------------------------------------------------------------

            # DataFrameからジオメトリタイプ取得
            result_array, \
                geometry_type_list, \
                index_array\
                = WM.calc_func_time(self.logger)(NSP.get_coordinates_array_from_geodataframe)(geodataframe)

            coordinates_dict\
                = WM.calc_func_time(self.logger)(NSP.get_shapely_dict_from_coordinates_array)(coordinates_array,
                                                                                              geometry_type_list)

            # geometry列更新
            geodataframe[geodataframe.geometry.name] = coordinates_dict.values()

            # --------------------------------------------------------------------------
            # 座標配列とGeoDataFrameでcrsが異なる場合はcrsを変換
            # --------------------------------------------------------------------------
            # input_coordinates_crsに入力がない場合はスキップ
            if input_coordinates_crs == '' or input_coordinates_crs == None:
                pass

            # input_coordinates_crsに入力がある場合はGeoDataFrameのcrsと比較し、
            # 一致すればスキップ、一致しない場合はinput_coordinates_crsをGeoDataFrameのcrsとして上書きする
            else:
                if input_coordinates_crs == str(geodataframe.crs).replace("EPSG:", ""):
                    pass

                else:
                    geodataframe = geodataframe.set_crs(
                        int(input_coordinates_crs), allow_override=True)

            # --------------------------------------------------------------------------

            # FieldSetFileに設定
            target_dwh_list = []
            target_type_list = []
            target_value_list = []

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        output_dwh_name,
                                                                                        DDC.CONTENTS_CODE_GEODATAFRAME,
                                                                                        geodataframe)

            # --------------------------------------------------------------------------
            # FieldSetFile構成要素の各リストをFieldSetFileに変換
            # --------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(target_dwh_list,
                                                                                 target_type_list,
                                                                                 target_value_list)
            # --------------------------------------------------------------------------

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes={"crs": str(
                                               input_coordinates_crs)}
                                           )

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
