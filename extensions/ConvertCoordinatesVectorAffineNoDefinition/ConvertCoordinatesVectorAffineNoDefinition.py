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
# 基準GCPと対象データGCPからTINを生成し、対象データ（座標配列）のアフィン変換を行う
# --------------------------------------------------------------------------------------------
# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")


class ConvertCoordinatesVectorAffineNoDefinition(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """基準GCPと対象データGCPからTINを生成し、対象データ（座標配列）のアフィン変換を行うプロセッサ。"""
        tags = ["python", "pandas", "Coordinates", "Affine", "VectorAffine"]

    # 処理後のDwh名を指定
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="処理後のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 処理モード（2Dまたは3D）を指定
    CORRECT_MODE = PropertyDescriptor(
        name="Correct Mode",
        description="処理モード（2Dまたは3D）",
        default_value='2D',
        allowable_values=["2D", "3D"],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 座標の分割単位数を指定する
    SPLIT_UNIT_NUMBER = PropertyDescriptor(
        name="Split Unit Number",
        description="座標の分割単位数",
        default_value='100',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # TargetGCP側のファイルパス
    TARGET_FILE_PATH = PropertyDescriptor(
        name="Target GCP File Path",
        description="Target側のファイルパス（アフィン対象側、補正対象側のGCP）",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # TargetGCP側のEncoding
    TARGET_ENCODING = PropertyDescriptor(
        name="Target GCP Encoding",
        description="Target側のEncoding",
        default_value='utf-8',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # TargetGCP側の拡張子
    TARGET_EXTENSION = PropertyDescriptor(
        name="Target GCP Extension",
        description="Target側の拡張子",
        sensitive=False,
        allowable_values=DDC.INPUT_FILE_EXTENSION_LIST,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # Base側のファイルパス
    BASE_FILE_PATH = PropertyDescriptor(
        name="Base File Path",
        description="Base側のファイルパス（位置基準側のGCP）",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # Base側のEncoding
    BASE_ENCODING = PropertyDescriptor(
        name="Base  Encoding",
        description="Base側のEncoding",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value='utf-8',
        required=True
    )

    # Base側の拡張子
    BASE_EXTENSION = PropertyDescriptor(
        name="Base Extension",
        description="Base側の拡張子",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        allowable_values=DDC.INPUT_FILE_EXTENSION_LIST,
        required=True
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            CORRECT_MODE,
                            SPLIT_UNIT_NUMBER,
                            TARGET_FILE_PATH,
                            TARGET_ENCODING,
                            TARGET_EXTENSION,
                            BASE_FILE_PATH,
                            BASE_ENCODING,
                            BASE_EXTENSION]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # 処理後のDwh名
            output_dwh_name\
                = context.getProperty(self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 処理モード（2Dまたは3D）を指定
            correct_mode\
                = context.getProperty(self.CORRECT_MODE).evaluateAttributeExpressions(flowfile).getValue()

            # 座標の分割単位数
            split_unit_number\
                = int(context.getProperty(self.SPLIT_UNIT_NUMBER).evaluateAttributeExpressions(flowfile).getValue())

            # Target側のファイルパス
            target_file_path\
                = context.getProperty(self.TARGET_FILE_PATH).evaluateAttributeExpressions(flowfile).getValue()

            # Target側のEncoding
            target_encoding\
                = context.getProperty(self.TARGET_ENCODING).evaluateAttributeExpressions(flowfile).getValue()

            # Target側の拡張子
            target_extension\
                = context.getProperty(self.TARGET_EXTENSION).evaluateAttributeExpressions(flowfile).getValue()

            # Base側のファイルパス
            base_file_path\
                = context.getProperty(self.BASE_FILE_PATH).evaluateAttributeExpressions(flowfile).getValue()

            # Base側のEncoding
            base_encoding\
                = context.getProperty(self.BASE_ENCODING).evaluateAttributeExpressions(flowfile).getValue()

            # Base側の拡張子
            base_extension\
                = context.getProperty(self.BASE_EXTENSION).evaluateAttributeExpressions(flowfile).getValue()
            # ---------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # flowfile→field_set_fileのdataframe
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(NSP.convert_flowfile_to_field_set_file_dataframe)(flowfile)

            # --------------------------------------------------------------------------
            # フィールド集合ファイルのdataframeからValue列、Dwh列のListを取得
            # --------------------------------------------------------------------------
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)
            affine_target_dwh_name = target_dwh_list[0]
            # --------------------------------------------------------------------------
            # field_set_fileのdataframeから補正対象座標取り出し
            # ★フィールド集合ファイルは、「カラム名の行」+「補正対象の地物」の２行であることが前提
            # --------------------------------------------------------------------------
            correction_target_feature_array\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          target_dwh_list[0])

            affine_target_array = correction_target_feature_array[:, :4]

            # --------------------------------------------------------------------------
            # ファイルパスで取得
            # --------------------------------------------------------------------------
            correction_target_gcp_geodataframe\
                = WM.calc_func_time(self.logger)(NSP.get_geodataframe_from_datasource)(target_file_path,
                                                                                       target_encoding,
                                                                                       extension=target_extension)

            position_standard_gcp_geodataframe\
                = WM.calc_func_time(self.logger)(NSP.get_geodataframe_from_datasource)(base_file_path,
                                                                                       base_encoding,
                                                                                       extension=base_extension)

            correction_target_gcp_array, \
                _, \
                _\
                = WM.calc_func_time(self.logger)(NSP.get_coordinates_array_from_geodataframe)(correction_target_gcp_geodataframe)

            position_standard_gcp_array, \
                _, \
                _\
                = WM.calc_func_time(self.logger)(NSP.get_coordinates_array_from_geodataframe)(position_standard_gcp_geodataframe)

            # --------------------------------------------------------------------------

            # -------------------------------------------------------------------------------------------------------------------------------------------------
            # ベクトルアフィン実行
            # execute_vector_affine(関数名候補)
            # -------------------------------------------------------------------------------------------------------------------------------------------------
            # 調整後のTINと補正対象地物をベクトルアフィンにセット
            if correct_mode == "2D":

                affine_target_array\
                    = WM.calc_func_time(self.logger)(NCP.calculate_geometry_coordinate_by_vector_affine_2D)(correction_target_gcp_array,
                                                                                                            position_standard_gcp_array,
                                                                                                            affine_target_array,
                                                                                                            split_unit_number,
                                                                                                            self.logger)

            else:

                affine_target_array\
                    = WM.calc_func_time(self.logger)(NCP.calculate_geometry_coordinate_by_vector_affine_3D)(correction_target_gcp_array,
                                                                                                            position_standard_gcp_array,
                                                                                                            affine_target_array,
                                                                                                            split_unit_number,
                                                                                                            self.logger)

            # -------------------------------------------------------------------------------------------------------------------------------------------------
            # arrayをfieldsetfileにし、出力
            # -------------------------------------------------------------------------------------------------------------------------------------------------
            correction_target_feature_array[:, :4] = affine_target_array
            geometry_type = "geometry"

            # 出力FieldSetFile仕様にリスト化
            output_dwh_name_list = [output_dwh_name]
            geometry_type_list = [geometry_type]
            coordinates_array_list = [correction_target_feature_array]
            affine_target_dwh_name_list = [affine_target_dwh_name]

            # プロパティでdwh名を入力した場合は上書き
            if output_dwh_name == "":
                attributes_dict = {"DWH": affine_target_dwh_name}
                output_field_set_file\
                    = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(affine_target_dwh_name_list,
                                                                             geometry_type_list,
                                                                             coordinates_array_list)

            else:
                attributes_dict = {"DWH": output_dwh_name}
                output_field_set_file\
                    = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_name_list,
                                                                             geometry_type_list,
                                                                             coordinates_array_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
