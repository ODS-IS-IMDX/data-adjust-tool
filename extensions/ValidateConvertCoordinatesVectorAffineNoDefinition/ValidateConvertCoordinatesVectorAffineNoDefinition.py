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

# Python標準モジュール
import io

# 外部モジュール
from importlib import import_module

# Nifi自作ライブラリ
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
from common.error_code_list import ErrorCodeList
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import cad.common.cad_utils as CU

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")


class ValidateConvertCoordinatesVectorAffineNoDefinition(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """ConvertCoordinatesVectorAffineNoDefinition に必要な座標配列があるか、GCPから作成された座標配列が反時計回りで作成されないか検証。"""
        )
        tags = ["validate", "python"]

    # 座標の分割単位数を指定する
    SPLIT_UNIT_NUMBER = PropertyDescriptor(
        name="Split Unit Number",
        description="座標の分割単位数",
        default_value='100',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False,
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
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # TargetGCP側の拡張子
    TARGET_EXTENSION = PropertyDescriptor(
        name="Target GCP Extension",
        description="Target側の拡張子",
        allowable_values=DDC.INPUT_FILE_EXTENSION_LIST,
        sensitive=False,
        required=True
    )

    # Base側のファイルパス
    BASE_FILE_PATH = PropertyDescriptor(
        name="Base File Path",
        description="Base側のファイルパス（位置基準側のGCP）",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # Base側のEncoding
    BASE_ENCODING = PropertyDescriptor(
        name="Base  Encoding",
        description="Base側のEncoding",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value='utf-8',
        sensitive=False,
        required=True
    )

    # Base側の拡張子
    BASE_EXTENSION = PropertyDescriptor(
        name="Base Extension",
        description="Base側の拡張子",
        allowable_values=DDC.INPUT_FILE_EXTENSION_LIST,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [SPLIT_UNIT_NUMBER,
                            TARGET_FILE_PATH,
                            TARGET_ENCODING,
                            TARGET_EXTENSION,
                            BASE_FILE_PATH,
                            BASE_ENCODING,
                            BASE_EXTENSION]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_data(self, context, flowfile):
        """
        入力データの検証を実行する

        :param context: プロセッサの設定値が格納されているデータ
        :param flowfile: プロセッサに渡されるデータ

        :return 検証結果 正常="success"、異常="failure"
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # データの空検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式検証
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            try:
                # 座標の分割単位数
                split_unit_number\
                    = int(context.getProperty(self.SPLIT_UNIT_NUMBER).evaluateAttributeExpressions(flowfile).getValue())

            except Exception:
                args = {
                    "error_code": ErrorCodeList.ED00041,
                    "対象プロパティ": "Split Unit Number"
                }
                self.validate_logger.write_log(**args)

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

            # flowfileから、csv形式のFieldSetFileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            input_field_set_dataframe = pd.read_csv(
                io.StringIO(input_field_set_file))

            try:
                # flowfileから"crs"属性の内容を取得する
                crs = flowfile.getAttribute("crs")
            except Exception:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00039)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = NSP.get_value_dwh_list_from_field_set_file_dataframe(input_field_set_dataframe)

            correction_target_feature_array\
                = NSP.get_value_field_from_value_dwh_list(target_value_list,
                                                          target_dwh_list,
                                                          target_dwh_list[0])

            if not self.validate_input_type(correction_target_feature_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_coordinates_is_3D(correction_target_feature_array):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_path(target_file_path):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_path(base_file_path):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # ファイルパスで取得
            # --------------------------------------------------------------------------
            try:
                correction_target_gcp_geodataframe\
                    = NSP.get_geodataframe_from_datasource(target_file_path,
                                                           target_encoding,
                                                           extension=target_extension)
            except Exception:
                args = {
                    "error_code": ErrorCodeList.EC00007,
                    "Target GCP Encoding": target_encoding,
                }
                result = False
                self.validate_logger.write_log(**args)
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            try:
                position_standard_gcp_geodataframe\
                    = NSP.get_geodataframe_from_datasource(base_file_path,
                                                           base_encoding,
                                                           extension=base_extension)
            except Exception:
                args = {
                    "error_code": ErrorCodeList.EC00007,
                    "Base GCP Encoding": base_encoding,
                }
                result = False
                self.validate_logger.write_log(**args)
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            correction_target_gcp_array, \
                _, \
                _\
                = NSP.get_coordinates_array_from_geodataframe(correction_target_gcp_geodataframe)

            if not self.validate_input_type(correction_target_gcp_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            position_standard_gcp_array, \
                _, \
                _\
                = NSP.get_coordinates_array_from_geodataframe(position_standard_gcp_geodataframe)

            if not self.validate_input_type(position_standard_gcp_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # GCPが反時計回りか検証
            if not self.validate_correct_GCP(position_standard_gcp_array,
                                             correction_target_gcp_array):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            geodataframe = CU.field_set_file_to_geodataframe(input_data, crs)

            if not self.validate_gdf_shape(geodataframe, data_name="GeoDataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS

            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
