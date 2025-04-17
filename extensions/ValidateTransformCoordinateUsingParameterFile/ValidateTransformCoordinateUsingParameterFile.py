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

# 外部モジュール
from importlib import import_module

# Nifiライブラリ
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from common.error_code_list import ErrorCodeList
import cad.common.cad_utils as UC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP

tky2jgd = import_module("tky2jgd")


class ValidateTransformCoordinateUsingParameterFile(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """
                TransformCoordinateUsingParameterFileで必要なZ値を持った座標配列を持っているか、
                プロパティで指定したパラメータファイルのパスは有効か検証。
            """
        )
        tags = ["Validate", "Python"]

    PARAMETER_FILE_PATH = PropertyDescriptor(
        name="Parameter File Path",
        description="パラメータファイル（.par）のパス",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [PARAMETER_FILE_PATH]

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

            result = True

            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # パラメータファイルのパス
            parameter_file_path = context.getProperty(
                self.PARAMETER_FILE_PATH).getValue()

            # --------------------------------------------------------------------------
            # データの空検証
            # --------------------------------------------------------------------------
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # FieldSetFileの形式検証
            # --------------------------------------------------------------------------
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            # 1行のFieldSetFileのValue列を抜きだす。
            _, \
                _, \
                _, \
                input_coordinates_array\
                = (PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # --------------------------------------------------------------------------
            # coordinates_arrayか検証
            # --------------------------------------------------------------------------
            if not self.validate_input_type(input_coordinates_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # Zを持った3Dの座標配列か検証
            # --------------------------------------------------------------------------
            if not self.validate_coordinates_array(input_coordinates_array, multiflag=False):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # 指定されたパスが有効なフォルダかを検証
            # --------------------------------------------------------------------------
            if not self.validate_path(parameter_file_path):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            try:
                # --------------------------------------------------------------------------
                # パラメータファイルが有効か検証
                # --------------------------------------------------------------------------
                tky2jgd.load_parameter(parameter_file_path)
            except Exception:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00038)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            target_lon_lat_coordinates_array = input_coordinates_array[:, 1:3]

            try:
                # --------------------------------------------------------------------------
                # 座標変換が実行できるか検証
                # --------------------------------------------------------------------------
                for i in range(len(target_lon_lat_coordinates_array)):
                    # 緯度経度を引数として変換
                    dB, dL = tky2jgd.bilinear(
                        target_lon_lat_coordinates_array[i, 1], target_lon_lat_coordinates_array[i, 0])

            except Exception:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00055)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            dataframe = UC.field_set_file_to_dataframe(input_data)

            if not self.validate_gdf_shape(dataframe, data_name="DataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS

            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
