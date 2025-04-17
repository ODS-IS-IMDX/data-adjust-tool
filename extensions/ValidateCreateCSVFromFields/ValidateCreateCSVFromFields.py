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

# Python標準ライブラリ
import io

# 外部モジュール
from importlib import import_module

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
from common.error_code_list import ErrorCodeList
import cad.common.cad_utils as CU

pd = import_module("pandas")


class ValidateCreateCSVFromFields(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """CreateCSVFromFieldsに必要なフィールドを持っているか、データ定義が存在するか検証。"""
        )
        tags = ["validate", "python"]

    # データ定義ファイルの区切り文字
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=True
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER]

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

            # データ定義の区切り文字を取得
            data_definition_delimiter = context.getProperty(
                self.DATA_DEFINITION_DELIMITER).evaluateAttributeExpressions(flowfile).getValue()

            # データの空検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式検証
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            try:
                # flowfileから"DataDefinition"属性の内容を取得する
                data_definition = flowfile.getAttribute("DataDefinition")
            except Exception:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00024)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # data_definitionがNoneTypeの場合はエラー
            if data_definition:
                pass
            else:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00024)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、パスとして認識させる
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換
            field_set_data_frame = pd.read_csv(field_set_file_stringio)

            # 区切り文字が正しいか検証
            if not self.validate_data_definition_delimiter(data_definition,
                                                           data_definition_delimiter):
                return self.RESULT_FAILURE

            if data_definition_delimiter == DDC.DELIMITER_TAB:
                data_definition_data_frame = pd.read_csv(io.StringIO(data_definition),
                                                         sep='\t')

            else:
                data_definition_data_frame = pd.read_csv(
                    io.StringIO(data_definition))

            # columnがあっているか検証
            if not self.validate_data_definition_column(data_definition_data_frame,
                                                        False):
                return self.RESULT_FAILURE

            # データ定義に書かれているDWHファイル名がFieldSetFile内にあるか
            if not self.validate_data_definition_in_fsf(data_definition,
                                                        data_definition_delimiter,
                                                        field_set_data_frame):
                return self.RESULT_FAILURE

            dataframe = CU.field_set_file_to_dataframe(input_data)

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
