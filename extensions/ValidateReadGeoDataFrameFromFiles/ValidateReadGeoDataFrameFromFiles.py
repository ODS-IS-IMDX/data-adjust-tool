# MIT License
# 
# Copyright (c) 2025,2026 NTT InfraNet
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

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor

import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP

from common.error_code_list import ErrorCodeList


class ValidateReadGeoDataFrameFromFiles(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ReadGeoDataFrameFromFilesで必要なデータを持っているか、プロパティで指定したエンコードが正しいか検証。
                      """
        tags = ["Validate", "Python"]

    # 入荷対象ファイルのEncoding
    INPUT_ENCODING = PropertyDescriptor(
        name="Input Encoding",
        description="入荷対象ファイルのEncoding",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value='utf-8',
        required=True,
        sensitive=False
    )

    # 入荷対象ファイルの拡張子
    INPUT_EXTENSION = PropertyDescriptor(
        name="Input Extension",
        description="入荷対象ファイルの拡張子",
        allowable_values=DDC.INPUT_FILE_EXTENSION_LIST,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    property_descriptors = [INPUT_ENCODING,
                            INPUT_EXTENSION]

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

            # --------------------------------------------------------------------------
            # データの空検証
            # --------------------------------------------------------------------------
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # flowfileのデータをfile_objectへ変換
            input_stream = io.BytesIO(input_data)

            # 入荷対象ファイルのEncoding
            input_encoding\
                = context.getProperty(self.INPUT_ENCODING).evaluateAttributeExpressions(flowfile).getValue()

            # 入荷対象ファイルの拡張子
            input_extension\
                = context.getProperty(self.INPUT_EXTENSION).evaluateAttributeExpressions(flowfile).getValue()

            try:
                geodataframe\
                    = NSP.get_geodataframe_from_datasource(input_stream,
                                                           input_encoding,
                                                           extension=input_extension)

            except UnicodeDecodeError:
                args = {"error_code": ErrorCodeList.EC00007,
                        "Input Encoding": str(input_encoding)}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            except LookupError:
                args = {"error_code": ErrorCodeList.EC00007,
                        "Input Encoding": str(input_encoding)}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

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
