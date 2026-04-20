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
import pickle

# Nifiライブラリ
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from common.error_code_list import ErrorCodeList

import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC


class ValidateConvertGeoDataFrameToContents(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ConvertGeoDataFrameToContentsに必要なデータを持っているか検証。
                      """
        tags = ["Validate", "Python"]

    # 出力ファイルのEncoding
    OUTPUT_FILE_ENCODING = PropertyDescriptor(
        name="output file encoding",
        description="出力ファイルのEncoding",
        default_value='utf-8',
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力ファイルの拡張子
    OUTPUT_FILE_EXTENSION = PropertyDescriptor(
        name="output file extension",
        description="出力ファイルの拡張子",
        allowable_values=DDC.OUTPUT_FILE_EXTENSION_LIST,
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [OUTPUT_FILE_ENCODING,
                            OUTPUT_FILE_EXTENSION]

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

            # 出力ファイルのEncoding
            output_file_encoding\
                = context.getProperty(self.OUTPUT_FILE_ENCODING).evaluateAttributeExpressions(flowfile).getValue()

            # 出力ファイルの拡張子
            output_file_extension\
                = context.getProperty(self.OUTPUT_FILE_EXTENSION).evaluateAttributeExpressions(flowfile).getValue()

            layer_name = flowfile.getAttribute(DDC.GPKG_ARGS)

            # --------------------------------------------------------------------------
            # layer_nameが存在するか検証
            # --------------------------------------------------------------------------
            if not layer_name or layer_name == "":
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00061)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE
            else:
                pass

            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # --------------------------------------------------------------------------
            # データの空検証
            # --------------------------------------------------------------------------
            if not self.validate_empty_data(input_data):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # シリアライズされたデータか検証
            # --------------------------------------------------------------------------
            if not self.validate_serialized_data(input_data):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # バイトデータからGeoDataFrameを復元する
            geodataframe = pickle.loads(input_data)

            # --------------------------------------------------------------------------
            # GeoDataFrameかどうかの検証
            # --------------------------------------------------------------------------
            if not self.validate_input_type(geodataframe, "GeoDataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # GeoDataFrameのカラム名に"FID"が含まれていないか検証
            # --------------------------------------------------------------------------
            if output_file_extension == ".gpkg":
                columns = geodataframe.columns.str.lower()
                if 'fid' in columns:
                    # FIDが見つかった場合、エラーコードED00075を出力
                    self.validate_logger.write_log(
                        error_code=ErrorCodeList.ED00076)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # GeoDataFrameをcsv,geojson,gpkgに変換できるか検証
            # --------------------------------------------------------------------------
            if not self.validate_geodataframe_conversion(geodataframe,
                                                         output_file_extension,
                                                         output_file_encoding,
                                                         layer_name):
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
