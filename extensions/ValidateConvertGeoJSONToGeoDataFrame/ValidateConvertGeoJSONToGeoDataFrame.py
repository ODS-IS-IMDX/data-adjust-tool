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

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor

from common.error_code_list import ErrorCodeList

gpd = import_module("geopandas")


class ValidateConvertGeoJSONToGeoDataFrame(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ConvertGeoJSONToGeoDataFrameで必要なデータを持っているか検証。
                      """
        tags = ["Validate", "Python"]

    # GeoJSONのエンコーディングを示すプロパティ値
    GEOJSON_FILE_ENCODE = PropertyDescriptor(
        name='GeoJSON File Encode',
        description='GeoJSONの文字コード',
        default_value='shift-jis',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # GeoJSONの座標参照系（CRS）を示すプロパティ値
    GEOJSON_FILE_CRS = PropertyDescriptor(
        name='GeoJSON File CRS',
        description='GeoJSONのCRS(epsgコード)',
        default_value='6677',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    property_descriptors = [GEOJSON_FILE_ENCODE,
                            GEOJSON_FILE_CRS]

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

            # GeoJSONのエンコーディング取得
            geojson_file_encode = context.getProperty(
                self.GEOJSON_FILE_ENCODE).getValue()

            # GeoJSONの座標参照系（CRS）取得
            geojson_file_crs = context.getProperty(
                self.GEOJSON_FILE_CRS).getValue()

            # --------------------------------------------------------------------------
            # geojson_file_crsが正しいepsgかどうかの検証
            # --------------------------------------------------------------------------
            if not self.check_epsg(geojson_file_crs):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # --------------------------------------------------------------------------
            # データの空検証
            # --------------------------------------------------------------------------
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # flowfileのデータをファイルオブジェクトへ変換
            input_stream = io.BytesIO(input_data)

            # --------------------------------------------------------------------------
            # GeoDataFrameに変換できるか検証
            # --------------------------------------------------------------------------
            try:

                geojson_dataframe = gpd.read_file(input_stream,
                                                  driver='GeoJSON',
                                                  crs=geojson_file_crs,
                                                  encoding=geojson_file_encode
                                                  )

            except UnicodeDecodeError:
                self.validate_logger.write_log(ErrorCodeList.EC00007)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            except LookupError:
                self.validate_logger.write_log(ErrorCodeList.EC00007)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            except Exception:
                self.validate_logger.write_log(ErrorCodeList.ED00070)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_gdf_shape(geojson_dataframe, data_name="DataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS

            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
