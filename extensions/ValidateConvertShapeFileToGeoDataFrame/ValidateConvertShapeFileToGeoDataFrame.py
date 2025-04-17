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

# 定数を定義
MILLIMETER = "ミリメートル(mm)"
CENTIMETER = "センチメートル(cm)"
DECIMETER = "デシメートル(dm)"
METER = "メートル(m)"
KILOMETER = "キロメートル(km)"
DEGREE = "経緯度"


class ValidateConvertShapeFileToGeoDataFrame(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ConvertShapeFileToGeoDataFrameで必要なデータを持っているか検証。
                      """
        tags = ["Validate", "Python"]

    # shapefileのエンコーディングを示すプロパティの値
    SHAPE_FILE_ENCODE = PropertyDescriptor(
        name='Shapefile Encode',
        description='Shapeファイルの文字コード',
        default_value='shift-jis',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # shapefileの座標参照系（CRS）を示すプロパティの値
    SHAPE_FILE_CRS = PropertyDescriptor(
        name='Shapefile CRS',
        description='シェープファイルのCRS(epsgコード)',
        default_value='6677',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # x座標の平行移動値
    X_OFFSET = PropertyDescriptor(
        name='Latitude(X) Offset',
        description='緯度(x座標)の平行移動値',
        expression_language_scope=ExpressionLanguageScope.NONE,
        default_value="0",
        sensitive=False,
        required=False
    )

    # y座標の平行移動値
    Y_OFFSET = PropertyDescriptor(
        name='Longitude(Y) Offset',
        description='経度(y座標)の平行移動値',
        expression_language_scope=ExpressionLanguageScope.NONE,
        default_value="0",
        sensitive=False,
        required=False
    )

    property_descriptors = [SHAPE_FILE_ENCODE,
                            SHAPE_FILE_CRS,
                            X_OFFSET,
                            Y_OFFSET]

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

            # shapefileのエンコーディング取得
            shape_file_encode = context.getProperty(
                self.SHAPE_FILE_ENCODE).getValue()

            # shapefileの座標参照系（CRS）取得
            shape_file_crs = context.getProperty(
                self.SHAPE_FILE_CRS).getValue()

            # x座標の平行移動値取得
            x_offset = context.getProperty(self.X_OFFSET).getValue()

            # y座標の平行移動値取得
            y_offset = context.getProperty(self.Y_OFFSET).getValue()

            # --------------------------------------------------------------------------
            # input_crsが正しいepsgかどうかの検証
            # --------------------------------------------------------------------------
            if not self.check_epsg(shape_file_crs):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # x座標の平行移動値が正しいかどうかの検証
            # --------------------------------------------------------------------------
            if x_offset not in (None, ""):
                try:
                    float(x_offset)

                except ValueError:
                    args = {"error_code": ErrorCodeList.ED00042,
                            "x_offset": x_offset}
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # y座標の平行移動値が正しいかどうかの検証
            # --------------------------------------------------------------------------
            if y_offset not in (None, ""):
                try:
                    float(y_offset)

                except ValueError:
                    args = {"error_code": ErrorCodeList.ED00042,
                            "y_offset": y_offset}
                    self.validate_logger.write_log(**args)
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

            # flowfileのデータをfile_objectへ変換
            input_stream = io.BytesIO(input_data)

            # --------------------------------------------------------------------------
            # GeoDataFrameに変換できるか検証
            # --------------------------------------------------------------------------
            try:

                shape_dataframe = gpd.read_file(input_stream,
                                                driver='ESRI Shapefile',
                                                crs=shape_file_crs,
                                                encoding=shape_file_encode
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
                self.validate_logger.write_log(ErrorCodeList.ED00069)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_gdf_shape(shape_dataframe, data_name="DataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS

            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
