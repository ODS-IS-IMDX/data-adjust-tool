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
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC

pd = import_module("pandas")
np = import_module("numpy")


class ValidateConvertLinkCSVToSpatialIDCenterPoint(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """ConvertLinkCSVToSpatialIDCenterPointで必要なCSVデータを持っているか検証。"""
        tags = ["Validate", "Python"]

    # 空間IDのsplit文字列
    SPATIAL_ID_SPLIT_STRING = PropertyDescriptor(
        name="SPATIAL_ID_SPLIT_STRING",
        description="空間IDの区切り文字",
        default_value="/",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # 空間IDのズームレベル文字列
    # LinkDataのCSVにはすべて一律のズームレベルの空間IDが設定されているものとする
    SPATIAL_ID_ZOOM_LEVEL = PropertyDescriptor(
        name="SPATIAL_ID_ZOOM_LEVEL",
        description="空間IDのズームレベル",
        default_value="26",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # 出力データである空間IDのCRS
    OUTPUT_SPATIAL_ID_CRS = PropertyDescriptor(
        name="OUTPUT_SPATIAL_ID_CRS",
        description="出力データである空間IDのCRS",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [SPATIAL_ID_SPLIT_STRING,
                            SPATIAL_ID_ZOOM_LEVEL,
                            OUTPUT_SPATIAL_ID_CRS]

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

            # 空間IDの区切り文字
            spatial_id_split_string\
                = context.getProperty(self.SPATIAL_ID_SPLIT_STRING).evaluateAttributeExpressions(flowfile).getValue()

            # 空間IDのズームレベル
            spatial_id_zoom_level\
                = context.getProperty(self.SPATIAL_ID_ZOOM_LEVEL).evaluateAttributeExpressions(flowfile).getValue()

            # 出力データである空間IDのCRS
            output_spatial_id_crs\
                = int(context.getProperty(self.OUTPUT_SPATIAL_ID_CRS).evaluateAttributeExpressions(flowfile).getValue())

            # --------------------------------------------------------------------------
            # geojson_file_crsが正しいepsgかどうかの検証
            # --------------------------------------------------------------------------
            if not self.check_epsg(output_spatial_id_crs):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            try:
                spatial_id_zoom_level = float(spatial_id_zoom_level)

            except ValueError:
                args = {"error_code": ErrorCodeList.ED00042,
                        "spatial_id_zoom_level": spatial_id_zoom_level}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # spatial_id_zoom_levelが0以上かどうか検証
            # --------------------------------------------------------------------------
            if spatial_id_zoom_level < 0:
                args = {"error_code": ErrorCodeList.ED00053,
                        "spatial_id_zoom_level": spatial_id_zoom_level}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # データの空検証
            # --------------------------------------------------------------------------
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、パスとして認識させる
            link_csv_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換
            csv_data_frame = pd.read_csv(link_csv_stringio)

            # --------------------------------------------------------------------------
            # 入力データに空間IDカラム名が存在するか検証
            # --------------------------------------------------------------------------
            try:
                spatial_id_list\
                    = csv_data_frame[DDC.SPATIAL_ID_COLUMN_NAME].to_list()

            except Exception:
                self.validate_logger.write_log(ErrorCodeList.ED00071)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # spatial_id_listの1要素目を取得
            first_spatial_id = spatial_id_list[0]

            # --------------------------------------------------------------------------
            # 入力データにpatial_id_split_stringが含まれているか検証
            # --------------------------------------------------------------------------
            if spatial_id_split_string in first_spatial_id:
                pass
            else:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00072)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # float64に変換できるか検証
            # --------------------------------------------------------------------------
            try:
                # splitを行い2次元配列へ[空間ID]>[z,f,x,y]
                spatial_id_array\
                    = np.array([temp_id.split(spatial_id_split_string) for temp_id in spatial_id_list], dtype=np.float64)

            except Exception:
                self.validate_logger.write_log(ErrorCodeList.ED00073)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # [z,f,x,y]の4要素かどうかの検証
            # --------------------------------------------------------------------------
            for i, row in enumerate(spatial_id_array):

                if len(row) != 4:

                    args = {"error_code": ErrorCodeList.ED00073,
                            "空間ID": f"行 {i}, 対象データ{row}"}
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                else:
                    pass

            if not self.validate_gdf_shape(csv_data_frame, data_name="DataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS

            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
