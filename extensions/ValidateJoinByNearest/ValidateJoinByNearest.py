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
import base64
import pickle

# 外部モジュール
from importlib import import_module

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
import cad.common.cad_utils as UC
from common.error_code_list import ErrorCodeList

pd = import_module("pandas")


class ValidateJoinByNearest(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """JoinByNearestで必要な座標配列を持っているか検証。"""
        )
        tags = ["Validate", "Python"]
    POINT_DWH_NAME = PropertyDescriptor(
        name="Point DWH Name",
        description="ポイントの座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    LINESTRING_DWH_NAME = PropertyDescriptor(
        name="LineString DWH Name",
        description="ラインの座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    INPUT_CRS = PropertyDescriptor(
        name="Input CRS",
        description="ポイントとラインのCRS(epsgコード)",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    VOXEL_CRS = PropertyDescriptor(
        name="Voxel CRS",
        description="空間IDのCRS(epsgコード)",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    ZOOM_LEVEL = PropertyDescriptor(
        name="Zoom Level",
        description="分割用空間IDのズームレベル",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    BUFFER_DISTANCE = PropertyDescriptor(
        name="Buffer Distance",
        description="""
図郭を覆う範囲を作成する際に、図郭の1辺を延長する長さ(メートル)。
※ 図郭外に最近傍線がある場合に備えて、図郭より広い範囲を指定しておく必要がある
                    """,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [POINT_DWH_NAME,
                            LINESTRING_DWH_NAME,
                            INPUT_CRS,
                            VOXEL_CRS,
                            ZOOM_LEVEL,
                            BUFFER_DISTANCE]

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

            point_dwh_name\
                = context.getProperty(self.POINT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            linestring_dwh_name\
                = context.getProperty(self.LINESTRING_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            input_crs\
                = int(context.getProperty(self.INPUT_CRS).evaluateAttributeExpressions(flowfile).getValue())
            voxel_crs\
                = int(context.getProperty(self.VOXEL_CRS).evaluateAttributeExpressions(flowfile).getValue())
            zoom_level\
                = int(context.getProperty(self.ZOOM_LEVEL).evaluateAttributeExpressions(flowfile).getValue())
            buffer_distance\
                = float(context.getProperty(self.BUFFER_DISTANCE).evaluateAttributeExpressions(flowfile).getValue())

            # --------------------------------------------------------------------------
            # input_crsが正しいepsgかどうかの検証
            # --------------------------------------------------------------------------
            if not self.check_epsg(input_crs):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # voxel_crsが正しいepsgかどうかの検証
            # --------------------------------------------------------------------------
            if not self.check_epsg(voxel_crs):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            try:
                float(zoom_level)

            except ValueError:
                args = {"error_code": ErrorCodeList.ED00042,
                        "zoom_level": zoom_level}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            try:
                float(buffer_distance)

            except ValueError:
                args = {"error_code": ErrorCodeList.ED00042,
                        "buffer_distance": buffer_distance}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # buffer_distanceが0以上かどうか検証
            # --------------------------------------------------------------------------
            if zoom_level < 0:
                args = {"error_code": ErrorCodeList.ED00053,
                        "zoom_level": zoom_level}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # zoom_levelが0以上かどうか検証
            # --------------------------------------------------------------------------
            if buffer_distance <= 0:
                args = {"error_code": ErrorCodeList.ED00053,
                        "buffer_distance": buffer_distance}
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

            # --------------------------------------------------------------------------
            # FieldSetFileの形式検証
            # --------------------------------------------------------------------------
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、パスとして認識させる
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換
            field_set_data_frame = pd.read_csv(field_set_file_stringio)

            # --------------------------------------------------------------------------
            # point_dwh_nameがFieldSetFileに存在するか検証
            # --------------------------------------------------------------------------
            if not self.validate_target_in_fsf(point_dwh_name, field_set_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            point_array = pickle.loads(base64.b64decode(
                field_set_data_frame.loc[field_set_data_frame["Dwh"] == point_dwh_name, "Value"].values[0]))

            # --------------------------------------------------------------------------
            # point_dwh_nameが正しい形か検証
            # --------------------------------------------------------------------------
            if not self.validate_input_type(point_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # マルチパッチの座標配列ではないか検証
            # --------------------------------------------------------------------------
            if not self.validate_coordinates_array(point_array, multiflag=False):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # linestring_dwh_nameがFieldSetFileに存在するか検証
            # --------------------------------------------------------------------------
            if not self.validate_target_in_fsf(linestring_dwh_name, field_set_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            linestring_array = pickle.loads(base64.b64decode(
                field_set_data_frame.loc[field_set_data_frame["Dwh"] == linestring_dwh_name, "Value"].values[0]))

            # --------------------------------------------------------------------------
            # linestring_dwh_nameが正しい形か検証
            # --------------------------------------------------------------------------
            if not self.validate_input_type(linestring_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # マルチパッチの座標配列ではないか検証
            # --------------------------------------------------------------------------
            if not self.validate_coordinates_array(linestring_array, multiflag=False):
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
