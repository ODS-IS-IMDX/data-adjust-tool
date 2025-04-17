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
import base64
import pickle
import io

# 外部モジュール
from importlib import import_module

# Nifiライブラリ
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

from common.error_code_list import ErrorCodeList
import cad.common.cad_utils as UC

pd = import_module("pandas")


class ValidateAddPointCoordinatesToLineStringNearestNeighbor(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """AddPointCoordinatesToLineStringNearestNeighborで必要な座標配列を持っているか検証。"""
        )
        tags = ["Validate", "Python"]

    # スナップ対象ポイントのDwh名
    POINT_DWH_NAME = PropertyDescriptor(
        name="Point Dwh Name",
        description="スナップ対象ポイントのDwh名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # スナップ先のラインとなるDwh名
    LINESTRING_DWH_NAME = PropertyDescriptor(
        name="Linestring Dwh Name",
        description="スナップ先のラインとなるDwh名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # ポイントとラインの距離において同一構成点と判定する閾値
    MINIMUM_THRESHOLD = PropertyDescriptor(
        name="Minimum Threshold",
        description="ポイントとラインの距離において同一構成点と判定する閾値",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # ポイントとラインが紐づかないとするときの閾値
    SNAP_MAXIMUM_DISTANCE = PropertyDescriptor(
        name="Snap Maximum Distance",
        description="ポイントとラインが紐づかないとするときの閾値",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [POINT_DWH_NAME,
                            LINESTRING_DWH_NAME,
                            MINIMUM_THRESHOLD,
                            SNAP_MAXIMUM_DISTANCE]

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

            # スナップ対象ポイントのDwh名
            point_dwh_name\
                = context.getProperty(self.POINT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # スナップ先のラインとなるDwh名
            linestring_dwh_name\
                = context.getProperty(self.LINESTRING_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # ポイントとラインの距離において同一構成点と判定する閾値
            minimum_threshold\
                = float(context.getProperty(self.MINIMUM_THRESHOLD).evaluateAttributeExpressions(flowfile).getValue())

            # ポイントとラインが紐づかないとするときの閾値
            snap_maximum_distance\
                = float(context.getProperty(self.SNAP_MAXIMUM_DISTANCE).evaluateAttributeExpressions(flowfile).getValue())

            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # --------------------------------------------------------------------------
            # minimum_thresholdが0以上かどうか検証
            # --------------------------------------------------------------------------
            if minimum_threshold <= 0:
                args = {"error_code": ErrorCodeList.ED00058,
                        "minimum_threshold": minimum_threshold}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # snap_maximum_distanceが0以上かどうか検証
            # --------------------------------------------------------------------------
            if snap_maximum_distance <= 0:
                args = {"error_code": ErrorCodeList.ED00058,
                        "snap_maximum_distance": snap_maximum_distance}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

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
            field_set_file_data_frame = pd.read_csv(field_set_file_stringio)

            # --------------------------------------------------------------------------
            # point_dwh_nameがFieldSetFileに存在するか検証
            # --------------------------------------------------------------------------
            if not self.validate_target_in_fsf(point_dwh_name, field_set_file_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            point_array = pickle.loads(base64.b64decode(
                field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == point_dwh_name, "Value"].values[0]))

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
            if not self.validate_2d_or_3d_coordinates_array(point_array):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # 座標配列がポイントジオメトリに変換できるか検証
            # --------------------------------------------------------------------------
            if not self.validate_point_coordinates(point_array):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # linestring_dwh_nameがFieldSetFileに存在するか検証
            # --------------------------------------------------------------------------
            if not self.validate_target_in_fsf(linestring_dwh_name, field_set_file_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            linestring_array = pickle.loads(base64.b64decode(
                field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == linestring_dwh_name, "Value"].values[0]))

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
            if not self.validate_2d_or_3d_coordinates_array(linestring_array):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # 座標配列がラインジオメトリに変換できるか検証
            # --------------------------------------------------------------------------
            if not self.validate_line_coordinates(linestring_array):
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
