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

pd = import_module("pandas")


class ValidateSubtractDepthFromZValue(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """
                SubtractDepthFromZValueで必要な3D座標配列を持っているか、
                必要なフィールドが存在するか検証。
            """
        )
        tags = ["Validate", "Python"]

    # 座標DWH名
    COORDINATES_NAME = PropertyDescriptor(
        name="Coordinates DWH Name",
        description="座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 始点から減算する値を持つDWH名
    START_DEPTH = PropertyDescriptor(
        name="Start Depth DWH Name",
        description="始点から減算する値を持つDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 終点から減算する値を持つDWH名
    END_DEPTH = PropertyDescriptor(
        name="End Depth DWH Name",
        description="終点から減算する値を持つDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [COORDINATES_NAME,
                            START_DEPTH,
                            END_DEPTH]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            coordinates_name: 座標配列のdwhファイル名
            start_depth: 始点から減算する値を持つDWH名
            end_depth: 終点から減算する値を持つDWH名
        """
        coordinates_name\
            = context.getProperty(self.COORDINATES_NAME).evaluateAttributeExpressions(flowfile).getValue()

        start_depth\
            = context.getProperty(self.START_DEPTH).evaluateAttributeExpressions(flowfile).getValue()

        end_depth\
            = context.getProperty(self.END_DEPTH).evaluateAttributeExpressions(flowfile).getValue()
        return coordinates_name, start_depth, end_depth

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

            # --------------------------------------------------------------------------
            # FieldSetFileの形式検証
            # --------------------------------------------------------------------------
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            coordinates_name, start_depth, end_depth, \
                = self.get_property(context, flowfile)

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、パスとして認識させる
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換
            field_set_file_data_frame = pd.read_csv(field_set_file_stringio)

            # --------------------------------------------------------------------------
            # coordinates_nameがFieldSetFileに存在するか検証
            # --------------------------------------------------------------------------
            if not self.validate_target_in_fsf(coordinates_name, field_set_file_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 座標配列を取得
            coordinates_array = pickle.loads(base64.b64decode(
                field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == coordinates_name, "Value"].values[0]))

            # --------------------------------------------------------------------------
            # 座標配列になっているのか検証
            # --------------------------------------------------------------------------
            if not self.validate_input_type(coordinates_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # マルチパッチになっていないか検証
            # --------------------------------------------------------------------------
            if not self.validate_coordinates_array(coordinates_array, multiflag=False):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # start_depthがFieldSetFileに存在するか検証
            # --------------------------------------------------------------------------
            if not self.validate_target_in_fsf(start_depth, field_set_file_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 始点から減算のリストを取得
            start_list = pickle.loads(base64.b64decode(
                field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == start_depth, "Value"].values[0]))

            # --------------------------------------------------------------------------
            # フィードの形か検証
            # --------------------------------------------------------------------------
            if not self.validate_input_type(start_list, "fields"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # フィールド値が数字か検証
            # --------------------------------------------------------------------------
            if not self.validate_fields_type_is_num(start_depth, start_list):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # end_depthがFieldSetFileに存在するか検証
            # --------------------------------------------------------------------------
            if not self.validate_target_in_fsf(end_depth, field_set_file_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # 終点から減算のリストを取得
            # --------------------------------------------------------------------------
            end_list = pickle.loads(base64.b64decode(
                field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == end_depth, "Value"].values[0]))

            # --------------------------------------------------------------------------
            # フィードの形か検証
            # --------------------------------------------------------------------------
            if not self.validate_input_type(end_list, "fields"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # フィールド値が数字か検証
            # --------------------------------------------------------------------------
            if not self.validate_fields_type_is_num(end_depth, end_list):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_gdf_shape(field_set_file_data_frame, data_name="DataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            return self.RESULT_SUCCESS
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
