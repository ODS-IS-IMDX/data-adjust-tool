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

pd = import_module("pandas")

# Nifiライブラリ
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
import cad.common.cad_utils as CU
from common.error_code_list import ErrorCodeList

class ValidateConvertCoordinatesToCylindricalMultipatch(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """マルチパッチを作成できるのか属性データか、座標配列が3D座標配列かチェックする。"""
        )
        tags = ["validate", "python"]

    #:
    CIRCLE_RADIUS = PropertyDescriptor(
        name="Circle Radius DWH Name",
        description="入力データの円筒の半径のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
    )

    property_descriptors = [CIRCLE_RADIUS]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_data(self, context, flowfile):
        """
        入力データのチェックを実行する

        :param context: プロセッサの設定値が格納されているデータ
        :param flowfile: プロセッサに渡されるデータ

        :return チェック結果 正常="success"、異常="failure"
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # データの空チェック
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            try:
                # FlowFileから"crs"属性の内容を取得する
                crs = flowfile.getAttribute("crs")
            except Exception:
                self.validate_logger.write_log(error_code=ErrorCodeList.ED00039)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 半径のdwhファイル名
            circle_radius_dwh_name = context.getProperty(self.CIRCLE_RADIUS).evaluateAttributeExpressions(flowfile).getValue()

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、pathとして認識させる。
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換。
            field_set_file_data_frame = pd.read_csv(field_set_file_stringio)

            # DataFrameからValue列、Dwh列をリスト化。
            value_list = field_set_file_data_frame["Value"].tolist()
            dwh_list = field_set_file_data_frame["Dwh"].tolist()

            # circle_radius_dwh_nameがFieldSetFileに存在するかチェック
            if not self.validate_target_in_fsf(circle_radius_dwh_name, field_set_file_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # リストからプロパティで入力した、半径のdwh名のインデックスを抽出。
            circle_radius_dwh_name_index = dwh_list.index(circle_radius_dwh_name)

            # dwh_listの中には半径,座標の2つの要素が含まれている。
            # 半径のdwh名はプロパティで指定したが、geometry(座標の値)は指定されていない。そのためここでgeometryのインデックスを取得。
            geometry_index = None
            for index in range(len(dwh_list)):
                if index != circle_radius_dwh_name_index:
                    geometry_index = index

            # 半径がフィールドかチェック
            radius_list = pickle.loads(base64.b64decode(value_list[circle_radius_dwh_name_index]))
            if not self.validate_input_type(radius_list, "fields"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 半径の値が数字かチェック
            if not self.validate_fields_type_is_num(circle_radius_dwh_name, radius_list):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_field_null(radius_list):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 座標配列が、配列且つ3Dであるかチェック
            coordinates_array = pickle.loads(base64.b64decode(value_list[geometry_index]))
            if not self.validate_input_type(coordinates_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_coordinates_is_3D(coordinates_array):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_line_coordinates(coordinates_array):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            geodataframe = CU.field_set_file_to_geodataframe(input_data, crs)

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
