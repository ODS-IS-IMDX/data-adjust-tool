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
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import cad.common.cad_utils as UC
from common.error_code_list import ErrorCodeList

pd = import_module("pandas")


class ValidateConvertCoordinatesToGeometry(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ConvertCoordinatesToGeometryで必要な座標配列を持っているか、
                        プロパティで指定したGeometryTypeで変換できるか検証。
                      """
        tags = ["validate", "python"]

    # 変換後のジオメトリのタイプ
    GEOMETRY_TYPE = PropertyDescriptor(
        name="Geometry Type",
        description="出力データのジオメトリ",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=["Point", "LineString", "Polygon"],
        sensitive=False
    )
    property_descriptors = [GEOMETRY_TYPE]

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

            # 変換後のジオメトリのタイプ取得
            geometry_type = context.getProperty(
                self.GEOMETRY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

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
            field_set_file_dataframe = pd.read_csv(field_set_file_stringio)

            # --------------------------------------------------------------------------
            # target_arrayが正しい形か検証
            # --------------------------------------------------------------------------
            target_array = pickle.loads(base64.b64decode(
                field_set_file_dataframe.loc[0, "Value"]))
            if not self.validate_input_type(target_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # マルチパッチの座標配列ではないか検証
            # --------------------------------------------------------------------------
            if not self.validate_coordinates_array(target_array, multiflag=False):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # IDがキー、構成点がvalueのdictに変更
            # --------------------------------------------------------------------------
            coordinates_dict = NSP.split_geometry_points_dict(
                target_array[:, :4])

            # --------------------------------------------------------------------------
            # プロパティ設定値のGeometryTypeで変換できるか検証
            # --------------------------------------------------------------------------
            try:
                if geometry_type == "Point":
                    geometry = NSP.convert_dict_to_point_geometries(
                        coordinates_dict)

                elif geometry_type == "LineString":
                    geometry = NSP.convert_dict_to_linestring_geometries(
                        coordinates_dict)
                else:
                    geometry = NSP.convert_dict_to_polygon_geometries(
                        coordinates_dict)

            except ValueError:
                args = {"error_code": ErrorCodeList.ED00021,
                        "geometry_type": geometry_type}
                self.validate_logger.write_log(**args)
                return False

            # --------------------------------------------------------------------------
            # プロパティ設定値のGeometryTypeで変換できるか検証
            # --------------------------------------------------------------------------
            if not self.validate_gdf_shape(field_set_file_dataframe, data_name="DataFrame"):
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
