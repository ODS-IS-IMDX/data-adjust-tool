# MIT License
# 
# Copyright (c) 2026 NTT InfraNet
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
        version = "2.0.0"
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

    # 内周の座標配列のDWH名
    INPUT_POLYGON_INTERIOR_DWH_NAME = PropertyDescriptor(
        name="Input Polygon Interior DWH Name",
        description="内周の座標配列のDWH名(内周が存在しない場合は未入力)",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )

    # 内周のindex配列のDWH名
    INPUT_POLYGON_INTERIOR_INDEX_DWH_NAME = PropertyDescriptor(
        name="Input Polygon Interior Index DWH Name",
        description="内周のindex配列のDWH名(内周が存在しない場合は未入力)",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )

    property_descriptors = [GEOMETRY_TYPE,
                            INPUT_POLYGON_INTERIOR_DWH_NAME,
                            INPUT_POLYGON_INTERIOR_INDEX_DWH_NAME]

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

            input_polygon_interior_dwh_name = context.getProperty(
                self.INPUT_POLYGON_INTERIOR_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            input_polygon_interior_index_dwh_name = context.getProperty(
                self.INPUT_POLYGON_INTERIOR_INDEX_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

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

            # プロパティで入力した、内周座標配列のDWH、内周index配列のDWHがない場合は、内周がないものとして扱う
            if input_polygon_interior_dwh_name in (None, "") or input_polygon_interior_index_dwh_name in (None, ""):

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

            # 20250610_横田追加分
            else:
                field_set_dwh_list = set(field_set_file_dataframe["Dwh"])

                # 内周が入力される場合は、絶対に3行必要な為、3行ではない場合はエラーを出力し中断
                if len(field_set_dwh_list) != 3:
                    args = {"error_code":ErrorCodeList.ED00080,
                            "必要な行数": "3",
                            "入力された行数": str(len(field_set_dwh_list))}
                    self.validate_logger.write_log(**args)
                    return False

                # プロパティで指定したDWHをlistに格納し、FieldSetFileのDWH列にあるか検証
                verification_dwh_list = [input_polygon_interior_dwh_name,
                                         input_polygon_interior_index_dwh_name]
                
                # FieldSetFileのDWH列に存在しなかったDWHをlistに格納
                missing_dwh_names = [dwh_name for dwh_name in verification_dwh_list if dwh_name not in field_set_dwh_list]

                # 存在しないDWHがあった場合は、エラーを出力し、中断
                if missing_dwh_names:
                    args = {"error_code": ErrorCodeList.ED00013,
                            "存在しないDWH名": missing_dwh_names}
                    self.validate_logger.write_log(**args)
                    return False

                # 存在する場合は、後続の検証を実行
                else:
                    pass

                # FieldSetFileを1行ずつ検証
                for _, row in field_set_file_dataframe.iterrows():

                    # 内周の座標配列を取得する
                    if row["Dwh"] == input_polygon_interior_dwh_name:
                        polygon_interior_coordinates_array = pickle.loads(base64.b64decode(row["Value"]))

                        # 内周座標配列が配列か検証する
                        if not self.validate_ndarray(polygon_interior_coordinates_array, input_polygon_interior_dwh_name):
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

                        # 内周座標配列が、2次元配列且つ、2次元目の要素数が3 また 4(id,x,y(z))か検証
                        if not self.validate_2d_or_3d_coordinates_array(polygon_interior_coordinates_array, input_polygon_interior_dwh_name):
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

                    # 内周index配列を取得する
                    elif row["Dwh"] == input_polygon_interior_index_dwh_name:
                        polygon_interior_index_array = pickle.loads(base64.b64decode(row["Value"]))

                        # 内周index配列が配列か検証する
                        if not self.validate_ndarray(polygon_interior_index_array, input_polygon_interior_index_dwh_name):
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

                        # 内周indexが1次元配列化検証
                        if not self.validate_array_ndim(polygon_interior_index_array, input_polygon_interior_index_dwh_name):
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

                    # 指定されていないものを、外周の座標配列とし、取得する
                    else:
                        polygon_exterior_coordinates_array = pickle.loads(base64.b64decode(row["Value"]))
                        
                        if not self.validate_ndarray(polygon_exterior_coordinates_array, row["Value"]):
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

                        # 内周座標配列が、2次元配列且つ、2次元目の要素数が3 また 4(id,x,y(z))か検証
                        if not self.validate_2d_or_3d_coordinates_array(polygon_exterior_coordinates_array, row["Value"]):
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # inputのCSVの行数列数を記録
            # --------------------------------------------------------------------------
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
