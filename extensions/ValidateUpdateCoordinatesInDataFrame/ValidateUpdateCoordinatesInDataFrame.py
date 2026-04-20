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
import io

# 外部モジュール
from importlib import import_module

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor

import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import cad.common.cad_utils as UC
from common.error_code_list import ErrorCodeList

gpd = import_module("geopandas")
pd = import_module("pandas")
np = import_module("numpy")
BaseGeometry = import_module("shapely.geometry.base").BaseGeometry

class ValidateUpdateCoordinatesInDataFrame(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        UpdateCoordinatesInDataFrameで必要な座標配列とGeoDataFrame持っているか、
                        プロパティで指定したGeometryTypeで変換できるか検証。
                      """
        tags = ["validate", "python"]

    # 更新する座標配列のCRSを指定
    INPUT_COORDINATES_CRS = PropertyDescriptor(
        name="Input Coordinates CRS",
        description="更新する座標配列のCRS（未入力の場合はGeoDataFrameのCRSを維持）",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [INPUT_COORDINATES_CRS]

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

            # 更新する座標配列のCRS取得
            input_coordinates_crs\
                = context.getProperty(self.INPUT_COORDINATES_CRS).evaluateAttributeExpressions(flowfile).getValue()

            if not input_coordinates_crs or input_coordinates_crs == "":
                pass
            else:
                # --------------------------------------------------------------------------
                # input_crsが正しいepsgかどうかの検証
                # --------------------------------------------------------------------------
                if not self.check_epsg(input_coordinates_crs):
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
            field_set_file_dataframe = pd.read_csv(field_set_file_stringio)

            # FieldSetFileのDataFrameからValue列、Dwh列のListを取得
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = NSP.get_value_dwh_list_from_field_set_file_dataframe(field_set_file_dataframe)

            # FieldSetFileの1行目をオブジェクトに戻す
            line1_object\
                = NSP.get_value_field_from_value_dwh_list(target_value_list,
                                                          target_dwh_list,
                                                          target_dwh_list[0])

            # FieldSetFileの2行目をオブジェクトに戻す
            line2_object\
                = NSP.get_value_field_from_value_dwh_list(target_value_list,
                                                          target_dwh_list,
                                                          target_dwh_list[1])

            # 1行目が、GeoDataFrameの場合
            if isinstance(line1_object, gpd.GeoDataFrame):

                # 2行目が配列、shapelyオブジェクトか検証
                if isinstance(line2_object, np.ndarray) or (isinstance(line2_object, list) and all(isinstance(obj, BaseGeometry) for obj in line2_object)):

                    # 座標配列の場合は、2d,3dの座標配列か検証
                    if isinstance(line2_object, np.ndarray):
                        # 座標配列か検証
                        if not self.validate_2d_or_3d_coordinates_array(line2_object, target_dwh_list[1]):
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

                    # shapelyは検証なし
                    else:
                        pass

                # 2行目が配列でも、shapelyオブジェクトでもない場合はエラーを出力
                else:
                    result = False
                    args = {"error_code": ErrorCodeList.EC00008,
                            "問題の行のDWH":target_dwh_list[1],
                            "期待するデータ型": "np.array(coordinates), Shapely list(ジオメトリリスト)",
                            "入力されたデータ型": str(type(line2_object))
                            }
                    self.validate_logger.write_log(**args)

                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

            # 2行目がGeoDataFrameの場合
            elif isinstance(line2_object, gpd.GeoDataFrame):

                # 1行目が配列、shapelyオブジェクトか検証
                if isinstance(line1_object, np.ndarray) or (isinstance(line1_object, list) and all(isinstance(obj, BaseGeometry) for obj in line1_object)):

                    # 座標配列の場合は、2d,3dの座標配列か検証
                    if isinstance(line1_object, np.ndarray):
                        # 座標配列が、2d,3dの座標配列
                        if not self.validate_2d_or_3d_coordinates_array(line1_object, target_dwh_list[0]):
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

                    # Shapelyの場合は検証なし
                    else:
                        pass

                # 1行目が配列でも、shapelyオブジェクトでもない場合はエラーを出力
                else:
                    result = False
                    args = {"error_code": ErrorCodeList.EC00008,
                            "問題の行のDWH":target_dwh_list[0],
                            "期待するデータ型": "np.array(coordinates), Shapely list(ジオメトリリスト)",
                            "入力されたデータ型": str(type(line1_object))
                            }
                    self.validate_logger.write_log(**args)

                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

            else:
                # 1行目と2行目の両方がGeoDataFrameでない場合 → エラーを出力
                result = False
                args = {
                    "error_code": ErrorCodeList.EC00008,
                    "問題の行のDWH": f"{target_dwh_list[0]}, {target_dwh_list[1]}",
                    "期待するデータ型": "GeoDataFrame",
                    "入力されたデータ型": f"{type(line1_object)}, {type(line2_object)}"
                }
                self.validate_logger.write_log(**args)

                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if isinstance(line1_object, gpd.GeoDataFrame):

                geodataframe = line1_object.copy()
                coordinates_array_or_geometry_list = line2_object.copy()

            else:
                geodataframe = line2_object.copy()
                coordinates_array_or_geometry_list = line1_object.copy()

            if isinstance(coordinates_array_or_geometry_list, np.ndarray):

                try:
                    # DataFrameからジオメトリタイプ取得
                    _, \
                        geometry_type_list, \
                        _\
                        = NSP.get_coordinates_array_from_geodataframe(geodataframe)

                except Exception:
                    self.validate_logger.write_log(
                        error_code=ErrorCodeList.ED00067)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                try:
                    coordinates_dict\
                        = NSP.get_shapely_dict_from_coordinates_array(coordinates_array_or_geometry_list,  geometry_type_list)

                except Exception:
                    self.validate_logger.write_log(
                        error_code=ErrorCodeList.ED00068)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

            # 20250611_横田追加分
            else:
                if len(coordinates_array_or_geometry_list) == len(geodataframe):
                    pass
                else:
                    args = {"error_code": ErrorCodeList.EC00004,
                            "ジオメトリリストのレコード数": f"{len(coordinates_array_or_geometry_list)}",
                            "GeoDataFrameのレコード数": f"{len(geodataframe)}"}
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

            if not self.validate_gdf_shape(field_set_file_dataframe, data_name="DataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS

            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
