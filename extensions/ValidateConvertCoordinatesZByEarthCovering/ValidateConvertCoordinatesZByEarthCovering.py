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

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
from common.error_code_list import ErrorCodeList
import cad.common.cad_utils as CU

pd = import_module("pandas")


class ValidateConvertCoordinatesZByEarthCovering(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """
                ConvertCoordinatesZByEarthCoveringに必要な座標配列と
                フィールドを持っているか検証。
            """
        )
        tags = ["validate", "python"]

    # 土被りのDWH名
    EARTH_COVERING_DWH_NAME = PropertyDescriptor(
        name="Earth Covering DWH Name",
        description="土被りのDWH名を入力",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    # 幅のDWH名
    WIDTH_DWH_NAME = PropertyDescriptor(
        name="Width DWH Name",
        description="幅のDWH名を入力。",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [WIDTH_DWH_NAME,
                            EARTH_COVERING_DWH_NAME]

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
            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # データの空検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式検証
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            # 幅のDWHファイル名
            width_dwh_name = context.getProperty(
                self.WIDTH_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 土被りのDWHファイル名
            earth_covering_dwh_name = context.getProperty(
                self.EARTH_COVERING_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、パスとして認識させる
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換
            field_set_file_data_frame = pd.read_csv(field_set_file_stringio)

            try:
                # flowfileから"crs"属性の内容を取得する
                crs = flowfile.getAttribute("crs")
            except Exception:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00039)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # DataFrameからValue列、Dwh列をリスト化。
            value_list = field_set_file_data_frame["Value"].tolist()
            dwh_list = field_set_file_data_frame["Dwh"].tolist()

            # 1行の場合の処理
            if len(dwh_list) == 1:

                field_set_file_dataframe, \
                    coordinates_dwh_name, \
                    geometry_type, \
                    coordinates_array\
                    = PBP.get_dataframe_and_value_from_field_set_file(flowfile)

                # 座標配列か検証
                if not self.validate_input_type(coordinates_array, "coordinates_array"):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                # マルチパッチではない座標配列か検証
                if not self.validate_coordinates_array(coordinates_array, multiflag=False):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                earth_covering_list = []
                width_list = []

            # 複数行の場合の処理
            else:
                # リストからプロパティで入力した、幅のDWH名のインデックスを抽出
                earth_covering_dwh_name_index = None
                if earth_covering_dwh_name != "" and earth_covering_dwh_name is not None:

                    # FieldSetFile内に存在するか検証
                    if not self.validate_target_in_fsf(earth_covering_dwh_name, field_set_file_data_frame):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                    earth_covering_dwh_name_index = dwh_list.index(
                        earth_covering_dwh_name)
                    earth_covering_list = pickle.loads(base64.b64decode(
                        value_list[earth_covering_dwh_name_index]))

                    # 属性型になっているのか検証
                    if not self.validate_input_type(earth_covering_list, "fields"):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                    # 土被りの値が数字か検証
                    if not self.validate_fields_type_is_num(earth_covering_dwh_name, earth_covering_list):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                else:
                    earth_covering_list = []

                # リストからプロパティで入力した、土被りのDWH名のインデックスを抽出
                width_dwh_name_index = None
                if width_dwh_name != "" and width_dwh_name is not None:

                    # FieldSetFile内に存在するか検証
                    if not self.validate_target_in_fsf(width_dwh_name, field_set_file_data_frame):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                    width_dwh_name_index = dwh_list.index(width_dwh_name)
                    width_list = pickle.loads(base64.b64decode(
                        value_list[width_dwh_name_index]))

                    # 属性型になっているのか検証
                    if not self.validate_input_type(width_list, "fields"):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                    # 幅の値が数字か検証
                    if not self.validate_fields_type_is_num(width_dwh_name_index, width_list):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                else:
                    width_list = []

                # dwh_listの中には幅,深さ,座標の3つの要素が含まれている。値が同じものが含まれている場合は要素数が2つになってしまうので矯正が必要。
                # 幅,深さのDWH名はプロパティで指定したが、geometry(座標の値)は指定されていない。そのためここでgeometryのインデックスを取得。
                geometry_index = None
                for index in range(len(dwh_list)):
                    if index != width_dwh_name_index and index != earth_covering_dwh_name_index:
                        geometry_index = index

                # Value_listの各要素を該当のインデックスを用いて行を指定し、
                # デコード、デシリアライズし、listまたはarrayに変換する。
                coordinates_array = pickle.loads(
                    base64.b64decode(value_list[geometry_index]))

                # 座標配列か検証
                if not self.validate_input_type(coordinates_array, "coordinates_array"):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                # マルチパッチの座標配列になってないか検証
                if not self.validate_coordinates_array(coordinates_array, multiflag=False):
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
