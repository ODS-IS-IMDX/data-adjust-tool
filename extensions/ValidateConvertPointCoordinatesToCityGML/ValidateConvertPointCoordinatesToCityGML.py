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

# Python標準ライブラリ
import base64
import pickle
import io

# 外部モジュール
from importlib import import_module

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
from common.error_code_list import ErrorCodeList
import cad.common.cad_utils as CU

pd = import_module("pandas")
np = import_module("numpy")


class ValidateConvertPointCoordinatesToCityGML(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """
                ConvertPointCoordinatesToCityGMLに必要なマルチパッチ座標配列と
                フィールドを持っているか、データ定義が存在するか検証。
            """
        )
        tags = ["validate", "python"]

    # データ定義ファイルの区切り文字
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True
    )

    # マルチパッチ前の配列のDWH名
    CENTER_DWH_NAME = PropertyDescriptor(
        name="Center DWH Name",
        description="マルチパッチ前の配列名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # gml_idのDWH名
    GML_ID_DWH_NAME = PropertyDescriptor(
        name="gml_id DWH Name",
        description="入力データのgml_idのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 地図情報2500レベルの図郭コードを設定
    LEVEL2500_UNIT_CODE_STRING = PropertyDescriptor(
        name="Level2500 Unit Code String",
        description="地図情報2500レベルの図郭コード",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="${unit_code}",
    )

    # 出力時のCRS（座標参照系）を設定
    OUTPUT_TARGET_CRS_STRING = PropertyDescriptor(
        name="Output Target CRS String",
        description="出力データの座標のCRS(epsgコード)",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="${crs}",
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            CENTER_DWH_NAME,
                            GML_ID_DWH_NAME,
                            LEVEL2500_UNIT_CODE_STRING,
                            OUTPUT_TARGET_CRS_STRING]

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

            # 幅のDWH名
            data_definition_delimiter = context.getProperty(
                self.DATA_DEFINITION_DELIMITER).evaluateAttributeExpressions(flowfile).getValue()

            # 中心線のDWH名
            center_dwh_name = context.getProperty(
                self.CENTER_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # uuidのDWH名
            uuid_dwh_name = context.getProperty(
                self.GML_ID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 出力対象レベル2500国土基本図図郭コード文字列郡
            level2500_unit_code_string = context.getProperty(
                self.LEVEL2500_UNIT_CODE_STRING).evaluateAttributeExpressions(flowfile).getValue()

            # 出力対象CRS
            output_target_crs_string = context.getProperty(
                self.OUTPUT_TARGET_CRS_STRING).evaluateAttributeExpressions(flowfile).getValue()

            # データの空検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式検証
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            try:
                # flowfileから"DataDefinition"属性の内容を取得する
                data_definition = flowfile.getAttribute("DataDefinition")
            except Exception:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00024)
                return self.RESULT_FAILURE

            # data_definitionがNoneTypeの場合はエラー
            if data_definition:
                pass
            else:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00024)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、パスとして認識させる
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換
            field_set_data_frame = pd.read_csv(field_set_file_stringio)

            # 区切り文字が正しいか検証
            if not self.validate_data_definition_delimiter(data_definition,
                                                           data_definition_delimiter):
                return self.RESULT_FAILURE

            if data_definition_delimiter == DDC.DELIMITER_TAB:
                data_definition_data_frame = pd.read_csv(io.StringIO(data_definition),
                                                         sep='\t')

            else:
                data_definition_data_frame = pd.read_csv(
                    io.StringIO(data_definition))

            if not self.validate_data_definition_column(data_definition_data_frame,
                                                        True):
                return self.RESULT_FAILURE

            if not self.validate_data_definition_in_geometry_fsf(data_definition,
                                                                 data_definition_delimiter,
                                                                 field_set_data_frame,
                                                                 data_type_flag=True,
                                                                 multipatch_flag=True):
                return self.RESULT_FAILURE

            # center_dwh_nameがFieldSetFileに存在するか検証
            if not self.validate_target_in_fsf(center_dwh_name, field_set_data_frame):
                return self.RESULT_FAILURE

            # center_dwh_nameが正しい形か検証
            linestring_array = pickle.loads(base64.b64decode(
                field_set_data_frame.loc[field_set_data_frame["Dwh"] == center_dwh_name, "Value"].values[0]))
            if not self.validate_input_type(linestring_array, "coordinates_array"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # マルチパッチの座標配列ではないか検証
            if not self.validate_coordinates_array(linestring_array, multiflag=False):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            linestring_id_array = np.unique(linestring_array[:, 0])
            linestring_dict = {linestring_id_array[i]: linestring_array[list(np.where(
                linestring_array[:, 0] == linestring_id_array[i])[0]), 1:4] for i in range(len(linestring_id_array))}

            # uuid_dwh_nameが正しい形か検証
            if not self.validate_target_in_fsf(uuid_dwh_name, field_set_data_frame):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # uuid_listを取得
            uuid_list = pickle.loads(base64.b64decode(
                field_set_data_frame.loc[field_set_data_frame["Dwh"] == uuid_dwh_name, "Value"].values[0]))

            # uuid_listが属性の形になっているのか検証
            if not self.validate_input_type(uuid_list, "fields"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            target_unit_code_list \
                = level2500_unit_code_string.split(DDC.UNIT_CODE_DELIMITER)

            try:
                target_plateau_crs = DDC.PLATEAU_CRS_DICT[output_target_crs_string]

            except KeyError:
                args = {"error_code": ErrorCodeList.ED00025,
                        "対象CRS": list(DDC.PLATEAU_CRS_DICT.keys()),
                        "入力されたCRS": output_target_crs_string}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            unit_flag = False

            # 出力対象図郭ごとに処理
            for target_unit_code_list_index in range(len(target_unit_code_list)):

                # -----------------------------------------------------------------------------------------------------------
                # 【取得】出力対象図郭情報取得
                # -----------------------------------------------------------------------------------------------------------
                try:
                    unit_origin_array, level_mesh_array \
                        = NSP.get_unit_origin(target_unit_code_list[target_unit_code_list_index])
                except Exception:
                    args = {"error_code": ErrorCodeList.ED00065,
                            "入力された図郭コード": target_unit_code_list[target_unit_code_list_index]}
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                # -----------------------------------------------------------------------------------------------------------
                # 【抽出】ジオメトリ指定図郭内に存在するか検証
                # -----------------------------------------------------------------------------------------------------------
                try:
                    feature_bool = [NSP.judge_citygml(
                        linestring_dict[linestring_id_array[i]][:, :2], unit_origin_array, level_mesh_array) for i in range(len(linestring_id_array))]
                except Exception:
                    self.validate_logger.write_log(ErrorCodeList.ED00066)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                # 出力対象がなければ次へ
                if np.any(feature_bool) == True:
                    unit_flag = True
                    pass
                else:
                    continue

            if unit_flag:
                pass
            else:
                self.validate_logger.write_log(ErrorCodeList.ED00065)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            dataframe = CU.field_set_file_to_dataframe(input_data)

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
