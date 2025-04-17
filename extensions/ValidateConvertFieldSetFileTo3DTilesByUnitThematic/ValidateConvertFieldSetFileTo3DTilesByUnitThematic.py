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


class ValidateConvertFieldSetFileTo3DTilesByUnitThematic(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """
                ConvertFieldSetFileTo3DTilesByUnitThematicに必要な座標配列と
                フィールドを持っているか、データ定義が存在するか検証。
            """
        )
        tags = ["validate", "pandas", "numpy",
                "coordinates", "json", "gltf", "python"]

    # データ定義ファイルの区切り文字
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # データ定義ファイルの文字コード
    DATA_DEFINITION_ENCODING = PropertyDescriptor(
        name="Data Definition Encoding",
        description="データ定義ファイルの文字コード",
        default_value="shift-jis",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # 入力元CRS 6677(平面直角) 37654(UTM)など想定
    INPUT_CRS = PropertyDescriptor(
        name="Input CRS",
        description="入力元のCRS(epsgコード)",
        default_value="0",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # パラメータ計算用CRS
    # 計算するパラメータはgltfのmatrix（経緯度から計算）と、
    #  tileset.jsonのboundingboxの出力範囲最小最大（経緯度→ラジアンに変換）
    PARAMETER_CRS = PropertyDescriptor(
        name="Parameter CRS",
        description="パラメータ計算用のCRS(epsgコード)",
        default_value="0",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力単位となる図郭のx座標の大きさ
    X_UNIT = PropertyDescriptor(
        name="X UNIT",
        description="x座標の大きさ",
        default_value="0",
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False
    )

    # 出力単位となる図郭のy座標の大きさ
    Y_UNIT = PropertyDescriptor(
        name="Y UNIT",
        description="y座標の大きさ",
        default_value="0",
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False
    )

    # 内包、交差判定用座標取得用流通項目名
    # データ定義書の流通項目名列を検索し判定用座標とそのジオメトリタイプを取得する
    # 判定用座標のジオメトリタイプはPointかLineStringとする。
    # それぞれ以下の種類の地物を想定している
    # Point：MHのThematicShape(代表点)、空間IDの中心座標
    # LineString：管路のThematicShape（中心線）
    JUDGE_COORDINATES_DISTRIBUTION_NAME = PropertyDescriptor(
        name="JUDGE COORDINATES DISTRIBUTION NAME",
        description="図郭の内包判定用座標取得用流通項目名",
        default_value="thematic",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            DATA_DEFINITION_ENCODING,
                            INPUT_CRS,
                            PARAMETER_CRS,
                            X_UNIT,
                            Y_UNIT,
                            JUDGE_COORDINATES_DISTRIBUTION_NAME]

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

            # データ定義の区切り文字を取得
            data_definition_delimiter = context.getProperty(
                self.DATA_DEFINITION_DELIMITER).evaluateAttributeExpressions(flowfile).getValue()

            # データの空検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式検証
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            try:
                # 入力元CRSをプロパティから取得
                input_crs = int(context.getProperty(self.INPUT_CRS).getValue())
            except Exception:
                args = {"error_code": ErrorCodeList.ED00025,
                        "対象プロパティ": "Input CRS"}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # input_crsが正しいか検証
            if not self.check_epsg(input_crs):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            try:
                # パラメータ計算用CRSをプロパティから取得
                parameter_crs = int(context.getProperty(
                    self.PARAMETER_CRS).getValue())
            except Exception:
                args = {"error_code": ErrorCodeList.ED00025,
                        "対象プロパティ": "Parameter CRS"}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # parameter_crsが正しいか検証
            if not self.check_epsg(parameter_crs):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            try:
                # x座標の単位をプロパティから取得
                x_unit = float(context.getProperty(self.X_UNIT).getValue())
            except Exception:
                args = {"error_code": ErrorCodeList.ED00026,
                        "対象プロパティ": "X UNIT"}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if x_unit > 2000:
                self.logger.warn("プロパティ:X UNIT の値が2000以上の場合、精度が低くなります。")

            try:
                # y座標の単位をプロパティから取得
                y_unit = float(context.getProperty(self.Y_UNIT).getValue())
            except Exception:
                args = {"error_code": ErrorCodeList.ED00026,
                        "対象プロパティ": "Y UNIT"}
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if y_unit > 2000:
                self.logger.warn("プロパティ:X UNIT の値が2000以上の場合、精度が低くなります。")

            # 内包、交差判定用座標取得用流通項目名
            judge_coordinates_distribution_name\
                = context.getProperty(self.JUDGE_COORDINATES_DISTRIBUTION_NAME).getValue()

            attribute_dict = flowfile.getAttributes()
            # 値を追加するためのリスト
            data_definition_list = []

            # 辞書のキーに指定の文字が含まれているか確認
            for key, value in attribute_dict.items():
                if "DataDefinition" in key:
                    if value:
                        data_definition_list.append(value)

                    else:
                        pass

            if not data_definition_list:
                # データ定義が存在しない場合にエラー
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00024)
                return self.RESULT_FAILURE

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、パスとして認識させる
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換
            field_set_data_frame = pd.read_csv(field_set_file_stringio)

            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = NSP.get_value_dwh_list_from_field_set_file_dataframe(field_set_data_frame)

            for data_definition in data_definition_list:
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

                if not self.validate_data_definition_in_fsf(data_definition,
                                                            data_definition_delimiter,
                                                            field_set_data_frame):
                    return self.RESULT_FAILURE

                geometry_type_list, \
                    geometry_dwh_file_name_list, \
                    geometry_distribution_name_list, \
                    attribute_file_type_list, \
                    attribute_dwh_file_name_list, \
                    attribute_distribution_name_list, \
                    attribute_const_value_list, \
                    attribute_data_type_list\
                    = NSP.get_data_definition_index_datatype(io.StringIO(data_definition),
                                                             data_definition_delimiter=data_definition_delimiter)

                # glTFにするマルチパッチファイルのDWH名取得（判定用）
                target_index_list = []
                judge_index_list = []

                for i in range(len(geometry_distribution_name_list)):

                    if geometry_distribution_name_list[i] == judge_coordinates_distribution_name:
                        judge_index_list.append(i)
                    else:
                        target_index_list.append(i)

                # 両者とも1件ずつのみなら処理続行
                if (len(target_index_list) == 1) and (len(judge_index_list) == 1):
                    pass
                # それ以外の場合はエラー
                else:
                    self.validate_logger.write_log(
                        error_code=ErrorCodeList.ED00056)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                try:
                    coordinates_array, \
                        coordinates_id_array, \
                        coordinates_dict\
                        = NSP.create_coordinates_id_array_and_dict_from_coordinates_array(field_set_data_frame,
                                                                                          [geometry_dwh_file_name_list[target_index_list[0]]])
                except Exception:
                    args = {"error_code": ErrorCodeList.ED00013,
                            "target_dwh": [geometry_dwh_file_name_list[target_index_list[0]]]}
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                if not self.validate_input_type(coordinates_array, "coordinates_array"):
                    return False

                try:
                    judge_coordinates_array\
                        = NSP.get_value_field_from_value_dwh_list(target_value_list,
                                                                  target_dwh_list,
                                                                  geometry_dwh_file_name_list[judge_index_list[0]])
                except Exception:
                    args = {"error_code": ErrorCodeList.ED00013,
                            "target_dwh": geometry_dwh_file_name_list[judge_index_list[0]]}
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                if not self.validate_input_type(judge_coordinates_array, "coordinates_array"):
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
