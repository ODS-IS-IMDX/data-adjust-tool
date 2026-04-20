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

# Python標準ライブラリ
import io
import pickle
import base64

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


class ValidateConvertFieldSetFileToGeoDataFrame(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "2.0.0"
        description = (
            """
                ConvertFieldSetFileToGeoDataFrameに必要な座標配列と
                フィールドを持っているか、データ定義が存在するか検証。
            """
        )
        tags = ["validate", "python"]

    # データ定義ファイルの区切り文字
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    POLYGON_INTERIOR_DISTRIBUTION_NAME = PropertyDescriptor(
        name="Polygon Interior Distribution Name",
        description="ポリゴンの内周座標配列の流通項目名",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=False,
        sensitive=False
    )

    POLYGON_INTERIOR_INDEX_DISTRIBUTION_NAME = PropertyDescriptor(
        name="Polygon Interior Index Distribution Name",
        description="ポリゴンの内周Index配列の流通項目名",
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=False,
        sensitive=False
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            POLYGON_INTERIOR_DISTRIBUTION_NAME,
                            POLYGON_INTERIOR_INDEX_DISTRIBUTION_NAME]

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

            # データ定義ファイルの区切り文字
            data_definition_delimiter = context.getProperty(
                self.DATA_DEFINITION_DELIMITER).evaluateAttributeExpressions(flowfile).getValue()

            # 内周座標配列取得用流通項目名
            polygon_interior_distribution_name = context.getProperty(
                self.POLYGON_INTERIOR_DISTRIBUTION_NAME).getValue()

            # 内周index配列取得用流通項目名
            polygon_interior_index_distribution_name = context.getProperty(
                self.POLYGON_INTERIOR_INDEX_DISTRIBUTION_NAME).getValue()

            # データの空検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式検証
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            try:
                # flowfileから"DataDefinition"Attributeの内容を取得する
                data_definition = flowfile.getAttribute("DataDefinition")
            except Exception:
                self.validate_logger.write_log(error_code=ErrorCodeList.ED00024)

                # 取得できない場合はエラーを出力
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

                else:
                    pass

            # data_definitionがNoneTypeか検証
            if data_definition:
                pass
            else:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00024)

                #data_definitionがNoneTypeの場合はエラーを出力
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

                else:
                    pass

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

            else:
                pass

            geometry_type_list, \
                geometry_dwh_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list, \
                = NSP.get_data_definition_index(io.StringIO(data_definition),
                                                data_definition_delimiter=data_definition_delimiter
                                                )

            # 座標配列が1つの場合は、今まで通りのvalidate
            if len(geometry_dwh_file_name_list) == 1:
                if not self.validate_data_definition_in_geometry_fsf(data_definition,
                                                                    data_definition_delimiter,
                                                                    field_set_data_frame,
                                                                    False,
                                                                    False):
                    return self.RESULT_FAILURE

                else:
                    pass

            # ジオメトリが複数入荷した場合は、新しい処理のため、新validate
            # 20250612横田追加分
            elif len(geometry_distribution_name_list) == 3:

                # fields型の検証
                # -2の場合、属性値が入力されているか
                # -1の場合Field型でFieldSetFile内に入力されているか
                if not self.validate_field_list_from_data_definition_list(all_dwh_file_name_list,
                                                                          attribute_file_type_list,
                                                                          attribute_const_value_list,
                                                                          dwh_file_name_list,
                                                                          field_set_data_frame):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

                    else:
                        pass

                # 検証成功の場合はpass
                else:
                    pass

                # 3行ジオメトリが入荷される想定(データ定義の要素)だが、プロパティに内周の流通項目名がない場合エラーを出力
                if polygon_interior_distribution_name in ("", None) or polygon_interior_index_distribution_name in ("", None):
                    args = {"error_code": ErrorCodeList.ED00086,
                            "必須プロパティ": "[Polygon Interior Index Distribution Name], [Polygon Interior Distribution Name]"}
                    self.validate_logger.write_log(**args)
                    return self.RESULT_FAILURE

                else:

                    # プロパティで入力する内周の流通項目名が、重複している場合はエラーを出力
                    if polygon_interior_distribution_name == polygon_interior_index_distribution_name:
                        args = {"error_code": ErrorCodeList.ED00083,
                                "重複していたプロパティ": "[Polygon Interior Index Distribution Name], [Polygon Interior Distribution Name]"}
                        self.validate_logger.write_log(**args)

                        return self.RESULT_FAILURE

                    # 重複していなかった場合は後続の検証
                    else:
                        # 流通項目名列にプロパティで指定した値がなければ、エラーを出力
                        verification_distribution_list = [polygon_interior_distribution_name,
                                                          polygon_interior_index_distribution_name]

                        # データ定義の流通項目名と、一致しなかったものをまとめる
                        missing_distribution_names = [distribution_name for distribution_name in verification_distribution_list if distribution_name not in geometry_distribution_name_list]

                        # 存在しない流通項目名があった場合は、エラーを出力
                        if missing_distribution_names:
                            args = {"error_code": ErrorCodeList.ED00084,
                                    "データ定義の列名": "流通項目名",
                                    "データ定義に存在しない値": missing_distribution_names}
                            self.validate_logger.write_log(**args)
                            return self.RESULT_FAILURE

                        else:
                            pass

                # ジオメトリの検証。
                # ファイルタイプが+が複数入荷する特殊型なので、1つずつ検証
                for i in range(len(geometry_dwh_file_name_list)):

                    # FieldSetFile内に存在するか検証
                    if not self.validate_dwh_in_field_set_data_frame(field_set_data_frame, geometry_dwh_file_name_list[i]):

                        # 存在しない場合はエラーを出力
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                        else:
                            pass

                    # FieldSetFile内に存在する場合はデコード
                    else:
                        temp_array = pickle.loads(base64.b64decode(
                            field_set_data_frame.loc[field_set_data_frame['Dwh'] == geometry_dwh_file_name_list[i], 'Value'].values[0]))

                        # numpy配列か検証
                        if not self.validate_ndarray(temp_array, geometry_dwh_file_name_list[i]):

                            # numpy配列ではない場合はエラーを出力
                            result = False
                            if self.mode_value == self.MODE_STOP:
                                return self.RESULT_FAILURE

                            else:
                                pass

                        # numpy配列であれば次の検証
                        else:
                            pass

                        # index配列か
                        if geometry_distribution_name_list[i] == polygon_interior_index_distribution_name:

                            # index配列の場合は、１次元配列であるか検証
                            if not self.validate_array_ndim(temp_array, array_ndim=1, dwh=geometry_dwh_file_name_list[i]):

                                # 1次元配列でない場合は、エラーを出力
                                result = False
                                if self.mode_value == self.MODE_STOP:
                                    return self.RESULT_FAILURE

                            # 内周index配列を要素数検証で使う為、変数に切り出し
                            else:
                                index_array = temp_array.copy()

                        # 内周座標配列と外周座標配列は二次元配列かどうか検証
                        else:
                            if not self.validate_array_ndim(temp_array, array_ndim=2, dwh=geometry_dwh_file_name_list[i]):

                                # 2次元配列ではない場合は、エラーを出力
                                result = False
                                if self.mode_value == self.MODE_STOP:
                                    return self.RESULT_FAILURE

                            # 内周座標配列を要素数検証で使う為、変数に切り出し
                            if geometry_distribution_name_list[i] == polygon_interior_distribution_name:
                                inner_array = temp_array.copy()

                            else:
                                # 外周配列はpass
                                pass

                # for分を出たら、内周のlenを検証
                if len(index_array) == len(inner_array):
                    pass

                # 内周座標配列と、内周index配列の要素数が一致しない場合はエラーを出力
                else:
                    args = {"error_code": ErrorCodeList.ED00085,
                            "対象DWH名": verification_distribution_list}
                    self.validate_logger.write_log(**args)
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE
                    else:
                        pass

            # +の値が1でも3でもない場合は、エラーを出力
            else:
                args = {"error_code": ErrorCodeList.ED00082,
                        "期待する行数":"1行 or 3行",
                        "入力された行数": f"{len(geometry_dwh_file_name_list)}行"}

                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

                else:
                    pass

            # 統計情報を出力
            if not self.validate_gdf_shape(field_set_data_frame, data_name="DataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS

            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
