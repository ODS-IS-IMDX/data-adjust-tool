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
import cad.common.cad_utils as CU
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
import base64
import pickle
import io

# 外部モジュール
from importlib import import_module

pd = import_module("pandas")

# Nifiライブラリ


class ValidateConvertCoordinatesVectorAffine(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """ConvertCoordinatesVectorAffineに必要な座標配列があるか、GCPから作成された座標配列が反時計回りで作成されないか検証。"""
        )
        tags = ["validate", "python"]

    # target_arrayのDWH名
    TARGET_DWH_NAME = PropertyDescriptor(
        name="Target DWH Name",
        description="入力データの補正対象点の座標配列のDWH名",
        default_value='target',
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # Base_arrayのDWH名
    BASE_DWH_NAME = PropertyDescriptor(
        name="Base DWH Name",
        description="入力データの位置基準点の座標配列のDWH名",
        default_value='Base',
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )
    property_descriptors = [TARGET_DWH_NAME,
                            BASE_DWH_NAME]

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

            # 補正対象点配列のDWH名を取得
            target_dwh_name = context.getProperty(
                self.TARGET_DWH_NAME).getValue()

            # 位置基準点配列のDWH名を取得
            base_dwh_name = context.getProperty(self.BASE_DWH_NAME).getValue()

            # flowfileから、CSV形式のfield_set_fileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            input_field_set_dataframe = pd.read_csv(
                io.StringIO(input_field_set_file))

            # Dwh列とValue列のリストを抽出
            dwh_list = input_field_set_dataframe["Dwh"].tolist()
            value_list = input_field_set_dataframe["Value"].tolist()

            if not self.validate_target_in_fsf(target_dwh_name, input_field_set_dataframe):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_target_in_fsf(base_dwh_name, input_field_set_dataframe):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # dwh_listで各要素とそのインデックスを抜き出し、どれが補正対象点、位置基準点、補正対象なのかを判別し、Value列の値をデコードデシリアライズしarrayに変換する。
            for index, dwh in enumerate(dwh_list):
                decoded_value = pickle.loads(
                    base64.b64decode(value_list[index]))

                if target_dwh_name in dwh:
                    correction_target_gcp_array = decoded_value
                    if not self.validate_input_type(correction_target_gcp_array, "coordinates_array"):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                elif base_dwh_name in dwh:
                    position_standard_gcp_array = decoded_value
                    if not self.validate_input_type(position_standard_gcp_array, "coordinates_array"):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                else:
                    correction_target_feature_array = decoded_value
                    if not self.validate_input_type(correction_target_feature_array, "coordinates_array"):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

            # GCPが反時計回りか検証
            if not self.validate_correct_GCP(position_standard_gcp_array,
                                             correction_target_gcp_array):
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
