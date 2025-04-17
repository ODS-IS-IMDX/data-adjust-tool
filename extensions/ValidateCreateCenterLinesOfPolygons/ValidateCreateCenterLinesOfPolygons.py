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
from importlib import import_module

# 外部ライブラリの動的インポート
Polygon = getattr(import_module("shapely.geometry"), "Polygon")

from common.error_code_list import ErrorCodeList
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor


class ValidateCreateCenterLinesOfPolygons(CadBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """プロセッサー「CreateCenterLinesOfPolygons」の入力データのチェックを行う"""
        )
        tags = ["GeoDataFrame", "python", "validate"]

    #:
    FEATURE_ID_COLUMN_NAME = PropertyDescriptor(
        name='Feature ID Column Name',
        description='地物IDのカラム名を指定する。',
        default_value='',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [FEATURE_ID_COLUMN_NAME]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_duplicates_fid_data(self, df, fid_col_name):
        """
        地物ID情報に重複がないかをチェックする
        
        :param df: DataFrame
        :type df: pandas.DataFrame

        :param fid_col_name: 地物ID情報のカラム名
        :type fid_col_name: str

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        result = True

        for i, row in df.iterrows():
            if row["Dwh"].split("/")[1] != fid_col_name:
                # 対象レコードが地物ID情報以外の場合はコンティニュー
                continue
            # 地物ID情報の重複チェックを行う
            if not self.validate_data_uniqueness(row["Value"], 0)[0] or not self.validate_data_uniqueness(row["Value"], 1)[0]:
                result = False
                args = {
                    "error_code": ErrorCodeList.EC00013,
                    "対象": row["Dwh"]
                }
                self.validate_logger.write_log(**args)
                if self.mode_value == self.MODE_STOP:
                    return result

        return result

    def validate_data(self, context, flowfile):
        """
        入力データのチェックを実行する

        :param context: プロセッサの設定値が格納されているデータ
        :param flowfile: プロセッサに渡されるデータ

        :return: チェック結果 正常="success"、異常="failure"
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()
            fid_col_name = context.getProperty(self.FEATURE_ID_COLUMN_NAME).getValue()

            # インプットデータの検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            # ジオメトリの存在チェック
            if not self.validate_exist_geometry(df):
                return self.RESULT_FAILURE
            
            # 指定の属性名が存在するかチェック
            if not self.validate_exist_attribute(df, [fid_col_name]):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 地物IDの重複チェックを行う
            if not self.validate_duplicates_fid_data(df, fid_col_name):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # MainGeometryのジオメトリ型チェック
            check_df = df[df["Type"] == "geometry"]
            check_list = []
            for geom in check_df["Value"].values.tolist()[0]:
                check_list.append(self.validate_data_types(geom, Polygon))
            if not all(check_list):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE
            
            # バリデータログ作成
            if not self.validate_log_of_fsf(input_data, df):
                return self.RESULT_FAILUER

            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
