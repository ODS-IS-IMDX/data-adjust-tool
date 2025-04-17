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
LineString = getattr(import_module("shapely.geometry"), "LineString")

from common.error_code_list import ErrorCodeList
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators


class ValidateIntegrateLineString(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「IntegrateLineString」の入力データのチェックを行う'''
        tags = [
            "fieldsetfile",
            "dataframe",
            'python',
            'validate'
        ]

    #:
    INPUT_GEOMETRY_NAME = PropertyDescriptor(
        name="Input Geometry Name",
        description="Geometry Name for Input",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

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

    property_descriptors = [INPUT_GEOMETRY_NAME, FEATURE_ID_COLUMN_NAME]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_duplicates_fid_data(self, row):
        """
        地物ID情報に重複がないかをチェックする
        
        :param row: DataFrameの1レコード分の情報
        :type row: Pandas.Series

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        no_duplicates = False  # 重複チェックフラグ

        # 両辺の値についてそれぞれ重複チェックを行う
        if self.validate_data_uniqueness(row["Value"], 0)[0] and self.validate_data_uniqueness(row["Value"], 1)[0]:
            no_duplicates = True
        else:
            args = {
                "error_code": ErrorCodeList.EC00013,
                "対象": row["Dwh"]
            }
            self.validate_logger.write_log(**args)

        return no_duplicates

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
            geom_name = context.getProperty(self.INPUT_GEOMETRY_NAME).getValue()
            fid_col_name = context.getProperty(self.FEATURE_ID_COLUMN_NAME).getValue()

            # インプットデータ取得
            fsf = flowfile.getContentsAsBytes()

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(fsf)
            if not result:
                return self.RESULT_FAILURE

            # geometryとFIDが含まれているかチェック
            if not self.validate_exist_attribute(df, ["geometry", "FID"]):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # Dwh名が{geom_name}/FIDの存在チェック
            if not any(df["Dwh"] == f"{geom_name}/FID"):
                args = {
                    "error_code": ErrorCodeList.EC00009,
                    "列名称": f"{geom_name}/FID"
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 地物IDの重複チェックを行う
            for i, row in df.iterrows():
                if row["Dwh"].split("/")[1] != fid_col_name:
                    # 対象レコードが地物ID情報以外の場合はコンティニュー
                    continue
                # 地物ID情報の重複チェックを行う
                if not self.validate_duplicates_fid_data(row):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

            # Dwh名が{geom_name}/geometryのValueを取得
            if any(df["Dwh"] == f"{geom_name}/geometry"):
                # geometryのValueがLinestringかどうかチェック
                geometry_value_list = df.loc[df["Dwh"] == f"{geom_name}/geometry", "Value"].values[0]
                if not all(self.validate_data_types(value, LineString) for value in geometry_value_list):
                    return self.RESULT_FAILURE
            else:
                args = {
                    "error_code": ErrorCodeList.EC00009,
                    "列名称": f"{geom_name}/geometry"
                }
                self.validate_logger.write_log(**args)
                return self.RESULT_FAILURE

            # ValidateLogを生成
            if not self.validate_log_of_fsf(fsf, df):
                return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
