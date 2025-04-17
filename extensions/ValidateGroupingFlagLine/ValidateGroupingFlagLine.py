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
Point = getattr(import_module("shapely.geometry"), "Point")

from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from common.error_code_list import ErrorCodeList
from nifiapi.properties import PropertyDescriptor, StandardValidators, ExpressionLanguageScope


class ValidateGroupingFlagLine(CadBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """プロセッサー「GroupingFlagLine」の入力データのチェックを行う"""
        )
        tags = ["DataFrame", "python", "validate"]

    #:
    MAIN_RESULTS_NAME = PropertyDescriptor(
        name="Main Results Name",
        description="メインのFIndexのフィールドセットファイルのDwh列の接尾語。<Geometry Name>/<Main Results Name>というDwhの値を取得。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUB_RESULTS_NAME = PropertyDescriptor(
        name="Sub Results Name",
        description="サブのFIndexのフィールドセットファイルのDwh列の接尾語。<Geometry Name>/<Sub Results Name>というDwhの値を取得。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    GROUP_RESULTS_NAME = PropertyDescriptor(
        name="Group Results Name",
        description="FIndexのグループのフィールドセットファイルのDwh列の接尾語。<Geometry Name>/<Group Results Name>というDwhの値を取得。",
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

    property_descriptors = [MAIN_RESULTS_NAME, SUB_RESULTS_NAME, GROUP_RESULTS_NAME, FEATURE_ID_COLUMN_NAME]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_duplicates_fid_data(self, row):
        """
        地物ID情報に重複がないかをチェックする
        
        :param row: DataFrameの1レコード分の情報
        :type row: pandas.Series

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
            # プロパティ取得
            main_results_name = context.getProperty(self.MAIN_RESULTS_NAME).getValue()
            sub_results_name = context.getProperty(self.SUB_RESULTS_NAME).getValue()
            group_results_name = context.getProperty(self.GROUP_RESULTS_NAME).getValue()
            fid_col_name = context.getProperty(self.FEATURE_ID_COLUMN_NAME).getValue()

            # インプットデータ取得
            fsf = flowfile.getContentsAsBytes()

            # データの存在チェック
            if not self.validate_empty_data(fsf):
                return self.RESULT_FAILURE

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(fsf)
            if not result:
                return self.RESULT_FAILURE

            # ジオメトリの存在チェック
            if not self.validate_exist_grouping_geometry(df):
                return self.RESULT_FAILURE
            # 属性の存在チェック
            if not self.validate_exist_attribute(df, [fid_col_name, main_results_name, sub_results_name, group_results_name, f"{main_results_name}_Cross"]):
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

            # ペアリングデータのFIDマッチングチェック
            if not self.validate_fsf_match_record(fsf):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # geometryのデータにLineStringが一つ以上含まれていること
            check_geom_list = df[df["Type"] == "geometry"]["Value"].apply(
                lambda geom_list: [isinstance(geom, LineString) for geom in geom_list] if isinstance(geom_list, list) else []
            )
            if not any([item for sub_list in check_geom_list for item in sub_list]):
                args = {
                    "error_code": ErrorCodeList.EC00010,
                    "対象": "LineString"
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # _crossのジオメトリチェック(Findex, Point())
            # (Findex, Point())のリストであることを期待
            check_point_list = df.loc[df["Dwh"].str.contains("_Cross"), "Value"]
            check_list = []
            for _, check_val in check_point_list.values.tolist()[0]:
                check_list.append(self.validate_data_types(check_val, Point))
            if not all(check_list):
                result = False
                if self.mode_value == self.MODE_STOP:
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
