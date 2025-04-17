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

from abc import ABC, abstractmethod
from datetime import datetime
from importlib import import_module
pd = import_module("pandas")
gpd = import_module("geopandas")

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import (
    PropertyDescriptor,
    ExpressionLanguageScope,
    StandardValidators,
)
from common.base_validate_logger import BaseValidateLogger
from common.error_code_list import ErrorCodeList


class BaseValidateProcessor(FlowFileTransform, ABC):
    """
    バリデータの基底クラス
    """

    MODE_STOP = "Stop"
    MODE_CONTINUE = "Continue"

    RESULT_SUCCESS = "success"
    RESULT_FAILURE = "failure"

    Mode = PropertyDescriptor(
        name="Mode",
        description="エラー発生時の動作モードを設定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="Stop",
        allowable_values=[MODE_STOP, MODE_CONTINUE],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    property_descriptors = [Mode]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def __init__(self, **kwargs):
        self.validate_logger = None
        self.mode_value = None
        self.validate_log_text = None

    @abstractmethod
    def validate_data(self, context, flowfile):
        pass

    def log_with_timestamp(self, message):
        """
        現在時刻（ミリ秒まで）を先頭に付加したログメッセージを出力する。

        :param message: ログメッセージ
        :type message: str

        :return: 現在時刻（ミリ秒まで）を先頭に付加したログメッセージ
        :rtype: str
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # ミリ秒部分を3桁に調整
        msg_with_timestamp = f"[{timestamp}] {message}"

        return msg_with_timestamp

    def log_geodataframe_shape(self, processor_name, data_name, row_count, column_count):
        """
        行数・列数をログ出力する。

        :param processor_name: プロセッサ名
        :type processor_name: str
        :param data_name: データ名
        :type data_name: str
        :param row_count: 行数
        :type row_count: int
        :param column_count: 列数
        :type column_count: int

        :return: ログメッセージ
        :rtype: str
        """
        row_msg = self.log_with_timestamp(f"{processor_name}.{data_name}.row_count={row_count}")
        col_msg = self.log_with_timestamp(f"{processor_name}.{data_name}.column_count={column_count}")

        return "\n".join([row_msg, col_msg])  # 改行コードで繋げて１つの文字列として返す

    def log_csv_shape(self, processor_name, row_count, max_cols, min_cols):
        """
        CSVの行数・最大列数・最小列数・列数の差分をログ出力する。

        :param processor_name: プロセッサ名
        :type processor_name: str
        :param row_count: 行数
        :type row_count: int
        :param max_cols: 最大列数
        :type max_cols: int
        :param min_cols: 最小列数
        :type min_cols: int

        :return: ログメッセージ
        :rtype: str
        """
        column_diff = max_cols - min_cols

        row_msg = self.log_with_timestamp(f"{processor_name}.csv_row_count={row_count}")
        max_col_msg = self.log_with_timestamp(f"{processor_name}.csv_max_column_count={max_cols}")
        min_col_msg = self.log_with_timestamp(f"{processor_name}.csv_min_column_count={min_cols}")
        col_diff_msg = self.log_with_timestamp(f"{processor_name}.csv_column_diff={column_diff}")

        return "\n".join([row_msg, max_col_msg, min_col_msg, col_diff_msg])  # 改行コードで繋げて１つの文字列として返す

    def get_geodataframe_shape(self, data):
        """
        GeoDataFrameまたはDataFrameのレコード数（行数）とカラム数（列数）を取得する。

        :param data: GeoDataFrameまたはDataFrame
        :type data: geopandas.GeoDataFrame|pandas.DataFrame

        :return: (レコード数, カラム数)
        :rtype: tuple[int, int]|None
        """
        if isinstance(data, gpd.GeoDataFrame) or isinstance(data, pd.DataFrame):
            return data.shape  # (行数, 列数)
        else:
            self.validate_logger.write_log(ErrorCodeList.EC00006)  # エラーログを記録
            return None

    def get_csv_shape(self, csv_text):
        """
        CSVのテキストデータを解析し、行数、最大列数、最小列数を取得する。

        :param csv_text: CSV形式の文字列
        :type csv_text: str

        :return: (行数, 最大列数, 最小列数)
        :rtype: tuple(int, int, int)
        """
        try:
            lines = csv_text.strip().split("\n")
            if not lines:
                return 0, 0, 0  # データが空の場合

            column_counts = [len(line.split(",")) for line in lines]

            return len(lines), max(column_counts), min(column_counts)

        except Exception:
            return 0, 0, 0  # 例外発生時も (0, 0, 0) を返す

    def validate_gdf_shape(self, gdf, data_name="GeoDataFrame"):
        """
        GeoDataFrameまたはDataFrameの行数列数チェック

        :param gdf: 行数列数をチェックするデータ
        :type gdf: geopandas.GeoDataFrame|pandas.DataFrame
        :param data_name: データ名
        :type data_name: str

        :return: GeoDataframeかDataframeであるか
        :rtype: bool
        """
        try:
            # データの行数・列数チェック
            shape_data = self.get_geodataframe_shape(gdf)
            if shape_data is None:
                return False

            # 行数・列数を取得し、ログ出力用データを取得しメンバ変数に格納
            if self.validate_log_text is None:
                # バリデータログが未設定の場合
                self.validate_log_text = self.log_geodataframe_shape(
                    self.__class__.__name__, data_name, shape_data[0], shape_data[1]
                ) + "\n"
            else:
                # バリデータログが既に設定されている場合
                self.validate_log_text += self.log_geodataframe_shape(
                    self.__class__.__name__, data_name, shape_data[0], shape_data[1]
                ) + "\n"
            return True

        except Exception as e:
            return False

    def validate_csv_shape(self, csv):
        """
        CSVの行数列数チェック

        :param csv: csv
        :type csv: str

        :return: 空ではないCSVデータであるか
        :rtype: bool
        """
        try:
            # CSVの行数・列数チェック
            shape_data = self.get_csv_shape(csv)
            if shape_data == (0, 0, 0):
                return False

            # CSVの行数・列数を取得し、ログ出力用データを取得しメンバ変数に格納
            if self.validate_log_text is None:
                # バリデータログが未設定の場合
                self.validate_log_text = self.log_csv_shape(
                    self.__class__.__name__, shape_data[0], shape_data[1], shape_data[2]
                ) + "\n"
            else:
                # バリデータログが既に設定されている場合
                self.validate_log_text += self.log_csv_shape(
                    self.__class__.__name__, shape_data[0], shape_data[1], shape_data[2]
                ) + "\n"

            return True

        except Exception as e:
            return False

    def transform(self, context, flowfile):
        """
        プロセスのエントリーポイントとなる関数。

        :param context: コンテキスト
        :param flowfile: FieldSetFileを持つフローファイル

        :return: 処理結果を含んだFlowFileTransformResult
        :rtype: FlowFileTransformResult

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        self.validate_logger = BaseValidateLogger(self.logger)
        self.mode_value = context.getProperty(self.Mode).getValue()

        # 入力データにValidateLog属性が設定されている場合
        base_validate_log = flowfile.getAttribute("ValidateLog")
        if base_validate_log is not None and base_validate_log.strip() != "":
            # 末尾の改行コードの有無をチェックし、出力用ログに格納
            if base_validate_log.endswith("\n"):
                self.validate_log_text = base_validate_log
            else:
                self.validate_log_text = base_validate_log + "\n"
        else:
            self.validate_log_text = None

        # Validatorの処理を実行
        result = self.validate_data(context, flowfile)

        # 実行時間を取得
        priority = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # 属性にログと実行時間をセット（他のFlowFileと結合時に末尾の改行が必要）
        attributes = {"ValidateLog": self.validate_log_text, "priority": priority}

        return FlowFileTransformResult(relationship=result, attributes=attributes)

