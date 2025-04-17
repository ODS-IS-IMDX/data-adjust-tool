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
import io

from importlib import import_module

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor

from common.error_code_list import ErrorCodeList

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")


class ValidateReadGeoDataFrameFromFiles(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ReadGeoDataFrameFromFilesで必要なデータを持っているか、プロパティで指定したCSVが正しいか検証。
                      """
        tags = ["Validate", "Python"]

    # GeoDataframeに設定するオプションCSV
    INPUT_OPTION_CSV = PropertyDescriptor(
        name="Input Option CSV",
        description="""GeoDataframeに設定するオプション（CSV形式）。
                      ※GeoPandasライブラリのread_fileメソッドで用いるオプションを指定
                    """,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="OPTION,VALUE",
        sensitive=False,
        required=True
    )

    property_descriptors = [INPUT_OPTION_CSV]

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

            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # flowfileのデータをfile_objectへ変換(※exec_read_code部分で使用)
            input_stream = io.BytesIO(input_data)

            # --------------------------------------------------------------------------
            # データの空検証
            # --------------------------------------------------------------------------
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # オプション指定用CSV取得
            input_option_csv\
                = context.getProperty(self.INPUT_OPTION_CSV).evaluateAttributeExpressions(flowfile).getValue()

            input_option_stream = io.StringIO(input_option_csv)

            # --------------------------------------------------------------------------
            # GeoDataframeに設定するオプションCSVの検証
            # --------------------------------------------------------------------------
            try:
                # GeoPandasに設定するオプションを取得
                input_option_dataframe = pd.read_csv(
                    input_option_stream, quoting=3)

            except pd.errors.ParserError as e:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00079)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            except pd.errors.EmptyDataError as e:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00034)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            except SyntaxError:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00033)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # input_option_dataframe に"OPTION"・"VALUE"カラムが存在するかの検証
            # --------------------------------------------------------------------------
            try:
                option_list = input_option_dataframe["OPTION"].to_list()
                value_list = input_option_dataframe["VALUE"].to_list()

            except KeyError:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00034)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # オプションにcrsが存在した場合指定したCRSが正しいか検証
            # --------------------------------------------------------------------------
            for option, value in zip(option_list, value_list):
                if option == "crs":
                    if not self.check_epsg(value):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE
                    break

            # 必須のファイルパスを引数に設定
            exec_read_code\
                = 'target_gdf = gpd.read_file(input_stream'

            # オプションの文字列を結合
            for i in range(len(option_list)):

                option_code\
                    = ', {} = {}'.format(option_list[i],
                                         value_list[i])
                exec_read_code = exec_read_code+option_code

            # カッコで閉じる
            exec_read_code = exec_read_code+')'

            global_vars = globals().copy()
            local_vars = locals()

            # --------------------------------------------------------------------------
            # GeoDataframeに変換できるかの検証
            # --------------------------------------------------------------------------
            try:
                # コードの実行
                exec(exec_read_code, global_vars, local_vars)

            except SyntaxError:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00033)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            except NameError:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00080)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            except UnicodeDecodeError:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.EC00007)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            except LookupError:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.EC00007)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            geodataframe = local_vars['target_gdf']

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
