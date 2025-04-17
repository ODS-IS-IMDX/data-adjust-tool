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
import pickle
import io

# Nifiライブラリ
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from common.error_code_list import ErrorCodeList

# 外部モジュール
from importlib import import_module
pd = import_module("pandas")


class ValidateConvertGeoDataFrameToContents(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ConvertGeoDataFrameToContentsに必要なデータを持っているか検証。
                      """
        tags = ["Validate", "Python"]

    # GeoDataFrameに設定するオプションCSV
    OUTPUT_OPTION_CSV = PropertyDescriptor(
        name="Output Option CSV",
        description="""GeoDataFrameに設定するオプション（CSV形式）。
                      ※GeoPandasライブラリのto_fileメソッドで用いるオプションを指定
                    """,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="OPTION,VALUE",
        sensitive=False,
        required=True
    )

    property_descriptors = [OUTPUT_OPTION_CSV]

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

            # GeoDataFrameに設定するオプションCSVを取得
            output_option_csv\
                = context.getProperty(self.OUTPUT_OPTION_CSV).evaluateAttributeExpressions(flowfile).getValue()

            # 空文字の場合はエラーコード(CSV形式ではない)を返す。
            if output_option_csv in (None, ""):
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00033)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # --------------------------------------------------------------------------
            # データの空検証
            # --------------------------------------------------------------------------
            if not self.validate_empty_data(input_data):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # --------------------------------------------------------------------------
            # シリアライズされたデータか検証
            # --------------------------------------------------------------------------
            if not self.validate_serialized_data(input_data):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # バイトデータからGeoDataFrameを復元する
            geodataframe = pickle.loads(input_data)

            # --------------------------------------------------------------------------
            # GeoDataFrameかどうかの検証
            # --------------------------------------------------------------------------
            if not self.validate_input_type(geodataframe, "GeoDataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            try:
                # GeoPandasに設定するオプションを取得
                output_option_dataframe = pd.read_csv(
                    io.StringIO(output_option_csv), quoting=3)

            except pd.errors.ParserError as e:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00079)
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
            # output_option_dataframe に"OPTION"・"VALUE"カラムが存在するかの検証
            # --------------------------------------------------------------------------
            try:
                option_list = output_option_dataframe["OPTION"].to_list()
                value_list = output_option_dataframe["VALUE"].to_list()

            except KeyError:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00034)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # OPTION列にdriverがなければエラー
            if "driver" in option_list:
                pass
            else:
                self.logger.error('driverを設定してください')
                return self.RESULT_FAILURE

            temp_object = io.BytesIO()

            #driver取得
            driver_index=option_list.index('driver')
            driver_string=value_list[driver_index]

            #CSVの場合 to_csvのメソッド使用
            if len(driver_string)==5 and driver_string[1:4]=='CSV':

                exec_read_code\
                    ='result = geodataframe.to_csv(temp_object'

                #オプションの文字列を結合
                for i in range(len(option_list)):

                    if option_list[i]=='driver':
                        continue
                    else:
                        pass
                    option_code\
                        =', {} = {}'.format(option_list[i],
                                            value_list[i])
                    exec_read_code=exec_read_code+option_code

            else:

                exec_read_code\
                    ='result = geodataframe.to_file(temp_object'

                # オプションの文字列を結合
                for i in range(len(option_list)):

                    option_code\
                        =', {} = {}'.format(option_list[i],
                                            value_list[i])
                    exec_read_code=exec_read_code+option_code

            # カッコで閉じる
            exec_read_code=exec_read_code+')'


            global_vars = globals().copy()
            local_vars = locals()

            try:
                # コードの実行
                exec(exec_read_code, global_vars, local_vars)

            # 作成できない場合はプロパティに問題がある為、エラーコードを返す
            except Exception:
                args = {
                    "error_code": ErrorCodeList.ED00081,
                    "対象プロパティ": "Output Option CSV"
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

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
