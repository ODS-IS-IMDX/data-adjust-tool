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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# flowfileのcontentsをFieldSetFileのValue列に格納するプロセッサ。
# --------------------------------------------------------------------------------------------

# python標準モジュール
import traceback
import pickle

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope


class ConvertContentsToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        入力データをFieldSetFileに変換する。
                        ①input: 任意のファイル。
                        ②output: 入荷データが格納された1行のFieldSetFile
                      """
        tags = ['Contents', 'FieldSetFile', 'Python']

    # 処理後のDwh名を指定する。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 文字コードの選択
    FILE_ENCODING = PropertyDescriptor(
        name="File Encoding",
        description="入力ファイルの文字コード",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        default_value="utf-8",
        required=False
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            FILE_ENCODING
                            ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            output_dwh_name: output_field_set_fileのDwh列に格納される文字列
            file_encoding: 文字コードの選択
        """

        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
        file_encoding = context.getProperty(
            self.FILE_ENCODING).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name, file_encoding

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:
            # プロパティで入力した値を取得する
            output_dwh_name, file_encoding = WM.calc_func_time(
                self.logger)(self.get_property)(context, flowfile)

            try:
                # binary文字を想定し、flowfileからデータを受け取る。
                flowfile_contents_data = flowfile.getContentsAsBytes()
                flowfile_contents_data = pickle.loads(flowfile_contents_data)

            # pickle.loads出来ない場合は、文字列型なので↓
            except pickle.UnpicklingError:

                try:
                    # flowfileからデータを取得
                    flowfile_contents_data = flowfile.getContentsAsBytes().decode(file_encoding)

                except UnicodeDecodeError:
                    self.logger.error(
                        f"file_encodingが適切ではありません。{file_encoding}")

            # データ型を、抽出
            target_type = type(flowfile_contents_data).__name__

            # FieldSetFile用に各要素をリスト化
            dwh_list = [output_dwh_name]
            target_type_list = [target_type]
            flowfile_contents_data_list = [flowfile_contents_data]

            # FieldSetFileに加工
            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                dwh_list, target_type_list, flowfile_contents_data_list)

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file, attributes={"DWH": output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
