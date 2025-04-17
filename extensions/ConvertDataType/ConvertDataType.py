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
# 属性データの型を変換する
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

STR = "文字列型"
INT = "整数型"
FLOAT = "浮動小数点型"


class ConvertDataType(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        フィールドの型を変換する
                        ①input: 任意のフィールドを持った1行のFieldSetFile。
                        ②output: 入力したフィールドを持った1行のFieldSetFile。
                      """
        tags = ["Type", "Change", "Python"]

    # 変換する属性の型
    CHANGE_TYPE = PropertyDescriptor(
        name="Change Type",
        description="変換するフィールドの型",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        allowable_values=[INT,
                          FLOAT,
                          STR]
    )

    # 処理後のDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        sensitive=False,
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [CHANGE_TYPE,
                            OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        プロパティで入力した値を取得する関数

        引数
            context - プロセッサの設定値が格納されているデータ
            flowfile - プロセッサに入ってくるデータ
        戻り値
            change_type - 変換後属性の型
            output_dwh_name - 処理後のDWH名
        """

        change_type = context.getProperty(
            self.CHANGE_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return change_type, output_dwh_name

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # Nifiのプロパティで入力したfilenameの呼び出し。
            change_type, output_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # 1行のFieldSetFileのValue列を抜きだす。
            field_set_file_dataframe, \
                input_dwh_name, \
                input_type, \
                input_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # inputの形が属性データであるか
            if isinstance(input_list, list) and len(input_list) > 0 and isinstance(input_list[0], tuple):
                pass
            else:
                self.logger("ValueError: インプットのデータが属性データではない。")
                return FlowFileTransformResult(relationship="failure")

            # output_dwh名が、プロパティで入力されていない場合、元のDWH名から/で区切った１つ目の値に、/geometryをつける
            if output_dwh_name is None or output_dwh_name == "":
                output_dwh_name = input_dwh_name
            try:
                # タイプ別で、listに加工
                if change_type == STR:
                    output_list = [(left, str(right))
                                   for left, right in input_list]
                    field_set_file = WM.calc_func_time(self.logger)(
                        PBP.set_field_set_file)([output_dwh_name], ["str"], [output_list])

                elif change_type == INT:
                    output_list = [(left, int(right))
                                   for left, right in input_list]
                    field_set_file = WM.calc_func_time(self.logger)(
                        PBP.set_field_set_file)([output_dwh_name], ["int"], [output_list])

                else:
                    output_list = [(left, float(right))
                                   for left, right in input_list]
                    field_set_file = WM.calc_func_time(self.logger)(
                        PBP.set_field_set_file)([output_dwh_name], ["float"], [output_list])

            except Exception:
                self.logger(traceback.format_exc())
                return FlowFileTransformResult(relationship="failure")

            return FlowFileTransformResult(relationship="success",
                                           contents=field_set_file,
                                           attributes={"DWH": output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
