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

# -----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 属性に対し、加算か乗算を行うプロセッサ。
# -----------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")

# 定数を定義
ADDITION = "加算(+)"
MULTIPLICATION = "乗算(*)"


class SetValueAdditionOrMultiplication(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        フィールドデータの値に指定した数値で四則演算を行う。
                        ①input: 1つの属性を持った1行のFieldSetFile。
                        ②output: 1つの属性を持った1行のFieldSetFile。
                      """
        tags = ['Addition', 'Multiplication', 'Python']

    # 計算タイプ
    CALCULATION_TYPE = PropertyDescriptor(
        name="Calculation Type",
        description="計算タイプ",
        default_value=MULTIPLICATION,
        allowable_values=[MULTIPLICATION, ADDITION],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    # 計算する値
    CALCULATION_VALUE = PropertyDescriptor(
        name="Calculation Value",
        description="一律で演算に用いる既定値",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    # 処理後のDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    property_descriptors = [CALCULATION_TYPE,
                            CALCULATION_VALUE,
                            OUTPUT_DWH_NAME]

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
            flowfile: プロセッサに渡されるデータ

        戻り値:
            calculation_type: 計算タイプ
            calculation_value: 計算の値
            output_dwh_name: 処理後のDWH名
        """

        calculation_type \
            = context.getProperty(self.CALCULATION_TYPE).getValue()

        calculation_value \
            = float(context.getProperty(self.CALCULATION_VALUE).getValue())

        output_dwh_name \
            = context.getProperty(self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return calculation_type, calculation_value, output_dwh_name

    def calculation_value(self, calculation_type, calculation_value, source_number_list):
        """
        概要:
            計算を行う関数

        引数:
            calculation_type: 計算タイプ
            calculation_value: 計算の値
            source_number_list: 計算元となる数

        戻り値:
            calculated_number_list: 出力に用いるリスト
        """

        source_number_array = np.array(source_number_list)
        source_number_array = source_number_array.astype(float)
        if calculation_type == MULTIPLICATION:
            source_number_array[:, 1] *= calculation_value

        else:
            source_number_array[:, 1] += calculation_value
        calculated_number_list = [tuple(float(y)
                                        for y in x) for x in source_number_array]

        return calculated_number_list

    def transform(self, context, flowfile):
        try:

            # プロパティの値を取得
            calculation_type, \
                calculation_value, \
                output_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # FieldSetFileをDataFrameにし、各列の値を抜き出す。Value列だけデシリアライズ、デコードを行う
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                source_number_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 計算を行う
            new_number_list\
                = WM.calc_func_time(self.logger)(self.calculation_value)(calculation_type,
                                                                         calculation_value,
                                                                         source_number_list)

            # set_field_set_file引数用にリスト化
            target_dwh_list = [output_dwh_name]
            target_type_list = [target_type]
            target_value_list = [new_number_list]

            # 加算されたlistをFieldSetFileに加工
            output_field_set_file \
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(target_dwh_list,
                                                                         target_type_list,
                                                                         target_value_list)

            # プロパティでDWH名を入力した場合は上書き。
            if output_dwh_name == "":
                attributes_dict = {}

            else:
                attributes_dict = {"DWH": output_dwh_name}

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
