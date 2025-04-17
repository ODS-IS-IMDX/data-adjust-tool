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

# ----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 属性のindexが同じものを指定された計算方法で計算する
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import base64
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
pd = import_module("pandas")

# 定数を定義
ADDITION = "加算(+)"
SUBTRACTION = "減算(-)"
MULTIPLICATION = "乗算(*)"
DIVISION = "除算(/)"


class CalculateFields(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        フィールド同士の四則演算を行う。
                        ①input: 四則演算を行いたい2つのフィールドを持った2行のFieldSetFile。
                        ②output: フィールドを持った1行のFieldSetFile。
                      """
        tags = ["Calculate", "Plus", "Minus",
                "Multiplication", "Division", "Python"]

    # target_geometry_type
    CALCULATION_METHOD = PropertyDescriptor(
        name="Calculation Method",
        description="計算方法",
        allowable_values=[ADDITION, SUBTRACTION, MULTIPLICATION, DIVISION],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    # 出力時のデータのdwh名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力時のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
    )

    # 四則演算のベース（演算元）となるDWH名
    BASE_DWH_NAME = PropertyDescriptor(
        name="Base DWH Name",
        description="四則演算のベース（演算元）となるDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [CALCULATION_METHOD,
                            OUTPUT_DWH_NAME,
                            BASE_DWH_NAME]

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            results_dwh_name: 座標データ(DataFrame)のDWH
        """

        # プロパティで設定した値を取得

        calculation_method = context.getProperty(
            self.CALCULATION_METHOD).evaluateAttributeExpressions(flowfile).getValue()

        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        base_dwh_name = context.getProperty(
            self.BASE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return calculation_method, output_dwh_name, base_dwh_name

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):

        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # プロパティで設定した値を取得
            calculation_method, \
                output_dwh_name, \
                base_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileから、csv形式のfield_set_fileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            field_set_file_data_frame = pd.read_csv(
                io.StringIO(input_field_set_file))

            value_list = field_set_file_data_frame["Value"].tolist()
            dwh_list = field_set_file_data_frame["Dwh"].tolist()

            base_dwh_name_index = dwh_list.index(base_dwh_name)

            calc_index = None
            for index in range(len(dwh_list)):
                if index != base_dwh_name_index:
                    calc_index = index

            calculate_list1 = pickle.loads(
                base64.b64decode(value_list[base_dwh_name_index]))
            calculate_list2 = pickle.loads(
                base64.b64decode(value_list[calc_index]))

            # NumPy配列に変換し、右側の値を取得
            findexes = np.array(
                [value[0] for value in calculate_list1], dtype=float)  # 左の値
            calculate_array1 = np.array(
                [float(value[1]) for value in calculate_list1])
            calculate_array2 = np.array(
                [float(value[1]) for value in calculate_list2])

            # NumPyで計算
            if calculation_method == ADDITION:
                result_values = calculate_array1 + calculate_array2

            elif calculation_method == SUBTRACTION:
                result_values = calculate_array1 - calculate_array2

            elif calculation_method == MULTIPLICATION:
                result_values = calculate_array1 * calculate_array2

            elif calculation_method == DIVISION:
                result_values = calculate_array1 / calculate_array2

            # 結果をタプルリストに戻す
            result \
                = [(float(key), float(value)) for key, value in zip(findexes, result_values)]

            # outputのFieldSetFileを作成
            output_field_set_file \
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)([output_dwh_name],
                                                                         ["float"],
                                                                         [result])

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
