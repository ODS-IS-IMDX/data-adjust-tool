# MIT License
# 
# Copyright (c) 2026 NTT InfraNet
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
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
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
UPDATE='フィールドの値で座標値を上書き'

# 座標配列の軸
X_AXIS='X'
Y_AXIS='Y'
Z_AXIS='Z'

XYZ_DICT={X_AXIS:1,Y_AXIS:2,Z_AXIS:3}



class CalculateFieldsCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標とフィールドの四則演算を行う。
                        ①input: 四則演算を行いたい2つのフィールドを持った2行のFieldSetFile。
                        ②output: フィールドを持った1行のFieldSetFile。
                      """
        tags = ["Calculate",
                "Plus",
                "Minus",
                "Multiplication",
                "Division",
                "Update",
                "Python"]


    # 座標配列のdwh名
    TARGET_COORDINATES_DWH_NAME = PropertyDescriptor(
        name="Target Coordinates DWH Name",
        description="GeoNdArrayのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
    )

    # 計算対象の軸
    TARGET_AXIS = PropertyDescriptor(
        name="Target Axis",
        description="計算対象の軸",
        allowable_values=[X_AXIS,
                          Y_AXIS,
                          Z_AXIS],
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
    )

    # フィールドDWH名
    TARGET_FIELDS_DWH_NAME = PropertyDescriptor(
        name="Target Fields DWH Name",
        description="フィールドのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 座標配列に対する計算方法
    CALCULATION_METHOD = PropertyDescriptor(
        name="Calculation Method",
        description="GeoNdArrayに対する計算方法",
        allowable_values=[ADDITION,
                          SUBTRACTION,
                          MULTIPLICATION,
                          DIVISION,
                          UPDATE],
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


    property_descriptors = [TARGET_COORDINATES_DWH_NAME,
                            TARGET_AXIS,
                            TARGET_FIELDS_DWH_NAME,
                            CALCULATION_METHOD,
                            OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):

        return self.property_descriptors


    def calculate_coordinates_with_fields(self,
                                          target_coordinates_array,
                                          target_axis,
                                          si,
                                          ei,
                                          calculation_method,
                                          fields_value_array):
        
        result_array=target_coordinates_array.copy()
        
        # 上書き
        if calculation_method == UPDATE:
            for i in range(len(fields_value_array)):
                result_array[si[i]:ei[i]+1,target_axis]\
                    = fields_value_array[i]
        else:
            
            # 計算用メソッド選択
            if calculation_method == ADDITION:
                calculation_method=np.add

            elif calculation_method == SUBTRACTION:
                calculation_method=np.subtract

            elif calculation_method == MULTIPLICATION:
                calculation_method=np.multiply

            elif calculation_method == DIVISION:
                calculation_method=np.divide
            
            # 計算式適用
            for i in range(len(fields_value_array)):
                result_array[si[i]:ei[i]+1,target_axis]\
                    = calculation_method(result_array[si[i]:ei[i]+1,target_axis],
                                         fields_value_array[i])

        return result_array

    def transform(self, context, flowfile):

        try:

            # プロパティで設定した値を取得
            target_coordinates_dwh_name = context.getProperty(
                self.TARGET_COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            target_axis_string = context.getProperty(
                self.TARGET_AXIS).evaluateAttributeExpressions(flowfile).getValue()
            target_fields_dwh_name = context.getProperty(
                self.TARGET_FIELDS_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            calculation_method = context.getProperty(
                self.CALCULATION_METHOD).evaluateAttributeExpressions(flowfile).getValue()
            output_dwh_name = context.getProperty(
                self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # flowfileから、csv形式のfield_set_fileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            field_set_file_dataframe = pd.read_csv(
                io.StringIO(input_field_set_file))

            value_list = field_set_file_dataframe["Value"].tolist()
            dwh_list = field_set_file_dataframe["Dwh"].tolist()

            # 計算対象の座標配列、属性のインデックス取得
            target_coordinates_index = dwh_list.index(target_coordinates_dwh_name)
            target_fields_index = dwh_list.index(target_fields_dwh_name)

            # 座標値取得
            target_coordinates_array = pickle.loads(
                base64.b64decode(value_list[target_coordinates_index]))
            
            # 計算対象の軸のインデックス取得
            target_axis=XYZ_DICT.get(target_axis_string)
            
            # 属性値のみ取得
            target_fields_list = pickle.loads(
                base64.b64decode(value_list[target_fields_index]))

            # 地物ごとのインデックス取得
            si, ei=NCP.get_start_end_index_by_1d_array(target_coordinates_array[:,0])

            # 属性Listから1次元配列の取得
            fields_value_array = np.array(
                        [float(value[1]) for value in target_fields_list])

            # NumPyで計算
            result_array\
                =self.calculate_coordinates_with_fields(target_coordinates_array,
                                                        target_axis,
                                                        si,
                                                        ei,
                                                        calculation_method,
                                                        fields_value_array)

            # outputのFieldSetFileを作成
            output_field_set_file \
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)([output_dwh_name],
                                                                         ["geometry"],
                                                                         [result_array])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes={"DWH": output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
