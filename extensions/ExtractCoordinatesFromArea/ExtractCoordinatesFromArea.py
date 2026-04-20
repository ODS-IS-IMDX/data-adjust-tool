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

# python 標準ライブラリ
import traceback
from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリを動的インポート
np = import_module("numpy")

# 定数
# 最大値
MAX_VALUE = np.inf
MIN_VALUE = -np.inf


class ExtractCoordinatesFromArea(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列の最大最小値座標を指定し、構成点を抽出する。
                        ①input: 座標配列(Z値は必須)を持った1行のFieldSetFile。
                        ②output: 座標配列を持った1行のFieldSetFile。
                      """
        tags = ['Geometry', 'Python']

    # 出力座標配列のDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力される、ポリゴンの内周座標配列のDWH名
    MAX_X_VALUE = PropertyDescriptor(
        name="Max X Value",
        description="抽出エリアの最大X座標",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    MIN_X_VALUE = PropertyDescriptor(
        name="Min X Value",
        description="抽出エリアの最小X座標",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力される、ポリゴンの内周座標配列のDWH名
    MAX_Y_VALUE = PropertyDescriptor(
        name="Max Y Value",
        description="抽出エリアの最大Y座標",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    MIN_Y_VALUE = PropertyDescriptor(
        name="Min Y Value",
        description="抽出エリアの最小Y座標",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 出力される、ポリゴンの内周座標配列のDWH名
    MAX_Z_VALUE = PropertyDescriptor(
        name="Max Z Value",
        description="抽出エリアの最大Z座標",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    MIN_Z_VALUE = PropertyDescriptor(
        name="Min Z Value",
        description="抽出エリアの最小Z座標",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            MAX_X_VALUE,
                            MIN_X_VALUE,
                            MAX_Y_VALUE,
                            MIN_Y_VALUE,
                            MAX_Z_VALUE,
                            MIN_Z_VALUE]

    def __init__(self, **kwargs):
        pass

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
            output_dwh_name: 出力される、ポイント、ライン、ポリゴン(外周)座標配列ののDWH名
            max_x_value: X座標の最大値
            min_x_value: X座標の最小値
            max_y_value: Y座標の最大値
            min_y_value: Y座標の最小値
            max_z_value: Z座標の最大値
            min_z_value: Z座標の最小値
        """

        # 出力される、ポイント、ライン、ポリゴン(外周)座標配列のDWH名を取得
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        max_x_value = context.getProperty(
            self.MAX_X_VALUE).evaluateAttributeExpressions(flowfile).getValue()

        min_x_value = context.getProperty(
            self.MIN_X_VALUE).evaluateAttributeExpressions(flowfile).getValue()

        max_y_value = context.getProperty(
            self.MAX_Y_VALUE).evaluateAttributeExpressions(flowfile).getValue()

        min_y_value = context.getProperty(
            self.MIN_Y_VALUE).evaluateAttributeExpressions(flowfile).getValue()

        max_z_value = context.getProperty(
            self.MAX_Z_VALUE).evaluateAttributeExpressions(flowfile).getValue()

        min_z_value = context.getProperty(
            self.MIN_Z_VALUE).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name, max_x_value, min_x_value, max_y_value, min_y_value, max_z_value, min_z_value

    def define_max_min_value(self, coord, define_value=MAX_VALUE):
        """
        概要:
            プロパティで、値を指定しなかった場合、最小値と最大値を定義する

        引数:
            coord: プロパティで入力された値
            define_value: 最小値(-np.inf) or 最大値(np.inf)

        戻り値:
            引数そのまま or (-np.inf or np.inf)
        """

        if coord == None:
            coord = define_value
        
        else:
            return coord

        return coord

    def axis_range_filter(self, array, min_coordinate, max_coordinate):
        """
        概要:
            配列の最大最小値内に存在するフィルターする

        引数:
            array: 1軸の座標が入った1次元配列
            min_coordinate: 最小値
            max_coordinate: 最大値

        戻り値:
            bool_array: 座標が範囲の場合はTrue、範囲外の場合False
        """
        return (array >= min_coordinate) & (array <= max_coordinate)

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # NiFiのプロパティで入力した値を取得
            output_dwh_name, \
                max_x_value, \
                min_x_value, \
                max_y_value, \
                min_y_value, \
                max_z_value, \
                min_z_value\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # Noneがあった場合は、np.inf or -np.inf に置き換える
            max_x_value = self.define_max_min_value(max_x_value, MAX_VALUE)
            max_y_value = self.define_max_min_value(max_y_value, MAX_VALUE)
            max_z_value = self.define_max_min_value(max_z_value, MAX_VALUE)

            min_x_value = self.define_max_min_value(min_x_value, MIN_VALUE)
            min_y_value = self.define_max_min_value(min_y_value, MIN_VALUE)
            min_z_value = self.define_max_min_value(min_z_value, MIN_VALUE)

            # 取得したFieldSetFile(csv)をDataFrameに変換、Value列だけを抜きだし、base64でデコード、pickleでデシリアライズを行う。
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 指定エリア内に存在するか
            x_bool = self.axis_range_filter(coordinates_array[:, 1], float(min_x_value), float(max_x_value))
            y_bool = self.axis_range_filter(coordinates_array[:, 2], float(min_y_value), float(max_y_value))
            z_bool = self.axis_range_filter(coordinates_array[:, 3], float(min_z_value), float(max_z_value))

            # 出力対象を取得
            extract_coordinates_array = coordinates_array[np.logical_and.reduce([x_bool, y_bool, z_bool])]

            # 出力用FieldSetFileを作成する
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)([output_dwh_name],
                                                                         ["geometry"],
                                                                         [extract_coordinates_array])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
