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

# ----------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 前プロセッサ(ConvertGeometryToCoordinates)で、
# 座標の配列となったValueの各要素(x,y,z軸)に対して加算を行うプロセッサ。
# ----------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope


class ConvertCoordinatesToOffsetCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列のX,Y,Z座標に対して、入力した数値を加算する。
                        ①input: 座標配列を持った1行のFieldSetFile。マルチパッチの座標配列でも可。
                        ②output: 座標配列を持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Python']

    # x軸に対しての移動量を設定。
    X_OFFSET = PropertyDescriptor(
        name="X Offset",
        description="x軸に対しての移動量",
        required=True,
        default_value="0",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # y軸に対しての移動量を設定。
    Y_OFFSET = PropertyDescriptor(
        name="Y Offset",
        description="y軸に対しての移動量",
        required=True,
        default_value="0",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # z軸に対しての移動量を設定。
    Z_OFFSET = PropertyDescriptor(
        name="Z Offset",
        description="z軸に対しての移動量",
        required=True,
        default_value="0",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [X_OFFSET,
                            Y_OFFSET,
                            Z_OFFSET]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context):
        """
        概要
            プロパティで入力した値を取得する関数
        引数
            context: プロセッサの設定値が格納されているデータ
        戻り値
            x_offset: x軸に対する移動量
            y_offset: y軸に対する移動量
            z_offset: z軸に対する移動量
        """
        # プロパティで設定した数値をfloat型にする。
        x_offset = float(context.getProperty(self.X_OFFSET).getValue())
        y_offset = float(context.getProperty(self.Y_OFFSET).getValue())
        z_offset = float(context.getProperty(self.Z_OFFSET).getValue())

        return x_offset, y_offset, z_offset

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------

    def transform(self, context, flowfile):

        try:
            # プロパティで入力したx,y,z軸に対する移動量の取得
            x_offset, \
                y_offset, \
                z_offset\
                = WM.calc_func_time(self.logger)(self.get_property)(context)

            # get_dataframe_and_value_from_field_set_file:
            # 取得したfieldsetfile(csv)をデータフレームに変換、Value列だけを抜きだし、base64でデコード、pickleでデシリアライズを行う。
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # x,y,z軸に対して加算
            geometry_value_coordinates_array = WM.calc_func_time(self.logger)(
                NSP.plus_coordinate_array)(geometry_value_coordinates_array, x_offset, 1)
            geometry_value_coordinates_array = WM.calc_func_time(self.logger)(
                NSP.plus_coordinate_array)(geometry_value_coordinates_array, y_offset, 2)
            geometry_value_coordinates_array = WM.calc_func_time(self.logger)(
                NSP.plus_coordinate_array)(geometry_value_coordinates_array, z_offset, 3)

            # 戻り値の要素をリスト化
            geometry_dwh_list = [geometry_dwh]
            geometry_type_list = [geometry_type]
            geometry_value_coordinates_array_list = [
                geometry_value_coordinates_array]

            # set_field_set_file : pickleによるシリアライズ、base64でのデコード、utf-8でのエンコード処理を行う。
            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(geometry_dwh_list,
                                                                                           geometry_type_list,
                                                                                           geometry_value_coordinates_array_list
                                                                                           )

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
