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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 指定された座標配列の軸成分（x, y, z）を入れ替える処理を行う。
# 入力は座標配列1行のFieldSetFile。
# 入力座標配列は3列、4列または8列（マルチパッチ形式）に対応。

# 処理では、プロパティで指定された軸ペア（x列とy列、x列とz列、またはy列とz列）を
# 入れ替え、8列形式の場合は法線ベクトル成分も合わせて入れ替え。

# 必要に応じて出力データのDWH名をプロパティで上書き可能。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 軸ごとのインデックスマッピング（4列または8列を想定）
# id: 0, x: 1, y: 2, z: 3, xn: 4, yn: 5, zn: 6, idn: 7
SWAP_MAP = {
    'x列とy列の入れ替え': [1, 2, 4, 5], # x, y, xn, yn
    'x列とz列の入れ替え': [1, 3, 4, 6], # x, z, xn, zn
    'y列とz列の入れ替え': [2, 3, 5, 6]  # y, z, yn, zn
}


class ReorderAxisCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        指定された座標配列の軸成分（x, y, z）を入れ替える。
                        ①input: 座標配列を含む1行のFieldSetFile。
                                入力座標配列は3列（id, x, y）、4列（id, x, y, z）または8列（マルチパッチ形式）に対応。
                        ②output: 軸成分の入れ替えを行った座標配列を含む1行のFieldSetFile。
                      """


        tags = ["Reorder", "Axis", "Coordinates", "Python"]

    # 入れ替える軸
    AXIS_SWAP_OPTION = PropertyDescriptor(
        name="Axis Swap Option",
        description="入れ替える軸",
        allowable_values=["x列とy列の入れ替え", "x列とz列の入れ替え", "y列とz列の入れ替え"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    # 出力データのDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name = "Output DWH Name",
        description = "出力データのDWH名",
        required = False,
        sensitive = False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [AXIS_SWAP_OPTION,
                            OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        プロパティで設定した値を取得

        Parameters:
            context: プロセッサの設定値
            flowfile: プロセッサに入るデータ

        Returns:
            axis_swap_option: 入れ替える軸
            output_dwh_name: 出力データのDWH名
        """

        # 入れ替える軸
        axis_swap_option = context.getProperty(self.AXIS_SWAP_OPTION).getValue()

        # 出力データのDWH名
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return axis_swap_option, output_dwh_name

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # -----------------------------------------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # -----------------------------------------------------------------------------------------------------------
            axis_swap_option, \
                output_dwh_name\
                = self.get_property(context, flowfile)

            # -----------------------------------------------------------------------------------------------------------
            #  1行のFieldSetFileから各Dwh, Type, Value列の値を取得
            # -----------------------------------------------------------------------------------------------------------
            _, \
                target_dwh, \
                target_type, \
                coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            coordinates_array_copy = coordinates_array.copy()

            # 3列形式（id, x, y）の場合は "xy" 以外のスワップは不正
            if coordinates_array.shape[1] == 3 and axis_swap_option != "x列とy列の入れ替え":
                self.logger.error("'xy' 以外のスワップは列数3では実行できません。")
                return FlowFileTransformResult(relationship="failure")

            # 3列, 4列, 8列 以外の形式には対応していない
            elif coordinates_array.shape[1] not in (3, 4, 8):
                self.logger.error("対応していない形式です。列数は 3, 4, 8 のいずれかである必要があります。")
                return FlowFileTransformResult(relationship="failure")

            else:
                pass

            # 入れ替える列のインデックス取得
            swap_idx1, swap_idx2, nv_swap_idx1, nv_swap_idx2 = SWAP_MAP[axis_swap_option]

            # x/y/z 成分を入れ替え
            coordinates_array_copy[:, [swap_idx1, swap_idx2]] = coordinates_array[:, [swap_idx2, swap_idx1]]

            # マルチパッチ形式（列数8）の場合は、法線ベクトル側の軸も入れ替える
            if coordinates_array.shape[1] == 8:
                coordinates_array_copy[:, [nv_swap_idx1, nv_swap_idx2]] = coordinates_array[:, [nv_swap_idx2, nv_swap_idx1]]

            else:
                pass

            # -----------------------------------------------------------------------------------------------------------
            # 出力データの設定
            # -----------------------------------------------------------------------------------------------------------
            # プロパティでDWH名を入力した場合は上書き
            if output_dwh_name:

                # 指定されたDWH名を使用
                output_dwh_name_list = [output_dwh_name]
                # 属性にも設定
                attributes_dict = {"DWH": output_dwh_name}

            else:

                # 既存のDWH名を使用
                output_dwh_name_list = [target_dwh]
                # 属性にも設定
                attributes_dict = {"DWH": target_dwh}

            output_type_list = [target_type]
            output_value_list = [coordinates_array_copy]

            # -----------------------------------------------------------------------------------------------------------
            # 出力用FieldSetFileを生成
            # -----------------------------------------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_name_list, output_type_list, output_value_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
