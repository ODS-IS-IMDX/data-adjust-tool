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
# マルチパッチの各点の座標をOBJ出力のため変換するプロセッサ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult


class ConvertMultipatchToOBJ(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        マルチパッチからOBJファイルを作成する。
                        ①input: 出力したいデータ項目を持ったFieldSetFile。
                        ②output: 出力したいデータ項目を持ったOBJファイル。
                      """
        tags = ['Multipatch', 'OBJ', 'Python']

    property_descriptors = []

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def convert_newlines_windows(self, obj_string):
        """
        概要:
            OBJ形式の文字列の改行文字をWindows用に書き換える関数

        引数:
            obj_string: 変換したいOBJ形式の文字列

        戻り値:
            obj_string.replace("\n", "\r\n"): 改行文字がWindows用に書き換えられた文字列
        """

        return obj_string.replace("\n", "\r\n")

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # DataFrameに変換された座標データを取得
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # ジオメトリnpy成形
            # 地物ごとにuniqueな座標List(要素はnumpy.array) + マルチパッチの組み合わせList(要素はnumpy.array) + 地物IDのlist
            geometry_list, \
                geometry_combination_list\
                = WM.calc_func_time(self.logger)(NSP.get_geometry_information_list)(geometry_value_coordinates_array)

            # OBJ文字列化
            obj_string = WM.calc_func_time(self.logger)(NSP.create_obj_strings)(geometry_list,
                                                                                geometry_combination_list
                                                                                )

            # 改行文字を、windows用に書き換える
            obj_string = WM.calc_func_time(self.logger)(
                self.convert_newlines_windows)(obj_string)

            return FlowFileTransformResult(relationship="success", contents=obj_string)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
