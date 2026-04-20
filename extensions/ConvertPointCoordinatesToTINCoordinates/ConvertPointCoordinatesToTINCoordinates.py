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
# 入力された座標配列に対してドロネー三角形分割を行い、三角形メッシュ（TIN）を生成する。
# 処理後は生成した三角形メッシュ（TIN）の座標をFieldSetFileとして出力。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")

class ConvertPointCoordinatesToTINCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        入力された座標配列に対してドロネー三角形分割を行い、三角形メッシュ（TIN）を生成する。
                        ①input: ポイントの座標配列を持った1行のFieldSetFile。
                        ②output: ドロネー分割により生成されたTINの座標配列を持った1行のFieldSetFile。
                      """

        tags = ["TIN", "GCP", "Python"]

    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データの座標配列のDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    property_descriptors = [OUTPUT_DWH_NAME
                            ]

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
            output_dwh_name: 出力データの座標配列のDWH名
        """

        # 出力データの座標配列のDWH名
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name


    def assign_group_id(self, target_array, group_size):
        """
        配列に対して group_size ごとのグループIDを設定

        Parameters:
            target_array: 入力配列（2次元配列。先頭列にIDを書き換える）
            group_size: 各グループの行数（1グループに何行割り当てるか）

        Returns:
            result_array: グループIDを設定した配列
        """

        # 入力配列をコピー（元の配列を変更しないように）
        result_array = target_array.copy()

        # 総行数を取得
        row_length = result_array.shape[0]

        # グループIDを繰り返しで生成
        # 例: group_size=3, 行数=7 → [0,0,0,1,1,1,2]
        group_ids = np.repeat(np.arange((row_length + group_size - 1) // group_size), group_size)[:row_length]

        # 生成したグループIDを配列の1列目（ID列）に代入
        result_array[:, 0] = group_ids

        return result_array

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # -----------------------------------------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # -----------------------------------------------------------------------------------------------------------
            output_dwh_name\
                = self.get_property(context, flowfile)

            # -----------------------------------------------------------------------------------------------------------
            #  FieldSetFileで作成したdataframeから、プロパティで指定したDWH名のValue列を取得
            # -----------------------------------------------------------------------------------------------------------
            _, \
                target_dwh, \
                target_type, \
                target_coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # GCP座標配列(N, 3 or 4) → ドロネー化された座標配列(N, 3, 3 or 4)
            # -----------------------------------------------------------------------------------------------------------
            # GCP配列からTINインデックスを生成
            delaunay_index = NCP.generate_delaunay_index(target_coordinates_array)

            # 与えられたインデックスを使ってTIN（三角形の配列）を生成
            target_tin_array = NCP.create_tin_from_delaunay_index(target_coordinates_array,
                                                                  delaunay_index)

            target_tin_array_2d = target_tin_array.reshape(-1, target_tin_array.shape[2])

            # ジオメトリ作成のため、ポリゴン構成点ごとに同一id振り直し
            target_tin_array_2d = self.assign_group_id(target_tin_array_2d, 3)

            # -----------------------------------------------------------------------------------------------------------
            # 出力用前処理
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
            output_value_list = [target_tin_array_2d]

            # -----------------------------------------------------------------------------------------------------------
            # 更新した座標データを含むFieldSetFileを生成
            # -----------------------------------------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_name_list, output_type_list, output_value_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
