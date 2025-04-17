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
# 座標のデータから勾配の傾きを計算するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")


class CreateGradientFromCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        LineStringジオメトリの座標配列から1ジオメトリごとの勾配の傾きを計算する。
                        ①input: 座標配列を持った1行のFieldSetFile。
                        ②output: 地物ごとの勾配のフィールドを持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Python']

    # 勾配の配列用のDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)

    property_descriptors = [OUTPUT_DWH_NAME]

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
            output_dwh_name: 変更後のDWH名
        """
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name

    def get_target_id_array_and_target_dict(self, geometry_value_coordinates_array):
        """
        概要:
            geometry_value_coordinates_arrayから、IDごとのXYZ座標を含む辞書とIDの配列を取得する関数
        引数:
            geometry_value_coordinates_array: FieldSetFileに格納されていた、座標配列

        戻り値:
            target_id_array: IDの配列
            target_dict: IDごとのXYZ座標を含む辞書のタプル
        """

        # ID取得
        target_id_array = np.unique(geometry_value_coordinates_array[:, 0])

        # IDごとのXYZ座標を含む辞書を作成
        target_dict = {target_id_array[i]: geometry_value_coordinates_array[list(np.where(
            geometry_value_coordinates_array[:, 0] == target_id_array[i])[0]), 1:4] for i in range(len(target_id_array))}

        return target_id_array, target_dict

    def calculate_gradient(self, target_dict, target_id_array):
        """
        概要:
            IDごとのXYZ座標から勾配を計算する関数
        引数:
            target_dict: IDをキーとしたXYZ座標の辞書
            target_id_array: IDの配列

        戻り値:
            gradient_array: IDとそのIDの勾配のタプルのリスト
        """

        # 各IDごとに距離とZ値の差を計算
        distance_array = np.array([np.sum(NCP.get_distance(
            target_dict[target_id_array[i]][:, :2])) for i in range(len(target_id_array))])
        z_value_array = np.abs(np.array([target_dict[target_id_array[i]][-1, 2] -
                               target_dict[target_id_array[i]][0, 2] for i in range(len(target_id_array))]))

        # 勾配を計算し、パーセントミルで表現
        gradient_array = [(target_id_array[i], "" if distance_array[i] == 0.0 else str(np.round(
            z_value_array[i] / distance_array[i] * 10, 1))) for i in range(len(z_value_array))]

        return gradient_array

    def create_output_field_set_file(self, output_dwh_name, geometry_dwh, gradient_array):
        """
        概要:
            新しいDWH名が空の場合はgeometryのDWH名を使い、それ以外の場合は新しいDWH名を使ってoutput_field_set_fileを作成する関数。
        引数:
            output_dwh_name: 新しいDWH名。空の場合はgeometry_dwhが使われる。
            geometry_dwh: GeometryのDWH名
            gradient_array: 勾配の配列

        戻り値:
            output_field_set_file: 出力のFieldSetFile
        """

        geometry_dwh_list = [geometry_dwh]
        output_dwh_name_list = [output_dwh_name]
        geometry_type_list = ["float64"]
        gradient_array_list = [gradient_array]

        # 新しいDWH名が空の場合はgeometryのDWH名を使う
        if output_dwh_name == "" or output_dwh_name is None:
            output_field_set_file = PBP.set_field_set_file(
                geometry_dwh_list, geometry_type_list, gradient_array_list)

        else:
            output_field_set_file = PBP.set_field_set_file(
                output_dwh_name_list, geometry_type_list, gradient_array_list)

        return output_field_set_file

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで入力したDWHを取得。
            output_dwh_name = WM.calc_func_time(self.logger)(
                self.get_property)(context, flowfile)

            # flowfileから今回使用する値を取得
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # IDごとのdictを作成
            target_id_array, \
                target_dict\
                = WM.calc_func_time(self.logger)(self.get_target_id_array_and_target_dict)(geometry_value_coordinates_array)

            # パーセントミルで設定 傾きが計算できなければゼロ文字空白
            gradient_array = WM.calc_func_time(self.logger)(
                self.calculate_gradient)(target_dict, target_id_array)

            # output_field_set_fileの作成
            # プロパティでDWH名を入力しなかった場合はgeometryのDWHを使う。
            output_field_set_file\
                = WM.calc_func_time(self.logger)(self.create_output_field_set_file)(output_dwh_name,
                                                                                    geometry_dwh,
                                                                                    gradient_array)

            # プロパティでDWH名を入力した場合は上書き。
            if output_dwh_name == "" or output_dwh_name is None:
                attributes_dict = {"DWH": geometry_dwh}

            else:
                attributes_dict = {"DWH": output_dwh_name}

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file, attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
