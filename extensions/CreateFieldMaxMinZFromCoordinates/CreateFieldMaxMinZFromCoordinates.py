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
# geometry1つごとの最大、最小深度を作成するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import base64
import pickle
import traceback

from importlib import import_module


# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")


class CreateFieldMaxMinZFromCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ジオメトリごとの最大,最小深度を作成する。
                        ①input: マルチパッチの座標配列を持った1行のFieldSetFile。
                        ②output: 最大、最小深度のフィールドを持った2行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Python']

    # 最大深度のリストのDWH名
    OUTPUT_MAX_DWH_NAME = PropertyDescriptor(
        name="Output Max DWH Name",
        description="出力データの最大深度のリストのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)

    # 最小深度のリストのDWH名
    OUTPUT_MIN_DWH_NAME = PropertyDescriptor(
        name="Output Min DWH Name",
        description="出力データの最小深度のリストのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)

    property_descriptors = [OUTPUT_MAX_DWH_NAME,
                            OUTPUT_MIN_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            Nifiのプロパティで入力した値を取得する関数
        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            output_max_dwh_name: 最大深度のリストのDWH名
            output_min_dwh_name: 最小深度のリストのDWH名
        """

        # プロパティから取得する
        output_max_dwh_name = context.getProperty(
            self.OUTPUT_MAX_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
        output_min_dwh_name = context.getProperty(
            self.OUTPUT_MIN_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_max_dwh_name, output_min_dwh_name

    def get_target_id_array_and_target_dict(self, geometry_value_coordinates_array):
        """
        概要:
            geometry_value_coordinates_arrayから、IDごとのXYZ座標を含む辞書とIDの配列を取得する関数

        引数:
            geometry_value_coordinates_array: FieldSetFileに格納されていた、座標配列

        戻り値:
            target_id_array: IDの配列
            target_dict: IDも含むXYZ座標の辞書のタプル
        """

        # 座標のIDを配列に格納。
        target_id_array = np.unique(geometry_value_coordinates_array[:, 0])

        # 辞書型の座標情報を設定。
        target_dict = {target_id_array[i]: geometry_value_coordinates_array[list(np.where(
            geometry_value_coordinates_array[:, 0] == target_id_array[i])[0])] for i in range(len(target_id_array))}

        return target_id_array, target_dict

    def create_maximum_depth_list_and_minimum_depth_list(self, target_id_array, target_dict):
        """
        概要:
            IDごとに最大深度と最小深度のリストを作成する関数

        引数:
            target_id_array: IDの配列
            target_dict: IDごとのXYZ座標を含む辞書のタプル

        戻り値:
            maximum_depth_list - IDごとの最大深度のリスト
            minimum_depth_list - IDごとの最小深度のリスト

        """

        maximum_depth_list = []
        [maximum_depth_list.append((target_id_array[ti], np.max(
            target_dict[target_id_array[ti]][:, 3]))) for ti in range(len(target_id_array))]

        minimum_depth_list = []
        [minimum_depth_list.append((target_id_array[ti], np.min(
            target_dict[target_id_array[ti]][:, 3]))) for ti in range(len(target_id_array))]

        return maximum_depth_list, minimum_depth_list

    def create_output_field_set_file(self, maximum_depth_list, minimum_depth_list, output_max_dwh_name, output_min_dwh_name):
        """
        概要:
            最大、最小深度リストをシリアライズ、base64でエンコードし、バイト文字を文字列にエンコードしたデータを
            field_set_fileを作成し、その中に格納する関数。

        引数:
            maximum_depth_list        - 最大深度のリスト
            minimum_depth_list        - 最小深度のリスト
            output_max_dwh_name       - 最大深度のリストに対応するDWH名
            output_min_dwh_name       - 最小深度のリストに対応するDWH名

        戻り値:
            output_field_set_file     - output用に作成したfield_set_file

        """

        # maximum_depth_listをバイト列に変換してBase64エンコード
        maximum_depth_list_bytes = pickle.dumps(maximum_depth_list)
        maximum_depth_list_bytes_b64encode = base64.b64encode(
            maximum_depth_list_bytes).decode("utf-8")

        # minimum_depth_listをバイト列に変換してBase64エンコード
        minimum_depth_list_bytes = pickle.dumps(minimum_depth_list)
        minimum_depth_list_bytes_b64encode = base64.b64encode(
            minimum_depth_list_bytes).decode("utf-8")

        # 新しいデータ型
        new_type = "float64"

        # 辞書データの作成
        dict_data = {
            "Dwh": [output_max_dwh_name, output_min_dwh_name],
            "Type": [new_type, new_type],
            "Value": [maximum_depth_list_bytes_b64encode, minimum_depth_list_bytes_b64encode]
        }

        # DataFrameの作成
        result_data_frame = pd.DataFrame(dict_data)

        # CSV形式で出力
        output_field_set_file = result_data_frame.to_csv(index=False)

        return output_field_set_file

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------

    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            output_max_dwh_name, \
                output_min_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # geometryの座標情報を取得
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # IDごとのXYZ座標を含む辞書とIDの配列を取得
            target_id_array, \
                target_dict\
                = WM.calc_func_time(self.logger)(self.get_target_id_array_and_target_dict)(geometry_value_coordinates_array)

            # IDごとに最大深度と最小深度のリストを作成
            maximum_depth_list, \
                minimum_depth_list\
                = WM.calc_func_time(self.logger)(self.create_maximum_depth_list_and_minimum_depth_list)(target_id_array,
                                                                                                        target_dict)

            # 最大、最小深度のリストをValue列に格納したFieldSetFileを生成
            output_field_set_file = WM.calc_func_time(self.logger)(self.create_output_field_set_file)(maximum_depth_list,
                                                                                                      minimum_depth_list,
                                                                                                      output_max_dwh_name,
                                                                                                      output_min_dwh_name)

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
