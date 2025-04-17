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
# geometry1つごとに長さを計算するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# 自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")


class CreateLengthFromCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        LineStringジオメトリの座標配列から1ジオメトリごとの長さを計算する。
                        ①input: 座標配列を持った1行のFieldSetFile。
                        ②output: 地物ごとの延長のフィールドを持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Python']

    # 延長の配列用のDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [OUTPUT_DWH_NAME]

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
            out_dwh_name: 変更後のDWH名
        """

        out_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return out_dwh_name

    def get_target_id_array_and_target_dict(self, geometry_value_coordinates_array):
        """
        概要:
            geometry_value_coordinates_arrayから、IDごとのXYZ座標を含む辞書とIDの配列を取得する関数
        引数:
            geometry_value_coordinates_array: FieldSetFileに格納されていた、座標配列
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            target_id_array: IDの配列
            target_dict: IDごとのXYZ座標を含む辞書のタプル
        """

        # ID取得
        target_id_array = np.unique(geometry_value_coordinates_array[:, 0])

        # IDごとのXYZ座標を含む辞書を作成
        target_dict = {target_id_array[i]: geometry_value_coordinates_array[
            list(np.where(geometry_value_coordinates_array[:, 0] == target_id_array[i])[0]), 1:4]
            for i in range(len(target_id_array))}

        return target_id_array, target_dict

    def create_length(self, target_id_array, target_dict):
        """
        概要:
            与えられたIDごとの座標データから延長を計算し、各IDとその延長のタプルのリストを生成する関数
        引数:
            target_id_array: IDの配列
            target_dict: IDごとのXYZ座標を含む辞書のタプル

        戻り値:
            length_list: 各IDとその延長のタプルのリスト
        """

        # 延長計算
        length_list = [(target_id_array[i], np.sum(NCP.get_distance(target_dict[target_id_array[i]])))
                       for i in range(len(target_id_array))]

        return length_list

    def create_output_field_set_file(self, out_dwh_name, geometry_dwh, length_list):
        """
        概要:
            新しいDWH名が空の場合はgeometryのDWH名を使い、それ以外の場合は新しいDWH名を使ってoutput_field_set_fileを作成する関数
        引数:
            out_dwh_name: 新しいDWH名。空の場合はgeometry_dwhが使われる。
            geometry_dwh: GeometryのDWH名
            length_list: 勾配の配列

        戻り値:
            output_field_set_file: 出力のフィールドセットファイル
        """

        geometry_dwh_list = [geometry_dwh]
        out_dwh_name_list = [out_dwh_name]
        geometry_type_list = ["float64"]
        length_list_list = [length_list]

        # 新しいDWH名が空の場合はgeometryのDWH名を使う
        if out_dwh_name == "" or out_dwh_name is None:
            output_field_set_file = PBP.set_field_set_file(
                geometry_dwh_list, geometry_type_list, length_list_list)

        else:
            output_field_set_file = PBP.set_field_set_file(
                out_dwh_name_list, geometry_type_list, length_list_list)

        return output_field_set_file

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで入力したDWHをtransform内で定義。
            out_dwh_name = WM.calc_func_time(self.logger)(
                self.get_property)(context, flowfile)

            # flowfileから必要なデータを取得。
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # IDと、IDと座標のデータを辞書型に格納した値を取得。
            target_id_array, \
                target_dict\
                = WM.calc_func_time(self.logger)(self.get_target_id_array_and_target_dict)(geometry_value_coordinates_array)

            # 延長計算
            length_list = WM.calc_func_time(self.logger)(
                self.create_length)(target_id_array, target_dict)

            output_field_set_file = WM.calc_func_time(self.logger)(self.create_output_field_set_file)(out_dwh_name,
                                                                                                      geometry_dwh,
                                                                                                      length_list)

            # プロパティでDWH名を入力した場合は上書き
            if out_dwh_name == "" or out_dwh_name is None:
                attributes_dict = {"DWH": geometry_dwh}

            else:
                attributes_dict = {"DWH": out_dwh_name}

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file, attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
