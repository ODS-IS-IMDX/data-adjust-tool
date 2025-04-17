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
# 後続処理で使用するために、座標配列からUUIDを作成するプロセッサ。
# --------------------------------------------------------------------------------------------

# 標準ライブラリ
import uuid
import traceback

from importlib import import_module

# 自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")


class CreateUUIDFromCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列からUUIDを作成する。
                        ①input: 座標配列を持った1行のFieldSetFile。
                        ②output: UUIDをフィールドとして持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'UUID', 'gml_id', 'Python']

    # UUIDの配列用のDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値取得する関数
        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            out_dwh_name: 変更後のDWH名
        """

        # プロパティで入力したDWH名を取得。
        out_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return out_dwh_name

    def get_id_array(self, geometry_value_coordinates_array):
        """
        概要:
            geometry_value_coordinates_arrayのIDの行を抽出し、IDの配列を生成する関数
        引数:
            geometry_value_coordinates_array: FieldSetFileに格納されている座標配列

        戻り値:
            target_id_array: IDの配列
        """

        # ID取得
        target_id_array = np.unique(geometry_value_coordinates_array[:, 0])

        return target_id_array

    def create_uuid_list(self, target_id_array):
        """
        概要:
            target_id_arrayからUUIDを作成し、リスト化する関数
        引数:
            target_id_array: IDの配列

        戻り値:
            uuid_list: UUIDが格納されたリスト
        """

        # UUID格納用リストを作成。
        uuid_list = []

        # 必要個数分のUUIDを生成し、リストに格納。格納時に接頭辞と結合。
        [uuid_list.append((target_id_array[ti], str(uuid.uuid4())))
         for ti in range(len(target_id_array))]

        return uuid_list

    def create_output_field_set_file(self, out_dwh_name, geometry_dwh, uuid_list):
        """
        概要:
            output用のFieldSetFileの作成
        引数:
            out_dwh_name: プロパティで入力したDWH名
            geometry_dwh: input_field_set_fileで格納されていたDWH名
            uuid_list: UUIDが格納されたリスト

        戻り値:
            output_field_set_file: 出力のFieldSetFile
        """

        # UUIDのリストのデータ型
        uuid_type = "object"

        # set_field_set_file用にリスト化
        geometry_dwh_list = [geometry_dwh]
        out_dwh_name_list = [out_dwh_name]
        uuid_type_list = [uuid_type]
        uuid_value_list = [uuid_list]

        # 新しいDWH名が空の場合はgeometryのDWH名を使う
        if out_dwh_name == "" or out_dwh_name is None:
            output_field_set_file = PBP.set_field_set_file(
                geometry_dwh_list, uuid_type_list, uuid_value_list)

        else:
            output_field_set_file = PBP.set_field_set_file(
                out_dwh_name_list, uuid_type_list, uuid_value_list)

        return output_field_set_file

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            out_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context,
                                                                    flowfile)

            # FieldSetFileから、配列をデコード、デシリアライズし使えるようにする。geometryのDWH名を取得。
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # ID取得
            target_id_array\
                = WM.calc_func_time(self.logger)(self.get_id_array)(geometry_value_coordinates_array)

            # UUID作成。
            uuid_list\
                = WM.calc_func_time(self.logger)(self.create_uuid_list)(target_id_array)

            # Value列にUUIDを持ったFieldSetFileを生成
            output_field_set_file\
                = WM.calc_func_time(self.logger)(self.create_output_field_set_file)(out_dwh_name,
                                                                                    geometry_dwh,
                                                                                    uuid_list)

            # プロパティでDWH名を入力した場合はAttributeのDWHをout_dwh_nameで更新。
            if out_dwh_name == "" or out_dwh_name is None:
                attributes_dict = {"DWH": geometry_dwh}

            else:
                attributes_dict = {"DWH": out_dwh_name}

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
