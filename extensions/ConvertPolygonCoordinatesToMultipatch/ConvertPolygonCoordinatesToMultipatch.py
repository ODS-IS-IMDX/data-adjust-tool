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

# ----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】polygonからマルチパッチを作成
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
from io import StringIO
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

CREATE = "作成する"
NOT_CREATE = "作成しない"


class ConvertPolygonCoordinatesToMultipatch(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        Polygonをマルチパッチに変換する。
                        ①input: Polygonのジオメトリが入ったリストと、高さを持った2行のFieldSetFile。
                        ②output: マルチパッチ化した座標配列を持った、1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'Multipatch', 'Polygon', 'Python']

    # 角柱の入口にマルチパッチを作成するかのフラグを設定する。
    START_MULTIPATCH_FLAG = PropertyDescriptor(
        name="Start Multipatch Flag",
        description="角柱の入口にマルチパッチを作成するフラグ",
        required=True,
        allowable_values=[CREATE, NOT_CREATE],
        default_value=CREATE,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 角柱の出口にマルチパッチを作成するかのフラグを設定する。
    END_MULTIPATCH_FLAG = PropertyDescriptor(
        name="End Multipatch Flag",
        description="角柱の出口にマルチパッチを作成するフラグ",
        required=True,
        allowable_values=[CREATE, NOT_CREATE],
        default_value=CREATE,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 幅のDWH名を指定する。
    CENTER_DWH_NAME = PropertyDescriptor(
        name="Center DWH Name",
        description="ポリゴンのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 高さのDWH名を指定する。
    HEIGHT_DWH_NAME = PropertyDescriptor(
        name="Height DWH Name",
        description="高さのDWH名(埋設物の場合-値で設定)",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 新しくgeometryのDWH名を指定する。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [START_MULTIPATCH_FLAG,
                            END_MULTIPATCH_FLAG,
                            CENTER_DWH_NAME,
                            HEIGHT_DWH_NAME,
                            OUTPUT_DWH_NAME]

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
            start_multipatch_flag: 角柱の入口にマルチパッチを作成するかのフラグ
            end_multipatch_flag: 角柱の出口にマルチパッチを作成するかのフラグ
            center_dwh_name: polygonのDWH
            height_dwh_name: 高さのDWH
            output_dwh_name: output_field_set_file用のgeometryのDWH
        """

        # 以下5つをNifiのプロパティから取得
        # 角柱の入口にマルチパッチを作成するかのフラグ
        start_multipatch_flag = context.getProperty(
            self.START_MULTIPATCH_FLAG).getValue()

        if start_multipatch_flag == CREATE:
            start_multipatch_flag = 0
        else:
            start_multipatch_flag = 1

        # 角柱の出口にマルチパッチを作成するかのフラグ
        end_multipatch_flag = context.getProperty(
            self.END_MULTIPATCH_FLAG).getValue()

        if end_multipatch_flag == CREATE:
            end_multipatch_flag = 0
        else:
            end_multipatch_flag = 1

        # polygonのDWH
        center_dwh_name = context.getProperty(
            self.CENTER_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 高さのDWH
        height_dwh_name = context.getProperty(
            self.HEIGHT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # output_field_set_file用のgeometryのDWH
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return start_multipatch_flag, end_multipatch_flag, center_dwh_name, height_dwh_name, output_dwh_name

    def get_depth_width_list_and_coordinates_array(self, flowfile, center_dwh_name, height_dwh_name):
        """
        概要:
            field_set_fileのValue列の値を、デコード、デシリアライズしcoordinates_arrayとheight_arrayを作成

        引数:
            flowfile: プロセッサに入ってくるデータ
            center_dwh_name: polygonのDWH
            height_dwh_name: 高さのDWH

        戻り値:
            coordinates_array: 座標配列
            height_array: 高さ属性が入ったlist
        """

        # flowfileのデータをNifiのプロパティから取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # flowfileのデータを、pathとして認識させる
        field_set_file_stringio = StringIO(input_field_set_file)

        # DataFrameに変換
        field_set_file_data_frame = pd.read_csv(field_set_file_stringio)

        coordinates_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == center_dwh_name, "Value"].values[0]))

        height_value = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == height_dwh_name, "Value"].values[0]))
        height_array = np.array(height_value, dtype=np.float64)

        return coordinates_array, height_array

    def get_coordinates_by_id(self, coordinates_array):
        """
        概要:
            地物IDごとの座標の辞書とユニークな地物IDを返す

        引数:
            coordinates_array: 座標配列

        戻り値:
            id_unique_array: ユニークな地物ID
            id_coordinate_dict: IDごとの構成点座標

        """

        # 地物ごとにデータを分ける
        # 地物IDarray→抽出済みのIDになる
        # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
        id_unique_array = np.unique(coordinates_array[:, 0])
        id_coordinate_dict = {id_unique_array[i]: coordinates_array[list(np.where(
            coordinates_array[:, 0] == id_unique_array[i])[0]), 1:4] for i in range(len(id_unique_array))}

        return id_unique_array, id_coordinate_dict

    def generate_multi_patch(self, id_unique_array, id_coordinate_dict, height_array, start_multipatch_flag, end_multipatch_flag):
        """
        概要:
            マルチパッチを生成

        引数:
            id_unique_array: ユニークな地物ID
            id_coordinate_dict: IDごとの構成点座標
            height_array: 高さのIDと値の入った配列
            start_multipatch_flag: 角柱の入口にマルチパッチを作成するかのフラグ
            end_multipatch_flag: 角柱の出口にマルチパッチを作成するかのフラグ

        戻り値:
            coordinates_array: 座標配列

        """

        # 結果格納用
        multi_patch_list = []

        # 入力shapefileのジオメトリ数だけマルチパッチ生成
        for i in range(len(id_unique_array)):

            # 1ジオメトリからマルチパッチ生成
            temporary_multipatch_array = (NCP.get_multipatch_array_from_polygon)(
                id_coordinate_dict[id_unique_array[i]], height_array[i, 1], int(start_multipatch_flag), int(end_multipatch_flag))

            # ポリゴンを閉じるために先頭の点を4点目に設定
            multipatch_array = np.concatenate([temporary_multipatch_array, temporary_multipatch_array[:, 0:1, :]], axis=1).reshape(
                (len(temporary_multipatch_array)*4, 6))

            # マルチパッチジオメトリID生成
            multipatch_geometry_id_array = np.repeat(np.arange(
                0, len(temporary_multipatch_array)), 4).reshape(len(multipatch_array), 1)

            # 地物ID生成
            feature_id_array = np.array(
                len(multipatch_array) * [id_unique_array[i]]).reshape(len(multipatch_array), 1)

            # ID+xyz+multi_IDの形に結合
            multipatch_array = np.concatenate(
                [feature_id_array, multipatch_array, multipatch_geometry_id_array], axis=1)

            multi_patch_list.append(multipatch_array)

        coordinates_array = np.concatenate(multi_patch_list, 0)

        return coordinates_array

    def transform(self, context, flowfile):

        try:

            start_multipatch_flag, \
                end_multipatch_flag, \
                center_dwh_name, \
                height_dwh_name, \
                output_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            coordinates_array, height_array \
                = self.get_depth_width_list_and_coordinates_array(flowfile, center_dwh_name, height_dwh_name)

            # -----------------------------------------------------------------------------------------------------------
            # 【処理】1ジオメトリごとのインデックス取得
            # -----------------------------------------------------------------------------------------------------------
            id_unique_array, id_coordinate_dict\
                = WM.calc_func_time(self.logger)(self.get_coordinates_by_id)(coordinates_array)

            # -----------------------------------------------------------------------------------------------------------
            # 【処理】1ジオメトリごと分散処理で角柱を生成
            # -----------------------------------------------------------------------------------------------------------
            # ジオメトリ数だけマルチパッチ生成
            output_coordinates_array\
                = WM.calc_func_time(self.logger)(self.generate_multi_patch)(id_unique_array,
                                                                            id_coordinate_dict,
                                                                            height_array,
                                                                            start_multipatch_flag,
                                                                            end_multipatch_flag)

            output_dwh_list = [output_dwh_name]
            output_type_list = ["geometry"]
            output_value_list = [output_coordinates_array]

            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_list,
                                                                                           output_type_list,
                                                                                           output_value_list
                                                                                           )

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
