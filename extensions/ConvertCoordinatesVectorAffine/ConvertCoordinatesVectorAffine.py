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
# 基準GCPと対象データGCPからTINを生成し、対象データ（座標配列）のアフィン変換を行う
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback
import io
import pickle
import base64

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")


class ConvertCoordinatesVectorAffine(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列にアフィン変換処理を行う。
                        ①input: 3つの座標配列（位置基準点・補正対象点・補正対象）を持った3行のFieldSetFile。
                        ②output: 座標配列を持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'GCP', 'TIN', 'Python']

    # 処理後のDwh名を指定
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # 処理モード（2Dまたは3D）を指定
    CORRECT_MODE = PropertyDescriptor(
        name="Correct Mode",
        description="処理モード（2Dまたは3D）",
        default_value='2D',
        allowable_values=["2D", "3D"],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 座標の分割単位数を指定する
    SPLIT_UNIT_NUMBER = PropertyDescriptor(
        name="Split Unit Number",
        description="座標の分割単位数",
        default_value='100',
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # target_arrayのDWH
    TARGET_DWH_NAME = PropertyDescriptor(
        name="Target DWH Name",
        description="入力データの補正対象点の座標配列のDWH名",
        default_value='target',
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # Base_arrayのDWH
    BASE_DWH_NAME = PropertyDescriptor(
        name="Base DWH Name",
        description="入力データの位置基準点の座標配列のDWH名",
        default_value='Base',
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            CORRECT_MODE,
                            SPLIT_UNIT_NUMBER,
                            TARGET_DWH_NAME,
                            BASE_DWH_NAME]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要
            プロパティで入力した値取得する関数
        引数
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ
        戻り値
            output_dwh_name: 出力データのDWH名
            correct_mode: 処理モード
            split_unit_number: 座標の分割数
            target_dwh_name: 入力データの補正対象点のDWH名
            base_dwh_name: 入力データの位置基準点のDWH名
        """
        # 以下4つが、プロパティで取得
        # 角柱の入口にマルチパッチを作成するかのフラグをプロパティから取得
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        correct_mode = (context.getProperty(self.CORRECT_MODE).getValue())

        split_unit_number = int(context.getProperty(
            self.SPLIT_UNIT_NUMBER).getValue())

        target_dwh_name = context.getProperty(self.TARGET_DWH_NAME).getValue()

        base_dwh_name = context.getProperty(self.BASE_DWH_NAME).getValue()

        return output_dwh_name,  correct_mode, split_unit_number, target_dwh_name, base_dwh_name

    def extract_arrays_from_field_set_file(self, flowfile, target_dwh_name, base_dwh_name):
        """
        概要
            flowfileからカラムを除いた3行のfield_set_fileを抜き出し、Value列をデコードデシリアライズし配列に戻す関数
        引数
            flowfile: プロセッサに入ってくるデータ
            target_dwh_name: 入力データの補正対象点のDWH名
            base_dwh_name: 入力データの位置基準点のDWH名
        戻り値
            correction_target_gcp_array: 補正対象点の配列
            position_standard_gcp_array: 位置基準点の配列
            correction_target_feature_array: 補正対象の特徴量配列
            affine_target_array: アフィン変換対象の配列
        """
        # flowfileからCSV形式のfield_set_fileを抜き出し、文字列化し、データフレームに加工。
        input_field_set_dataframe = pd.read_csv(io.StringIO(
            flowfile.getContentsAsBytes().decode("utf-8")))

        # Dwh 列と Value 列のリストを抽出
        dwh_list = input_field_set_dataframe["Dwh"].tolist()
        value_list = input_field_set_dataframe["Value"].tolist()

        # dwh_listで各要素とそのindexを抜き出し、どれが補正対象点、位置基準点、補正対象なのかを判別し、Value列の値をデコードデシリアライズしarrayに変換する。
        for index, dwh in enumerate(dwh_list):
            decoded_value = pickle.loads(base64.b64decode(value_list[index]))

            if target_dwh_name in dwh:
                correction_target_gcp_array = decoded_value

            elif base_dwh_name in dwh:
                position_standard_gcp_array = decoded_value

            else:
                correction_target_feature_array = decoded_value
                affine_target_array = correction_target_feature_array[:, :4]

                # 現在のDWH名を取得(出力の際、OUTPUT_DWH_NAMEプロパティが空だった場合に使用)
                affine_target_dwh_name = dwh_list[index]

        return correction_target_gcp_array, position_standard_gcp_array, correction_target_feature_array, affine_target_array, affine_target_dwh_name

    def transform(self, context, flowfile):

        # カラムをのぞいた3行のfield_set_fileを読み込み、アフィン変換する
        try:
            output_dwh_name, correct_mode, split_unit_number, target_dwh_name, base_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            correction_target_gcp_array, \
                position_standard_gcp_array, \
                correction_target_feature_array, \
                affine_target_array, \
                affine_target_dwh_name\
                = WM.calc_func_time(self.logger)(self.extract_arrays_from_field_set_file)(flowfile, target_dwh_name, base_dwh_name)

            # -------------------------------------------------------------------------------------------------------------------------------------------------
            # ベクトルアフィン実行
            # execute_vector_affine(関数名候補)
            # -------------------------------------------------------------------------------------------------------------------------------------------------
            # 調整後のTINと補正対象地物をベクトルアフィンにセット
            if correct_mode == "2D":

                affine_target_array = WM.calc_func_time(self.logger)(NCP.calculate_geometry_coordinate_by_vector_affine_2D)(correction_target_gcp_array,
                                                                                                                            position_standard_gcp_array,
                                                                                                                            affine_target_array,
                                                                                                                            split_unit_number,
                                                                                                                            self.logger)

            else:

                affine_target_array = WM.calc_func_time(self.logger)(NCP.calculate_geometry_coordinate_by_vector_affine_3D)(correction_target_gcp_array,
                                                                                                                            position_standard_gcp_array,
                                                                                                                            affine_target_array,
                                                                                                                            split_unit_number,
                                                                                                                            self.logger)

            # -------------------------------------------------------------------------------------------------------------------------------------------------
            # arrayをfieldsetfileにし、出力
            # -------------------------------------------------------------------------------------------------------------------------------------------------
            correction_target_feature_array[:, :4] = affine_target_array
            geometry_type = "geometry"

            # 出力FieldSetFile仕様にリスト化
            output_dwh_name_list = [output_dwh_name]
            geometry_type_list = [geometry_type]
            coordinates_array_list = [correction_target_feature_array]
            affine_target_dwh_name_list = [affine_target_dwh_name]

            # プロパティでDWH名を入力した場合は上書き
            if output_dwh_name == "" or not output_dwh_name:
                attributes_dict = {"DWH": affine_target_dwh_name}
                output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                    affine_target_dwh_name_list, geometry_type_list, coordinates_array_list)

            else:
                attributes_dict = {"DWH": output_dwh_name}
                output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                    output_dwh_name_list, geometry_type_list, coordinates_array_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
