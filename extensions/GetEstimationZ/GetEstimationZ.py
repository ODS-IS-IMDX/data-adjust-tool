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
# 【プロセッサ概要】
# 座標配列に対して推定標高処理を行う
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
Point = import_module("shapely.geometry").Point
LineString = import_module("shapely.geometry").LineString
Polygon = import_module("shapely.geometry").Polygon


class GetEstimationZ(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列に対して推定標高処理を行う。
                        ①input: 座標配列1行のFieldSetFile。
                        ②output: 座標配列1行のFieldSetFile。
                      """
        tags = ["Coordinates", "Python"]

    # 出力時のデータのDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [OUTPUT_DWH_NAME]

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            output_dwh_name: 座標データ(データフレーム)のDWH名
        """

        # プロパティで設定した値を取得
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):

        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            output_dwh_name = WM.calc_func_time(self.logger)(
                self.get_property)(context, flowfile)

            # 1行のFieldSetFileのValue列を抜きだす。
            field_set_file_dataframe, \
                geometry_dwh_name, \
                target_type, \
                coordinates_array\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 線分の端点以外に付与するZ座標を算出し配列に格納する
            target_range = range(len(np.unique(coordinates_array[:, 0])))

            # estimated_unit_geometry_point_list = [LineString(base.get_estimation_Z(geometry_point_numpy[geometry_point_numpy[:,0] == i])[:,1:].tolist()) for i in target_range]
            # get_estimation_Zはxyz座標を引数とする。上記のコードはidごと渡していたためエラーとなった
            all_results = []

            for i in range(len(target_range)):

                # 入力データ範囲を抽出
                subset = coordinates_array[coordinates_array[:, 0] == i, 1:]

                # 標高予測関数呼び出し
                est_z_values = WM.calc_func_time(
                    self.logger)(NSP.get_estimation_Z)(subset)

                # 結果にIDを付与して格納
                result_with_id = np.hstack(
                    (np.full((est_z_values.shape[0], 1), i), est_z_values))

                # 結果をリストに追加
                all_results.append(result_with_id)

            output_array = np.vstack(all_results)

            dwh_list = [output_dwh_name]
            type_list = ["geometry"]
            value_list = [output_array]

            output_field_set_file = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                                           type_list,
                                                                                           value_list
                                                                                           )

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
