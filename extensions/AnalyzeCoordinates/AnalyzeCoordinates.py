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
# 座標配列の１地物ごとで最大,最小、平均を計算を作成する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
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

X_COLUMN = "X"
Y_COLUMN = "Y"
Z_COLUMN = "Z"

MAX_METHOD = "最大値"
MIN_METHOD = "最小値"
AVERAGE_METHOD = "平均値"

class AnalyzeCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列の指定された列から１地物ごとで最大値、最小値、平均値を取得する。
                        ①input: 座標配列を持った1行のFieldSetFile。
                        ②output: 指定した列の最大値、最小値、平均値のいずれかのフィールドを持った1行のFieldSetFile。
                      """
        tags = ['Coordinates', "max", "min", "average", 'Python']

    # 最大深度のリストのDWH名
    TARGET_COORDINATES_COLUMN = PropertyDescriptor(
        name="Target Coordinates Column",
        description="対象の座標配列の列",
        allowable_values=[X_COLUMN, Y_COLUMN, Z_COLUMN],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)

    # 最小深度のリストのDWH名
    SELECT_CALCULATE_METHOD = PropertyDescriptor(
        name="Selected Calculate Method",
        description="出力するデータの計算メソッド",
        allowable_values=[MAX_METHOD, MIN_METHOD, AVERAGE_METHOD],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)

    # 出力データの平均のリストのDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)

    property_descriptors = [TARGET_COORDINATES_COLUMN,
                            SELECT_CALCULATE_METHOD,
                            OUTPUT_DWH_NAME]

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
            target_coordinates_index: 対象の座標配列の列のindex
            select_calculate_method: 最少最大平均ののどれを取得するかのフラグ
            output_dwh_name: 出力時のDWHファイル名
        """

        # プロパティから取得する
        target_coordinates_column = context.getProperty(
            self.TARGET_COORDINATES_COLUMN).evaluateAttributeExpressions(flowfile).getValue()

        if target_coordinates_column == X_COLUMN:
            target_coordinates_index = 1

        elif target_coordinates_column == Y_COLUMN:
            target_coordinates_index = 2
            
        else:
            target_coordinates_index = 3

        select_calculate_method = context.getProperty(
            self.SELECT_CALCULATE_METHOD).evaluateAttributeExpressions(flowfile).getValue()

        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return target_coordinates_index, select_calculate_method, output_dwh_name

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

    def calculate_coordinates_array(self, target_id_array, target_dict, select_calculate_method, target_coordinates_index):
        """
        概要:
            IDごとのXYZ座標を含む辞書とIDの配列を取得

        引数:
            target_id_array: IDの配列
            target_dict: IDごとのXYZ座標を含む辞書のタプル
            select_calculate_method: 最少、最大、平均
            target_coordinates_index: x y zのどこを計算するのか

        戻り値:
            calculated_list - IDごとの最少、最大、平均のいずれかが格納されたリスト

        """

        calculated_list = []

        if select_calculate_method == MAX_METHOD:

            for tid in target_id_array:
                values = target_dict[tid][:, target_coordinates_index]
                calculated_list.append((tid, round(np.max(values), 3)))

        elif select_calculate_method == MIN_METHOD:
            for tid in target_id_array:
                values = target_dict[tid][:, target_coordinates_index]
                calculated_list.append((tid, round(np.min(values), 3)))

        else:
            for tid in target_id_array:
                values = target_dict[tid][:, target_coordinates_index]
                calculated_list.append((tid, round(np.average(values), 3)))

        return calculated_list

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------

    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            target_coordinates_index, \
                select_calculate_method, \
                output_dwh_name\
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

            # IDごとに最少最大平均のいずれかのリストを作成
            calculated_list\
                = WM.calc_func_time(self.logger)(self.calculate_coordinates_array)( target_id_array,
                                                                                    target_dict,
                                                                                    select_calculate_method,
                                                                                    target_coordinates_index)

            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)([output_dwh_name], ["float64"], [calculated_list])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes={"DWH":output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
