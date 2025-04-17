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

# ------------------------------------------------------------------------------
# 【共通】土被り分始点、終点のZ値を減算する
# ------------------------------------------------------------------------------

# 外部ライブラリ
import traceback

from importlib import import_module

# 使用パッケージimport
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
np = import_module("numpy")


class SubtractDepthFromZValue(FlowFileTransform):
    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        LineStringの端点それぞれに上流土被りと下流土被りを適用する。
                        ①input: 座標配列とLineStringの端点に適用するフィールドデータ2つを持った3行のFieldSetFile。
                        ②output: 座標配列1行のFieldSetFile。
                      """
        tags = ["Coordinates", "EarthCover", "Python"]

    # 座標配列のDWH名
    COORDINATES_NAME = PropertyDescriptor(
        name="Coordinates DWH Name",
        description="座標配列のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 始点から減算する値を持つDWH名
    START_DEPTH = PropertyDescriptor(
        name="Start Depth DWH Name",
        description="始点から減算する値を持つDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 終点から減算する値を持つDWH名
    END_DEPTH = PropertyDescriptor(
        name="Target Extent DWH Name",
        description="終点から減算する値を持つDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 土被り分始点、終点のZ値を減算した座標配列のDWH名
    RESULT_DWH_NAME = PropertyDescriptor(
        name="Output Coordinates DWH Name",
        description="土被り分始点、終点のZ値を減算した座標配列のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # ★プロパティを設定した後にlistへ格納
    property_descriptors = [COORDINATES_NAME,
                            START_DEPTH,
                            END_DEPTH,
                            RESULT_DWH_NAME
                            ]

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # --------------------------------------------------------------------------
            coordinates_name\
                = context.getProperty(self.COORDINATES_NAME).evaluateAttributeExpressions(flowfile).getValue()
            start_depth\
                = context.getProperty(self.START_DEPTH).evaluateAttributeExpressions(flowfile).getValue()
            end_depth\
                = context.getProperty(self.END_DEPTH).evaluateAttributeExpressions(flowfile).getValue()
            result_dwh_name\
                = context.getProperty(self.RESULT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # --------------------------------------------------------------------------
            # flowfile→FieldSetFileのDataFrame
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(DCP.convert_flowfile_to_field_set_file_dataframe)(flowfile)

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameからValue列、Dwh列のリストを取得
            # --------------------------------------------------------------------------
            target_value_list, target_type_list, target_dwh_list\
                = WM.calc_func_time(self.logger)(DCP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)

            # --------------------------------------------------------------------------
            # 座標値取得
            # --------------------------------------------------------------------------
            target_coordinates_array\
                = WM.calc_func_time(self.logger)(DCP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          coordinates_name
                                                                                          )
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 始点から減算する属性値取得
            # --------------------------------------------------------------------------
            start_depth_array\
                = WM.calc_func_time(self.logger)(DCP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          start_depth
                                                                                          )
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 終点から減算する属性値取得
            # --------------------------------------------------------------------------
            end_depth_array\
                = WM.calc_func_time(self.logger)(DCP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          end_depth
                                                                                          )
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 始点終点のインデックス取得 地物ごとの処理を行うため、地物数を把握するため
            # --------------------------------------------------------------------------
            start_index_array, end_index_array\
                = WM.calc_func_time(self.logger)(DCP.get_start_index_and_end_index)(target_coordinates_array)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 始点のインデックス指定→元のZ値から土被りを引いてを更新
            # --------------------------------------------------------------------------
            target_coordinates_array[start_index_array, 3]\
                = target_coordinates_array[start_index_array, 3]-np.array(start_depth_array, dtype=np.float64)[:, 1]
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 終点のインデックス指定→元のZ値から土被りを引いてを更新
            # --------------------------------------------------------------------------
            target_coordinates_array[end_index_array, 3]\
                = target_coordinates_array[end_index_array, 3]-np.array(end_depth_array, dtype=np.float64)[:, 1]
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 土被り分下げた配列を設定
            # --------------------------------------------------------------------------
            # 土被り分Z値を減算したFieldSetFileを出力するためここで初期化
            target_value_list = []
            target_type_list = []
            target_dwh_list = []

            target_dwh_list, target_type_list, target_value_list\
                = WM.calc_func_time(self.logger)(DCP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        result_dwh_name,
                                                                                        'float64',
                                                                                        target_coordinates_array
                                                                                        )

            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFile構成要素の各リストをFieldSetFileに変換
            # --------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(DCP.set_list_to_field_set_file)(target_dwh_list,
                                                                                 target_type_list,
                                                                                 target_value_list
                                                                                 )
            # --------------------------------------------------------------------------

            # 結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file
                                           )

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
