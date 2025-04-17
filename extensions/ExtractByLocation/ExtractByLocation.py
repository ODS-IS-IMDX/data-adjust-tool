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
# 【プロセッサ概要】場所による抽出
#  抽出対象のGeoDataFrameと比較対象のGeoDataFrameを指定して地物を抽出する
#  抽出後の形式は、GeoDataFrameをシリアライズしValueに設定したFieldSetFileとする。
#  抽出対象、比較対象ともに複数の地物を容認するがn:mだと処理時間が膨大になる恐れあり
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")


class ExtractByLocation(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """抽出対象のgeodataframeと比較対象のgeodataframeを指定して地物を抽出する"""
        tags = ["python", "pandas", "geopandas"]

    # 抽出後のDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="output dwh name",
        description="抽出後のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 抽出対象のGeoDataFrame
    INPUT_GEODATAFRAME_DWH_NAME = PropertyDescriptor(
        name="input geodataframe dwh name",
        description="抽出対象のgeodataframeが設定されているDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 比較対象のGeoDataFrame
    TARGET_TO_COMPARE_GEODATAFRAME_DWH_NAME = PropertyDescriptor(
        name="target to compare geodataframe dwh name",
        description="比較対象のgeodataframe",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 抽出条件
    EXTRACT_CONDITION = PropertyDescriptor(
        name="extract condition",
        description="抽出条件",
        required=True,
        allowable_values=DDC.EXTRACT_CONDITION_LIST,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            INPUT_GEODATAFRAME_DWH_NAME,
                            TARGET_TO_COMPARE_GEODATAFRAME_DWH_NAME,
                            EXTRACT_CONDITION]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # 抽出後のDWH名
            output_dwh_name\
                = context.getProperty(self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 抽出対象のGeoDataFrame
            input_geodataframe_dwh_name\
                = context.getProperty(self.INPUT_GEODATAFRAME_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 比較対象のGeoDataFrame
            target_to_compare_geodataframe_dwh_name\
                = context.getProperty(self.TARGET_TO_COMPARE_GEODATAFRAME_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 抽出条件
            extract_condition\
                = context.getProperty(self.EXTRACT_CONDITION).evaluateAttributeExpressions(flowfile).getValue()
            # ---------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # flowfile→FieldSetFileのDataFrame
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(NSP.convert_flowfile_to_field_set_file_dataframe)(flowfile)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFileのDataFrameからValue列、Dwh列のListを取得
            # --------------------------------------------------------------------------
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 抽出対象、比較対象をそれぞれ取得
            # --------------------------------------------------------------------------
            input_geodataframe\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          input_geodataframe_dwh_name)

            target_to_compare_geodataframe\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          target_to_compare_geodataframe_dwh_name)
            # --------------------------------------------------------------------------

            # 検索条件を指定して地物を抽出
            target_bool\
                = WM.calc_func_time(self.logger)(NSP.extract_by_location_bool_array)(input_geodataframe,
                                                                                     target_to_compare_geodataframe,
                                                                                     extract_condition)

            # FieldSetFileを選択した場合、GeoDataFrameをpickle形式でシリアライズしてValueに設定する。
            # Dwhは引数のファイル名に、Typeは'GeoDataFrame'とする
            target_dwh_list = []
            target_type_list = []
            target_value_list = []

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        output_dwh_name,
                                                                                        DDC.CONTENTS_CODE_GEODATAFRAME,
                                                                                        input_geodataframe[target_bool].reset_index(drop=True))

            # --------------------------------------------------------------------------
            # FieldSetFile構成要素の各リストをFieldSetFileに変換
            # --------------------------------------------------------------------------
            output_value\
                = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(target_dwh_list,
                                                                                 target_type_list,
                                                                                 target_value_list)
            # --------------------------------------------------------------------------

            # 結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=output_value)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
