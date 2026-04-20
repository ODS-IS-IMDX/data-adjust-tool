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
# シリアライズされたGeoDataFrame又は、DataFrameをFieldSetFileに変換する。
# ※GeoDataFrame、DataFrame以外は未対応。
# --------------------------------------------------------------------------------------------

# python標準モジュール
import traceback
import pickle
from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")


class ConvertGeoDataFrameFromContentsToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        シリアライズされたGeoDataFrame又は、DataFrameをFieldSetFileに変換する。
                        ※GeoDataFrame、DataFrame以外は未対応。
                        ①input: シリアライズされたGeoDataFrame又は、DataFrame
                        ②output: GeoDataFrame又は、DataFrameを持った1行のFieldSetFile
                      """
        tags = ['Contents', 'FieldSetFile', 'GeoDataFrame', 'DataFrame', 'Python']

    # 出力データのDWH名
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
        # 出力FieldSetFileのDWH名取得
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name

    def get_dataframe_from_flowfile(self, flowfile):
        # binary文字を想定し、flowfileからシリアライズされたデータフレームを受け取る。
        dataframe = pickle.loads(flowfile.getContentsAsBytes())

        return dataframe

    def get_target_type_from_dataframe(self, datafarme):
        # 出力するFieldSetFileのType列の値を取得
        if isinstance(datafarme, gpd.GeoDataFrame):
            target_type = DDC.GEODATAFRAME
        elif isinstance(datafarme, pd.DataFrame):
            target_type = DDC.DATAFRAME

        # GeoDataFrameでもDataFrameでもない場合は出力しない。
        else:
            self.logger.error(f"入力されたデータが、GeoDataFrame、DataFrameではない。入力された型:{type(datafarme).__name__}")
            return FlowFileTransformResult(relationship="failure")

        return target_type

    def transform(self, context, flowfile):
        try:
            # プロパティで入力した値を取得する
            output_dwh_name = WM.calc_func_time(
                self.logger)(self.get_property)(context, flowfile)

            # flowfileからDataFrameを取得
            dataframe = \
                WM.calc_func_time(self.logger)(
                    self.get_dataframe_from_flowfile)(flowfile)

            # 出力するFieldSetFileのType列の値を取得
            target_type = WM.calc_func_time(self.logger)(
                self.get_target_type_from_dataframe)(dataframe)

            # FieldSetFileに加工
            output_field_set_file = \
                WM.calc_func_time(self.logger)(PBP.set_field_set_file)(
                    [output_dwh_name], [target_type], [dataframe])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes={"DWH": output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
