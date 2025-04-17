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
# geometryのValue列を座標データに変換するプロセッサ。
# --------------------------------------------------------------------------------------------

# python 標準ライブラリ
import traceback

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope


class ConvertGeometryToCoordinates(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ジオメトリを座標配列に変換する。
                        ①input: ジオメトリ1行のFieldSetFile。
                        ②output: 座標配列を持った1行のFieldSetFile。
                      """
        tags = ['Geometry', 'Python']

    # 処理後のDWH名を指定する
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):

        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context         - プロセッサの設定値が格納されているデータ
            flowfile        - プロセッサに入ってくるデータ

        戻り値:
            output_dwh_name - 処理後のDWH名

        """

        # プロパティで入力した値を取得
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # Nifiのプロパティで取得したDWH名を取得
            output_dwh_name = WM.calc_func_time(self.logger)(
                self.get_property)(context, flowfile)

            # 取得したFieldSetFile(csv)をDataFrameに変換、Value列だけを抜きだし、base64でデコード、pickleでデシリアライズを行う。
            field_set_file_dataframe, \
                geometry_dwh, \
                geometry_type, \
                geometry_value_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # DataFrameに変換されたValueの値(geometry_value_list)を座標データ(配列)に変換する。
            coordinates_array = WM.calc_func_time(self.logger)(
                NSP.get_geometries_points_numpy)(geometry_value_list)

            # set_field_set_fileで使用するため戻り値の要素をリスト化
            output_dwh_name_list = [output_dwh_name]
            geometry_type_list = [geometry_type]
            coordinates_array_list = [coordinates_array]

            # set_field_set_file : pickleによるシリアライズ、base64でのデコード、utf-8でのエンコード処理を行う。
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_name_list,
                                                                         geometry_type_list,
                                                                         coordinates_array_list)

            # プロパティでDWH名を入力した場合は上書き。
            if output_dwh_name == "" or output_dwh_name is None:
                attributes_dict = {"DWH": geometry_dwh}

            else:
                attributes_dict = {"DWH": output_dwh_name}

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file, attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
