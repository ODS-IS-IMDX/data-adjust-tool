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
# 座標系変換 CRSをパラメータファイルを用いて変更
# --------------------------------------------------------------------------------------------

#  標準モジュール
import base64
import pickle
import traceback

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult


class TransformCoordinateUsingParameterFile(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        パラメータファイル（.par）に基づき、座標配列のCRSを変換する。
                        ①input: Z座標を含む座標配列を持った1行のFieldSetFile。
                        ②output: 座標配列1行のFieldSetFile。
                      """
        tags = ['Coordinates', 'CRS', 'Python']

    PARAMETER_FILE_PATH = PropertyDescriptor(
        name="Parameter File Path",
        description="パラメータファイル（.par）のパス",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    EXCHANGE_CRS = PropertyDescriptor(
        name="Exchange CRS",
        description="測地系の変換パターン",
        required=True,
        sensitive=False,
        allowable_values=["旧測地系からJGD2000", "JGD2000からJGD2011"],
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    # ★プロパティを設定した後にlistへ格納
    property_descriptors = [
        PARAMETER_FILE_PATH,
        EXCHANGE_CRS
    ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # --------------------------------------------------------------------------
            parameter_file_path\
                = context.getProperty(self.PARAMETER_FILE_PATH).getValue()

            exchange_CRS\
                = context.getProperty(self.EXCHANGE_CRS).getValue()

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
            # FieldSetFileのDataFrameから座標取り出し
            # --------------------------------------------------------------------------
            target_coordinates_array = pickle.loads(
                base64.b64decode(target_value_list[0]))
            # --------------------------------------------------------------------------
            # tky2jgdによる座標系変換
            # --------------------------------------------------------------------------
            result_coordinates_array\
                = WM.calc_func_time(self.logger)(DCP.transform_coordinate_using_parameter_file)(target_coordinates_array[:, 1:3],
                                                                                                parameter_file_path
                                                                                                )

            target_coordinates_array[:, 1:3] = result_coordinates_array
            # --------------------------------------------------------------------------
            # 変換後座標配列返却
            # --------------------------------------------------------------------------
            target_type_list = ['geometry']
            target_value_list = [base64.b64encode(
                pickle.dumps(target_coordinates_array)).decode("utf-8")]
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

            # Attributes用のcrsを設定
            if exchange_CRS == "旧測地系からJGD2000":
                crs = "4612"
            else:
                crs = "6668"

            # 結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes={"crs": crs}
                                           )

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
