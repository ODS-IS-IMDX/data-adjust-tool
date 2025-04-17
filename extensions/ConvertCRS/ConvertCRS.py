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
# 座標配列/GeoDataFrameに対し、CRS変換を行う。
# --------------------------------------------------------------------------------------------
#  Python標準モジュール
import base64
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリを動的インポート
np = import_module("numpy")


class ConvertCRS(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        座標配列もしくはGeoDataFrameに対し、CRS変換を行う。
                        ①input: GeoDataFrameか、座標配列もしくはGeoDataFrameを持ったFieldSetFile。
                        ②output: 入力データのFieldSetFileの座標がCRS変換されたもの。もしくはCRS変換されたGeoDataFrame。
                      """
        tags = ['GeoDataFrame', 'EPSG', 'Python']

    TARGET_COORDINATES_OR_GEODATAFRAME_DWH_NAME = PropertyDescriptor(
        name="Input Target DWH Name",
        description="変換前の座標配列、もしくはGeoDataFrameのDWH名(未入力時は1行目を対象)",
        required=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    TARGET_COORDINATES_CRS = PropertyDescriptor(
        name="Target Coordinates CRS",
        description="変換前の座標のCRS",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="${crs}",
        sensitive=False
    )

    RESULT_COORDINATES_CRS = PropertyDescriptor(
        name="Output Coordinates CRS",
        description="変換後の座標のCRS",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    # ★プロパティを設定した後にlistへ格納
    property_descriptors = [TARGET_COORDINATES_OR_GEODATAFRAME_DWH_NAME,
                            TARGET_COORDINATES_CRS,
                            RESULT_COORDINATES_CRS
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
            target_coordinates_or_geodata_frame_dwh_name\
                = context.getProperty(self.TARGET_COORDINATES_OR_GEODATAFRAME_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            target_coordinates_crs\
                = int(context.getProperty(self.TARGET_COORDINATES_CRS).evaluateAttributeExpressions(flowfile).getValue())
            result_coordinates_crs\
                = int(context.getProperty(self.RESULT_COORDINATES_CRS).evaluateAttributeExpressions(flowfile).getValue())

            # --------------------------------------------------------------------------
            # flowfile→FieldSetFileのDataFrame
            # --------------------------------------------------------------------------
            not_field_set_file_flag = False
            try:

                # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得する
                serialize_dataframe = flowfile.getContentsAsBytes()

                # バイトデータからGeoDataFrameを復元する
                target_coordinates_array_or_geodata_frame = pickle.loads(
                    serialize_dataframe)
                not_field_set_file_flag = True
            except pickle.UnpicklingError:

                target_field_set_file_dataframe\
                    = WM.calc_func_time(self.logger)(DCP.convert_flowfile_to_field_set_file_dataframe)(flowfile)
                # --------------------------------------------------------------------------
                # FieldSetFileのDataFrameからValue列、DWH列のListを取得
                # --------------------------------------------------------------------------
                target_value_list, \
                    target_type_list, \
                    target_dwh_list\
                    = WM.calc_func_time(self.logger)(DCP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)

                # --------------------------------------------------------------------------
                # FieldSetFileのDataFrameから座標取り出し
                # --------------------------------------------------------------------------

                # 1行のとき
                if target_coordinates_or_geodata_frame_dwh_name is None or target_coordinates_or_geodata_frame_dwh_name == "":
                    target_coordinates_array_or_geodata_frame = pickle.loads(
                        base64.b64decode(target_value_list[0]))

                # 1行ではないとき
                else:
                    target_coordinates_array_or_geodata_frame\
                        = WM.calc_func_time(self.logger)(DCP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                                  target_dwh_list,
                                                                                                  target_coordinates_or_geodata_frame_dwh_name)

            if isinstance(target_coordinates_array_or_geodata_frame, np.ndarray):
                # --------------------------------------------------------------------------
                # pyprojによる座標系変換
                # --------------------------------------------------------------------------
                result_coordinates_array\
                    = WM.calc_func_time(self.logger)(DCP._convert_geometry_crs)(target_coordinates_array_or_geodata_frame, target_coordinates_crs, result_coordinates_crs
                                                                                )

                # --------------------------------------------------------------------------
                # 変換後座標配列返却
                # --------------------------------------------------------------------------
                decoded_result_coordinates_array = base64.b64encode(
                    pickle.dumps(result_coordinates_array)).decode("utf-8")

                # 1行ではないとき
                if target_coordinates_or_geodata_frame_dwh_name:

                    target_field_set_file_dataframe.loc[target_field_set_file_dataframe["Dwh"]
                                                        == target_coordinates_or_geodata_frame_dwh_name, "Type"] = 'geometry'
                    target_field_set_file_dataframe.loc[target_field_set_file_dataframe["Dwh"] ==
                                                        target_coordinates_or_geodata_frame_dwh_name, "Value"] = decoded_result_coordinates_array

                # 1行のとき
                else:
                    target_field_set_file_dataframe.iloc[0, 1] = 'geometry'
                    target_field_set_file_dataframe.iloc[0, 2] = decoded_result_coordinates_array

                # --------------------------------------------------------------------------
                # target_field_set_file_dataframeを出荷の形(csv形式)に変換
                # --------------------------------------------------------------------------
                output_value \
                    = target_field_set_file_dataframe.to_csv(index=False)
            else:
                target_coordinates_array_or_geodata_frame \
                    = target_coordinates_array_or_geodata_frame.to_crs(epsg=result_coordinates_crs)
                self.logger.error(f"GeoDataFrameを処理→CRS:{target_coordinates_array_or_geodata_frame.crs}")

                # --------------------------------------------------------------------------
                # target_field_set_file_dataframeを出荷の形(csv形式)に変換
                # --------------------------------------------------------------------------
                if not_field_set_file_flag:
                    output_value = pickle.dumps(
                        target_coordinates_array_or_geodata_frame)

                else:
                    decoded_result_geodata_frame = base64.b64encode(pickle.dumps(target_coordinates_array_or_geodata_frame)).decode("utf-8")
                    if target_coordinates_or_geodata_frame_dwh_name:

                        target_field_set_file_dataframe.loc[target_field_set_file_dataframe["Dwh"]
                                                            == target_coordinates_or_geodata_frame_dwh_name, "Type"] = 'GeoDataFrame'
                        target_field_set_file_dataframe.loc[target_field_set_file_dataframe["Dwh"] ==
                                                            target_coordinates_or_geodata_frame_dwh_name, "Value"] = decoded_result_geodata_frame

                    else:
                        self.logger.error(f"1行である")
                        target_field_set_file_dataframe.iloc[0, 1] \
                            = 'GeoDataFrame'
                        target_field_set_file_dataframe.iloc[0, 2] \
                            = decoded_result_geodata_frame

                    # --------------------------------------------------------------------------
                    # target_field_set_file_dataframeを出荷の形(csv形式)に変換
                    # --------------------------------------------------------------------------
                    output_value \
                        = target_field_set_file_dataframe.to_csv(index=False)

            # 結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=output_value,
                                           attributes={"crs": str(
                                               result_coordinates_crs)}
                                           )

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
