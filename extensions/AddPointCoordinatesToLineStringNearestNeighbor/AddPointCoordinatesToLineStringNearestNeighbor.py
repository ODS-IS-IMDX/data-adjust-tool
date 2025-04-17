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
# ポイントをラインにスナップし、その点をラインの構成点に追加する
# スナップした距離が閾値を超える場合は構成点を増やさない
# スナップ対象が複数地物ある場合は距離の閾値を設けて複数のラインに対して構成点を追加する
# --------------------------------------------------------------------------------------------
# Python標準ライブラリ
import traceback

from importlib import import_module


# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")


class AddPointCoordinatesToLineStringNearestNeighbor(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """ポイントをラインにスナップし、その点をラインの構成点に追加する"""
        tags = ["python", "pandas", "Coordinates"]

    # 処理後のDwh名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="処理後のDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # スナップ対象ポイントのDwh名
    POINT_DWH_NAME = PropertyDescriptor(
        name="Point Dwh Name",
        description="スナップ対象ポイントのDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # スナップ先のラインとなるDwh名
    LINESTRING_DWH_NAME = PropertyDescriptor(
        name="Linestring Dwh Name",
        description="スナップ先のラインとなるDwh名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # ポイントとラインの距離において同一構成点と判定する閾値
    MINIMUM_THRESHOLD = PropertyDescriptor(
        name="Minimum Threshold",
        description="ポイントとラインの距離において同一構成点と判定する閾値",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # ポイントとラインが紐づかないとするときの閾値
    SNAP_MAXIMUM_DISTANCE = PropertyDescriptor(
        name="Snap Maximum Distance",
        description="ポイントとラインが紐づかないとするときの閾値",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            POINT_DWH_NAME,
                            LINESTRING_DWH_NAME,
                            MINIMUM_THRESHOLD,
                            SNAP_MAXIMUM_DISTANCE]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # 処理後のDwh名
            output_dwh_name\
                = context.getProperty(self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # スナップ対象ポイントのDwh名
            point_dwh_name\
                = context.getProperty(self.POINT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # スナップ先のラインとなるDwh名
            linestring_dwh_name\
                = context.getProperty(self.LINESTRING_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # ポイントとラインの距離において同一構成点と判定する閾値
            minimum_threshold\
                = float(context.getProperty(self.MINIMUM_THRESHOLD).evaluateAttributeExpressions(flowfile).getValue())

            # ポイントとラインが紐づかないとするときの閾値
            snap_maximum_distance\
                = float(context.getProperty(self.SNAP_MAXIMUM_DISTANCE).evaluateAttributeExpressions(flowfile).getValue())
            # ---------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # flowfile→field_set_fileのdataframe
            # --------------------------------------------------------------------------
            target_field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(NSP.convert_flowfile_to_field_set_file_dataframe)(flowfile)

            # --------------------------------------------------------------------------
            # フィールド集合ファイルのdataframeからValue列、Dwh列のListを取得
            # --------------------------------------------------------------------------
            target_value_list, \
                target_type_list, \
                target_dwh_list\
                = WM.calc_func_time(self.logger)(NSP.get_value_dwh_list_from_field_set_file_dataframe)(target_field_set_file_dataframe)

            # --------------------------------------------------------------------------
            # field_set_fileのdataframeから座標取り出し
            # --------------------------------------------------------------------------
            target_point_array\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          point_dwh_name)

            target_linestring_array\
                = WM.calc_func_time(self.logger)(NSP.get_value_field_from_value_dwh_list)(target_value_list,
                                                                                          target_dwh_list,
                                                                                          linestring_dwh_name)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # スナップ処理
            # --------------------------------------------------------------------------
            temp_distance_array, \
                temp_coordinates_array, \
                temp_linestring_index_array\
                = WM.calc_func_time(self.logger)(NCP.get_snapped_point_to_linestring_multi)(target_point_array,
                                                                                            target_linestring_array,
                                                                                            minimum_threshold)

            # スナップ後はidがないので追加（ラインに対して複数の点が紐づくので元のidを保持できない）
            point_index_array = np.arange(len(temp_coordinates_array))
            point_index_array = point_index_array.reshape(
                (len(point_index_array), 1))
            temp_coordinates_array\
                = np.concatenate([point_index_array,
                                 temp_coordinates_array], axis=1)

            # --------------------------------------------------------------------------
            # 構成点追加処理
            # --------------------------------------------------------------------------
            result_coordinates_array\
                = WM.calc_func_time(self.logger)(NCP._add_snapped_point_to_linestring)(target_linestring_array,
                                                                                       temp_coordinates_array,
                                                                                       temp_linestring_index_array,
                                                                                       minimum_threshold,
                                                                                       snap_maximum_distance)

            # --------------------------------------------------------------------------
            # フィールド集合ファイルListをフィールド集合ファイルに変換
            # --------------------------------------------------------------------------
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
                                                                                        'geometry',
                                                                                        result_coordinates_array)

            output_field_set_file\
                = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(target_dwh_list,
                                                                                 target_type_list,
                                                                                 target_value_list)
            # --------------------------------------------------------------------------

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
