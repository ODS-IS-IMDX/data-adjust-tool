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
# LinkDataのCSVから空間IDを取得し、
# 空間IDの中心点とLinkDataのCSVの属性をFieldSetFileの形式で出力する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import zipfile
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")

ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class ConvertLinkCSVToSpatialIDCenterPoint(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """LinkDataのCSVから空間IDのの中心点を生成するプロセッサ。処理結果をZIP圧縮することも可能。"""
        tags = ["SpatialID", "ZIP", "Unpack", "python"]

    # 空間IDのsplit文字列
    SPATIAL_ID_SPLIT_STRING = PropertyDescriptor(
        name="SPATIAL_ID_SPLIT_STRING",
        description="空間IDの区切り文字",
        default_value="/",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 空間IDのズームレベル文字列
    # LinkDataのCSVにはすべて一律のズームレベルの空間IDが設定されているものとする
    SPATIAL_ID_ZOOM_LEVEL = PropertyDescriptor(
        name="SPATIAL_ID_ZOOM_LEVEL",
        description="空間IDのズームレベル",
        default_value="26",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 出力データである空間IDのDWH名
    # ※DWH名は以下の通り出力する
    # １，LinkDataの空間ID中心点座標（属性分複数登録する）
    # ２，3DTilesやCityGMLにおける出力対象図郭の判定用座標(空間IDの場合は中心座標と同じ)
    # ３，LinkDataのCSV属性（空間ID文字列とオブジェクトID）
    OUTPUT_CENTER_POINT_COORDINATE_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_CENTER_POINT_COORDINATE_DWH_NAME",
        description="空間IDの中心座標のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    OUTPUT_THEMATIC_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_THEMATIC_DWH_NAME",
        description="出力対象図郭判定用座標のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    OUTPUT_SPATIAL_ID_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_SPATIAL_ID_DWH_NAME",
        description="空間IDのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    OUTPUT_OBJECT_ID_DWH_NAME = PropertyDescriptor(
        name="OUTPUT_OBJECT_ID_DWH_NAME",
        description="オブジェクトIDのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 出力データである空間IDのCRS
    OUTPUT_SPATIAL_ID_CRS = PropertyDescriptor(
        name="OUTPUT_SPATIAL_ID_CRS",
        description="出力データである空間IDのCRS",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # ZIP圧縮するかどうかのフラグ（圧縮するまたは圧縮しない）デフォルトは"圧縮しない"
    OUTPUT_ZIP_FLAG = PropertyDescriptor(
        name="Output ZIP Flag",
        description="出力結果をZIP圧縮するかどうかのフラグ",
        default_value=ZIP_COMPRESSION_DISABLED,
        allowable_values=[ZIP_COMPRESSION_ENABLED, ZIP_COMPRESSION_DISABLED],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [SPATIAL_ID_SPLIT_STRING,
                            SPATIAL_ID_ZOOM_LEVEL,
                            OUTPUT_CENTER_POINT_COORDINATE_DWH_NAME,
                            OUTPUT_THEMATIC_DWH_NAME,
                            OUTPUT_SPATIAL_ID_DWH_NAME,
                            OUTPUT_OBJECT_ID_DWH_NAME,
                            OUTPUT_SPATIAL_ID_CRS,
                            OUTPUT_ZIP_FLAG]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            filename = flowfile.getAttribute("filename")

            # ---------------------------------------------------------------------------------------------------
            # 各プロパティで設定した値を取得
            # ---------------------------------------------------------------------------------------------------

            # 空間IDの区切り文字
            spatial_id_split_string\
                = context.getProperty(self.SPATIAL_ID_SPLIT_STRING).evaluateAttributeExpressions(flowfile).getValue()

            # 空間IDのズームレベル
            spatial_id_zoom_level\
                = int(context.getProperty(self.SPATIAL_ID_ZOOM_LEVEL).evaluateAttributeExpressions(flowfile).getValue())

            # 出力データである空間IDのDWH名
            output_center_point_coordinate_dwh_name\
                = context.getProperty(self.OUTPUT_CENTER_POINT_COORDINATE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            output_thematic_dwh_name\
                = context.getProperty(self.OUTPUT_THEMATIC_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            output_spatial_id_dwh_name\
                = context.getProperty(self.OUTPUT_SPATIAL_ID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            output_object_id_dwh_name\
                = context.getProperty(self.OUTPUT_OBJECT_ID_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # 出力データである空間IDのCRS
            output_spatial_id_crs\
                = int(context.getProperty(self.OUTPUT_SPATIAL_ID_CRS).evaluateAttributeExpressions(flowfile).getValue())

            # ZIP圧縮するかどうかのフラグ
            output_zip_flag\
                = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()
            # ---------------------------------------------------------------------------------------------------

            # flowfileから、csv形式のFieldSetFileを取得。
            input_link_csv = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            csv_data_frame = pd.read_csv(io.StringIO(input_link_csv))
            csv_data_column_name_list = csv_data_frame.columns.to_list()

            # 空間IDの文字列を１次元配列で取得
            spatial_id_list = csv_data_frame[DDC.SPATIAL_ID_COLUMN_NAME].to_list(
            )

            # splitを行い2次元配列へ[空間ID]>[z,f,x,y]
            spatial_id_array\
                = np.array([temp_id.split(spatial_id_split_string) for temp_id in spatial_id_list], dtype=np.float64)

            # --------------------------------------------------------------------------
            # 空間IDの中心座標取得
            # --------------------------------------------------------------------------
            center_z, \
                center_lon, \
                center_lat\
                = WM.calc_func_time(self.logger)(NCP.get_spatial_id_center_point_from_spatial_id)(spatial_id_array[:, 1].astype(np.int64),
                                                                                                  spatial_id_array[:, 2].astype(
                                                                                                      np.int64),
                                                                                                  spatial_id_array[:, 3].astype(
                                                                                                      np.int64),
                                                                                                  spatial_id_zoom_level)

            # --------------------------------------------------------------------------
            # 空間IDの中心座標変換
            # --------------------------------------------------------------------------
            center_x, \
                center_y\
                = WM.calc_func_time(self.logger)(DCP.convert_xy_corrdinates_array)(center_lon,
                                                                                   center_lat,
                                                                                   6668,
                                                                                   output_spatial_id_crs)

            # --------------------------------------------------------------------------
            # 中心点座標２次元配列
            # --------------------------------------------------------------------------
            center_id_array = np.arange(len(center_lon))
            center_xyz_array\
                = np.stack([center_id_array,
                           center_x,
                           center_y,
                           center_z], axis=1)

            # --------------------------------------------------------------------------
            # 中心点座標設定
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
                                                                                        output_center_point_coordinate_dwh_name,
                                                                                        'float64',
                                                                                        center_xyz_array)

            # --------------------------------------------------------------------------
            # 出力対象図郭判定用座標設定（中心点と同じ）
            # --------------------------------------------------------------------------
            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        output_thematic_dwh_name,
                                                                                        'float64',
                                                                                        center_xyz_array)

            # --------------------------------------------------------------------------
            # 空間ID文字列
            # --------------------------------------------------------------------------
            spatial_id_list\
                = WM.calc_func_time(self.logger)(NSP.get_attribute_list)(csv_data_frame[DDC.SPATIAL_ID_COLUMN_NAME].to_numpy(
                ))

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        output_spatial_id_dwh_name,
                                                                                        'object',
                                                                                        spatial_id_list)

            # --------------------------------------------------------------------------
            # オブジェクトID文字列
            # --------------------------------------------------------------------------
            object_id_list\
                = WM.calc_func_time(self.logger)(NSP.get_attribute_list)(csv_data_frame[DDC.OBJECT_ID_COLUMN_NAME].to_numpy(
                ))

            target_dwh_list, \
                target_type_list, \
                target_value_list\
                = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                        target_type_list,
                                                                                        target_value_list,
                                                                                        output_object_id_dwh_name,
                                                                                        'object',
                                                                                        object_id_list)

            # --------------------------------------------------------------------------
            # 引数で指定したDWH名に属性のカラム名を結合してFieldSetFileへ設定
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # FieldSetFile構成要素の各リストをFieldSetFileに変換
            # --------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(target_dwh_list,
                                                                                 target_type_list,
                                                                                 target_value_list)
            # --------------------------------------------------------------------------

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # CSV形式の文字列をZIP圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, output_field_set_file)

                # ZIPデータを取得
                output_field_set_file = zip_buffer.getvalue()

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
