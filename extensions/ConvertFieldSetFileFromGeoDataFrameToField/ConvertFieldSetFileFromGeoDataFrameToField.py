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
# GeoDataFrame、又はDataFrameの全行列をFieldSetFileに変換する。
# カラムをDWH名にする。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
from io import StringIO
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")


class ConvertFieldSetFileFromGeoDataFrameToField(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        FieldSetFile内のGeoDataFrameを、それぞれ、ジオメトリ、フィールド変換する。
                        ①input: GeoDataFrame、又はDataFrameを持った1行のFieldSetFile。
                        ②output: ジオメトリ、フィールドを持った複数行ののFieldSetFile。
                      """

        tags = ["GeoDataFrame", "Python"]

    # 地物IDが存在する場合にカラム名を取得
    FEATURE_ID_COLUMN_NAME = PropertyDescriptor(
        name="Feature ID Column Name",
        description="地物IDのカラム名（EmptyStringでFIDになる）",
        default_value="",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # FieldSetFileに出力されるジオメトリの型を選択
    GEOMETRY_TYPE = PropertyDescriptor(
        name="Geometry Type",
        description=f"""
                    FieldSetFileに出力されるジオメトリの型を選択
                    ただし、変換するジオメトリがない場合は未入力、又は"{DDC.PROPERTY_NOT_USE_GEOMETRY}"を選択してください。
                    """,
        allowable_values=[DDC.GEOMETRY, DDC.GEONDARRAY, DDC.PROPERTY_NOT_USE_GEOMETRY],
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 入力GeoDataFrameのDWH名
    INPUT_GEODATAFRAME_DWH_NAME = PropertyDescriptor(
        name="Input GeoDataFrame DWH Name",
        description="入力GeoDataFrameのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [FEATURE_ID_COLUMN_NAME,
                            GEOMETRY_TYPE,
                            INPUT_GEODATAFRAME_DWH_NAME
                            ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, flowfile ,context):
        # 地物IDのカラム名を取得
        feature_id_column_name = context.getProperty(
            self.FEATURE_ID_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 変換するジオメトリタイプを取得
        geometry_type\
            = context.getProperty(self.GEOMETRY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        # 入力されるGeoDataFrameのDWH名を取得
        input_geodataframe_dwh_name\
            = context.getProperty(self.INPUT_GEODATAFRAME_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return feature_id_column_name, geometry_type, input_geodataframe_dwh_name

    def get_geodataframe(self, flowfile, input_geodataframe_dwh_name):
        """
        概要
            GeoDataFrameをFlowFile内のFieldSetFileから取得

        引数
            flowfile: GeoDataFrameのバイトデータとデータ定義を含むFlowFileオブジェクト

        戻り値
            geodataframe: FieldSetFile内のGeoDataFrame
        """
        # flowfileから、CSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # input_field_set_fileをパスとして扱い、DataFrameに加工
        field_set_file_dataframe = pd.read_csv(StringIO(input_field_set_file))

        # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得する
        geodataframe = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, input_geodataframe_dwh_name)

        return geodataframe

    def add_index_column(self, geodataframe, findex_type="float"):
        """
        概要
            GeoDataFrameにインデックス列"Findex"を追加する関数

        引数
            shape_dataframe: インデックス列を追加するGeoDataFrameオブジェクト

        戻り値
            shape_dataframe: インデックス列を追加した後のGeoDataFrameオブジェクト
        """

        # インデックス列"Findex"を追加
        index_list = np.arange(0, len(geodataframe), dtype=findex_type)

        geodataframe["Findex"] = index_list

        return geodataframe

    def create_field_set_file(self, field_type_list, field_value_list, dwh_file_name_list, feature_id_list, feature_id_type="object"):
        """
        概要
            フィールドセットファイルを作成する関数

        引数
            field_type_list: 各フィールドのデータ型を格納したリスト
            field_value_list: 各フィールドの値を格納したリスト
            dwh_file_name_list: DWHファイル名を格納したリスト
            feature_id_list: FIDを格納したリスト

        戻り値
            field_set_file: 出力用のFieldSetFile
        """

        # 型をフィールドタイプリストに追加
        field_type_list.append(feature_id_type)

        # FIDをフィールド値リストに追加
        field_value_list.append(feature_id_list)

        # FIDをDWHファイル名リストに追加
        dwh_file_name_list.append("FID")

        # FieldSetFileを取得
        field_set_file = WM.calc_func_time(self.logger)(NSP.get_field_set_file)(field_type_list,
                                                                                field_value_list,
                                                                                dwh_file_name_list
                                                                                )

        return field_set_file

    def transform(self, context, flowfile):
        try:

            # 地物IDのカラム名、変換ジオメトリタイプ、inputされるGeoDataFrameのDWH名をプロパティから取得
            feature_id_column_name, \
                geometry_type, \
                input_geodataframe_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(flowfile, context)

            # flowfileからシリアライズされたGeoDataFrame取得
            geodataframe = WM.calc_func_time(
                self.logger)(self.get_geodataframe)(flowfile, input_geodataframe_dwh_name)

            # 出力対象のカラムを作成
            target_column = list(geodataframe.columns)

            # カラムをDWHに変更しない為、空dict
            field_to_dwh_dict = {}

            # nullジオメトリの場合はポイント。
            null_geometry_file_type = DDC.POINT_GEOMETRY_TYPE

            # GeoDataFrameにインデックス列"Findex"を追加
            add_findex_geodataframe = WM.calc_func_time(self.logger)(
                self.add_index_column)(geodataframe)

            # GeoDataFrameからフィールド値リストとフィールドタイプリストを取得
            field_value_list, \
                field_type_list, \
                output_dwh_file_name_list\
                = WM.calc_func_time(self.logger)(NSP.write_field_file_from_geodataframe)(add_findex_geodataframe,
                                                                                        target_column,
                                                                                        field_to_dwh_dict, 
                                                                                        geometry_type,
                                                                                        null_geometry_file_type,
                                                                                        feature_id_column_name)

            # GeoDataFrameからFIDが格納されたlistとFID数を取得
            feature_id_list, \
                fid_count\
                = WM.calc_func_time(self.logger)(NSP.write_feature_file)(add_findex_geodataframe,
                                                                         feature_id_column_name)

            # FieldSetFileを作成
            field_set_file = WM.calc_func_time(self.logger)(self.create_field_set_file)(field_type_list,
                                                                                        field_value_list,
                                                                                        output_dwh_file_name_list,
                                                                                        feature_id_list
                                                                                        )

            # 成功リレーションと共に処理結果を返す
            return FlowFileTransformResult(relationship="success",
                                           attributes={"fid_count": str(fid_count),
                                                       "feature_id_column_name": feature_id_column_name},
                                           contents=field_set_file
                                           )

            # エラーが発生した場合、失敗リレーションを返す
        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
