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
# GeoDataFrameを操作し、FieldSetFileを生成するためのプロセッサ。
# GeoDataFrameをシリアライズし、データ定義を取得し、フィールドの情報を処理している。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
from io import StringIO
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")

# 固定値
PROPERTY_NOT_USE_GEOMETRY = "ジオメトリ変換無し"

class ConvertGeoDataFrameToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = "2.0.0"
        description = '''
                        GeoDataFrameをFieldSetFileに変換する。
                        ①input: GeoDataFrameか、GeoDataFrameを持った1行のFieldSetFile。
                        ②output: 出力したいデータ項目を持ったFieldSetFile。
                      '''

        tags = ['GeoDataFrame', 'Python']

    # 地物IDが存在する場合にカラム名を取得
    FEATURE_ID_COLUMN_NAME = PropertyDescriptor(
        name='Feature ID Column Name',
        description='地物IDのカラム名（EmptyStringでFIDになる）',
        default_value='',
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # データ定義の区切り文字を指定するためのPropertyDescriptorを定義
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name='Data Definition Delimiter',
        description='データ定義ファイルの区切り文字',
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # データ定義ファイルを入力
    DATA_DEFINITION = PropertyDescriptor(
        name='Data Definition',
        description='データ定義ファイルを入力',
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # FieldSetFileに出力されるジオメトリの種類を選択',
    GEOMETRY_TYPE = PropertyDescriptor(
        name='Geometry Type',
        description="""FieldSetFileに出力されるジオメトリの種類を選択
        出力ジオメトリがない場合は、「ジオメトリ変換無し」を入力してください。
        """,
        default_value=DDC.GEOMETRY,
        allowable_values=[DDC.GEOMETRY, DDC.GEONDARRAY, PROPERTY_NOT_USE_GEOMETRY],
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # GeoDataFrameのすべてをFieldSetFileにするか、データ定義に書かれている要素のみ出力するか
    ALL_FIELD_FLAG = PropertyDescriptor(
        name='All Field Flag',
        description='GeoDataFrameのすべてをFieldSetFileにするか、データ定義に書かれている要素のみ出力するか',
        default_value=DDC.USE_SELECT_FIELD,
        allowable_values=[DDC.USE_ALL_FIELD, DDC.USE_SELECT_FIELD],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            FEATURE_ID_COLUMN_NAME,
                            DATA_DEFINITION,
                            GEOMETRY_TYPE,
                            ALL_FIELD_FLAG
                            ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, flowfile ,context):
        """
        概要
            プロパティからデータ定義の区切り文字を取得し、ログ出力

        引数
            context: processorの設定値が格納されているデータ

        戻り値
            feature_id_column_name: 地物IDのカラム名
            data_definition_delimiter: データ定義の区切り文字
            input_data_type: プロセッサに入ってくる種類
        """

        # プロパティから地物IDのカラム名を取得する
        feature_id_column_name = context.getProperty(
            self.FEATURE_ID_COLUMN_NAME).getValue()

        # プロパティからデータ定義の区切り文字を取得する
        data_definition_delimiter = context.getProperty(
            self.DATA_DEFINITION_DELIMITER).getValue()

        # propertyからデータ定義を取得
        property_data_definition\
            = context.getProperty(self.DATA_DEFINITION).evaluateAttributeExpressions(flowfile).getValue()

        # flowfileのアトリビュートからデータ定義を取得する
        attribute_data_definition = flowfile.getAttribute('DataDefinition')

        geometry_type\
            = context.getProperty(self.GEOMETRY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        all_field_flag\
            = context.getProperty(self.ALL_FIELD_FLAG).evaluateAttributeExpressions(flowfile).getValue()

        if all_field_flag == DDC.USE_ALL_FIELD:
            all_field_flag = True
        else:
            all_field_flag = False

        # 区切り文字をログ出力する
        self.logger.info(
            f'[Property], Data Definition Delimiter: {data_definition_delimiter}')

        return feature_id_column_name, data_definition_delimiter, attribute_data_definition, property_data_definition, geometry_type, all_field_flag

    def extract_geodata_from_flowfile(self, flowfile):
        """
        概要
            FlowFileからシリアライズされたGeoDataFrameのバイトデータを取得し、GeoDataFrameを復元する

        引数
            flowfile: GeoDataFrameのバイトデータとデータ定義を含むFlowFileオブジェクト

        戻り値
            geodataframe: 復元されたGeoDataFrameオブジェクト
            data_definition_stream: データ定義をStringIOストリームとして扱うオブジェクト
        """

        # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得する
        geodataframe = WM.calc_func_time(self.logger, False)(
            PBP.get_value_from_field_Set_file_or_serialized)(flowfile)

        return geodataframe

    def add_index_column(self, geodataframe, findex_type='float'):
        """
        概要
            GeoDataFrameにインデックス列'Findex'を追加する関数

        引数
            shape_dataframe: インデックス列を追加するGeoDataFrameオブジェクト

        戻り値
            shape_dataframe: インデックス列を追加した後のGeoDataFrameオブジェクト
        """

        # インデックス列'Findex'を追加
        index_list = np.arange(0, len(geodataframe), dtype=findex_type)

        geodataframe['Findex'] = index_list

        return geodataframe

    def check_geometry_type(self, geometry_type_list):
        """
        概要
            geometry_type_list 内の各要素に__geometry_type_list の ['+1','+2','+3'] が含まれるかどうかを確認する

        引数
            geometry_type_list: チェックするジオメトリタイプのリスト

        戻り値
            基本的になし。エラーの場合valueErrorを返す
        """

        # 検証するジオメトリタイプのリスト
        __geometry_type_list__ = ['+1', '+2', '+3']

        # ジオメトリタイプの真偽値の配列を作成する
        geometry_type_bool_array = np.array([geometry_type_list[i] in __geometry_type_list__
                                             for i in range(len(geometry_type_list))], dtype=np.bool_)

        # ジオメトリタイプの確認を行う
        WM.calc_func_time(self.logger, False)(
            self.confirmation_of_check_geometry_types)(geometry_type_bool_array)

    def confirmation_of_check_geometry_types(self, geometry_type_bool_array):
        """
        概要
            ジオメトリタイプのチェックを行う関数

        引数
            geometry_type_bool_array: ジオメトリタイプの真偽値の配列

        戻り値
            基本的になし。エラーの場合valueErrorを返す
        """

        # もしジオメトリタイプの真偽値の配列がすべて真でない場合
        if np.all(geometry_type_bool_array) is False:

            # ValueErrorを発生させる
            raise ValueError

    def create_field_set_file(self, field_type_list, field_value_list, dwh_file_name_list, feature_id_list, feature_id_type='string'):
        """
        概要
            フィールドセットファイルを作成する関数

        引数
            field_type_list: 各フィールドのデータ型を格納したリスト
            field_value_list: 各フィールドの値を格納したリスト
            dwh_file_name_list: DWHファイル名を格納したリスト
            base64_geometry_items_list: Base64エンコードされたジオメトリアイテムを格納したリスト

        戻り値
            field_set_file: フィールドセットファイル
        """

        # 'string'をフィールド名リストに追加する
        field_type_list.append(feature_id_type)

        # Base64エンコードされたジオメトリアイテムリストをフィールド値リストに追加する
        field_value_list.append(feature_id_list)

        # FIDのDWH名を適した形に変形する。
        name = dwh_file_name_list[0]

        # /が入力されていれば、ファイル名/フィールド名なのでファイル名を採用
        if "/" in name:
            fid_dwh_name = name.split("/")[0] + "/FID"
        
        # ない場合はそのままFID
        else:
            fid_dwh_name = "FID"

        # FIDをDWHファイル名リストに追加する
        dwh_file_name_list.append(fid_dwh_name)

        # FieldSetFileを取得する
        field_set_file = WM.calc_func_time(self.logger)(NSP.get_field_set_file)(field_type_list,
                                                                                field_value_list,
                                                                                dwh_file_name_list
                                                                                )

        return field_set_file

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # 地物IDのカラム名とデータ定義の区切り文字を取得する
            feature_id_column_name, \
                data_definition_delimiter, \
                property_data_definition, \
                attribute_data_definition, \
                geometry_type, \
                all_field_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(flowfile, context)

            # flowfileからシリアライズされたGeoDataFrame取得する
            geodataframe = WM.calc_func_time(
                self.logger)(self.extract_geodata_from_flowfile)(flowfile)

            # GeoDataFrameにインデックス列'Findex'を追加
            add_findex_geodataframe = WM.calc_func_time(self.logger)(
                self.add_index_column)(geodataframe.copy())

            data_definition = WM.calc_func_time(self.logger)(NSP.check_data_definition)(property_data_definition, attribute_data_definition, all_field_flag)

            # データ定義がある場合
            if data_definition != DDC.ALL_FIELDS:
                data_definition_stream = StringIO(str(data_definition))
                # データ定義からジオメトリタイプlist、フィールド名リスト、DWHファイル名リストを取得する
                # get_data_definition_indexの戻り値に合わせて今回使用しない変数も設定している。
                geometry_type_list, \
                    geometry_file_name_list, \
                    geometry_distribution_name_list, \
                    dwh_file_name_list, \
                    attribute_name_list, \
                    attribute_const_value_list, \
                    attribute_file_type_list, \
                    all_attribute_name_list, \
                    data_definition_dwh_file_name_list, \
                    = WM.calc_func_time(self.logger)(NSP.get_data_definition_index)(data_definition_stream,
                                                                                    data_definition_delimiter=data_definition_delimiter
                                                                                    )

                # Field→DWHの対応dict
                field_to_dwh_dict = dict(zip(all_attribute_name_list, data_definition_dwh_file_name_list))

                # カラム重複チェック
                NSP.validate_no_duplicate_columns(field_to_dwh_dict, list(geodataframe.columns))

                # ジオメトリタイプのチェックを行う
                WM.calc_func_time(self.logger)(
                    self.check_geometry_type)(geometry_type_list)

                # データ定義にジオメトリが入力されている場合、nullジオメトリ作成用のふぁジオメトリファイルタイプを抽出
                if geometry_type_list:
                    geometry_file_type = geometry_type_list[0]

                else:
                    # 入力されていない場合、nullジオメトリの場合はファイルタイプはポイントとする。
                    geometry_file_type = DDC.POINT_GEOMETRY_TYPE

                target_column = WM.calc_func_time(self.logger)(NSP.get_target_column_name_list)(all_field_flag, geodataframe, all_attribute_name_list)

            # データ定義が入力されていない且つ、全行出力の場合必要listを用意する。
            else:
                target_column = list(geodataframe.columns)
                field_to_dwh_dict = {}
                # 入力されていない場合、nullジオメトリの場合はポイントとする。
                geometry_file_type = DDC.POINT_GEOMETRY_TYPE

            # GeoDataFrameからフィールド値リストとフィールドタイプリストを取得する
            field_value_list, \
                field_type_list, \
                output_dwh_file_name_list\
                = WM.calc_func_time(self.logger)(NSP.write_field_file_from_geodataframe)(add_findex_geodataframe,
                                                                                        target_column,
                                                                                        field_to_dwh_dict, 
                                                                                        geometry_type,
                                                                                        geometry_file_type,
                                                                                        feature_id_column_name)

            # GeoDataFrameからBase64エンコードされたジオメトリアイテムリストとFID数を取得する
            feature_id_list, \
                fid_count\
                = WM.calc_func_time(self.logger)(NSP.write_feature_file)(add_findex_geodataframe,
                                                                         feature_id_column_name)

            # FieldSetFileを作成する
            field_set_file = WM.calc_func_time(self.logger)(self.create_field_set_file)(field_type_list,
                                                                                        field_value_list,
                                                                                        output_dwh_file_name_list,
                                                                                        feature_id_list
                                                                                        )

            # 成功リレーションと共に処理結果を返す
            return FlowFileTransformResult(relationship='success',
                                           attributes={'fid_count': str(
                                               fid_count), 'feature_id_column_name': feature_id_column_name},
                                           contents=field_set_file
                                           )

            # エラーが発生した場合、失敗リレーションを返す
        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
