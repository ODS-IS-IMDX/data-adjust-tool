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


class ConvertGeoDataFrameToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''
                        データ定義記載項目に従い、GeoDataFrameをFieldSetFileに変換する。
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
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # データ定義の区切り文字を指定するためのPropertyDescriptorを定義
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name='Data Definition Delimiter',
        description='データ定義ファイルの区切り文字',
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            FEATURE_ID_COLUMN_NAME
                            ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_data_definition_delimiter(self, context):
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

        # 区切り文字をログ出力する
        self.logger.info(
            f'[Property], Data Definition Delimiter: {data_definition_delimiter}')

        return feature_id_column_name, data_definition_delimiter

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

        # flowfileの属性からデータ定義を取得する
        data_definition = flowfile.getAttribute('DataDefinition')

        # データ定義をStringIOストリームとして扱う
        data_definition_stream = StringIO(data_definition)

        return geodataframe, data_definition_stream

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
        fid_dwh_name = dwh_file_name_list[0].split("/")[0] + "/FID"

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
                = WM.calc_func_time(self.logger)(self.get_data_definition_delimiter)(context)

            # flowfileからシリアライズされたGeoDataFrameとデータ定義を取得する
            geodataframe, data_definition_stream = WM.calc_func_time(
                self.logger)(self.extract_geodata_from_flowfile)(flowfile)

            # GeoDataFrameにインデックス列'Findex'を追加
            geodataframe = WM.calc_func_time(self.logger)(
                self.add_index_column)(geodataframe)

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
                all_dwh_file_name_list, \
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index)(data_definition_stream,
                                                                                data_definition_delimiter=data_definition_delimiter
                                                                                )

            # ジオメトリタイプのチェックを行う
            WM.calc_func_time(self.logger)(
                self.check_geometry_type)(geometry_type_list)

            # GeoDataFrameからフィールド値リストとフィールドタイプリストを取得する
            field_value_list, \
                field_type_list\
                = WM.calc_func_time(self.logger)(NSP.write_field_file)(geodataframe,
                                                                       all_attribute_name_list
                                                                       )

            # GeoDataFrameからBase64エンコードされたジオメトリアイテムリストとFID数を取得する
            feature_id_list, \
                fid_count\
                = WM.calc_func_time(self.logger)(NSP.write_feature_file)(geodataframe,
                                                                         feature_id_column_name)

            # FieldSetFileを作成する
            field_set_file = WM.calc_func_time(self.logger)(self.create_field_set_file)(field_type_list,
                                                                                        field_value_list,
                                                                                        all_dwh_file_name_list,
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
