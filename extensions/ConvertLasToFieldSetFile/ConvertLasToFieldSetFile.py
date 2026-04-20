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
# lasファイルから、FieldSetFileを生成するためのプロセッサ。
# lasファイルを受け取り、データ定義を取得し、フィールドの情報を処理している。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
from io import StringIO, BytesIO
import zipfile
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.LasPackage as LP

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
laspy = import_module("laspy")

ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"

class ConvertLasToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データ定義記載項目に従い、LasをFieldSetFileに変換
                        ①input: Bytes形式のLas
                        ②output: 出力したいデータ項目を持ったFieldSetFile。
                      """

        tags = ["LAS", "Python"]

    # データ定義の区切り文字を指定するためのPropertyDescriptorを定義
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
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

    property_descriptors = [DATA_DEFINITION_DELIMITER,
                            OUTPUT_ZIP_FLAG
                            ]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context):
        """
        概要
            プロパティからデータ定義の区切り文字を取得し、ログ出力

        引数
            context: processorの設定値

        戻り値
            data_definition_delimiter: データ定義の区切り文字
        """

        # プロパティからデータ定義の区切り文字を取得
        data_definition_delimiter = context.getProperty(
            self.DATA_DEFINITION_DELIMITER).getValue()

        # ZIP圧縮するかどうかのフラグ
        output_zip_flag\
            = context.getProperty(self.OUTPUT_ZIP_FLAG).getValue()

        return data_definition_delimiter, output_zip_flag

    def extract_las_bytes_and_data_definition_from_flowfile(self, flowfile):
        """
        概要
            FlowFileからlasのバイトデータを取得

        引数
            flowfile: GeoDataFrameのバイトデータとデータ定義を含むFlowFileオブジェクト

        戻り値
            las_bytes: lasのバイトデータ
            data_definition_stream: データ定義をStringIOストリームとして扱うオブジェクト
        """

        # flowfileからlasファイルのバイトデータを取得
        las_bytes = flowfile.getContentsAsBytes()

        # flowfileの属性からデータ定義を取得
        data_definition = flowfile.getAttribute("DataDefinition")

        # データ定義をStringIOストリームとして扱う
        data_definition_stream = StringIO(data_definition)

        return las_bytes, data_definition_stream

    def read_las(self, las_bytes):
        """
        概要
            bytes形式のlasを読み込み、laspyのオブジェクトに変換
        
        引数
            las_bytes: byte形式のlasファイル
        
        戻り値
            laspy_object: lasファイルから取得したlasオブジェクト
        """
        laspy_object = laspy.read(BytesIO(las_bytes))

        return laspy_object

    def get_coordinates_from_las_object(self, las_object):
        """
        概要
            lasオブジェクトからxyz座標取得し、listに格納
            ※小文字のxyzで取得をすると、スケーリングされた状態で取得できる
        
        引数
            las_object: lasファイルから取得したlasオブジェクト
        
        戻り値
            result_array: lasオブジェクトから取得したxyz座標
        """

        las_x_array  = las_object.x.copy()
        las_y_array  = las_object.y.copy()
        las_z_array  = las_object.z.copy()
        las_id_array = np.arange(0, len(las_x_array))
        
        result_array = np.stack([las_id_array, las_x_array, las_y_array, las_z_array], axis=1)

        return result_array

    def get_field_from_las_object(self,
                                  las_object,
                                  index_array,
                                  attribute_distribution_name_list,
                                  attribute_dwh_file_name_list,
                                  attribute_data_type_list):
        """
        概要
            lasオブジェクトからフィールドを取得し、listに格納
        
        引数
            las_object: lasファイルから取得したlasオブジェクト
            index_array: 地物のindexが格納された配列
            attribute_distribution_name_list: データ定義の流通項目名列の要素を持ったlist
            attribute_dwh_file_name_list: データ定義のDWHファイル名列の要素を持ったlist
            attribute_data_type_list: データ定義のデータ型列の要素を持ったlist

        戻り値
            attribute_field_dwh_list: フィールド用のFieldSetFileのDwh列に格納するlist
            attribute_field_type_list: フィールド用のFieldSetFileのType列に格納するlist
            attribute_field_value_list: フィールド用のFieldSetFileのValue列に格納するlist
        """
        attribute_field_dwh_list = []
        attribute_field_type_list = []
        attribute_field_value_list = []

        # 各属性をlist化
        for i in range(len(attribute_distribution_name_list)):

            # 地物IDと出力対象属性をlistに変換
            attribute_list = list(zip(index_array, las_object[attribute_distribution_name_list[i]]))

            # 属性のDWH名、Type、出力対象属性listを,それぞれのlistに格納
            attribute_field_dwh_list.append(attribute_dwh_file_name_list[i])
            attribute_field_type_list.append(attribute_data_type_list[i])
            attribute_field_value_list.append(attribute_list)

        return attribute_field_dwh_list, attribute_field_type_list, attribute_field_value_list

    def create_feature_id_list_from_coordinates_id_array(self, index_array):

        # 地物IDをlistに格納
        feature_id_list = list(zip(index_array,
                                   index_array))

        return feature_id_list

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            output_dwh_list = []
            output_type_list = []
            output_value_list = []
            # contextからデータ定義の区切り文字と、zip化するかのフラグを取得
            data_definition_delimiter,\
                output_zip_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context)

            # flowfileからlasファイルのバイトデータとデータ定義を取得
            las_bytes, data_definition_stream = WM.calc_func_time(
                self.logger)(self.extract_las_bytes_and_data_definition_from_flowfile)(flowfile)

            # データ定義からジオメトリタイプlist、フィールド名list、DWHファイル名list、フィールドのデータ型listを取得
            # get_data_definition_indexの戻り値に合わせて今回使用しない変数も設定している。
            geometry_type_list, \
                geometry_dwh_file_name_list, \
                geometry_distribution_name_list, \
                attribute_file_type_list, \
                attribute_dwh_file_name_list, \
                attribute_distribution_name_list, \
                attribute_const_value_list, \
                attribute_data_type_list\
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index_datatype)(data_definition_stream,
                                                                                         data_definition_delimiter=data_definition_delimiter
                                                                                         )

            # bytes形式のlasを読み込み、laspyのオブジェクトに変換
            laspy_object =  WM.calc_func_time(self.logger)(
                LP.read_las)(las_bytes)

            # lasオブジェクトから座標配列に変換
            las_coordinates_array =\
                LP.get_coordinates_from_las_object(laspy_object)

            # 座標配列と、そのDWH、Typeを出力用のlistに格納
            output_dwh_list.append(geometry_dwh_file_name_list[0])
            output_type_list.append("geometry")
            output_value_list.append(las_coordinates_array.copy())

            # lasオブジェクトからフィールドを取得し、listに格納
            attribute_field_dwh_list, \
                attribute_field_type_list, \
                attribute_field_value_list =\
                self.get_field_from_las_object(laspy_object,
                                               las_coordinates_array[:,0],
                                               attribute_distribution_name_list,
                                               attribute_dwh_file_name_list,
                                               attribute_data_type_list)

            # lasオブジェクトから取得したフィールドのlistを出力用のlistに結合
            output_dwh_list += attribute_field_dwh_list
            output_type_list += attribute_field_type_list
            output_value_list += attribute_field_value_list

            # 地物IDを格納したlistを作成
            feature_id_list = \
                self.create_feature_id_list_from_coordinates_id_array(las_coordinates_array[:,0])

            # 地物IDのlistと、そのDWH、Typeを出力用のlistに格納
            output_dwh_list.append(attribute_dwh_file_name_list[0].split("/")[0] + "/FID")
            output_type_list.append("int")
            output_value_list.append(feature_id_list)

            # lasから取得した座標配列、フィールド、地物IDと、そのDWH、Typeを格納したlistをFieldSetFileをに加工
            output_field_set_file = WM.calc_func_time(self.logger)(
                PBP.set_field_set_file)(output_dwh_list,
                                        output_type_list,
                                        output_value_list)

            # 出力されるFieldSetFileのバイト数を取得
            csv_bytes = output_field_set_file.encode('utf-8')

            # 出力されるFieldSetFileのバイト数が2GBを超えている場合はflowfileに出力できない為、処理中止
            if len(csv_bytes) > 2000000000:
                self.logger.error(f"出力されるFieldSetFileのサイズが2GBを超える為、処理不可 出力されるFieldSetFileのサイズ:{len(csv_bytes) / 1000000000}GB")
                return FlowFileTransformResult(relationship="failure")

            # FieldSetFileのバイト数が4GBを超えていない場合は出力
            else:
                pass

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # flowfileの属性からfilenameを取得(writestrに渡すため)
                filename = flowfile.getAttribute("filename")

                # CSV形式の文字列をZIP圧縮
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, output_field_set_file)

                # ZIPデータを取得
                output_field_set_file = zip_buffer.getvalue()

                zip_buffer.close()

            # zip化しない場合は出力用FieldSetFileをそのままcontentsへ格納
            else:
                pass

            # 成功リレーションと共に処理結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file
                                           )

        # エラーが発生した場合、失敗リレーションを返す
        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
