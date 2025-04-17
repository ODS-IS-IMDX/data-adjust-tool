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
# マルチパッチをFieldSetFileをglTF形式に変換するプロセッサ。
# 製品データ定義ファイルから型を読み取る。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import json
import pickle
import base64
import traceback

from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")


class ConvertFieldSetFileToGLTF(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        データ定義記載項目に従い、glTFファイルを作成する。
                        ①input: 出力したいデータ項目を持ったFieldSetFile。
                        ②output: 出力したいデータ項目を持ったglTFファイル。
                      """
        tags = ['Multipatch', 'GLTF', 'Python']

    # データ定義ファイルの区切り文字を設定する。
    DATA_DEFINITION_DELIMITER = PropertyDescriptor(
        name="Data Definition Delimiter",
        description="データ定義ファイルの区切り文字",
        default_value=DDC.DELIMITER_COMMA,
        allowable_values=[DDC.DELIMITER_COMMA, DDC.DELIMITER_TAB],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [DATA_DEFINITION_DELIMITER]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ

        戻り値:
            data_definition_delimiter: データ定義ファイルの区切り文字(例:Comma)
        """

        # データ定義ファイルの区切り文字を、プロパティから取得。
        data_definition_delimiter = context.getProperty(
            self.DATA_DEFINITION_DELIMITER).getValue()

        return data_definition_delimiter

    def get_flowfile(self, flowfile):
        """
        概要:
            flowfileからFieldSetFileを受け取り、Attributeからデータ定義ファイルを取得。

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            data_definition_stream: データ定義ファイルをstringioしたもの read_csvで開く為
            field_set_file_data_frame: FieldSetFileをデータフレームにしたもの 属性を取得する為
        """

        # attributeの"DataDefinition"に直接書かれたデータ定義ファイルを取得
        data_definition = flowfile.getAttribute("DataDefinition")

        # 取得したデータ定義をパスとして扱う。
        data_definition_stream = io.StringIO(data_definition)

        # flowfileから、csv形式のFieldSetFileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # 取得したFieldSetFile(csv)を読み取り、パスとして扱う。
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        return data_definition_stream, field_set_file_data_frame

    def extract_coordinates_array_from_field_set_file(self, field_set_file_data_frame, geometry_file_name_list):
        """
        概要:
            field_set_file_data_frameのDwh列で、
            geometry_file_name_listの0番目(geometryのDwhファイル名)の値の行とValue列の値を抽出し
            base64でデコード、デシリアライズし配列に戻す関数。

        引数:
            field_set_file_data_frame: FieldSetFileをデータフレームに加工した物
            geometry_file_name_list: データ定義ファイル内のgeometryのDWHが格納されているリスト(基本要素は1つ)

        戻り値:
            coordinates_array: 座標配列
        """

        # field_set_file_data_frameから、クリエイトマルチパッチ後の配列を抽出。
        # Value列且つ、Dwh列の値がgeometry_file_name_listのインデックスが0番目
        coordinates_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_file_name_list[0], "Value"].values[0]))

        # 欠損値を置換
        coordinates_array = np.nan_to_num(coordinates_array)

        return coordinates_array

    def convert_gltf_to_obj_and_format_with_windows_newline(self, gltf_data):
        """
        概要:
            glTFをflowfileに送るために、JSON形式の文字列型にし、改行文字を{\n}から{\r\n}に変える関数。

        引数:
            gltf_data: マルチパッチをglTFにしたデータ

        戻り値:
            target_gltf_object_json_string_windows: JSON形式で改行文字もwindows用に変更したデータ
        """
        # flowfileにデータを送るために、JSON形式にする。
        target_gltf_object_json = gltf_data.to_json()

        # インデントと、区切り文字の整形を行う。これによってデータが１行で表されないようになる。
        target_gltf_object_json_formatted = json.dumps(json.loads(
            target_gltf_object_json), indent=2, separators=(",", ": "))

        # このままでは、改行文字がwindows用ではないので変更。
        target_gltf_object_json_string_windows = target_gltf_object_json_formatted.replace(
            "\n", "\r\n")

        return target_gltf_object_json_string_windows

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------

    def transform(self, context, flowfile):
        try:

            # プロパティで入力したdata_definition_delimiter(区切り文字)を取得する。
            data_definition_delimiter = WM.calc_func_time(
                self.logger)(self.get_property)(context)

            # flowfileのattributeからデータ定義ファイル、FieldSetFileを取得。
            data_definition_stream, \
                field_set_file_data_frame\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile)

            # データ定義ファイルから出力時ジオメトリ項目ファイル名List、出力時属性項目ファイル名List、入力ファイル元の属性名List取得を取得。
            geometry_type_list, \
                geometry_file_name_list, \
                geometry_distribution_name_list, \
                dwh_file_name_list, \
                attribute_name_list, \
                attribute_const_value_list, \
                attribute_file_type_list, \
                all_attribute_name_list, \
                all_dwh_file_name_list\
                = WM.calc_func_time(self.logger)(NSP.get_data_definition_index)(data_definition_stream,
                                                                                data_definition_delimiter=data_definition_delimiter
                                                                                )

            # field_set_file_data_frameから、マルチパッチを抽出。
            # Value列且つ、Dwh列の値がgeometry_file_name_listのインデックスが0番目
            coordinates_array = WM.calc_func_time(self.logger)(self.extract_coordinates_array_from_field_set_file)(field_set_file_data_frame,
                                                                                                                   geometry_file_name_list)

            # データ定義ファイルに指定された属性項目ファイルをすべて読み込み一つのDataFrameとする
            all_attribute_dataframe = WM.calc_func_time(self.logger)(NSP.create_attribute_dataframe)(field_set_file_data_frame,
                                                                                                     dwh_file_name_list,
                                                                                                     attribute_name_list,
                                                                                                     attribute_const_value_list,
                                                                                                     attribute_file_type_list,
                                                                                                     len(np.unique(
                                                                                                         coordinates_array[:, 0])),
                                                                                                     encoding="UTF-8",
                                                                                                     input_file_type=1
                                                                                                     )

            # glTFオブジェクトの取得
            target_gltf_object = WM.calc_func_time(self.logger)(NSP.create_gltf_object)(coordinates_array,
                                                                                        all_attribute_dataframe
                                                                                        )

            # 出力用にJSON形式の文字列型にし、改行文字をwindowsに対応するように変更。
            target_gltf_object_json_string_windows\
                = WM.calc_func_time(self.logger)(self.convert_gltf_to_obj_and_format_with_windows_newline)(target_gltf_object)

            return FlowFileTransformResult(relationship="success", contents=target_gltf_object_json_string_windows)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
