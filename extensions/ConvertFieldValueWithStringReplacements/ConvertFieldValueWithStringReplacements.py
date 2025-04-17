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
# プロパティからCSVデータを取得し、材料名から任意のコードへの置換を行い、
# 新しいFieldSetFileを作成するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準モジュール
import io
import traceback

from importlib import import_module

# Nifi自作モジュール
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")


class ConvertFieldValueWithStringReplacements(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        フィールドの中で、指定した文字列を指定した文字列に置換する。
                        ①input: 任意のフィールドを持った1行のFieldSetFile。
                        ②output: 入力データのフィールドを持った1行のFieldSetFile。フィールド値はすべて文字列型になる。
                      """
        tags = ['Replace', 'Python']

    # CSVを取得するためのプロパティ
    REPLACEMENT_DEFINITION_CSV = PropertyDescriptor(
        name="Replacement Definition CSV",
        description="カンマ区切りで、置換前の文字列と置換後の文字列を入力したCSV",
        default_value="before_string,after_string",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    # 新しいDWHファイル名をプロパティで入力。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [REPLACEMENT_DEFINITION_CSV,
                            OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            replacement_definition_csv: 変更前と変更後の文字列が格納されたCSV
            output_dwh_name: アウトプット時のDWHファイル名
        """

        # replacement_definition_csvプロパティの値を取得
        # プロパティにCSV形式で直接記入する
        replacement_definition_csv = \
            context.getProperty(self.REPLACEMENT_DEFINITION_CSV).getValue()

        # output用のDWH名をプロパティの値から取得
        output_dwh_name \
            = context.getProperty(self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return replacement_definition_csv, output_dwh_name

    def csv_to_dataframe(self, replacement_definition_csv):
        """
        概要:
            CSVデータをDataFrameに変換する関数

        引数:
            replacement_definition_csv: 変更前と変更後の文字列が格納されたCSV
        戻り値:
            before_string_list: 変更前の文字列のリスト
            after_string_list: 変更後の文字列のリスト
        """

        # CSVデータをDateFrameに読み込み
        field_set_file_data_frame \
            = pd.read_csv(io.StringIO(replacement_definition_csv))

        # 'before_string'列の欠損値を'None'で埋める
        field_set_file_data_frame['before_string'] \
            = field_set_file_data_frame['before_string'].fillna('None')

        # 'before_string'列をリストに変換
        before_string_list \
            = field_set_file_data_frame['before_string'].tolist()

        # 'after_string'列をリストに変換
        after_string_list = field_set_file_data_frame['after_string'].tolist()

        return before_string_list, after_string_list

    def replace_in_list(self, target_list, before_string_list, after_string_list):
        """
        概要:
            材質に何を使っているのを記入されたリストの材質名を置き換える関数
        引数:
            target_list: IDと材質名のtupleが格納されたリスト
            before_string_list: 変更前の材質名のリスト(例: ポリエチレン管)
            after_string_list: 変更後の材質コードのリスト(例:10001)
        戻り値:
            target_list: 変更されたリスト
        """

        for before_string, after_string in zip(before_string_list, after_string_list):

            target_list = [(ti[0], str(ti[1]).replace(str(before_string), str(after_string)))
                           if str(before_string) == (str(ti[1]))
                           else (ti[0], str(ti[1]))
                           for ti in target_list]
        return target_list

    def create_output_field_set_file(self, output_dwh_name, target_dwh, target_list):
        """
        概要:
            新しいDWH名が空の場合はinput時のDWH名を使い、それ以外の場合は新しいDWH名を使ってoutput_field_set_fileを作成する関数。
        引数:
            output_dwh_name: アウトプット時のDWH名
            target_dwh: インプット時のDWH名
            target_list: 文字列置換後のlist
        戻り値:
            output_field_set_file: 出力のフィールドセットファイル
        """

        # object型に設定
        new_type = "object"

        # set_field_set_file用にリスト化
        output_dwh_name_list = [output_dwh_name]
        target_dwh_list = [target_dwh]
        output_type_list = [new_type]
        output_target_list = [target_list]

        # 新しいDWH名が空の場合はgeometryのDWH名を使う
        if output_dwh_name == "":
            output_field_set_file = WM.calc_func_time(self.logger, False)(
                PBP.set_field_set_file)(target_dwh_list, output_type_list, output_target_list)
            attributes_dict = {"DWH": target_dwh}

        else:
            output_field_set_file = WM.calc_func_time(self.logger, False)(
                PBP.set_field_set_file)(output_dwh_name_list, output_type_list, output_target_list)
            attributes_dict = {"DWH": output_dwh_name}

        return output_field_set_file, attributes_dict

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティから replacement_definition_csv と output_dwh_name を取得
            replacement_definition_csv, \
                output_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # CSVデータをDataFrameに変換
            before_string_list, \
                after_string_list\
                = WM.calc_func_time(self.logger)(self.csv_to_dataframe)(replacement_definition_csv)

            # FieldSetFileからDWHと材質リストを取得
            field_set_file_dataframe, \
                target_dwh, \
                material_type, \
                target_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 属性値を置き換える
            target_list\
                = WM.calc_func_time(self.logger)(self.replace_in_list)(target_list,
                                                                       before_string_list,
                                                                       after_string_list)

            # 出力のFieldSetFileを作成
            output_field_set_file, \
                attributes_dict\
                = WM.calc_func_time(self.logger)(self.create_output_field_set_file)(output_dwh_name,
                                                                                    target_dwh,
                                                                                    target_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
