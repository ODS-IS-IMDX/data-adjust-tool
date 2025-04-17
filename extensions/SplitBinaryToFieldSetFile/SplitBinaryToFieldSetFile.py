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

# -----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 可変長バイナリに対して、内部に書かれた繰り返し回数を読みつつ各レコードに区切る。
# デコードは行わない。
# -----------------------------------------------------------------------------------------------------------

# 標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
pd = import_module("pandas")

# Constants
COLUMN_NAME = "カラム名"
DATA_TYPE = "データ型"
DIGITS = "バイト数"

CONST_HEX = "16進数"
CONST_BINARY = "2進数"

COLUMN_FINDEX = "Findex"


class SplitBinaryToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        バイナリをデコードせずに指定のバイト数で分割し、FieldSetFileにする。
                        ①input: 主キーのリストとバイナリのリストを持った2行のFieldSetFile。
                        ②output: バイナリを指定した属性データごとに分割した1行以上のFieldSetFile。
                      """
        tags = ['Binary', 'Python']

    # バイナリの進数
    ENCODING_NUMBER_SYSTEM = PropertyDescriptor(
        name='Encoding Number System',
        description='バイナリの進数',
        default_value=CONST_HEX,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False,
        allowable_values=[CONST_HEX, CONST_BINARY],
    )

    # バイナリデータを区切り、値を格納するためのデータフレームの定義。
    HEADER_DEFINITION_CSV = PropertyDescriptor(
        name='Header Definition CSV',
        description="バイナリのヘッダ構成の定義（CSV形式）",
        default_value='''
下記を参考にして修正すること

==========ここから==========
カラム名,データ型,バイト数
項目A,3,4
項目B,1,2
項目C,1,10
==========ここまで==========

※1行目のカラムは固定かつ必須
    2行目以降は定義項目を入力

    [データ型列入力規則]
    1…整数型
    2…浮動小数点型
    3…文字列型
                      ''',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # ヘッダ内にある、1レコードあたりのボディ部の繰り返す回数を示した数値のインデックス番号。
    LOOP_INDEX = PropertyDescriptor(
        name='Loop Index',
        description="ボディのループ回数を指定した、ヘッダのカラム番号（定義項目の1行目が0)",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    # バイナリデータを区切り、値を格納するためのDataFrameの定義。
    BODY_DEFINITION_CSV = PropertyDescriptor(
        name='Body Definition CSV',
        description="バイナリのボディ構成の定義（CSV形式）",
        default_value='''
下記を参考にして修正すること

==========ここから==========
カラム名,データ型,バイト数
項目A,3,4
項目B,1,2
項目C,1,10
==========ここまで==========

※1行目のカラムは固定かつ必須
    2行目以降は定義項目を入力

    [データ型列入力規則]
    1…整数型
    2…浮動小数点型
    3…文字列型
                      ''',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # バイナリデータを区切り、値を格納するためのDataFrameの定義。
    FOOTER_DEFINITION_CSV = PropertyDescriptor(
        name='Footer Definition CSV',
        description="バイナリのフッタ構成の定義（CSV形式）",
        default_value='''
下記を参考にして修正すること

==========ここから==========
カラム名,データ型,バイト数
項目A,3,4
項目B,1,2
項目C,1,10
==========ここまで==========

※1行目のカラムは固定かつ必須
    2行目以降は定義項目を入力

    [データ型列入力規則]
    1…整数型
    2…浮動小数点型
    3…文字列型
                      ''',
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=False
    )

    # 入荷時主キーのDWH名
    INPUT_PRIMARY_KEY_DWH = PropertyDescriptor(
        name="Input Primary Key DWH",
        description="入力データの主キーのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # 出荷時のバイナリデータのDWH名
    INPUT_BINARY_DWH = PropertyDescriptor(
        name="Output Binary DWH",
        description="出力データのバイナリのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    property_descriptors = [ENCODING_NUMBER_SYSTEM,
                            HEADER_DEFINITION_CSV,
                            LOOP_INDEX,
                            BODY_DEFINITION_CSV,
                            FOOTER_DEFINITION_CSV,
                            INPUT_PRIMARY_KEY_DWH,
                            INPUT_BINARY_DWH]

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
            flowfile: プロセッサに渡されるデータ

        戻り値:
            encoding_number_system: バイナリの進数
            header_definition_csv: バイナリデータのヘッダ部を区切り、値を格納するためのCSV形式のデータフレームの定義
            loop_index: ヘッダ内部でbodyを繰り返す回数を示した数のインデックス番号
            body_definition_csv: バイナリデータのボディ部を区切り、値を格納するためのCSV形式のデータフレームの定義
            footer_definition_csv: バイナリデータのフッタ部を区切り、値を格納するためのCSV形式のデータフレームの定義
            input_primary_key_dwh: 区切る際に何を主キーとして用いるもののDWH名
            input_binary_dwh: バイナリのDWH名
        """
        encoding_number_system = context.getProperty(
            self.ENCODING_NUMBER_SYSTEM).evaluateAttributeExpressions(flowfile).getValue()

        header_definition_csv = context.getProperty(
            self.HEADER_DEFINITION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        loop_index = int(context.getProperty(
            self.LOOP_INDEX).evaluateAttributeExpressions(flowfile).getValue())

        body_definition_csv = context.getProperty(
            self.BODY_DEFINITION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        footer_definition_csv = context.getProperty(
            self.FOOTER_DEFINITION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        input_primary_key_dwh = context.getProperty(
            self.INPUT_PRIMARY_KEY_DWH).evaluateAttributeExpressions(flowfile).getValue()

        input_binary_dwh = context.getProperty(
            self.INPUT_BINARY_DWH).evaluateAttributeExpressions(flowfile).getValue()

        return encoding_number_system, header_definition_csv, loop_index, body_definition_csv, footer_definition_csv, input_primary_key_dwh, input_binary_dwh

    def make_list_from_flowfile(self, flowfile, input_primary_key_dwh, input_binary_dwh):
        """
        概要:
            フローファイルの中でも必要な情報を取得する関数。

        引数:
            flowfile: フローファイル
            input_primary_key_dwh: 主キーのDWH名
            input_binary_dwh: バイナリのDWH名

        戻り値:
            primary_key_and_binary_list: 主キーとバイナリを[(主キー, バイナリ), (主キー, バイナリ)...]という形にしたもの
        """
        # flowfileから、CSV形式のFieldSetFileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # CSVファイルの読み込み
        field_set_dataframe = pd.read_csv(io.StringIO(input_field_set_file))

        # 2つのリストを取得
        primary_key_list = pickle.loads(base64.b64decode(
            field_set_dataframe.loc[field_set_dataframe["Dwh"] == input_primary_key_dwh, "Value"].item()))
        binary_list = pickle.loads(base64.b64decode(
            field_set_dataframe.loc[field_set_dataframe["Dwh"] == input_binary_dwh, "Value"].item()))

        # DataFrameにする。
        primary_key_dataframe = pd.DataFrame(primary_key_list, columns=[
                                             COLUMN_FINDEX, input_primary_key_dwh])
        binary_dataframe = pd.DataFrame(
            binary_list, columns=[COLUMN_FINDEX, input_binary_dwh])

        # Findexをキーとしてマージ
        merged_dataframe = pd.merge(
            primary_key_dataframe, binary_dataframe, on=COLUMN_FINDEX)

        primary_key_and_binary_list = list(
            zip(merged_dataframe[input_primary_key_dwh], merged_dataframe[input_binary_dwh]))

        return primary_key_and_binary_list

    def get_encoding_temporary(self, encoding_number_system):
        """
        概要:
            進数を数字に直し、"バイト数"を"文字数"に変える関数。

        引数:
            encoding_number_system: バイナリの進数

        戻り値:
            encoding_number: 進数
            bytes_to_characters_number: "バイト数"を"文字数"に変えるためのかける数
        """

        if encoding_number_system == CONST_HEX:
            encoding_number = 16
            bytes_to_characters_number = 2

        elif encoding_number_system == CONST_BINARY:
            encoding_number = 2
            bytes_to_characters_number = 8

        return encoding_number, bytes_to_characters_number

    def load_data_type_code(self, data_type_code):
        """
        概要:
            CSV内の"データ型"をコードからtypeに変える関数。

        引数:
            data_type_code: CSVで指定した型のコード。

        戻り値:
            data_type: コードに対応する型
        """

        if data_type_code == 1:
            data_type = int
        elif data_type_code == 2:
            data_type = float
        elif data_type_code == 3:
            data_type = str
        elif data_type_code == 4:
            data_type = bytes

        return data_type

    def csv_to_dataframe(self, definition_csv_list):
        """
        概要:
            CSVデータをDataFrameに変換する関数

        引数:
            definition_csv_list: CSVのリスト。[ヘッダ, ボディ, フッタ]の順で入る。

        戻り値:
            definition_data_frame_list: それぞれのCSVをデータフレームにしたもの。
                                        プロパティで入力しなかった場合、各要素は0として扱う。
        """

        # 関数return用のリストを作成
        definition_data_frame_list = []

        for definition_csv in definition_csv_list:

            if definition_csv is not None and definition_csv != "":

                # CSVデータをDataFrameに読み込み
                definition_data_frame = pd.read_csv(io.StringIO(definition_csv), usecols=[
                                                    COLUMN_NAME, DATA_TYPE, DIGITS])

                # "カラム名"列の欠損値を"None"で埋める
                definition_data_frame[COLUMN_NAME] = definition_data_frame[COLUMN_NAME].fillna(
                    'None')

                # 定義ファイルでは"データ型"列がコードになっているので、それぞれをデータ型に直す
                definition_data_frame[DATA_TYPE] = definition_data_frame[DATA_TYPE].apply(
                    WM.calc_func_time(self.logger, False)(self.load_data_type_code))

            else:
                definition_dict = {
                    COLUMN_NAME: [0],
                    DATA_TYPE: [0],
                    DIGITS: [0]
                }

                definition_data_frame = pd.DataFrame(definition_dict)

            definition_data_frame_list.append(definition_data_frame)

        return definition_data_frame_list

    def append_row_list(self, definition_dataframe, one_row, record, row_list, bytes_to_characters_number, after_character, before_character):
        """
        概要:
            データを区切り、リストにデータを1行分ごとに格納していく関数。

        引数:
            definition_dataframe: 定義となるDataFrame
            one_row: 行数
            record: 区切る前の1行のデータ
            row_list: 格納先のリスト
            bytes_to_characters_number: "バイト数"を"文字数"に変えるためのかける数
            after_character: 区切りの文字数
            before_character: 区切りの文字数

        戻り値:
            row_list: データを格納してreturnする。
            before_character: 区切りの文字数
            after_character: 区切りの文字数
        """

        # バイナリの区切り文字数
        after_character += bytes_to_characters_number * \
            (definition_dataframe.loc[one_row, DIGITS])

        # 01の文字列を区切る。
        one_data = record[before_character:after_character]

        # データタイプを変数にいれておく
        data_type = definition_dataframe.loc[one_row, DATA_TYPE]

        # DataFrameに格納される値
        target_value = data_type(one_data)

        # 1つ分のデータをリストに格納する
        row_list.append(target_value)

        # バイナリの区切り文字数
        before_character += bytes_to_characters_number * \
            (definition_dataframe.loc[one_row, DIGITS])

        return row_list, before_character, after_character

    def transform(self, context, flowfile):
        try:

            # プロパティ入力情報取得
            encoding_number_system, \
                header_definition_csv, \
                loop_index, \
                body_definition_csv, \
                footer_definition_csv, \
                input_primary_key_dwh, \
                input_binary_dwh\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # Findexをキーとしてリストを結合する。
            primary_key_and_binary_list\
                = WM.calc_func_time(self.logger)(self.make_list_from_flowfile)(flowfile,
                                                                               input_primary_key_dwh,
                                                                               input_binary_dwh)

            # リストにしておく。
            definition_csv_list = [header_definition_csv,
                                   body_definition_csv, footer_definition_csv]

            # プロパティから取得した区切りの定義から必要なカラムを抜き出しリストにする。
            definition_data_frame_list\
                = WM.calc_func_time(self.logger)(self.csv_to_dataframe)(definition_csv_list)

            # 進数によって見る文字数が異なるのでそれを計算
            encoding_number, \
                bytes_to_characters_number\
                = WM.calc_func_time(self.logger)(self.get_encoding_temporary)(encoding_number_system)

            # 全量のデータを格納するリスト
            full_list = []

            # 1行ごとにループ
            for key, record in primary_key_and_binary_list:

                # 1行の最大数を取得
                max_character = len(record)

                # 1行ごとに区切り文字数を初期化
                before_character, after_character = 0, 0

                # 可変のためwhile文でのループ。ヘッダごとにループをする。
                while max_character > before_character:

                    # 1行分のリストを初期化
                    row_list = []

                    # ヘッダを取得
                    for header_row in range(len(definition_data_frame_list[0])):

                        row_list, \
                            before_character, \
                            after_character\
                            = WM.calc_func_time(self.logger)(self.append_row_list)(definition_data_frame_list[0],
                                                                                   header_row,
                                                                                   record,
                                                                                   row_list,
                                                                                   bytes_to_characters_number,
                                                                                   after_character,
                                                                                   before_character)

                    # ヘッダ内に書かれたボディの個数を取得
                    loop_times = int(row_list[loop_index], encoding_number)

                    # ボディ部分を格納するリスト
                    body_list = []

                    # ヘッダ部で指定された回数ボディ部をループする。
                    for times in range(loop_times):

                        # 区切りは同じボディ部分の処理
                        for body_row in range(len(definition_data_frame_list[1])):

                            body_list, \
                                before_character, \
                                after_character\
                                = WM.calc_func_time(self.logger)(self.append_row_list)(definition_data_frame_list[1],
                                                                                       body_row,
                                                                                       record,
                                                                                       body_list,
                                                                                       bytes_to_characters_number,
                                                                                       after_character,
                                                                                       before_character)

                    coordinates_binary = ''.join(body_list)

                    # 1つ分のデータをリストに格納する
                    row_list.append(coordinates_binary)

                    if definition_data_frame_list[2][DIGITS].sum() != 0:

                        for footer_row in range(len(definition_data_frame_list[2])):

                            row_list, \
                                before_character, \
                                after_character\
                                = WM.calc_func_time(self.logger)(self.append_row_list)(definition_data_frame_list[2],
                                                                                       footer_row,
                                                                                       record,
                                                                                       row_list,
                                                                                       bytes_to_characters_number,
                                                                                       after_character,
                                                                                       before_character)

                    # 各行で対応するkeyを結合する。
                    row_list = [key] + row_list

                    # 全体のリスト
                    full_list.append(row_list)

            # value_listを作成
            full_dataframe = pd.DataFrame(full_list, index=None)
            value_dataframe = full_dataframe.apply(
                lambda col: [(float(i), col[i]) for i in col.index])
            value_list = [value_dataframe[col].tolist()
                          for col in value_dataframe.columns]

            # dwh_name_listを作成
            header_column_name_list = definition_data_frame_list[0][COLUMN_NAME].tolist(
            )
            dwh_name_list = [input_primary_key_dwh] + \
                header_column_name_list + [input_binary_dwh]

            # フッタがあるなら結合
            if definition_data_frame_list[2][DIGITS].sum() != 0:
                footer_column_name_list = definition_data_frame_list[2][COLUMN_NAME].tolist(
                )
                dwh_name_list += footer_column_name_list

            # type_listを作成
            key_type = "キー"
            body_type = encoding_number_system
            definition_data_frame_list[0][DATA_TYPE] = encoding_number_system
            type_list = [
                key_type] + definition_data_frame_list[0][DATA_TYPE].tolist() + [body_type]

            # フッタがあるなら結合
            if definition_data_frame_list[2][DIGITS].sum() != 0:
                definition_data_frame_list[2][DATA_TYPE] = encoding_number_system
                type_list += definition_data_frame_list[2][DATA_TYPE].tolist()

            output_field_set_file = WM.calc_func_time(self.logger)(
                PBP.set_field_set_file)(dwh_name_list, type_list, value_list)

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
