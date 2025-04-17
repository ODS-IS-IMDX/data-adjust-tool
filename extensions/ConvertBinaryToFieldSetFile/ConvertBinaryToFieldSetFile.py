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
# バイナリファイルでの入荷プロセッサ。
# バイナリファイルをデコードし、定義通りに分割を行ったうえでFieldSetFileに分割する。
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
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")

# Constants
COLUMN_NAME = "カラム名"
DATA_TYPE = "データ型"
DIGITS = "桁数"

DATA_TYPE_BINARY = "バイナリ"
DATA_TYPE_TEXT = "固定長テキスト"

ENCODING_BINARY = "2進数"
ENCODING_HEX = "16進数"
ENCODING_SHIFT_JIS = "shift-jis"
ENCODING_UTF_8 = "utf-8"
ENCODING_UTF_16_BE = "utf-16-be"
ENCODING_UTF_16_LE = "utf-16-le"

ENCODING_ALLOWABLE_LIST = [
    ENCODING_BINARY,
    ENCODING_HEX,
    ENCODING_SHIFT_JIS,
    ENCODING_UTF_8,
    ENCODING_UTF_16_BE,
    ENCODING_UTF_16_LE
]

NEGATIVE_FLAG_TRUE = "解釈する"
NEGATIVE_FLAG_FALSE = "解釈しない"


class ConvertBinaryToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        バイナリをヘッダ,ボディ,フッタに分けて、FieldSetFileに変換する。
                        ①input: バイナリデータかバイナリデータを持ったFieldSetFile。もしくは固定長テキストか固定長テキストを持ったFieldSetFile。
                        ②output: バイナリをヘッダ,ボディ,フッタに分けたDataFrameを持ったFieldSetFile。
                      """
        tags = ['Binary', 'Dataframe', 'Python']

    INPUT_DATA_TYPE = PropertyDescriptor(
        name='Input Data Type',
        description='フローファイルとして取り込むデータの種類',
        default_value=DATA_TYPE_BINARY,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False,
        allowable_values=[DATA_TYPE_BINARY, DATA_TYPE_TEXT],
    )

    # バイナリファイルのデコードに用いる文字コード
    BINARY_ENCODING_CODE = PropertyDescriptor(
        name='Binary Encoding Code',
        description='バイナリファイルのデコードに用いる文字コード',
        default_value=ENCODING_SHIFT_JIS,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False,
        allowable_values=ENCODING_ALLOWABLE_LIST
    )

    BINARY_NEGATIVE_FLAG = PropertyDescriptor(
        name='Binary Negative Flag',
        description='2進数の先頭ビットを符号ビットとして解釈するかどうかのフラグ',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=False,
        sensitive=False,
        default_value=NEGATIVE_FLAG_TRUE,
        allowable_values=[NEGATIVE_FLAG_TRUE, NEGATIVE_FLAG_FALSE]
    )

    # バイナリデータを区切り、値を格納するためのDataFrameの定義。
    BODY_DEFINITION_CSV = PropertyDescriptor(
        name='Body Definition CSV',
        description="バイナリのボディ構成の定義（CSV形式）",
        default_value="""下記を参考にして修正すること

==========ここから==========
カラム名,データ型,桁数
項目A,3,4
項目B,1,2
項目C,1,10
==========ここまで==========

※1行目のカラムは固定かつ必須
　2行目以降は定義項目を入力

　[データ型列入力規則]
　　1…整数型
　　2…浮動小数点型
　　3…整数型
                      """,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False,
    )

    # ボディのDWH
    BODY_OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Body Output DWH Name",
        description="出力データのボディのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # バイナリデータを区切り、値を格納するためのDataFrameの定義。
    HEADER_DEFINITION_CSV = PropertyDescriptor(
        name='Header Definition CSV',
        description="バイナリのヘッダ構成の定義（CSV形式）",
        default_value="""下記を参考にして修正すること

==========ここから==========
カラム名,データ型,桁数
項目A,3,4
項目B,1,2
項目C,1,10
==========ここまで==========

※1行目のカラムは固定かつ必須
　2行目以降は定義項目を入力

　[データ型列入力規則]
　　1…整数型
　　2…浮動小数点型
　　3…整数型
                      """,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    # ヘッダのDWH
    HEADER_OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Header Output DWH Name",
        description="出力データのヘッダのDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # バイナリデータを区切り、値を格納するためのDataFrameの定義。
    FOOTER_DEFINITION_CSV = PropertyDescriptor(
        name='Footer Definition CSV',
        description="バイナリのフッタ構成の定義（CSV形式）",
        default_value="""下記を参考にして修正すること

==========ここから==========
カラム名,データ型,桁数
項目A,3,4
項目B,1,2
項目C,1,10
==========ここまで==========

※1行目のカラムは固定かつ必須
　2行目以降は定義項目を入力

　[データ型列入力規則]
　　1…整数型
　　2…浮動小数点型
　　3…整数型
                      """,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    # フッタのDWH
    FOOTER_OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Footer Output DWH Name",
        description="出力データのフッタのDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [INPUT_DATA_TYPE,
                            BINARY_ENCODING_CODE,
                            BINARY_NEGATIVE_FLAG,
                            BODY_DEFINITION_CSV,
                            BODY_OUTPUT_DWH_NAME,
                            HEADER_DEFINITION_CSV,
                            HEADER_OUTPUT_DWH_NAME,
                            FOOTER_DEFINITION_CSV,
                            FOOTER_OUTPUT_DWH_NAME]

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
            input_data_type: フローファイルとして取り込むデータの種類
            binary_encoding_code: バイナリのデコードに用いる文字コード
            binary_negative_flag: 負数を扱うかのフラグ
            body_definition_csv: バイナリデータのボディ部を区切り、値を格納するためのCSV形式のDataFrameの定義
            body_output_dwh_name: 出力後のボディのDWH
            header_definition_csv: バイナリデータのヘッダ部を区切り、値を格納するためのCSV形式のDataFrameの定義
            header_output_dwh_name: 出力後のヘッダのDWH
            footer_definition_csv: バイナリデータのフッタ部を区切り、値を格納するためのCSV形式のDataFrameの定義
            footer_output_dwh_name出力後のフッタのDWH
        """
        input_data_type = context.getProperty(
            self.INPUT_DATA_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        binary_encoding_code = context.getProperty(
            self.BINARY_ENCODING_CODE).evaluateAttributeExpressions(flowfile).getValue()

        binary_negative_flag = context.getProperty(
            self.BINARY_NEGATIVE_FLAG).getValue()

        if binary_negative_flag == NEGATIVE_FLAG_TRUE:
            binary_negative_flag = True
        else:
            binary_negative_flag = False

        body_definition_csv = context.getProperty(
            self.BODY_DEFINITION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        body_output_dwh_name = context.getProperty(
            self.BODY_OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        header_definition_csv = context.getProperty(
            self.HEADER_DEFINITION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        header_output_dwh_name = context.getProperty(
            self.HEADER_OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        footer_definition_csv = context.getProperty(
            self.FOOTER_DEFINITION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        footer_output_dwh_name = context.getProperty(
            self.FOOTER_OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return input_data_type, binary_encoding_code, binary_negative_flag, body_definition_csv, body_output_dwh_name, \
            header_definition_csv, header_output_dwh_name, footer_definition_csv, footer_output_dwh_name

    def load_data_type_code(self, data_type_code):
        """
        概要:
            csv内の"データ型"をコードからtypeに変える関数

        引数:
            data_type_code: csvで指定した型のコード

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
            definition_csv_list:プロパティで入力したヘッダ、ボディ、フッタの定義

        戻り値:
            definition_data_frame_list:csvファイルをDataFrameに変換したもの。
                                       プロパティを入力していないものは各データが0のDataFrameを作成。
        """

        # 関数return用のリストを作成
        definition_data_frame_list = []

        for definition_csv in definition_csv_list:

            # CSV系のプロパティ入力時
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

            # CSV系のプロパティ未入力時
            else:
                definition_dict = {
                    COLUMN_NAME: [0],
                    DATA_TYPE: [0],
                    DIGITS: [0]
                }

                definition_data_frame = pd.DataFrame(definition_dict)

            definition_data_frame_list.append(definition_data_frame)

        return definition_data_frame_list

    def get_binary_data(self, flowfile, input_data_type, binary_encoding_code):
        """
        概要:
            flowfileのバイト列をエンディアンを考慮したバイト列であるエンディアン値に変換する。

        引数:
            flowfile: プロセッサに渡されるデータ
            input_data_type: 入ってくるデータのタイプ
            binary_encoding_code: エンコーディングコード

        戻り値:
            binary_data_list: 入荷のバイナリファイルのコンテンツをFindexと合わせてリストにしたもの
        """

        # FieldSetFileがインプットされた場合
        try:

            # Flowfileから、csv形式のfield_set_fileを取得。
            # ★utf-8エンコードではない生データの場合UnicodeDecodeErrorでexceptに移動。。
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # flowfile_contentsをpathとして扱う。
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに加工。
            field_set_file_dataframe = pd.read_csv(field_set_file_stringio)

            # Findexとバイナリのリスト。
            # ★生データの場合KeyErrorでexceptに移動。
            binary_data_list = pickle.loads(base64.b64decode(
                field_set_file_dataframe.loc[0, "Value"]))

        # FieldSetFileではなく生データがインプットされた場合
        except (UnicodeDecodeError, KeyError):

            # バイナリの場合
            if input_data_type == DATA_TYPE_BINARY:

                binary_data_list = [(0, flowfile.getContentsAsBytes())]

            # テキストの場合
            elif input_data_type == DATA_TYPE_TEXT:

                binary_data_list = [
                    (0, flowfile.getContentsAsBytes().decode(binary_encoding_code))]

        return binary_data_list

    def null_replace(self, decode_value, data_type):
        """
        概要:
            null値が含まれていた際に処理に影響のない値に置換する関数。

        引数:
            decode_value: デコードされた値
            data_type: 想定されるデータ型

        戻り値:
            null_replaced_value: nullを置換した値
        """
        if '\x00' in decode_value:

            # 数値型はここでは文字列の0に置換しておく
            if data_type == int or data_type == float:
                null_replaced_value = decode_value.replace('\x00', '0')
                return null_replaced_value

            # 文字列型は空文字にする
            elif data_type == str:
                null_replaced_value = decode_value.replace('\x00', '')
                return null_replaced_value

        # nullがなければそのままの値を返す。
        return decode_value

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

        # 16進数バイナリの場合
        if encoding_number_system == ENCODING_HEX:
            encoding_number = 16
            bytes_to_characters_number = 2

        # 2進数バイナリの場合
        elif encoding_number_system == ENCODING_BINARY:
            encoding_number = 2
            bytes_to_characters_number = 8

        return encoding_number, bytes_to_characters_number

    def append_processed_binary(self, definition_data_frame, binary_data_from_contents, element_index, encoding_number,
                                bytes_to_characters_number, row_list, binary_negative_flag, after_character, before_character):
        """
        概要:
            数字のバイナリの時、1行分のリストを作成する関数。

        引数:
            definition_data_frame: 定義となるDataFrame
            binary_data_from_contents: binary1つ分
            element_index: 要素のインデックス番号
            encoding_number: 進数
            bytes_to_characters_number: バイト数を文字数に変えるときにかける数
            row_list: 格納先リスト
            binary_negative_flag: バイナリで負数を扱うかのフラグ
            before_character: 区切りの文字数
            after_character: 区切りの文字数

        戻り値:
            row_list: 1行分のリスト
            before_character: 区切りの文字数
            after_character: 区切りの文字数
        """

        character_number = bytes_to_characters_number * \
            (definition_data_frame.loc[element_index, DIGITS])

        # バイナリの区切り文字数
        after_character += character_number

        # 01の文字列を区切る。
        binary = binary_data_from_contents[before_character:after_character]

        # 指定の進数から10進数に変換する。
        decode_value = int(binary, encoding_number)

        # バイナリの最上位桁を負数のフラグとして解釈する場合
        if binary_negative_flag:
            decode_value = int(binary, encoding_number)
            if int(binary[0], encoding_number) > (encoding_number/2):
                over_number_string = "1" + character_number*"0"
                over_number = int(over_number_string, encoding_number)
                decode_value -= over_number

        # データタイプを変数にいれておく
        data_type = definition_data_frame.loc[element_index, DATA_TYPE]

        # DataFrameに格納される値
        target_value = data_type(decode_value)

        # 1つ分のデータをリストに格納する
        row_list.append(target_value)

        # バイナリの区切り文字数
        before_character += character_number

        return row_list, after_character, before_character

    def make_row_list(self, input_data_type, definition_data_frame, binary_data_from_contents,
                      binary_encoding_code, binary_negative_flag, before_character=0, after_character=0):
        """
        概要:
            1行分のリストを作成する関数。

        引数:
            input_data_type: フローファイルのデータ形式
            definition_data_frame: 定義となるDataFrame
            binary_data_from_contents: binary1つ分
            binary_encoding_code: バイナリのエンコーディング
            binary_negative_flag: バイナリで負数を扱うかのフラグ
            before_character: 区切りの文字数
            after_character: 区切りの文字数


        戻り値:
            row_list: 1行分のリスト
            before_character: 区切りの文字数
            after_character: 区切りの文字数
        """
        # 1行分のリスト
        row_list = []

        # エンコーディングが01のバイナリではないとき
        if binary_encoding_code != ENCODING_BINARY and binary_encoding_code != ENCODING_HEX:
            # 1行を読み込んでいく
            for j in range(len(definition_data_frame[DIGITS])):

                # バイナリの区切り文字数
                after_character += definition_data_frame.loc[j, DIGITS]

                # データタイプを変数にいれておく
                data_type = definition_data_frame.loc[j, DATA_TYPE]

                # 固定長テキスト以外の場合は
                if input_data_type != DATA_TYPE_TEXT:
                    # コンテンツをデコードする
                    decode_value = binary_data_from_contents[before_character: after_character].decode(
                        binary_encoding_code)

                    # nullバイトをそれぞれの型に則した空文字に変換
                    null_replaceded_value = WM.calc_func_time(self.logger, False)(
                        self.null_replace)(decode_value, data_type)

                else:
                    # テキストのときはデコードが不要
                    null_replaceded_value = binary_data_from_contents[before_character: after_character]

                # DataFrameに格納される値
                target_value = data_type(null_replaceded_value)

                # 1つ分のデータをリストに格納する
                row_list.append(target_value)

                # バイナリの区切り文字数
                before_character += definition_data_frame.loc[j, DIGITS]

        else:

            # 16進数か2進数かで後続処理で行う乗算の定数を変更する。
            encoding_number, \
                bytes_to_characters_number\
                = self.get_encoding_temporary(binary_encoding_code)

            # 区切りたい要素の個数分ループ
            for j in range(len(definition_data_frame[DIGITS])):

                row_list, \
                    after_character, \
                    before_character\
                    = WM.calc_func_time(self.logger, False)(self.append_processed_binary)(definition_data_frame,
                                                                                          binary_data_from_contents,
                                                                                          j,
                                                                                          encoding_number,
                                                                                          bytes_to_characters_number,
                                                                                          row_list,
                                                                                          binary_negative_flag,
                                                                                          after_character,
                                                                                          before_character)

        return row_list, before_character, after_character

    def get_body_data_frame(self, input_data_type, binary_data_from_contents, definition_data_frame_list,
                            binary_encoding_code, binary_negative_flag, before_character, after_character):
        """
        概要:
            ボディ部のDataFrameを作成する関数。

        引数:
            input_data_type: フローファイルのデータ形式
            binary_data_from_contents: フローファイルのバイナリデータ
            definition_data_frame_list: 定義となるDataFrameのリスト。[1]がボディ部。
            binary_encoding_code: バイナリのエンコーディング
            binary_negative_flag: バイナリで負数を扱うかのフラグ
            before_character: 区切りの文字数
            after_character: 区切りの文字数

        戻り値:
            body_data_frame: ボディのDataFrame
        """

        # ボディ部のDataFrameを変数に格納
        body_definition_data_frame = definition_data_frame_list[1]

        # DataFrameの元となるリスト
        value_list = []

        # 後続のループのため、バイナリの総文字数
        binary_data_length = len(binary_data_from_contents)

        if binary_encoding_code != ENCODING_BINARY and binary_encoding_code != ENCODING_HEX:
            data_character = binary_data_length - \
                (definition_data_frame_list[0][DIGITS].sum(
                ) + definition_data_frame_list[2][DIGITS].sum())
            loop_times = int(int(data_character) /
                             int(body_definition_data_frame[DIGITS].sum()))

        elif binary_encoding_code == ENCODING_BINARY:
            data_character = binary_data_length - 8 * \
                (definition_data_frame_list[0][DIGITS].sum(
                ) + definition_data_frame_list[2][DIGITS].sum())
            loop_times = int(int(data_character) /
                             (8*int(body_definition_data_frame[DIGITS].sum())))

        elif binary_encoding_code == ENCODING_HEX:
            data_character = binary_data_length - 2 * \
                (definition_data_frame_list[0][DIGITS].sum(
                ) + definition_data_frame_list[2][DIGITS].sum())
            loop_times = int(int(data_character) /
                             (2*int(body_definition_data_frame[DIGITS].sum())))

        # 行数分のループ
        for i in range(loop_times):

            # 1行のデータを作成する。
            row_list, \
                before_character, \
                after_character\
                = WM.calc_func_time(self.logger, False)(self.make_row_list)(input_data_type,
                                                                            body_definition_data_frame,
                                                                            binary_data_from_contents,
                                                                            binary_encoding_code,
                                                                            binary_negative_flag,
                                                                            before_character,
                                                                            after_character)

            # 1行ずつのデータをリストに格納
            value_list.append(row_list)

        # カラム名のリストを作成
        column_name_list = body_definition_data_frame[COLUMN_NAME].values.tolist(
        )

        # DataFrameに変換する
        body_data_frame = pd.DataFrame(value_list, columns=column_name_list)

        return body_data_frame

    def add_findex_column_and_list_append(self, data_frame, findex, part_list):
        """
        概要:
            DataFrameにfindexを付与し、出力用のリストに格納していく関数。

        引数:
            data_frame: 1行分のDataFrame
            findex: 繰り返しのインデックス
            part_list: パーツ（ヘッダ,ボディ,フッタ）のいずれかのリスト

        戻り値:
            part_list: findexを付与したDataFrameを格納したリスト
        """

        num_rows = data_frame.shape[0]

        # 要素がすべてfindexのDataFrameを作成
        findex_data_frame = pd.DataFrame({'Findex': [findex] * num_rows})

        data_frame_with_findex = pd.concat(
            [findex_data_frame, data_frame], axis=1)

        part_list.append(data_frame_with_findex)

        return part_list

    def transform(self, context, flowfile):
        try:

            # プロパティの値を取得
            input_data_type, \
                binary_encoding_code, \
                binary_negative_flag, \
                body_definition_csv, \
                body_output_dwh_name, \
                header_definition_csv, \
                header_output_dwh_name, \
                footer_definition_csv, \
                footer_output_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(context,
                                                                    flowfile)

            # リストにしておく。
            definition_csv_list = [header_definition_csv,
                                   body_definition_csv, footer_definition_csv]

            # プロパティから取得した区切りの定義から必要なカラムを抜き出しリストにする。
            definition_data_frame_list\
                = WM.calc_func_time(self.logger)(self.csv_to_dataframe)(definition_csv_list)

            # バイナリとバイナリデータ長を抽出
            binary_data_list\
                = WM.calc_func_time(self.logger)(self.get_binary_data)(flowfile,
                                                                       input_data_type,
                                                                       binary_encoding_code)

            # FieldSetFileにする際のリストを作成
            dwh_name_list = []
            type_list = []
            value_list = []

            # 各部のリストを作成
            header_list = []
            body_list = []
            footer_list = []

            # コンテンツのリスト[findex, binary_data_from_contents]をfor文で回す。
            for findex, binary_data_from_contents in binary_data_list:

                # プロパティでヘッダを入力していれば作成
                if definition_data_frame_list[0][DIGITS].sum() != 0:

                    # 1行分を作成
                    header_data_list, \
                        before_character, \
                        after_character\
                        = WM.calc_func_time(self.logger)(self.make_row_list)(input_data_type,
                                                                             definition_data_frame_list[0],
                                                                             binary_data_from_contents,
                                                                             binary_encoding_code,
                                                                             binary_negative_flag,
                                                                             before_character=0,
                                                                             after_character=0)

                    # リストをDataFrameにする。
                    header_data_frame = pd.DataFrame(
                        [header_data_list], columns=definition_data_frame_list[0][COLUMN_NAME].values.tolist())

                    # 出力用のリストに1行分を加える。
                    header_list\
                        = WM.calc_func_time(self.logger)(self.add_findex_column_and_list_append)(header_data_frame,
                                                                                                 findex,
                                                                                                 header_list)

                # なければbefore_character, after_characterを0とする。
                else:
                    before_character, after_character = 0, 0

                # バイナリのデコードを行い、DataFrameを作成する。
                body_data_frame\
                    = WM.calc_func_time(self.logger)(self.get_body_data_frame)(input_data_type,
                                                                               binary_data_from_contents,
                                                                               definition_data_frame_list,
                                                                               binary_encoding_code,
                                                                               binary_negative_flag,
                                                                               before_character,
                                                                               after_character)

                body_list\
                    = WM.calc_func_time(self.logger)(self.add_findex_column_and_list_append)(body_data_frame,
                                                                                             findex,
                                                                                             body_list)

                # プロパティでヘッダを入力していれば作成
                if definition_data_frame_list[2][DIGITS].sum() != 0:

                    # フッタはバイナリの後ろから読むために文字数をずらす
                    character_temporary = len(
                        binary_data_from_contents) - definition_data_frame_list[2][DIGITS].sum()

                    # 1行分を作成
                    footer_data_list, \
                        before_character, \
                        after_character\
                        = WM.calc_func_time(self.logger)(self.make_row_list)(input_data_type,
                                                                             definition_data_frame_list[2],
                                                                             binary_data_from_contents,
                                                                             binary_encoding_code,
                                                                             binary_negative_flag,
                                                                             before_character=character_temporary,
                                                                             after_character=character_temporary)

                    # リストをDataFrameにする。
                    footer_data_frame = pd.DataFrame(
                        [footer_data_list], columns=definition_data_frame_list[2][COLUMN_NAME].values.tolist())

                    # 出力用のリストに1行分を加える。
                    footer_list\
                        = WM.calc_func_time(self.logger)(self.add_findex_column_and_list_append)(footer_data_frame,
                                                                                                 findex,
                                                                                                 footer_list)

            # リストの要素を合体させる
            # FieldSetFile出荷処理（dwh_name_listにいれていく。/空なら追加しない）
            if header_list != []:
                concatenate_header_data_frame = pd.concat(
                    header_list, ignore_index=True)
                dwh_name_list.append(header_output_dwh_name)
                type_list.append("DataFrame")
                value_list.append(concatenate_header_data_frame)

            concatenate_body_data_frame = pd.concat(
                body_list, ignore_index=True)
            dwh_name_list.append(body_output_dwh_name)
            type_list.append("DataFrame")
            value_list.append(concatenate_body_data_frame)

            if footer_list != []:
                concatenate_footer_data_frame = pd.concat(
                    footer_list, ignore_index=True)
                dwh_name_list.append(footer_output_dwh_name)
                type_list.append("DataFrame")
                value_list.append(concatenate_footer_data_frame)

            # FieldSetFileを取得する
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_name_list,
                                                                         type_list,
                                                                         value_list)

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file, attributes={'encode': binary_encoding_code})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
