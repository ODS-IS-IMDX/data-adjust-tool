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
# バイナリファイルをデコードし、定義通りに分割を行ったうえでデータフレームに変換する。
# -----------------------------------------------------------------------------------------------------------

# 標準ライブラリ
import io
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
pd = import_module("pandas")

# Constants
COLUMN_NAME = "カラム名"
DATA_TYPE = "データ型"
DIGITS = "桁数"


class ConvertBinaryToDataFrame(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        バイナリデータを区切りながらデコードし、DataFrameに変換する。
                        ①input: バイナリデータ。
                        ②output: 指定したカラムのデータを持ったDataFrame。
                      """
        tags = ['Binary', 'Dataframe', 'Python']

    # バイナリファイルのデコードに用いる文字コード
    BINARY_ENCODING_CODE = PropertyDescriptor(
        name='Binary Encoding Code',
        description='バイナリファイルのデコードに用いる文字コード',
        default_value='shift-jis',
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    # バイナリデータを区切り、値を格納するためのデータフレームの定義。
    PARTITION_DEFINITION_CSV = PropertyDescriptor(
        name='Partition Definition CSV',
        description='バイナリ構成の定義（CSV形式）',
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
　　3…文字列型
                      """,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    property_descriptors = [BINARY_ENCODING_CODE,
                            PARTITION_DEFINITION_CSV]

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
            binary_encoding_code: エンコード
            partition_definition_csv:
        """
        binary_encoding_code = context.getProperty(
            self.BINARY_ENCODING_CODE).getValue()

        partition_definition_csv = context.getProperty(
            self.PARTITION_DEFINITION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        return binary_encoding_code, partition_definition_csv

    def load_data_type_code(self, data_type):
        """
        概要:
            データ型のコードをTypeのクラスに変換する関数。

        引数:
            data_type: 定義に記載のデータ型のコード

        戻り値:
            コードに対応するデータ型
        """
        if data_type == 1:
            return int
        elif data_type == 2:
            return float
        elif data_type == 3:
            return str

    def csv_to_dataframe(self, partition_definition_csv):
        """
        概要:
            CSVデータをデータフレームに変換する関数

        引数:
            partition_definition_csv: 取得したCSVデータ

        戻り値:
            partition_definition_data_frame: csvをdataframeにしたもの
        """
        # CSVデータをデータフレームに読み込み
        partition_definition_data_frame = pd.read_csv(io.StringIO(
            partition_definition_csv), usecols=[COLUMN_NAME, DATA_TYPE, DIGITS])

        # "カラム名"列の欠損値を"None"で埋める
        partition_definition_data_frame[COLUMN_NAME] = partition_definition_data_frame[COLUMN_NAME].fillna(
            'None')

        # 定義ファイルでは"データ型"列がコードになっているので、それぞれをデータ型に直す
        partition_definition_data_frame[DATA_TYPE] = partition_definition_data_frame[DATA_TYPE].apply(
            WM.calc_func_time(self.logger, False)(self.load_data_type_code))

        return partition_definition_data_frame

    def get_binary_data(self, flowfile):
        """
        概要:
            flowfileのバイト列をエンディアンを考慮したバイト列であるエンディアン値に変換する。

        引数:
            flowfile: プロセッサに渡されるデータ

        戻り値:
            binary_data_from_contents: 入荷のバイナリファイルのコンテンツ
            binary_data_length: バイナリの文字数
        """
        # フローファイルのコンテンツを取得
        binary_data_from_contents = flowfile.getContentsAsBytes()

        # バイナリ長を取得
        binary_data_length = len(binary_data_from_contents)

        return binary_data_from_contents, binary_data_length

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

    def get_output_data_frame(self, binary_data_from_contents, partition_definition_data_frame, binary_encoding_code, binary_data_length):
        """
        概要:
            データフレームを作成する関数。

        引数:
            binary_data_from_contents: フローファイルのバイナリデータ
            partition_definition_data_frame: 定義となるデータフレーム
            binary_encoding_code: バイナリのエンコーディング
            binary_data_length: バイナリデータ長

        戻り値:
            output_data_frame: 値を入れ、シリアライズしたデータフレーム
        """
        # バイナリを区切る文字数
        before_character, after_character = 0, 0

        # DataFrameの元となるリスト
        value_list = []

        # 行数分のループ
        while (after_character < binary_data_length):

            # 1行分のリスト
            row_list = []

            # 1行を読み込んでいく
            for j in range(len(partition_definition_data_frame[DIGITS])):

                # バイナリの区切り文字数
                after_character += partition_definition_data_frame.loc[j, DIGITS]

                # エンディアン値をデコードする
                decode_value = binary_data_from_contents[before_character:after_character].decode(
                    binary_encoding_code)

                # データタイプを変数にいれておく
                data_type = partition_definition_data_frame.loc[j, DATA_TYPE]

                # nullバイトをそれぞれの型に則した空文字に変換
                null_replaceded_value = WM.calc_func_time(self.logger, False)(
                    self.null_replace)(decode_value, data_type)

                # データフレームに格納される値
                target_value = data_type(null_replaceded_value)

                # 1つ分のデータをリストに格納する
                row_list.append(target_value)

                # バイナリの区切り文字数
                before_character += partition_definition_data_frame.loc[j, DIGITS]

            # 1行ずつのデータをリストに格納
            value_list.append(row_list)

        # カラム名のリストを作成
        column_name_list = partition_definition_data_frame[COLUMN_NAME].values.tolist(
        )

        # DataFrameに変換する
        data_frame_from_value_list = pd.DataFrame(
            value_list, columns=column_name_list)

        # 出力のためシリアライズ
        output_data_frame = pickle.dumps(data_frame_from_value_list)

        return output_data_frame

    def transform(self, context, flowfile):
        try:

            # プロパティの値を取得
            binary_encoding_code, \
                partition_definition_csv\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # プロパティから取得した区切りの定義から必要なカラムを抜き出しリストにする。
            partition_definition_data_frame\
                = WM.calc_func_time(self.logger)(self.csv_to_dataframe)(partition_definition_csv)

            # バイナリとバイナリデータ長を抽出
            binary_data_from_contents, \
                binary_data_length\
                = WM.calc_func_time(self.logger)(self.get_binary_data)(flowfile)

            # バイナリのデコードを行い、データフレームを作成する。
            output_data_frame\
                = WM.calc_func_time(self.logger)(self.get_output_data_frame)(binary_data_from_contents,
                                                                             partition_definition_data_frame,
                                                                             binary_encoding_code,
                                                                             binary_data_length)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_data_frame,
                                           attributes={'encode': binary_encoding_code})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
