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
# ２つのCityGMLを文字列結合し、１つのCityGMLにするプロセッサ
# --------------------------------------------------------------------------------------------
# Python標準モジュール
import io
import zipfile
import base64
import pickle
import traceback

# 外部ライブラリを動的インポート
from importlib import import_module

# Nifi自作モジュール
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")

ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class JoinCityGML(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        2つ以上のCityGMLを結合する。処理結果をZIP圧縮することも可能。
                        ①input: 図郭コードが同じ、CityGMLが格納された2行以上のFieldSetFile。
                        ②output: CityGML1行のFieldSetFile。
                      """
        tags = ["CityGML", "ZIP", "Unpack", "python"]

    # 結合したい箇所の１行目のタグ
    START_TAG = PropertyDescriptor(
        name="Start Tag",
        description="結合したい箇所の１行目のタグ",
        default_value="<core:cityObjectMember>",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 結合したい箇所の最終行のタグ
    END_TAG = PropertyDescriptor(
        name="End Tag",
        description="結合したい箇所の最終行のタグ",
        default_value="</core:cityObjectMember>",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 出力データのファイル名(拡張子.gmlは必須)
    FILENAME = PropertyDescriptor(
        name="File Name",
        description="出力データのファイル名(拡張子.gmlは必須)",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
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

    property_descriptors = [START_TAG,
                            END_TAG,
                            FILENAME,
                            OUTPUT_ZIP_FLAG]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数
        引数:
            context:プロセッサの設定値が格納されているデータ
            flowfile:プロセッサに入ってくるデータ
        戻り値:
            start_tag: 結合したい箇所の１行目のタグ
            end_tag: 結合したい箇所の最終行のタグ
            filename: 出力ファイル名
            output_zip_flag: ZIP圧縮するかどうかのフラグ
        """
        start_tag = context.getProperty(self.START_TAG).getValue()

        end_tag = context.getProperty(self.END_TAG).getValue()

        filename = context.getProperty(
            self.FILENAME).evaluateAttributeExpressions(flowfile).getValue()

        output_zip_flag\
            = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()

        return start_tag, end_tag, filename, output_zip_flag

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # 各プロパティで設定した値を取得
            start_tag, \
                end_tag, \
                filename, \
                output_zip_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # flowfileから、CSV形式のFieldSetFileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            field_set_file_data_frame = pd.read_csv(
                io.StringIO(input_field_set_file))

            value_list = field_set_file_data_frame["Value"].tolist()

            # 最初のCityGMLをベースにする
            base_string = pickle.loads(base64.b64decode(value_list[0]))

            # 他のCityGMLを順番に追加
            for value in value_list[1:]:
                target_string = pickle.loads(base64.b64decode(value))

                # targetの最初と最後の出現位置を取得
                start_index = target_string.find(start_tag)
                end_index = target_string.rfind(end_tag)

                # 取得したインデックスを使って部分文字列を抽出
                if start_index != -1 and end_index != -1:
                    substring = target_string[start_index:end_index +
                                              len(end_tag)]
                else:
                    substring = ""

                # another_stringを行ごとのリストに分割
                another_lines = base_string.strip().splitlines()

                # 最後から2行目のインデントを取得
                if len(another_lines) >= 2:
                    last_two_indent = another_lines[-2][:len(
                        another_lines[-2]) - len(another_lines[-2].lstrip('\t'))]
                else:
                    last_two_indent = ""  # インデントがない場合は空文字

                # 最後から2番目の行にインデントを適用したsubstringを挿入
                if len(another_lines) >= 2:
                    indented_substring = last_two_indent + substring.strip()  # インデントを追加
                    another_lines.insert(-1, indented_substring)  # 最後から2行目に追加

                # 最終的な結果を再結合
                base_string = "\n".join(another_lines)

            # 改行文字をwindows用に変換
            final_string = base_string.replace("\n", "\r\n")
            final_string = "\r\n".join(
                [line for line in final_string.splitlines() if line.strip()])

            # 最後の行が改行で終わっていない場合のみ \r\n を追加
            if not final_string.endswith('\r\n'):
                final_string += '\r\n'

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # CSV形式の文字列をZip圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, final_string)

                # Zipデータを取得
                final_string = zip_buffer.getvalue()

            # ファイルの名前を設定する。
            attribute = {"filename": filename}

            return FlowFileTransformResult(relationship="success",
                                           contents=final_string,
                                           attributes=attribute)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
