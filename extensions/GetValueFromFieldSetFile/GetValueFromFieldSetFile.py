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
# FieldSetFileに格納された文字列型のデータかGeoDataFrameを抽出するプロセッサ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import zipfile
import traceback
import pickle


from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

gpd = import_module("geopandas")

ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class GetValueFromFieldSetFile(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        FieldSetFileに格納された文字列型のデータかGeoDataFrameを抽出する。処理結果をZIP圧縮することも可能。
                        ①input: 文字列型のデータかGeoDataFrameを持った1行のFieldSetFile。
                        ①output: FieldSetFileが持っていた文字列型のデータかGeoDataFrameを抽出したデータ。
                      """
        tags = ["GeoDataFrame", "Filename", "ZIP", "Unpack", "Python"]

    # 出力データのファイル名
    FILENAME = PropertyDescriptor(
        name="File Name",
        description="出力データのファイル名",
        required=True,
        default_value="${DWH:append('')}",
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

    property_descriptors = [FILENAME,
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
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            filename: flowfileの属性のfilename（ファイル名そのもの）
            output_zip_flag: ZIP圧縮するかどうかのフラグ
        """

        filename = context.getProperty(
            self.FILENAME).evaluateAttributeExpressions(flowfile).getValue()

        output_zip_flag\
            = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()

        return filename, output_zip_flag

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # Nifiのプロパティで入力したfilenameの呼び出し。
            filename, \
                output_zip_flag\
                = WM.calc_func_time(self.logger)(
                    self.get_property)(context, flowfile)

            # 1行のFieldSetFileのValue列を抜きだす。
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                target_value \
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # target_valueがGeoDataFrameである場合の処理
            if isinstance(target_value, gpd.GeoDataFrame):

                # バイトデータからGeoDataFrameを復元する
                target_value = pickle.dumps(target_value)

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # CSV形式の文字列をZip圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, target_value)

                # Zipデータを取得
                target_value = zip_buffer.getvalue()

            # ファイルの名前を設定する。
            attribute = {"filename": filename}

            return FlowFileTransformResult(relationship="success", contents=target_value, attributes=attribute)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
