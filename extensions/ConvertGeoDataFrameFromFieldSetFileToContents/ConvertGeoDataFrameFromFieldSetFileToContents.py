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
# FieldSetFileに格納された文字列型のデータかGeoDataFrameを抽出するプロセッサ
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import base64
import traceback
import pickle


from importlib import import_module

# Nifi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
gpd = import_module("geopandas")
pd = import_module("pandas")


class ConvertGeoDataFrameFromFieldSetFileToContents(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        FieldSetFileに格納されたGeoDataFrame又、はDataFrameをcontentsに出力する。
                        ①input: GeoDataFrameを持った1行のFieldSetFile。
                        ①output: GeoDataFrame又は、DataFrame。
                      """
        tags = ["GeoDataFrame", "DataFrame", "Contents", "Python"]

    property_descriptors = []

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def check_field_set_file_count(self, field_set_file_dataframe):
        # 1行のFieldSetFileしか認められない為、行数チェック
        field_set_file_count = len(field_set_file_dataframe)

        if field_set_file_count == 1:
            pass
        else:
            raise ValueError("FieldSetFileが1行ではありません。FieldSetFileを分割してください。")

    def check_type_from_dataframe(self, target_value):
        # 出力対象がGeoDataFrame、又はDataFrameかチェック
        # target_valueがGeoDataFrame or DataFrameである場合は問題なし
        if isinstance(target_value, pd.DataFrame):
            pass
        else:
            raise ValueError("FieldSetFileの1行目が「GeoDataFrame」又は、「DataFrame」ではありません。")

    def transform(self, context, flowfile):
        try:
            # flowfileから、CSV形式のFieldSetFileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # DataFrameに加工
            field_set_file_dataframe = \
                pd.read_csv(io.StringIO(input_field_set_file))

            # 1行のFieldSetFileしか認められない為、行数チェック
            WM.calc_func_time(self.logger)(
                self.check_field_set_file_count)(field_set_file_dataframe)

            # FieldSetFileの1行目のValue列をデコードデシリアライズする
            target_value = pickle.loads(base64.b64decode(
                    field_set_file_dataframe.loc[0, "Value"]))

            # 出力対象がGeoDataFrame、又はDataFrameかチェック
            WM.calc_func_time(self.logger)(
                self.check_type_from_dataframe)(target_value)

            return FlowFileTransformResult(relationship="success",
                                           contents=pickle.dumps(target_value))

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
