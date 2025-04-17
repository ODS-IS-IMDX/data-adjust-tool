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
# 繰り返し部分が存在するCSVファイルをDataFrameに変換する。
# ---------------------------------------------------------------------------------------------------------
# Python標準ライブラリ
import io
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")


class RecreateRepeatColumnDataFrame(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        指定されたカラム名を基に、繰り返しデータを含むDataFrameの繰り返し部分を結合する。
                        ①input: 繰り返し項目を含むDataFrame
                        ②output: 繰り返し部分の結合を行ったDataFrame
                      """

        tags = ["DataFrame", "CSV", "Python"]

    REPEAT_START_COLUMN_NAME = PropertyDescriptor(
        name="Repeat Start Column Name",
        description="繰り返しが始まる箇所のカラム名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False
    )

    NEW_COLUMN_NAMES = PropertyDescriptor(
        name="New Column Names",
        description="再構成後のDataFrameのカラム名（カラム名は必須）",
        default_value="""下記を参考にして修正すること

==========ここから==========
カラム名
項目A
項目B
項目C
==========ここまで==========
                      """,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [REPEAT_START_COLUMN_NAME,
                            NEW_COLUMN_NAMES]

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
            repeat_start_column_name: ループが始まるカラム名
            new_column_names_csv: ループ対象データに含まれるすべてのカラム名
        """

        repeat_start_column_name = context.getProperty(
            self.REPEAT_START_COLUMN_NAME).evaluateAttributeExpressions(flowfile).getValue()

        new_column_names_csv = context.getProperty(
            self.NEW_COLUMN_NAMES).evaluateAttributeExpressions(flowfile).getValue()

        return repeat_start_column_name, new_column_names_csv

    def split_repeated_columns(self, dataframe, start_column_name, repeat_column_names_list):
        """
        概要:
            繰り返しの始まる列名と繰り返し単位を指定してデータを変換する関数

        引数:
            dataframe: 入力データフレーム
            repeat_column_names_list: 繰り返し項目名

        戻り値:
            pd.DataFrame: 変換後のデータフレーム
        """

        # 繰り返しが始まる列のインデックスを取得
        start_idx = dataframe.columns.get_loc(start_column_name)

        # 繰り返し前の列（固定部分）
        fixed_columns = dataframe.columns[:start_idx]

        # 繰り返し単位を取得
        repeat_unit = len(repeat_column_names_list)

        # 新しい列名を生成
        output_columns = list(fixed_columns) + repeat_column_names_list

        processed_rows_list = []

        for _, row in dataframe.iterrows():

            # 繰り返し前の部分を取得
            base = row[:start_idx]

            # 繰り返し部分を取得
            repeated_part = row[start_idx:]

            # 繰り返し部分を分割して追加
            for i in range(0, len(repeated_part), repeat_unit):
                repeated_chunk = repeated_part[i:i + repeat_unit]

                # 全て空の行は省く
                if repeated_chunk.isnull().all():
                    continue

                processed_rows_list.append(
                    base.tolist() + repeated_chunk.tolist())

        return pd.DataFrame(processed_rows_list, columns=output_columns)

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # --------------------------------------------------------------------------
            repeat_start_column_name, \
                new_column_names_csv\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # --------------------------------------------------------------------------
            # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得
            # --------------------------------------------------------------------------
            target_dataframe = WM.calc_func_time(self.logger, False)(
                PBP.get_value_from_field_Set_file_or_serialized)(flowfile)

            # --------------------------------------------------------------------------
            # プロパティで入力した繰り返し項目名をCSV→DataFrame→list
            # --------------------------------------------------------------------------
            definition_dataframe = pd.read_csv(
                io.StringIO(new_column_names_csv), dtype='str')

            repeat_column_names_list = definition_dataframe["カラム名"].to_list()

            # --------------------------------------------------------------------------
            # 繰り返しの始まる列名と繰り返し単位を指定してデータを変換
            # --------------------------------------------------------------------------
            output_dataframe\
                = WM.calc_func_time(self.logger)(self.split_repeated_columns)(target_dataframe,
                                                                              repeat_start_column_name,
                                                                              repeat_column_names_list)

            # --------------------------------------------------------------------------
            # Dataframeのシリアライズ
            # --------------------------------------------------------------------------
            output_dataframe = pickle.dumps(output_dataframe)

            return FlowFileTransformResult(relationship="success", contents=output_dataframe)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
