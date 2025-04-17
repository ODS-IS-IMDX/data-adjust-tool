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
# 属性の最近傍結合プロセッサで出力されたインデックスデータ更新用のデータを用いてポイント用のデータを作成
# ---------------------------------------------------------------------------------------------------------
# Python標準ライブラリ
import io
import pickle
import base64
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")


class AddLineFieldsToPoint(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        属性の最近傍結合プロセッサで出力されたインデックスデータ更新用のデータを用いてポイント用のデータを作成する。
                        ①input: インデックスデータ更新用のデータとポイントのフィールド作成に用いるラインのフィールドを持つ2行以上のFieldSetFile。
                        ②output: 作成したポイントのフィールド複数行のFieldSetFile。
                      """
        tags = ["JoinByNearest", "Python"]

    RESULTS_DWH_NAME = PropertyDescriptor(
        name="Index Data DWH Name",
        description="属性の最近傍結合プロセッサで出力されたインデックス更新用データ",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    REMOVE_DUPLICATE_CSV = PropertyDescriptor(
        name="Remove Duplicate CSV",
        description="最近傍線が複数あった際に、どの線のフィールドを使用するかを選択するための基準を指定するCSV",
        default_value="""下記を参考にして修正すること

フィールドデータが数値データの場合
==========ここから==========
Dwh名,判定,優先文字列
数値型フィールドA,1,
数値型フィールドB,1,
==========ここまで==========

フィールドデータが文字列データの場合
==========ここから==========
Dwh名,判定,優先文字列
文字列型フィールドA,2,データ
文字列型フィールドB,2,データ
==========ここまで==========

フィールド取得先の線を選択するために、フィールド値の比較を行う。
フィールド値の大小比較を行うが、フィールド値が等しかった場合は、
選択できるまでCSVの次の行のフィールドを比較し、最終行まで続ける。
全てのフィールド値が等しかった場合は、インデックス番号が最も小さい線のフィールドを選択する。

※1行目のカラムは固定かつ必須
  2行目以降は比較順に項目を入力


[Dwh名]
比較対象のフィールドDwh名

[判定]
比較時の選択基準
入力規則は以下

  ["判定"列入力規則]
    フィールドが数値データの場合
      1…フィールド値の中で最も大きい値を選択
      2…フィールド値の中で最も小さい値を選択
    フィールドが文字列データの場合
      1…優先文字列と一致する値を選択
      2…優先文字列と一致しない値を選択

[優先文字列]
文字列の比較に用いるフィールド値
※文字列型フィールドの場合にのみ記載する。
                      """,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output File Name",
        description="出力データの新規のファイル名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [RESULTS_DWH_NAME,
                            REMOVE_DUPLICATE_CSV,
                            OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def extract_and_process_flowfile(self, flowfile, results_dwh_name):
        """
        概要:
            flowfileからデータを抽出し、抽出したデータからresults型データと全てのFieldsデータをマージしたデータフレーム作成と、
            inputされたFieldsデータのDWH名を取得。

        引数:
            flowfile: プロセッサに入ってくるデータ
            results_dwh_name: results型のDwh

        戻り値:
            merged_dataframe: results型データと属性データをマージしたデータフレーム
            attributes_dwh_name_list: FieldsデータのDWH名のリスト
        """

        # flowfileから、csv形式のfield_set_fileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        # field_set_file_data_frame から results を抽出
        results_value = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == results_dwh_name, "Value"].values[0]))

        # DataFrameに変換
        results_dataframe = pd.DataFrame(
            results_value, columns=["Findex", "findex"])
        results_dataframe["findex"] = results_dataframe["findex"].astype(float)

        # 各FieldsのDataFrame・DWH名格納用リスト
        attributes_dataframe_list = []
        attributes_dwh_name_list = []

        for _, row in field_set_file_data_frame.iterrows():

            # 属性データである場合
            if row["Dwh"] != results_dwh_name:

                attributes_dwh = row["Dwh"]
                attributes_dwh_name_list.append(attributes_dwh)
                decoded_value = pickle.loads(base64.b64decode(row["Value"]))

                # DataFrameの列名としてfindex（インデックス）とDWH名を使用
                attributes_dataframe = pd.DataFrame(
                    decoded_value, columns=["findex", attributes_dwh])
                # Findex列のデータ型をfloat型に変換
                attributes_dataframe["findex"] = attributes_dataframe["findex"].astype(
                    float)

                attributes_dataframe_list.append(attributes_dataframe)

        # 最終的に全ての属性 DataFrame を結合
        for i, dataframe in enumerate(attributes_dataframe_list):

            if i == 0:
                attributes_dataframe = dataframe
            else:
                attributes_dataframe = attributes_dataframe.merge(
                    dataframe, on="findex", how="left")

        # results型データと属性データのDataFrameをマージ
        merged_dataframe = pd.merge(
            results_dataframe, attributes_dataframe, on="findex", how="left")

        return merged_dataframe, attributes_dwh_name_list

    def integer_float_remove_duplicate(self, merged_dataframe, remove_duplicate_csv_dataframe):
        """
        概要:
            Fieldsデータが数値データの場合に優先度に基づいて判定してindexの重複を削除する

        引数:
            merged_dataframe: results形式データとFieldsデータマージ後のデータフレーム、重複削除を行う対象データ
            remove_duplicate_csv_dataframe: 重複を削除するための基準となるCSVデータ

        戻り値:
            final_dataframe: 重複削除後のデータフレーム
        """

        # 同一indexでグルーピング
        grouped = merged_dataframe.groupby("Findex")

        # 最も優先順位が低い処理のDWH名を取得(最終の処理を判断するため)
        last_dwh_name = remove_duplicate_csv_dataframe["Dwh名"].iloc[-1]

        # 重複削除対象DataFrameからcolumnを取得した空DataFrameを作成(ここに重複を除いたDataFrameを結合する)
        final_dataframe = pd.DataFrame(columns=merged_dataframe.columns)

        for _, group in grouped:

            # index重複がない場合の処理
            if group.shape[0] == 1:
                final_dataframe = pd.concat(
                    [final_dataframe, group], ignore_index=True)
                continue

            # 条件結果一時格納用変数の初期化
            temp_value = None

            # 重複有のグループ
            for i, row in remove_duplicate_csv_dataframe.iterrows():

                dwh_name = row["Dwh名"]
                priority_type = row["判定"]

                # 次ループからtemp_value変数を対象にしたいため
                if i == 0:

                    # remove_duplicate_csvプロパティの値が大きい値を優先選択の場合
                    if priority_type == 1:
                        selected_value = group[dwh_name].max()

                    # remove_duplicate_csvプロパティの値が小さい値を優先選択の場合
                    if priority_type == 2:
                        selected_value = group[dwh_name].min()

                    # 判定で取得した値と一致する行を取得
                    matching_rows = group[group[dwh_name] == selected_value]

                    # 取得した行数が１行だった場合(今回の条件で重複が削除できた場合)
                    if matching_rows.shape[0] == 1:
                        final_dataframe = pd.concat(
                            [final_dataframe, matching_rows], ignore_index=True)
                        break

                    # 取得した行数が複数行だった場合(今回の条件では重複が削除できなかった場合)
                    else:
                        # temp_value変数を更新して次の条件に
                        temp_value = matching_rows

                # 2ループ目以降
                else:

                    # remove_duplicate_csvプロパティの値が大きい値を優先選択の場合
                    if priority_type == 1:
                        selected_value = temp_value[dwh_name].max()

                    # remove_duplicate_csvプロパティの値が小さい値を優先選択の場合
                    if priority_type == 2:
                        selected_value = temp_value[dwh_name].min()

                    # 判定で取得した値と一致する行を取得
                    matching_rows = temp_value[temp_value[dwh_name]
                                               == selected_value]

                    # 取得した行数が１行だった場合(今回の条件で重複が削除できた場合)
                    if matching_rows.shape[0] == 1:
                        final_dataframe = pd.concat(
                            [final_dataframe, matching_rows], ignore_index=True)
                        break

                    # 取得した行数が複数行だった場合(今回の条件では重複が削除できなかった場合)
                    else:
                        # temp_value変数を更新して次の条件に
                        temp_value = matching_rows

                # すべての条件で重複削除ができなかった場合、最初の要素を取得
                if dwh_name is last_dwh_name:
                    final_dataframe = pd.concat(
                        [final_dataframe, temp_value.iloc[[0]]], ignore_index=True)

        return final_dataframe

    def strings_remove_duplicate(self, merged_dataframe, remove_duplicate_csv_dataframe):
        """
        概要:
            Fieldsデータが文字列データの場合に優先度に基づいて判定してindexの重複を削除する

        引数:
            merged_dataframe: results形式データとFieldsデータマージ後のデータフレーム。重複削除を行う対象データ
            remove_duplicate_csv_dataframe: 重複を削除するための基準となるCSVデータ

        戻り値:
            final_dataframe: 重複削除後のデータフレーム
        """

        # 同一indexでグルーピング
        grouped = merged_dataframe.groupby("Findex")

        # 最も優先順位が低い処理のDWH名を取得(最終の処理を判断するため)
        last_dwh_name = remove_duplicate_csv_dataframe["Dwh名"].iloc[-1]

        # 重複削除対象DataFrameからcolumnを取得した空DataFrameを作成(ここに重複を除いたDataFrameを結合する)
        final_dataframe = pd.DataFrame(columns=merged_dataframe.columns)

        for _, group in grouped:

            # index重複がない場合の処理
            if group.shape[0] == 1:
                final_dataframe = pd.concat(
                    [final_dataframe, group], ignore_index=True)
                continue

            # 条件結果一時格納用変数の初期化
            temp_value = None

            # 重複有のグループ
            for i, row in remove_duplicate_csv_dataframe.iterrows():
                dwh_name = row["Dwh名"]
                priority_type = row["判定"]
                string_value = str(row["優先文字列"])

                # 次ループからtemp_value変数を対象にしたいため
                if i == 0:

                    # remove_duplicate_csvプロパティの値が一致する値を優先選択の場合
                    if priority_type == 1:

                        # 判定で取得した値と一致する行を取得
                        matching_rows = group[group[dwh_name] == string_value]
                        if matching_rows.empty:
                            matching_rows = group

                    # remove_duplicate_csvプロパティの値が一致しないを優先を優先選択の場合
                    elif priority_type == 2:

                        # 判定で取得した値と一致しない行を取得
                        matching_rows = group[group[dwh_name] != string_value]
                        if matching_rows.empty:
                            matching_rows = group

                    # 取得した行数が１行だった場合(今回の条件で重複が削除できた場合)
                    if matching_rows.shape[0] == 1:

                        final_dataframe = pd.concat(
                            [final_dataframe, matching_rows], ignore_index=True)
                        break

                    else:
                        # temp_value変数を更新して次の条件に
                        temp_value = matching_rows

                # 2ループ目以降
                else:

                    # remove_duplicate_csvプロパティの値が一致する値を優先選択の場合
                    if priority_type == 1:

                        # 判定で取得した値と一致する行を取得
                        matching_rows = temp_value[temp_value[dwh_name]
                                                   == string_value]
                        if matching_rows.empty:
                            matching_rows = temp_value

                    # remove_duplicate_csvプロパティの値が一致しないを優先を優先選択の場合
                    if priority_type == 2:

                        # 判定で取得した値と一致しない行を取得
                        matching_rows = temp_value[temp_value[dwh_name]
                                                   != string_value]
                        if matching_rows.empty:
                            matching_rows = temp_value

                    # 取得した行数が１行だった場合(今回の条件で重複が削除できた場合)
                    if matching_rows.shape[0] == 1:

                        final_dataframe = pd.concat(
                            [final_dataframe, matching_rows], ignore_index=True)
                        break

                    # 取得した行数が複数行だった場合(今回の条件では重複が削除できなかった場合)
                    else:
                        # temp_value変数を更新して次の条件に
                        temp_value = matching_rows

                # すべての条件で重複削除ができなかった場合、最初の要素を取得
                if dwh_name is last_dwh_name:

                    final_dataframe = pd.concat(
                        [final_dataframe, temp_value.iloc[[0]]], ignore_index=True)

        return final_dataframe

    def get_new_attributes_dwh_name(self, output_dwh_name, final_dataframe, attributes_dwh_name_list):
        """
        概要:
            attributes_dwh_name_listからスラッシュの右側を取得し、output_dwh_nameをスラッシュ区切りで前に追加
            その後、final_dataframeとattributes_dwh_name_listの名前が同じカラムにnew_attributes_dwh_name_listを適用

        引数:
            output_dwh_name: プロパティで入力された出力用DWH名
            final_dataframe: 最終的なデータフレーム。attributes_dwh_name_listのカラム名に基づいてnew_attributes_dwh_name_listを設定する
            attributes_dwh_name_list: 属性のDWH名を含むリスト

        戻り値:
            final_dataframe: カラム名にプレフィックスがついたDWM名を設定したデータフレーム
            new_attributes_dwh_name_list: プロパティで設定したプレフィックスがついたDWM名
        """
        # 1. attributes_dwh_name_listから/の右側を取得したリストを作成
        new_attributes_dwh_name_list = [name.split(
            '/')[-1] for name in attributes_dwh_name_list]

        # 2. new_attributes_dwh_name_listの前にoutput_dwh_nameをスラッシュ区切りでつける
        new_attributes_dwh_name_list = [
            f"{output_dwh_name}/{name}" for name in new_attributes_dwh_name_list]

        # 3. final_dataframeとattributes_dwh_name_listの名前が一致するカラムにnew_attributes_dwh_name_listを適用
        for original_dwh_name, new_dwh_name in zip(attributes_dwh_name_list, new_attributes_dwh_name_list):
            if original_dwh_name in final_dataframe.columns:
                final_dataframe[new_dwh_name] = final_dataframe[original_dwh_name]

        return final_dataframe, new_attributes_dwh_name_list

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # --------------------------------------------------------------------------
            results_dwh_name = context.getProperty(
                self.RESULTS_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            remove_duplicate_csv = context.getProperty(
                self.REMOVE_DUPLICATE_CSV).evaluateAttributeExpressions(flowfile).getValue()

            output_dwh_name = context.getProperty(
                self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            # --------------------------------------------------------------------------

            # プロパティで入力された判定用csvをDataFrameに
            remove_duplicate_csv_dataframe = pd.read_csv(
                io.StringIO(remove_duplicate_csv))

            # --------------------------------------------------------------------------
            # inputのFieldsデータとresults型データをマージしたDataFrame取得
            # --------------------------------------------------------------------------
            merged_dataframe, \
                attributes_dwh_name_list\
                = WM.calc_func_time(self.logger)(self.extract_and_process_flowfile)(flowfile, results_dwh_name)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # inputのFieldsデータとresults型データをマージしたDataFrame取得
            # --------------------------------------------------------------------------
            if remove_duplicate_csv_dataframe["優先文字列"].notna().any():
                # --------------------------------------------------------------------------
                # Fieldsデータが文字列の場合
                # --------------------------------------------------------------------------
                final_dataframe\
                    = WM.calc_func_time(self.logger)(self.strings_remove_duplicate)(merged_dataframe, remove_duplicate_csv_dataframe)
            else:
                # --------------------------------------------------------------------------
                # Fieldsデータが数値データの場合
                # --------------------------------------------------------------------------
                final_dataframe\
                    = WM.calc_func_time(self.logger)(self.integer_float_remove_duplicate)(merged_dataframe, remove_duplicate_csv_dataframe)
                # --------------------------------------------------------------------------
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # DWH名にプロパティで設定したプレフィックスを付ける
            # --------------------------------------------------------------------------
            final_dataframe, \
                new_attributes_dwh_name_list\
                = WM.calc_func_time(self.logger)(self.get_new_attributes_dwh_name)(output_dwh_name, final_dataframe, attributes_dwh_name_list)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # GeoDataFrameからフィールド値リストとフィールドタイプリストを取得する
            # --------------------------------------------------------------------------
            field_value_list, \
                field_type_list \
                = WM.calc_func_time(self.logger)(NSP.write_field_file)(final_dataframe,
                                                                       new_attributes_dwh_name_list)
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # set_field_set_file : pickleによるシリアライズ、base64でのデコード、utf-8でのエンコード処理を行う
            # --------------------------------------------------------------------------
            output_field_set_file = WM.calc_func_time(self.logger)(NSP.get_field_set_file)(field_type_list,
                                                                                           field_value_list,
                                                                                           new_attributes_dwh_name_list)
            # --------------------------------------------------------------------------

            return FlowFileTransformResult(relationship="success", contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
