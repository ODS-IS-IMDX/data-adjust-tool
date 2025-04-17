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

# Python標準ライブラリ
import re
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")

import cad.common.cad_utils as CU


class DivideGeometryLogic:

    def get_params(self, properties):
        """
        プロパティの値を取得し、辞書にして返す。

        :param properties: プロパティの設定値を格納した辞書。
        :type properties: dict

        :return: プロパティの属性値を辞書形式で返す。
        :rtype: dict

        :raises Exception: プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            params = {}

            # プロパティの値を取得する。
            params["src_geom_name"] = properties["SRC_GEOMETRY_NAME"]
            params["condition"] = properties["CONDITION"]
            params["dst_geom_name"] = properties["DST_GEOMETRY_NAME"]
            params["other_geom_name"] = properties["OTHER_GEOMETRY_NAME"]

            return params

        except Exception as e:
            raise Exception(f"[get_params Exception]: {str(e)}")

    def extract_src_dataframe(self, dataframe, all_params):
        """
        DataFrame形式の入力データから、操作対象のデータを抽出する。

        :param dataframe: 入力データのFieldSetFileをDataFrameに変換したもの。
        :type dataframe: pandas.DataFrame
        :param all_params: プロパティの設定値を格納した辞書。
        :type all_params: dict

        :return: 入力データから操作対象データのみを抽出したDataFrame、入力データのDwh値とType値を１対１で格納した辞書。
        :rtype: tuple(pandas.DataFrame, dict{str, str})

        :raises Exception: プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 操作対象データのジオメトリ情報・FID情報が入力データに存在しているかを確認
            if not (
                dataframe["Dwh"] == f"{all_params['src_geom_name']}/geometry"
            ).any():
                raise Exception(
                    f"{all_params['src_geom_name']}/geometry does not exist."
                )
            if not (dataframe["Dwh"] == f"{all_params['src_geom_name']}/FID").any():
                raise Exception(f"{all_params['src_geom_name']}/FID does not exist.")

            target_df = pd.DataFrame()  # 操作対象レコード格納先DataFrame

            # 入力データから操作対象のレコードのみを抽出し、操作対象DataFrameを生成
            for row in dataframe.itertuples(index=False):
                if (
                    re.match(f"{all_params['src_geom_name']}/", row.Dwh)
                    and row.Dwh
                    != f"{all_params['src_geom_name']}/{all_params['condition']}"
                ):
                    row_df = pd.DataFrame([row], columns=dataframe.columns)
                    target_df = pd.concat([target_df, row_df], ignore_index=True)

            # 各カラムの値を辞書型で取得
            src_column_dict = {
                col.split("/")[0]: target_df[col].tolist() for col in target_df.columns
            }
            src_dwh_list = [dwh.split("/")[1] for dwh in src_column_dict["Dwh"]]
            src_value_list = list(
                map(lambda x: list(x), zip(*src_column_dict["Value"]))
            )

            # 分割操作を行うDataFrameを生成
            src_dataframe = pd.DataFrame(src_value_list, columns=src_dwh_list)

            # DwhとTypeの関係を辞書に格納
            dwh_type_info = dict(zip(src_dwh_list, src_column_dict["Type"]))

            return src_dataframe, dwh_type_info

        except Exception as e:
            raise Exception(f"[convert_field_set_file_to_dataframe]: {str(e)}")

    def divide_dataframe_by_condition(self, df, src_df, all_params):
        """
        DataFrameを条件（Condition）で分割する。

        :param df: 入力データのFieldSetFileをDataFrameに変換したもの。
        :type df: pandas.DataFrame
        :param src_df: 分割操作対象のDataFrame。
        :type src_df: pandas.DataFrame
        :param all_params: プロパティの設定値を格納した辞書。
        :type all_params: dict

        :return: 分割条件に適合するDataFrame、分割条件に適合しないDataFrame。
        :rtype: tuple(pandas.DataFrame, pandas.DataFrame)

        :raises Exception: プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 分割条件データを取得
            if not (df["Dwh"] == f"{all_params['src_geom_name']}/geometry").any():
                # 分割条件データが存在しない場合は例外発生させる
                raise Exception(
                    f"{all_params['src_geom_name']}/geometry does not exist."
                )

            # 分割条件をDataFrameから取得
            condition_value = df[
                (
                    df["Dwh"]
                    == f"{all_params['src_geom_name']}/{all_params['condition']}"
                )
            ]["Value"].tolist()[0]

            # 分割条件データの値が真偽値かどうかチェック
            if not all(isinstance(value, bool) for findex, value in condition_value):
                raise ValueError(
                    f"'{all_params['src_geom_name']}/{all_params['condition']}' is not bool-type."
                )

            # Trueを持つFindexを抽出する
            true_findex_list = [
                findex for findex, value in condition_value if value is True
            ]

            # 分割対象DataFrameから条件に適合するレコードのみ取得する処理
            divide_condition = src_df["FID"].apply(
                lambda fid: fid[1] in true_findex_list
            )

            # 条件に適合するDataFrameを抽出し、操作後のデータ名をカラム名に設定
            src_df_match = src_df[divide_condition]
            src_df_match.columns = [
                f"{all_params['dst_geom_name']}/{col}" for col in src_df_match.columns
            ]

            # 条件に適合しないDataFrameを抽出し、操作後のデータ名をカラム名に設定
            src_df_mismatch = src_df[~divide_condition]
            src_df_mismatch.columns = [
                f"{all_params['other_geom_name']}/{col}"
                for col in src_df_mismatch.columns
            ]

            # 条件に適合するDataFrameと適合しないDataFrameを返す
            return src_df_match, src_df_mismatch

        except Exception as e:
            raise Exception(f"[divide_dataframe_by_condition]: {str(e)}")

    def convert_results_to_dataframe(self, df_match, df_mismatch, dwh_type_info):
        """
        分割結果のDataFrameを出力結果のFieldSetFile用のDataFrameの構造に変換する。

        :param df_match: 分割条件に適合するDataFrame。
        :type df_match: pandas.DataFrame
        :param df_mismatch: 分割条件に適合しないDataFrame。
        :type df_mismatch: pandas.DataFrame
        :param dwh_type_info: Dwh値とType値をセットで格納した辞書。
        :type dwh_type_info: dict{str, str}

        :return: FieldSetFile生成元となるDataFrame型の処理結果。
        :rtype: pandas.DataFrame

        :raises Exception: プロパティ取得中にエラーが発生した場合に例外をスローする。
        """

        def create_df_for_fsf(df, dwh_type_info):
            """
            分割結果のDataFrameからFieldSetFie用DataFrameを生成する。

            :param df: 処理対象のDataFrame。
            :type df: pandas.DataFrame
            :param dwh_type_info: Dwh値とType値をセットで格納した辞書。
            :type dwh_type_info: dict{str, str}

            :return: FieldSetFileに変換するために適切なカラムを含むDataFrame。
            :rtype: pandas.DataFrame

            :raises Exception: プロパティ取得中にエラーが発生した場合に例外をスローする。
            """
            try:
                df_for_fsf = pd.DataFrame()  # 変換後DataFrame格納先
                columns = df.columns.tolist()  # 操作対象DataFrameのカラムリスト

                # カラム数分ループ
                for col in columns:
                    # カラムごとのデータを１レコードとしてDataFrameを生成
                    new_record = pd.DataFrame(
                        {
                            "Dwh": [col],
                            "Type": [
                                val
                                for key, val in dwh_type_info.items()
                                if key == col.split("/")[1]
                            ],
                            "Value": [df[col].tolist()],
                        }
                    )
                    # 生成したDataFrameをFieldSetFile用構造のDataFrameに結合
                    df_for_fsf = pd.concat([df_for_fsf, new_record], ignore_index=True)

                return df_for_fsf

            except Exception as e:
                raise Exception(f"[create_df_for_fsf Exception]: {str(e)}")

        try:
            # 分割結果の各DataFrameをFieldSetFileにするための構造に変換
            df_for_fsf_match = create_df_for_fsf(df_match, dwh_type_info)
            df_for_fsf_mismatch = create_df_for_fsf(df_mismatch, dwh_type_info)

            # 変換後のDataFrameを１つのDataFrameに結合
            results_df = pd.concat(
                [df_for_fsf_match, df_for_fsf_mismatch], ignore_index=True
            )

            return results_df

        except Exception as e:
            raise Exception(f"[convert_results_to_dataframe Exception]: {str(e)}")

    def divide_geometry(self, input_fsf, all_params):
        """
        DivideGeometryプロセッサのメイン処理を行う。

        :param input_fsf: 入力データのFieldSetFile。
        :type input_fsf: bytes
        :param all_params: プロパティの設定値を持つ辞書。
        :type all_params: dict

        :return: 処理結果のFieldSetFile。
        :rtype: str

        :raises Exception: プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 入力データ（bytes型のFieldSetFile）をDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(input_fsf)

            # 操作対象データをDataFrame形式で抽出
            src_dataframe, dwh_type_info = self.extract_src_dataframe(
                dataframe, all_params
            )

            # DataFrameを条件（Condition）に基づいて２つに分割
            matching_dataframe, mismatching_dataframe = (
                self.divide_dataframe_by_condition(dataframe, src_dataframe, all_params)
            )

            # 分割結果をFieldSetFile生成用のDataFrameに変換
            results_dataframe = self.convert_results_to_dataframe(
                matching_dataframe, mismatching_dataframe, dwh_type_info
            )

            # 処理結果のDataFrameをFieldSetFileに変換
            field_set_file = CU.dataframe_to_field_set_file(results_dataframe)

            return field_set_file

        except Exception as e:
            raise Exception(f"[divide_geometry Exception]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: バイナリ形式のFieldSetFile
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 処理結果のFieldSetFileと、更新された属性情報を含むタプル
        :rtype: tuple

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティ値取得
            all_params = self.get_params(properties)

            # メイン処理を実行
            results = self.divide_geometry(byte_data, all_params)

            return results, attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
