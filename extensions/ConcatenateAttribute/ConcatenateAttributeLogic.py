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

import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class ConcatenateAttributeLogic:

    def extract_target_dataframe(self, dataframe, all_params, sub_attr_list):
        """
        DataFrameからメインのDataFrame、サブのDataFrame、サブグループのDataFrameを抽出する。

        :param dataframe: 全体のDataFrame。
        :type dataframe: pandas.DataFrame
        :param all_params: プロパティと属性情報を格納した辞書。
        :type all_params: dict
        :param sub_attr_list: サブデータの属性リスト。
        :type sub_attr_list: list[str]

        :return: メインのDataFrame、サブのDataFrame、サブグループのDataFrame（存在する場合）。
        :rtype: list[pandas.DataFrame]

        :raises ValueError: メインまたはサブグループの属性がDataFrameに存在しない場合に発生するエラー。
        :raises Exception: 抽出処理中にエラーが発生した場合に例外をスローする。
        """

        def align_output_format_with_pairs(df):
            """
            出力形式listのValueをpairs形式に変換する。

            :param df: 対象のDataFrame。
            :type df: pandas.DataFrame

            :return: pairs形式でValueを持つDataFrame。
            :rtype: pandas.DataFrame

            :raises Exception: 変換処理中にエラーが発生した場合に例外をスローする。
            """
            try:
                if not len(df) > 0:
                    return df

                value_list = df["Value"].values[0]
                index = df.index[0]
                df_pairs = df.copy()
                # list形式でデータを持っている場合、pairs形式に変換する
                pairs_value = []
                for sub_list in value_list:
                    if sub_list[1] is not None:
                        if isinstance(sub_list[1], list):
                            for elem in sub_list[1]:
                                pairs_value.append((sub_list[0], elem))
                        else:
                            pairs_value.append(sub_list)
                df_pairs.loc[index, "Value"] = pairs_value
                return df_pairs

            except Exception as e:
                raise Exception(f"[align_output_format_with_pairs]: {str(e)}")

        try:
            # DataFrameからメインデータのレコードを抽出
            if (
                dataframe["Dwh"]
                == f"{all_params['main_geom_param']}/{all_params['main_attr_param']}"
            ).any():
                main_df = dataframe[
                    dataframe["Dwh"]
                    == all_params["main_geom_param"]
                    + "/"
                    + all_params["main_attr_param"]
                ]
            else:
                raise ValueError(
                    f"{all_params['main_geom_param']}/{all_params['main_attr_param']} does not exist."
                )

            # サブの属性リストから、サブデータとして抽出したいDwh名のリストを生成
            sub_dwh_name = [
                all_params["sub_geom_param"] + "/" + sub_attr_name
                for sub_attr_name in sub_attr_list
            ]

            no_exist_sub_dwh = [
                sub_dwh
                for sub_dwh in sub_dwh_name
                if sub_dwh not in dataframe["Dwh"].values
            ]

            if len(no_exist_sub_dwh) > 0:
                raise ValueError(f"{','.join(no_exist_sub_dwh)} does not exist.")

            # DataFrameからサブデータのレコードを抽出
            sub_df = dataframe[dataframe["Dwh"].isin(sub_dwh_name)]

            # 出力形式をpairsに揃える
            main_df = align_output_format_with_pairs(main_df)
            sub_df = align_output_format_with_pairs(sub_df)

            # サブグループデータを抽出
            if (
                all_params["sub_group_param"]
                and all_params["sub_group_param"].strip() != ""
            ):
                if (
                    dataframe["Dwh"]
                    == f"{all_params['sub_geom_param']}/{all_params['sub_group_param']}"
                ).any():
                    sub_group_df = dataframe[
                        dataframe["Dwh"]
                        == all_params["sub_geom_param"]
                        + "/"
                        + all_params["sub_group_param"]
                    ].reset_index(drop=True)
                    # グループ内のペア相手がNoneの場合、Noneを始点と同じ値にする
                    sub_group_value = [
                        pair if pair[1] is not None else (pair[0], pair[0])
                        for pair in sub_group_df["Value"].tolist()[0]
                    ]
                    sub_group_df.loc[0, "Value"] = sub_group_value
                    # 出力形式をpairsに揃える
                    sub_group_df = align_output_format_with_pairs(sub_group_df)
                    return [main_df, sub_df, sub_group_df]
                else:
                    raise ValueError(
                        f"{all_params['sub_geom_param']}/{all_params['sub_group_param']} does not exist."
                    )
            else:
                return [main_df, sub_df]

        except Exception as e:
            raise Exception(f"[extract_target_dataframe]: {str(e)}")

    def create_field_set_file_from_dataframe(
        self, sub_df, all_params, main_value, sub_attr_list, sub_group_list
    ):
        """
        DataFrameからFieldSetFileを生成する。

        :param sub_df: サブのDataFrame。
        :type sub_df: pandas.DataFrame
        :param all_params: プロパティと属性情報を格納した辞書。
        :type all_params: dict
        :param main_value: メインデータのリスト。
        :type main_value: list[tuple]
        :param sub_attr_list: サブ側の属性リスト。
        :type sub_attr_list: list[str]
        :param sub_group_list: サブグループのリスト。
        :type sub_group_list: list[tuple]

        :return: FieldSetFileに格納するTypeデータ、Valueデータ、Dwhデータのリスト。
        :rtype: tuple(list[str], list[str], list[str])

        :raises ValueError: 結合データに重複がある場合に発生するエラー。
        :raises Exception: FieldSetFileの生成中にエラーが発生した場合に例外をスローする。
        """
        try:
            # FieldSetFile生成用データ格納リスト
            field_type_list = []
            field_value_list = []
            field_dwh_list = []

            # サブデータのレコード数分ループ
            for sub_df_idx in range(len(sub_df)):
                # サブデータの対象レコードを取得
                sub_row = sub_df.iloc[sub_df_idx]

                if len(sub_group_list) > 0:
                    # サブグループデータがある場合、サブデータとサブグループデータを結合する
                    sub_with_sub_group = [
                        (sub_group[0], sub[1])
                        for sub in sub_row["Value"]
                        for sub_group in sub_group_list
                        if sub[0] == sub_group[1]
                    ]
                    sub_value = sub_with_sub_group
                else:
                    sub_value = sub_row["Value"]

                # サブデータとメインデータをFIndexを基に結合し、データ生成
                processed_results = [(main[0], sub[1]) for main in main_value for sub in sub_value if main[1] == sub[0] and sub[1]]
                # 処理結果をFindex順に並び替える
                processed_results.sort(key=lambda ret: ret[0])

                # 処理結果に重複がある場合はエラー
                check_index = [result[0] for result in processed_results]
                if len(check_index) != len(set(check_index)):
                    raise ValueError(
                        f"Data for concatenation is duplicated.:{processed_results}"
                    )

                # シリアライズしてbase64デコード
                base64_serialized_results = CU.encode_value(processed_results)

                # Dwhを生成
                dwh_name_suffix = [
                    sub_attr for sub_attr in sub_attr_list if sub_attr in sub_row["Dwh"]
                ]
                field_dwh_value = (
                    f"{all_params['main_geom_param']}/{dwh_name_suffix[0]}"
                )

                # FieldSetFileの中身を準備
                field_type_list.append(sub_row["Type"])
                field_value_list.append(base64_serialized_results)
                field_dwh_list.append(field_dwh_value)

            return field_type_list, field_value_list, field_dwh_list

        except Exception as e:
            raise Exception(f"[create_field_set_file_from_dataframe]: {str(e)}")

    def concatenate_attribute(self, input_field_set_file, all_params):
        """
        特定のメイン側属性と特定のサブ側属性を連結する。

        :param input_field_set_file: 処理対象のFieldSetFile。
        :type input_field_set_file: bytes
        :param all_params: プロパティと属性情報を格納した辞書。
        :type all_params: dict

        :return: 処理結果のFieldSetFile。
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """

        def grouping_sub_group_value(sub_group_val):
            """
            各ペアの最小値と最大値を抽出し、隣接する範囲を結合する。

            :param sub_group_val: サブグループのIDペアのリスト。
            :type sub_group_val: list[tuple(int, int)]

            :return: 隣接する範囲が結合されたペアのリスト。
            :rtype: list[tuple(int, int)]
            """

            # 各ペアの最小値と最大値を抽出し、ソート
            sorted_ranges = sorted({(min(a, b), max(a, b)) for a, b in sub_group_val})
            # 隣接する範囲の結合処理
            result = []
            start, end = sorted_ranges[0]
            for s, e in sorted_ranges[1:]:
                if s <= end:  # 隣接している場合
                    end = max(end, e)  # 範囲を拡張
                else:
                    result.append((start, end))  # 新しいグループの端点
                    start, end = s, e
            # 最後の範囲を追加
            result.append((start, end))
            return result

        try:
            # FieldSetFileをDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(input_field_set_file)
            # Sub Attribute Nameをリスト化
            sub_attr_list = [
                attr.strip() for attr in all_params["sub_attr_param"].split(",")
            ]

            # メイン、サブ、サブグループのDataFrameをそれぞれ抽出
            dataframes = self.extract_target_dataframe(
                dataframe, all_params, sub_attr_list
            )
            main_df, sub_df = dataframes[0], dataframes[1]
            # メインデータからValue列の情報を取得
            main_value = main_df["Value"].tolist()[0]

            # サブグループのDataFrameがある場合
            if len(dataframes) == 3:
                sub_group_df = dataframes[2]
                sub_group_value = sub_group_df["Value"].tolist()[0]
                # サブグループのIDを端点情報に変換する
                sub_group_list = grouping_sub_group_value(sub_group_value)
            else:
                sub_group_list = []

            # FieldSetFile生成用データを取得
            field_type_list, field_value_list, field_dwh_list = (
                self.create_field_set_file_from_dataframe(
                    sub_df, all_params, main_value, sub_attr_list, sub_group_list
                )
            )

            # FieldSetFileを生成
            field_set_file = NSP.get_field_set_file(
                field_type_list, field_value_list, field_dwh_list
            )

            return field_set_file

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def get_params(self, properties):
        """
        プロパティの値を取得し、辞書にして返す。

        :param properties: プロパティ情報を格納した辞書。
        :type properties: dict

        :return: プロパティの値を格納した辞書。
        :rtype: dict

        :raises Exception: プロパティの取得中にエラーが発生した場合に例外をスローする。
        """
        try:

            all_params = {}

            # プロパティの値を取得する
            all_params["main_geom_param"] = properties["MAIN_GEOMETRY_NAME"]
            all_params["main_attr_param"] = properties["MAIN_ATTRIBUTE_NAME"]
            all_params["sub_geom_param"] = properties["SUB_GEOMETRY_NAME"]
            all_params["sub_attr_param"] = properties["SUB_ATTRIBUTE_NAME"]
            all_params["sub_group_param"] = properties.get("SUB_GROUP_CONDITION", "")

            return all_params

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def __call__(self, input_field_set_file, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてフィールドセットファイルを返す。

        :param input_field_set_file: バイナリ形式のFieldSetFile
        :type input_field_set_file: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 処理結果のFieldSetFile、更新された属性情報
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # 属性情報とプロパティの値を取得
            all_params = self.get_params(properties)

            # メイン処理を実行し、処理結果としてFieldSetFileを生成
            output_field_set_file = self.concatenate_attribute(
                input_field_set_file, all_params
            )

            return output_field_set_file, attribute

        except Exception as e:
            raise Exception(str(e))
