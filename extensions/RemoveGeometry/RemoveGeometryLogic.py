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
import base64
import pickle
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")

import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class RemoveGeometryLogic:

    def __init__(self):
        pass

    def convert_field_set_file_to_dataframe(
        self, field_set_file, geometry_name, condition
    ):
        """
        FieldSetFileをDataFrameに変換する。

        :param field_set_file: インプットのFieldSetFileデータ
        :type field_set_file: bytes
        :param geometry_name: 操作対象のジオメトリデータの名称
        :type geometry_name: str
        :param condition: 削除条件のデータ名
        :type condition: str

        :return: FieldSetFile全体のDataFrame、操作対象のDataFrame、DwhとTypeの関連情報(dict)
        :rtype: tuple(pandas.DataFrame, pandas.DataFrame, dict{str, str})

        :raises ValueError: 必要なカラムがFieldSetFileに存在しない場合に例外をスローする。
        :raises Exception: その他のエラーが発生した場合に例外をスローする。
        """
        try:
            # FieldSetFileをDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(field_set_file)

            if not len(dataframe) > 0:
                raise ValueError(f"Invalid input data.")

            if not (dataframe["Dwh"] == f"{geometry_name}/geometry").any():
                raise ValueError(f"{geometry_name}/geometry does not exist.")

            if not (dataframe["Dwh"] == f"{geometry_name}/FID").any():
                raise ValueError(f"{geometry_name}/FID does not exist.")

            if "Not_" in condition:
                if not (dataframe["Dwh"] == f"{geometry_name}/{condition[4:]}").any():
                    raise ValueError(f"{geometry_name}/{condition[4:]} does not exist.")

                if not (dataframe["Dwh"] == f"{geometry_name}/{condition}").any():
                    raise ValueError(f"{geometry_name}/{condition} does not exist.")
            else:
                if not (dataframe["Dwh"] == f"{geometry_name}/{condition}").any():
                    raise ValueError(f"{geometry_name}/{condition} does not exist.")

                if not (dataframe["Dwh"] == f"{geometry_name}/Not_{condition}").any():
                    raise ValueError(f"{geometry_name}/Not_{condition} does not exist.")

            # DataFrameから操作対象のカラムの値を抽出してリスト化
            # geometry情報を取得
            src_geometry_df = dataframe[
                (dataframe["Dwh"] == f"{geometry_name}/geometry")
            ]
            # FID情報を取得
            src_fid_df = dataframe[(dataframe["Dwh"] == f"{geometry_name}/FID")]

            # それ以外の情報を取得
            src_attr_df = dataframe[
                (dataframe["Dwh"].str.match(rf"^{geometry_name}/"))
                & (dataframe["Dwh"] != f"{geometry_name}/geometry")
                & (dataframe["Dwh"] != f"{geometry_name}/FID")
            ]

            # geometry情報とFID情報とその他情報を結合する
            src_df = pd.concat(
                [src_geometry_df, src_fid_df, src_attr_df],
                axis=0,
            )

            # それぞれの列の値をリストにする
            src_dwh_list = src_df["Dwh"].tolist()
            src_type_list = src_df["Type"].tolist()
            src_value_list = src_df["Value"].tolist()

            # 各要素の数が同じであることを確認
            src_value_list_lengths = [len(element) for element in src_value_list]
            first_length = src_value_list_lengths[0]
            for i, length in enumerate(src_value_list_lengths):
                if length != first_length:
                    raise ValueError(
                        f"The number of elements in the record is different."
                    )

            # 操作対象データのValueをまとめてリスト化
            src_values = list(map(lambda x: list(x), zip(*src_value_list)))

            # 操作対象データをインデックスごとにレコードを持つDataFrameに変換
            src_dataframe = pd.DataFrame(src_values, columns=src_dwh_list)

            # Dwh名とTypeの情報を組み合わせてdict型にする
            dwh_type_info = dict(zip(src_dwh_list, src_type_list))

            return dataframe, src_dataframe, dwh_type_info

        except Exception as e:
            raise Exception(f"[convert_field_set_file_to_dataframe]: {str(e)}")

    def remove_dataframe_by_condition(self, df, src_df, geometry_name, condition):
        """
        DataFrameの特定条件に基づきレコードを削除する。

        :param df: 元のDataFrame
        :type df: pandas.DataFrame
        :param src_df: 操作対象のDataFrame
        :type src_df: pandas.DataFrame
        :param geometry_name: ジオメトリデータの名前
        :type geometry_name: str
        :param condition: 削除条件
        :type condition: str

        :return: 条件を満たさないレコードのみのDataFrame
        :rtype: pandas.DataFrame

        :raises ValueError: 条件のデータ型が期待される型でない場合に例外をスローする。
        :raises Exception: その他のエラーが発生した場合に例外をスローする。
        """
        try:
            # 削除条件のDataFrameのFindexをリスト化

            if "Not_" in condition:
                condition_list = df[(df["Dwh"] == f"{geometry_name}/{condition}")][
                    "Value"
                ].tolist()[0]
                not_condition_list = df[
                    (df["Dwh"] == f"{geometry_name}/{condition[4:]}")
                ]["Value"].tolist()[0]

                # findexとValueに分ける
                condition_findex_list, condition_value_list = zip(*condition_list)
                not_condition_findex_list, not_condition_value_list = zip(
                    *not_condition_list
                )

                # conditionのデータがbool型かどうかチェック
                if not all(isinstance(value, bool) for value in condition_value_list):
                    raise ValueError(f"'{geometry_name}/{condition}' is not bool-type.")
                
                if not all(
                    isinstance(value, bool) for value in not_condition_value_list
                ):
                    raise ValueError(
                        f"'{geometry_name}/{condition[4:]}' is not bool-type."
                    )
            else:
                condition_list = df[(df["Dwh"] == f"{geometry_name}/{condition}")][
                    "Value"
                ].tolist()[0]
                not_condition_list = df[
                    (df["Dwh"] == f"{geometry_name}/Not_{condition}")
                ]["Value"].tolist()[0]

                # findexとValueに分ける
                condition_findex_list, condition_value_list = zip(*condition_list)
                not_condition_findex_list, not_condition_value_list = zip(
                    *not_condition_list
                )

                # conditionのデータがbool型かどうかチェック
                if not all(isinstance(value, bool) for value in condition_value_list):
                    raise ValueError(f"'{geometry_name}/{condition}' is not bool-type.")
                
                if not all(
                    isinstance(value, bool) for value in not_condition_value_list
                ):
                    raise ValueError(
                        f"'{geometry_name}/Not_{condition}' is not bool-type."
                    )

            # Trueのfindexのみを取得する
            filtered_findex_list = [
                findex
                for findex, tf in zip(condition_findex_list, condition_value_list)
                if tf
            ]
            # 操作対象DataFrameから条件に該当するレコードのみ取得
            remove_condition = src_df[f"{geometry_name}/FID"].apply(
                lambda fid: fid[-1] in filtered_findex_list
            )

            # 条件に該当するDataFrameと該当しないDataFrameを返す
            return src_df[~remove_condition]

        except Exception as e:
            raise Exception(f"[remove_dataframe_by_condition]: {str(e)}")

    def convert_dataframe_to_field_set_file(
        self,
        removed_dataframe,
        output_name,
        dwh_type_info,
    ):
        """
        DataFrameをFieldSetFile形式に変換する。

        :param removed_dataframe: 削除後のDataFrame
        :type removed_dataframe: pandas.DataFrame
        :param output_name: 出力するFieldSetFileの名前
        :type output_name: str
        :param dwh_type_info: DwhとTypeの関連情報
        :type dwh_type_info: dict

        :return: FieldSetFile形式のデータ
        :rtype: bytes

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """

        def extract_data_for_field_set_file_from_dataframe(
            df, new_geometry_name, dwh_type
        ):
            """
            DataFrameからFieldSetFile形式に変換するためのデータを抽出する。

            :param df: 操作対象のDataFrame
            :type df: pandas.DataFrame
            :param new_geometry_name: 新しいFieldSetFile内のデータ名
            :type new_geometry_name: str
            :param dwh_type: DwhとTypeの関連情報を格納した辞書
            :type dwh_type: dict{str, str}

            :return: FieldSetFileのデータ列（Type、Value、Dwh）に対応するリスト
            :rtype: tuple(list[str], list[str], list[str])

            :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
            """
            try:
                type_list = []
                value_list = []
                dwh_list = []

                for col in df:
                    # Valueの値をシリアライズしてデコード
                    base64_serialize_fid_list = base64.b64encode(
                        pickle.dumps(df[col].tolist())
                    ).decode("utf-8")

                    # Dwhの/以降を取得
                    dwh_suffix = col.split("/")[1]

                    # FieldSetFileに格納するデータを追加
                    type_list.append(dwh_type[col])
                    value_list.append(base64_serialize_fid_list)
                    dwh_list.append(f"{new_geometry_name}/{dwh_suffix}")

                return type_list, value_list, dwh_list

            except Exception as e:
                raise Exception(f"[extract_data_for_field_set_file_from_dataframe]: {str(e)}")

        try:
            # 条件に適合しないDataFrameをFieldSetFileに入れるデータを作成
            removed_type, removed_value, removed_dwh = (
                extract_data_for_field_set_file_from_dataframe(
                    removed_dataframe, output_name, dwh_type_info
                )
            )
            
            # FieldSetFileを作成
            field_set_file = NSP.get_field_set_file(
                removed_type,
                removed_value,
                removed_dwh,
            )
            return field_set_file

        except Exception as e:
            raise Exception(f"[convert_dataframe_to_field_set_file]: {str(e)}")

    def __call__(self, byte_data, attributes, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: 入力としてのCSV形式のFieldSetFileのバイトデータ
        :type byte_data: bytes
        :param attributes: フローファイルの属性情報を格納する辞書
        :type attributes: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のFieldSetFileと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # プロパティ値取得
            geometry_name = properties['GEOMETRY_NAME']
            condition = properties['CONDITION']
            output_name = properties['OUTPUT_NAME']

            # 操作対象のFieldSetFileをDataFrameに変換
            # 戻り値①：FieldSetFile全体をDataFrameに変換したもの（カラム：Dwh,Type,Value）
            # 戻り値②：FieldSetFileの操作対象データを、レコードごとにカラムを生成しDataFrameに変換したもの
            # 戻り値③：FieldSetFileの操作対象データからDwhとTypeの関係を辞書にしたもの
            dataframe, src_dataframe, dwh_type_info = (
                self.convert_field_set_file_to_dataframe(
                    byte_data, geometry_name, condition
                )
            )

            # 判定結果に基づいて削除する
            removed_dataframe = self.remove_dataframe_by_condition(
                dataframe, src_dataframe, geometry_name, condition
            )

            # 削除後のDataFrameをカラムごとにFieldSetFileへ変換する
            output_field_set_file = self.convert_dataframe_to_field_set_file(
                removed_dataframe,
                output_name,
                dwh_type_info,
            )

            return output_field_set_file, attributes

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
