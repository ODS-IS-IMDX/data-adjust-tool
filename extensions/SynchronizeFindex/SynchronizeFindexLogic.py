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

from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")
BaseGeometry = getattr(import_module("shapely.geometry.base"), "BaseGeometry")

import cad.common.cad_utils as CU

class SynchronizeFindexLogic:

    def check_type(self, value_list):
        """
        値のtypeをチェックし対応した文字列を返却する

        :param value_list: type判定に使うvalueリスト
        :type value_list: pandas.Series

        :return: Type判定した文字列
        :rtype: str

        :raises Exception: Findexのマッチング処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            for value in value_list:
                if value:
                    if isinstance(value, str):
                        return "string"
                    elif isinstance(value, float):
                        return "float64"
                    elif isinstance(value, int):
                        return "int"
                    elif isinstance(value, BaseGeometry):
                        return "geometry"
                    elif isinstance(value, bool):
                        return "bool"
                    else:
                        return type(value).__name__
            else:
                return "unknown"

        except Exception as e:
            raise Exception(f"[main_proc]: {str(e)}")

    def main_proc(self, input_fsf, all_params):
        """
        メインプロセス Findexのマッチング処理を行う

        :param input_fsf: 入力としてのCSV形式のFieldSetFileのバイトデータ
        :type input_fsf: bytes
        :param all_params: プロパティの値を格納した辞書。
        :type all_params: dict

        :return: 出力用のFieldSetFile
        :rtype: str

        :raises Exception: Findexのマッチング処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # DataFrameに変換
            df = CU.field_set_file_to_geodataframe(
                input_fsf, 6677, output_dataframe=True, no_geometry_flg=True
            )

            # 基準データ
            src_df = df[df["layer"] == all_params["src_data_name"]]
            # 基準データがない
            if src_df.empty:
                raise ValueError(f"{all_params['src_data_name']} is an invalid value.")
            # 基準データがBaseAttributeを含まない
            if not all_params["base_attribute"] in src_df.columns:
                raise ValueError(
                    f"{all_params['src_data_name']}/{all_params['base_attribute']} does not exist."
                )
            else:
                if not src_df[all_params["base_attribute"]].notna().any():
                    raise ValueError(
                        f"{all_params['src_data_name']}/{all_params['base_attribute']} does not exist."
                    )

            # 同期対象データ
            dst_df = df[df["layer"] == all_params["dst_data_name"]]
            # 同期対象データがない
            if dst_df.empty:
                raise ValueError(f"{all_params['dst_data_name']} is an invalid value.")
            # 同期対象データがBaseAttributeを含まない
            if not all_params["base_attribute"] in dst_df.columns:
                raise ValueError(
                    f"{all_params['dst_data_name']}/{all_params['base_attribute']} does not exist."
                )
            else:
                if not dst_df[all_params["base_attribute"]].notna().any():
                    raise ValueError(
                        f"{all_params['dst_data_name']}/{all_params['base_attribute']} does not exist."
                    )

            # マッチするFIDを探索
            suffix = "_src"
            merged_df = dst_df.merge(
                src_df[[all_params["base_attribute"], "FIndex"]],
                on=all_params["base_attribute"],
                how="left",
                suffixes=(None, suffix),
            )

            # FIDでマッチできないデータをチェック
            unmatched_fids = merged_df[merged_df[f"FIndex{suffix}"].isna()][
                all_params["base_attribute"]
            ].tolist()
            if unmatched_fids:
                raise ValueError(f"unmatched FID {','.join(unmatched_fids)}")

            # 基準データのFindexで同期対象データのFindex更新する
            merged_df["FIndex"] = merged_df[f"FIndex{suffix}"]
            dst_df = merged_df.drop(columns=[f"FIndex{suffix}"])

            # マッチング処理する予定のカラムを取得
            filter_col_name_list = list(
                filter(
                    lambda x: x not in ["layer", "FID", "FIndex"],
                    dst_df.columns.values,
                )
            )

            # 結果DataFrame作成
            result_df = pd.DataFrame(columns=["Dwh", "Type", "Value"])
            dwh_list = []
            type_list = []
            value_list = []

            # base_attributeについて整備
            dwh_list.append(
                f"{all_params['output_data_name']}/{all_params['base_attribute']}"
            )
            value_list.append(
                list(zip(dst_df[all_params["base_attribute"]], dst_df["FIndex"]))
            )
            type_list.append(self.check_type(dst_df[all_params["base_attribute"]]))

            # その他の属性について整備
            for col_nm in filter_col_name_list:
                if col_nm == "geometry":
                    # geometryの有無を確認する
                    if not all(
                        geometry is None
                        for geometry in dst_df["geometry"].values.tolist()
                    ):
                        dwh_list.append(f"{all_params['output_data_name']}/{col_nm}")
                        type_list.append(self.check_type(dst_df[col_nm]))
                        value_list.append(dst_df["geometry"].values.tolist())
                else:
                    dwh_list.append(f"{all_params['output_data_name']}/{col_nm}")
                    type_list.append(self.check_type(dst_df[col_nm]))
                    value_list.append(list(zip(dst_df["FIndex"], dst_df[col_nm])))

            # 結果用DataFrameに追加
            result_df["Dwh"] = dwh_list
            result_df["Type"] = type_list
            result_df["Value"] = value_list

            # FieldSetFileを生成する
            return CU.dataframe_to_field_set_file(result_df)

        except Exception as e:
            raise Exception(f"[main_proc]: {str(e)}")

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

            # プロパティの値を取得する
            all_params = {
                "src_data_name": properties["SRC_DATA_NAME"],
                "base_attribute": properties["BASE_ATTRIBUTE"],
                "dst_data_name": properties["DST_DATA_NAME"],
                "output_data_name": properties["OUTPUT_DATA_NAME"],
            }

            return all_params

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def __call__(self, input_field_set_file, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param input_field_set_file: 入力としてのCSV形式のFieldSetFileのバイトデータ
        :type input_field_set_file: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のFieldSetFileとFlowFileの属性情報
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティの値を取得し辞書にして返す
            all_params = self.get_params(properties)

            # メインプロセス
            output_field_set_file = self.main_proc(input_field_set_file, all_params)

            return output_field_set_file, attribute

        except Exception as e:
            raise Exception(str(e))
