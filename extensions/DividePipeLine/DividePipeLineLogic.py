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
from collections import defaultdict, Counter
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")
Point = getattr(import_module("shapely"), "Point")
nearest_points = getattr(import_module("shapely.ops"), "nearest_points")
LineString = getattr(import_module("shapely.geometry"), "LineString")

import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class DividePipeLineLogic:
    def create_field_set_file(self, results):
        """
        FieldSetFileを生成する

        :param results: 演算結果を持つDataFrame
        :type results: pandas.DataFrame

        :return: 処理結果としてのFieldSetFile。
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # Valueをシリアライズするために取り出す
            field_dwh_list = results["Dwh"].tolist()
            field_type_list = results["Type"].tolist()
            values = results["Value"].tolist()

            # 結果をシリアライズしてbase64でエンコード
            field_value_list = [CU.encode_value(value) for value in values]

            # FieldSetFileを生成
            field_set_file = NSP.get_field_set_file(
                field_type_list, field_value_list, field_dwh_list
            )

            return field_set_file

        except Exception as e:
            raise Exception(f"[ERROR create_field_set_file]: {str(e)}")

    def check_mult_line(self, dataframe):
        """
        管路情報１つに対して複数の旗上げ線があるケースを検出し、対象のFIDを返却する

        :param dataframe: 対象のDataFrame
        :type dataframe: pandas.Series

        :return: 管路情報に対する旗上げ線のリストの辞書
        :rtype: dict{int, list[float]}

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            df = pd.DataFrame(dataframe)
            value_list = [(x, y) for x, y in df["Value"].iloc[0]]

            # 旗上げ線が持つ管路情報のFIDが重複しているペアのみを抽出
            left_side_counts = Counter([pair[0] for pair in value_list])
            duplicates = [pair for pair in value_list if left_side_counts[pair[0]] > 1]

            if len(duplicates) == 0:
                return None

            return {
                "main": [main for main, _ in duplicates],
                "sub": [sub for _, sub in duplicates],
            }

        except Exception as e:
            raise Exception(f"[ERROR check_mult_line]: {str(e)}")

    def set_dict_list(self, geometry_list, fid_list):
        """
        geometryとFIDを紐づけたリストを作成する

        :param geometry_list: ジオメトリデータ
        :type geometry_list: pandas.DataFrame
        :param fid_list: FIDデータ
        :type fid_list: pandas.DataFrame

        :return: 管路情報に対する旗上げ線リスト
        :rtype: dict{list[int], list[shapely.geometry.LineString]}

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            return {
                "geometry": geometry_list["Value"].iloc[0],
                "FID": [pair[1] for pair in fid_list["Value"].iloc[0]],
            }

        except Exception as e:
            raise Exception(f"[ERROR set_dict_list]: {str(e)}")

    def calc_cross_point(self, target_lines, main_dict_list, sub_dict_list):
        """
        複数の旗上げ線を持つ管路情報の交点を求める

        :param target_lines: 対象の管路情報、旗上げ線のリスト
        :type target_lines: dict{list[float]}
        :param main_dict_list: 管路情報のリスト
        :type main_dict_list: dict{list[int], list[shapely.geometry.LineString]}
        :param sub_dict_list: 旗上げ線のリスト
        :type sub_dict_list: dict{list[int], list[shapely.geometry.LineString]}

        :return: 各管路情報に紐づく旗上げ線及び交点のリスト
        :rtype: list[list[tuple(float, shapely.geometry.LineString,
         list[dict{shapely.geometry.Point, float}, shapely.geometry.Point, shapely.geometry.Point])]]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            ret_list = []
            nearest_point = None
            farthest_point = None
            main_geom = None
            main_fid = None

            # 管路情報と旗上げ線を紐づけ、各交点を求める
            for set_main_fid in list(set(target_lines["main"])):
                linked_geometries = []
                for main, sub in list(zip(target_lines["main"], target_lines["sub"])):

                    # FIDが違う場合は処理をスキップ
                    if not set_main_fid == main:
                        continue

                    # 管路情報のFIDからgeometryを取得
                    main_geom = main_dict_list["geometry"][
                        main_dict_list["FID"].index(main)
                    ]
                    main_fid = main

                    # 旗上げ線のFIDのからgeometryを取得
                    sub_geom = sub_dict_list["geometry"][
                        sub_dict_list["FID"].index(sub)
                    ]

                    # 交点を取得
                    point_on_main, point_on_sub = nearest_points(main_geom, sub_geom)

                    # 管路情報の両端点を取得
                    start_point = Point(main_geom.coords[0])
                    end_point = Point(main_geom.coords[-1])

                    # 両端の座標と交点の距離を計算
                    distance_to_start = start_point.distance(point_on_main)
                    distance_to_end = end_point.distance(point_on_main)

                    # 両端点から交点の距離を求め、最も数値の低い交点を算出
                    if not nearest_point:
                        if distance_to_start < distance_to_end:
                            nearest_point = start_point
                        else:
                            nearest_point = end_point
                    else:
                        if distance_to_start < start_point.distance(nearest_point):
                            nearest_point = start_point
                        elif distance_to_end < end_point.distance(nearest_point):
                            nearest_point = end_point

                    # 交点と最も近い端点とは反対の端点を設定する
                    if nearest_point.distance(start_point) > nearest_point.distance(
                        end_point
                    ):
                        farthest_point = start_point
                    else:
                        farthest_point = end_point

                    # 結果を追加
                    linked_geometries.append(
                        {"sub_FID": sub, "cross_point": point_on_main}
                    )
                # 交点の距離順に並び替える
                sort_list = sorted(
                    linked_geometries,
                    key=lambda x: x["cross_point"].distance(nearest_point),
                )
                ret_list.append(
                    [(main_fid, main_geom, sort_list, nearest_point, farthest_point)]
                )
            return ret_list
        except Exception as e:
            raise Exception(f"[ERROR calc_cross_point]: {str(e)}")

    def get_split_line_list(self, cross_point_lists, max_main_fid):
        """
        管路情報の分割処理

        :param cross_point_lists: 交点のリスト
        :type cross_point_lists: list[list[tuple(float, shapely.geometry.LineString,
         list[dict{shapely.geometry.Point, float}, shapely.geometry.Point, shapely.geometry.Point])]]
        :param max_main_fid: 管路情報FIDの最大値
        :type max_main_fid: int

        :return: 各管路情報に紐づく旗上げ線及び交点のリスト
        :rtype: tuple(dict{float, shapely.geometry.LineString, list[int, shapely.geometry.LineString, float]}, list[tuple(int, float)])

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            split_line_list = []
            fid_table = []
            ret_dict = {}
            new_fid = max_main_fid + 1

            # 管路情報ごとの交点を元に管路情報を分割する
            for cross_point_list in cross_point_lists:
                for (
                    main_fid,
                    main_geom,
                    sort_list,
                    nearest_point,
                    farthest_point,
                ) in cross_point_list:
                    split_start_point = nearest_point  # 分割開始点
                    split_end_point = None  # 分割終了点
                    sub_fid = None

                    # 管路分割処理
                    for item in sort_list:
                        if not sub_fid:
                            sub_fid = item["sub_FID"]

                        cross_point = item["cross_point"]

                        # 初回は終了点を設定し処理をスキップ
                        if not split_end_point:
                            split_end_point = cross_point
                            continue

                        # 分割後の管路情報を追加
                        split_line_list.append(
                            {
                                "new_FID": new_fid,
                                "sub_FID": sub_fid,
                                "split_line": LineString(
                                    [split_start_point, split_end_point]
                                ),
                            }
                        )
                        # 新旧対応用のFID追加
                        fid_table.append((new_fid, main_fid))
                        sub_fid = item["sub_FID"]
                        split_start_point = split_end_point
                        split_end_point = cross_point

                        # 新規FID更新
                        new_fid += 1

                    # 分割対象の最後の管路情報を追加
                    split_line_list.append(
                        {
                            "new_FID": new_fid,
                            "sub_FID": sub_fid,
                            "split_line": LineString(
                                [split_start_point, farthest_point]
                            ),
                        }
                    )
                    fid_table.append((new_fid, main_fid))
                    new_fid += 1

                    ret_dict.update(
                        {
                            "main_FID": main_fid,
                            "main_geo": main_geom,
                            "split_list": split_line_list,
                        }
                    )

            return ret_dict, fid_table
        except Exception as e:
            raise Exception(f"[ERROR get_split_line_list]: {str(e)}")

    def update_specific_dwh_name(self, df, target_name_list, new_df):
        """
        指定したDwhを変更して追加する処理

        :param df: 変更するDataFrame
        :type df: pandas.DataFrame
        :param target_name_list: 変更対象の名称
        :type target_name_list: list[str]

        :return: 変更後のDataFrame
        :rtype: pandas.DataFrame

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            for target_name in target_name_list:
                # 対象をコピーし名称を変更する
                copy_row = df[df["Dwh"] == target_name].copy()
                copy_row["Dwh"] = target_name.replace("/", "/new_")

                # DataFrameに新しい行を追加
                new_df = pd.concat([new_df, copy_row], ignore_index=True)
            return new_df
        except Exception as e:
            raise Exception(f"[ERROR update_specific_dwh_name]: {str(e)}")

    def update_dataflame(
        self, df, target_list, attribute_main, attribute_sub, update_main, update_sub
    ):
        """
        DataFrameから対象項目とそれに紐づくインデックスを持つ別項目を更新する

        :param df: 変更するDataFrame
        :type df: pandas.DataFrame
        :param target_list: 更新対象のリスト
        :type target_list: list[tuple(str, float)]
        :param attribute_main: メインとなる更新対象の名称
        :type attribute_main: str
        :param attribute_sub: サブとなる更新対象の名称
        :type attribute_sub: str
        :param update_main: メイン更新データ
        :type update_main: list[tuple(str, int)]
        :param update_sub: サブ更新データ
        :type update_sub: tuple(shapely.geometry.LineString)

        :return: 変更後のDataFrame
        :rtype: pandas.DataFrame

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # 処理対象のインデックスを取得
            main_index = (
                df["Dwh"].index[df["Dwh"].str.contains(attribute_main)].values[0]
            )
            sub_index = df["Dwh"].index[df["Dwh"].str.contains(attribute_sub)].values[0]
            new_fid_values = [
                item for item in df.at[main_index, "Value"] if item not in target_list
            ]

            # 削除するインデックスを取得
            removed_indices = [
                df.at[main_index, "Value"].index(item)
                for item in target_list
                if item in df.at[main_index, "Value"]
            ]

            # FIDの行を更新
            df.at[main_index, "Value"] = new_fid_values

            # geometryから削除されたインデックスに対応するデータを削除
            df.at[sub_index, "Value"] = [
                df.at[sub_index, "Value"][i]
                for i in range(len(df.at[sub_index, "Value"]))
                if i not in removed_indices
            ]

            df.at[main_index, "Value"].extend(update_main)
            df.at[sub_index, "Value"].extend(update_sub)

            # インデックスをリセット
            df.reset_index(drop=True, inplace=True)

            return df
        except Exception as e:
            raise Exception(f"[ERROR update_dataflame]: {str(e)}")

    def rename_dwh(self, dataframe, old_name, new_name):
        """
        指定されたDwh名を変更する関数

        :param dataframe: FieldSetFileのデータを持つDataFrame
        :type dataframe: pandas.DataFrame
        :param old_name: 変更前のDwh名
        :type old_name: str
        :param new_name: 変更後のDwh名
        :type new_name: str

        :return: Dwh名を変更したデータ
        :rtype: pandas.DataFrame
        """
        dataframe.loc[dataframe["Dwh"] == old_name, "Dwh"] = new_name
        return dataframe

    def divide_pipe_line(self, properties, byte_data):
        """
        複数の注基線が存在する場合、２つ目以降の接する注基線から注基情報を分割する

        :param properties: プロパティ情報を含む辞書
        :type properties: dict
        :param byte_data: バイナリ形式のFieldSetFile
        :type byte_data: bytes

        :return: 処理結果としてのFieldSetFile。
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティの値を取得し辞書にして返す
            main_geom_name = properties["MAIN_GEOMETRY_NAME"]
            main_attr_param = properties["MAIN_ATTRIBUTE_NAME"]
            sub_geom_name = properties["SUB_GEOMETRY_NAME"]
            sub_attr_param = properties["SUB_ATTRIBUTE_NAME"]

            # 削除対象リスト
            remove_list = [main_geom_name + '/geometry', main_geom_name + '/FID', main_geom_name + '/' + main_attr_param
                , sub_geom_name + '/' + sub_attr_param]

            # DataFrameに加工
            dataframe = CU.field_set_file_to_dataframe(byte_data)

            # DataFrameが存在しない場合エラーとする
            if dataframe.empty or len(dataframe) == 0:
                raise ValueError(f"Dataframe empty")

            # 対象geometryの取得
            main_geom_list = dataframe[
                dataframe["Dwh"].str.contains(main_geom_name + "/" + sub_attr_param)
            ]

            # 管路１つに対して複数の旗上げ線があるケースを検出
            target_lines = self.check_mult_line(main_geom_list["Value"])

            # 分割対象がない場合、対象項目をリネームして処理終了
            if not target_lines:
                for remove_name in remove_list:
                    new_name = remove_name.replace("/", "/new_")
                    dataframe = self.rename_dwh(dataframe, remove_name, new_name)
                return self.create_field_set_file(dataframe)

            main_dict_list = self.set_dict_list(
                dataframe[dataframe["Dwh"].str.contains(main_geom_name + "/geometry")],
                dataframe[dataframe["Dwh"].str.contains(main_geom_name + "/FID")],
            )
            sub_dict_list = self.set_dict_list(
                dataframe[dataframe["Dwh"].str.contains(sub_geom_name + "/geometry")],
                dataframe[dataframe["Dwh"].str.contains(sub_geom_name + "/FID")],
            )

            # 旗上げ線と管路の交点を算出し、両端点から最も近い交点からの距離順に全交点を並べて交点リストにする
            cross_point_list = self.calc_cross_point(
                target_lines, main_dict_list, sub_dict_list
            )  # 交点のリスト

            # FIDの最大値を取得
            max_main_fid = max([fid for fid in main_dict_list["FID"]])
            new_df = dataframe

            # 分割管路リストを作成する。
            # 新しい管路のFIDと古い管路のFIDの対応表を作成する
            split_line_list, fid_table = self.get_split_line_list(
                cross_point_list, max_main_fid
            )

            # 各要素を抜き出す
            new_fids, sub_fids, split_lines = zip(
                *[
                    (item["new_FID"], item["sub_FID"], item["split_line"])
                    for item in split_line_list["split_list"]
                ]
            )

            # 管路情報 / geometry/FID _orgに変更する
            target_name_list = [
                main_geom_name + "/geometry",
                main_geom_name + "/FID",
                main_geom_name + "/" + main_attr_param,
                sub_geom_name + "/" + sub_attr_param,
            ]
            new_df = self.update_specific_dwh_name(dataframe, target_name_list, new_df)

            # 管路情報 / geometryからは、分割前の管路を省き、新しい管路のデータを追加する
            delete_main_lines = [
                (str(int(i)), i) for i in list(set(target_lines["main"]))
            ]
            new_df = self.update_dataflame(
                new_df,
                delete_main_lines,
                main_geom_name + "/new_FID",
                main_geom_name + "/new_geometry",
                [(str(int(i)), i) for i in new_fids],
                split_lines,
            )

            # 管路情報/旗上げと旗上げ情報/旗上げのリストを更新する
            delete_sub_lines = [
                (target_lines["main"][i], target_lines["sub"][i])
                for i in range(len(target_lines["main"]))
            ]
            update_data_main = [
                (float(main_val), float(sub_val))
                for main_val, sub_val in zip(new_fids, sub_fids)
            ]
            update_data_sub = [
                (float(sub_val), float(main_val))
                for sub_val, main_val in zip(sub_fids, new_fids)
            ]
            new_df = self.update_dataflame(
                new_df,
                delete_sub_lines,
                main_geom_name + "/new_" + sub_attr_param,
                sub_geom_name + "/new_" + sub_attr_param,
                update_data_main,
                update_data_sub,
            )

            # 新旧対応の項目を追加
            new_df.loc[new_df.index.max() + 1] = [
                main_geom_name + "/新旧対応",
                "results",
                fid_table,
            ]

            # 作成したデータから削除対象項目を削除する
            new_df = new_df[~new_df["Dwh"].isin(remove_list)]

            # DataFrameをFieldSetFileへ変換する
            field_set_file = self.create_field_set_file(new_df)

            return field_set_file

        except Exception as e:
            raise Exception(f"[ERROR divide_pipe_line]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: バイナリ形式のFieldSetFile
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のFieldSetFileと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # メインの処理関数に渡して実行
            out_fsf = self.divide_pipe_line(properties, byte_data)

            return out_fsf, attribute
        except Exception as e:
            # 例外発生時、エラーメッセージを記録し、失敗を返す
            raise Exception(f"[transform_Exception]:{e}")
