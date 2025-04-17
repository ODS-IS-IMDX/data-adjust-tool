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
import io
import pickle
from importlib import import_module

# 外部ライブラリの動的インポート
gpd = import_module("geopandas")
pd = import_module("pandas")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
CRS = getattr(import_module("pyproj"), "CRS")

import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import cad.common.cad_utils as CU


class ClipFlagLineRangesLogic:
    def __init__(self):
        pass

    def get_number_from_string(self, s: str, num_type: type = float):
        """
        文字列から指定された型の数値を取得する関数。数字に変換できない場合はValueErrorを発生させる。

        :param s: 変換対象の文字列。
        :type s: str
        :param num_type: 変換後の型（デフォルトは float）。
        :type num_type: type, optional

        :return: 変換された数値。
        :rtype: num_type

        :raises ValueError: 数字に変換できない場合に発生するエラー。
        """
        try:
            # 指定された型に変換
            return num_type(s)
        except (ValueError, TypeError):
            # 数字に変換できない場合、ValueErrorを発生させる
            raise ValueError(f"'{s}' を {num_type.__name__} に変換できませんでした")

    def create_gdf(self, dataframe, geometry_name, params):
        """
        指定したDataFrameからGeoDataFrameを作成する。

        :param dataframe: DataFrame。
        :type dataframe: pandas.DataFrame
        :param geometry_name: ジオメトリ名。
        :type geometry_name: str
        :param params: 属性値とプロパティの設定値を格納した辞書。
        :type params: dict

        :return: 作成されたGeoDataFrame。
        :rtype: geopandas.GeoDataFrame

        :raises ValueError: ジオメトリやFIDが存在しない場合に発生するエラー。
        :raises Exception: GeoDataFrameの作成中にエラーが発生した場合に例外をスローする。
        """
        try:
            # ジオメトリを取得
            # 対象データの存在チェック
            if (dataframe["Dwh"] == f"{geometry_name}/geometry").any():
                geometry = dataframe.loc[
                    dataframe["Dwh"] == f"{geometry_name}/geometry", "Value"
                ].values[0]
            else:
                raise ValueError(f"{geometry_name}/geometry does not exist.")

            # 対象データの存在チェック
            if (dataframe["Dwh"] == f"{geometry_name}/FID").any():
                fids = dataframe.loc[
                    dataframe["Dwh"] == f"{geometry_name}/FID", "Value"
                ].values[0]
            else:
                raise ValueError(f"{geometry_name}/FID does not exist.")

            gdf = gpd.GeoDataFrame({"FID": fids}, geometry=geometry, crs=params["crs"])

            return gdf

        except Exception as e:
            raise Exception(f"[create_gdf]: {str(e)}")

    def get_params(self, attribute, properties):
        """
        Attributeの値とプロパティの値を取得し辞書にして返す。

        :param attribute: 各属性の値を含む辞書。
        :type attribute: dict
        :param properties: 各プロパティの値を含む辞書。
        :type properties: dict

        :return: プロパティの属性値を辞書形式で返す。
        :rtype: dict

        :raises Exception: プロパティの取得処理でエラーが発生した場合に発生する。
        """
        try:
            params = {}

            # プロパティの値を取得する。
            params["flag_line"] = properties["FLAG_LINE_NAME"]
            params["circle"] = properties["CIRCLE_NAME"]
            params["pair"] = properties["PAIRING_NAME"]
            params["flag_line_grp"] = properties["FLAG_LINE_GROUP_NAME"]
            params["dst"] = properties["DST_INFO_NAME"]

            # Attributeの値を取得
            crs_value = (
                properties["CRS"]
                or attribute.get("crs", "")
                or attribute.get("CRS", "")
            )
            params["crs"] = CRS.from_epsg(
                CU.get_number_from_string(crs_value, int)
            ).srs  # CRS有効チェック

            return params
        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def prepare_for_create_flag_grp_data(
        self, key_flag_fids, flag_grp_dwh_val, flag_pair_dwh_val
    ):
        """
        サークル情報と旗上げ情報のIDをそれぞれのグループごとにまとめる。

        :param key_flag_fids: サークル情報とペアリングしている旗上げ情報のIDリスト。
        :type key_flag_fids: list[float]
        :param flag_grp_dwh_val: 旗上げ情報でtouchしている旗上げ情報のIDリスト。
        :type flag_grp_dwh_val: list[tuple(float, list[float])]
        :param flag_pair_dwh_val: 旗上げ情報に対するサークル情報のIDリスト。
        :type flag_pair_dwh_val: list[tuple(float)]

        :return: 連結している旗上げ情報のIDリスト、同じ旗上げ線にペアリングしているサークル情報のIDリスト。
        :rtype: tuple(dict{float, list[float]}, dict{float, list[float]})

        :raises Exception: 処理中にエラーが発生した場合に発生する。
        """
        try:
            # 旗上げ情報FIDを取得
            connected_ids = set()  # つながっているIDを保存するセット
            find_flag_fids = dict()

            # flag_grp_dwh_valがある場合、連結している旗揚げ情報のIDリストを探索
            if flag_grp_dwh_val:
                for start_id in key_flag_fids:
                    connected_ids = set()  # つながっているIDを保存するセット
                    to_visit = [start_id]  # 探索を開始するID

                    while to_visit:
                        current_id = to_visit.pop()
                        if current_id not in connected_ids:
                            connected_ids.add(current_id)  # IDをセットに追加

                            # 現在のIDに隣接するIDを探索
                            for id_pair in flag_grp_dwh_val:
                                if id_pair[0] == current_id:
                                    # 隣接するIDを訪問リストに追加（まだ訪問していないものだけ）
                                    if id_pair[1] is None:
                                        to_visit.extend([])
                                    else:
                                        to_visit.extend(
                                            [
                                                neighbor
                                                for neighbor in id_pair[1]
                                                if neighbor not in connected_ids
                                            ]
                                        )

                    # 探索が終了したら、すべてのつながるIDをリストに変換して保存
                    find_flag_fids[start_id] = list(connected_ids)

            # サークル情報FIDの抽出 dict key = flag_fid, value = [circle_fid,...]
            find_circle_fids = {
                flag_fid: list(
                    {pair[1] for pair in flag_pair_dwh_val if pair[0] == flag_fid}
                )
                for flag_fid in key_flag_fids
            }

            return find_flag_fids, find_circle_fids

        except Exception as e:
            raise Exception(f"[prepare_for_create_flag_grp_data]: {str(e)}")

    def create_flag_grp_data(
        self, key_flag_fids, find_flag_fids, find_circle_fids, flag_line_gdf, circle_gdf
    ):
        """
        旗上げグループを作成する。

        :param key_flag_fids: サークル情報とペアリングしている旗上げ情報のIDリスト。
        :type key_flag_fids: list[float]
        :param find_flag_fids: 連結している旗上げ情報のIDリストを格納した辞書。キーは旗上げ情報のID。
        :type find_flag_fids: dict{float, list[float]}
        :param find_circle_fids: 同じ旗上げ線にペアリングしているサークル情報のIDリストを格納した辞書。キーは旗上げ情報のID。
        :type find_circle_fids: dict{float, list[float]}
        :param flag_line_gdf: 旗上げ情報のGeoDataFrame。
        :type flag_line_gdf: geopandas.GeoDataFrame
        :param circle_gdf: サークル情報のGeoDataFrame。
        :type circle_gdf: geopandas.GeoDataFrame

        :return: 旗上げグループのGeoDataFrameのリスト。
        :rtype: list[geopandas.GeoDataFrame]

        :raises Exception:
            旗上げグループの作成中にエラーが発生した場合に例外をスローし、処理が失敗する。
        """
        try:
            # まとめたgeometryのリスト
            geometries = []
            for key_flag_fid in key_flag_fids:
                # 旗上げ情報
                # 連結している旗上げ線のIDリストがない場合 [key_flag_fid]:サークル情報とtoucheしている旗上げ情報ID
                #                                       find_flag_fids[key_flag_fid]:key_flag_fidの旗揚げ線と連結している旗揚げ情報ID
                flag_fid_list = [key_flag_fid] if not find_flag_fids else find_flag_fids[key_flag_fid]
                flag_geo_df = flag_line_gdf[
                    flag_line_gdf["FID"].apply(lambda x: x[1] in flag_fid_list)
                ]

                # サークル情報
                circle_fid_list = find_circle_fids[key_flag_fid]
                circle_geo_df = circle_gdf[
                    circle_gdf["FID"].apply(lambda x: x[1] in circle_fid_list)
                ]

                # 旗上げ情報とサークル情報をまとめる
                geo_df = pd.concat([flag_geo_df, circle_geo_df])
                geometries.append(geo_df)

            return geometries

        except Exception as e:
            raise Exception(f"[create_flag_grp_data]: {str(e)}")

    def make_result(self, geometries):
        """
        旗上げグループの外接矩形のジオメトリとクリッピング範囲のリストを作成する。

        :param geometries: 旗上げグループのGeoDataFrameのリスト。
        :type geometries: list[geopandas.GeoDataFrame]

        :return: 旗上げグループの外接矩形のクリッピング範囲リストと外接矩形のジオメトリリスト。
        :rtype: tuple(list[tuple(tuple(float, float), tuple(float, float))], list[shapely.geometry.Polygon])

        :raises Exception:
            外接矩形の作成中にエラーが発生した場合に例外をスローし、処理が失敗する。
        """
        try:
            clip_areas = []
            result_geometry_list = []
            for geometry in geometries:
                min_x, min_y, max_x, max_y = geometry["geometry"].total_bounds
                clip_areas.append(
                    ((float(min_x), float(min_y)), (float(max_x), float(max_y)))
                )

                # polygon作成
                polygon = Polygon(
                    [
                        (min_x, min_y),
                        (min_x, max_y),
                        (max_x, max_y),
                        (max_x, min_y),
                        (min_x, min_y),
                    ]
                )
                result_geometry_list.append(polygon)
            return clip_areas, result_geometry_list

        except Exception as e:
            raise Exception(f"[make_result]: {str(e)}")

    def prepare_main_process(self, params, field_set_file):
        """
        インプットデータの整形を行う。

        :param params: 属性の値とプロパティの設定値を格納した辞書。
        :type params: dict
        :param field_set_file: 入力としてのCSV形式のFieldSetFileのバイトデータ
        :type field_set_file: bytes

        :return: 旗上げ情報のGeoDataFrame、サークル情報のGeoDataFrame、グループ情報のIDリスト、ペアリング情報のIDリスト。
        :rtype: tuple(geopandas.GeoDataFrame, geopandas.GeoDataFrame,
                      list[tuple(float, list[float])], list[tuple(float)])

        :raises Exception:
            インプットデータの整形中にエラーが発生した場合に例外をスローし、処理が失敗する。
        """
        try:
            # CSV形式のFieldSetFileをDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(field_set_file)

            # 旗上げのGeoDataFrame
            flag_line_gdf = self.create_gdf(dataframe, params["flag_line"], params)

            # サークルのGeoDataFrame
            circle_gdf = self.create_gdf(dataframe, params["circle"], params)

            # グループ情報
            flag_grp_dwh_val = []

            # flag_grp_dwhに一致するデータがない場合は空
            flag_grp_dwh = f'{params["flag_line"]}/{params["flag_line_grp"]}'
            if not dataframe.loc[dataframe["Dwh"] == flag_grp_dwh].empty:
                flag_grp_dwh_val = dataframe.loc[
                    dataframe["Dwh"] == flag_grp_dwh, "Value"
                ].values[0]

            # ペアリング情報
            flag_pair_dwh = f'{params["flag_line"]}/{params["pair"]}'
            if (dataframe["Dwh"] == flag_pair_dwh).any():
                flag_pair_dwh_val = dataframe.loc[
                    dataframe["Dwh"] == flag_pair_dwh, "Value"
                ].values[0]
            else:
                raise ValueError(f"{flag_pair_dwh} does not exist.")

            return flag_line_gdf, circle_gdf, flag_grp_dwh_val, flag_pair_dwh_val

        except Exception as e:
            raise Exception(f"[prepare_main_process]: {str(e)}")

    def make_transform_result(self, params, clip_areas, result_geometry_list):
        """
        最終出力を作成する。

        :param params: 属性の値とプロパティの設定値を格納した辞書。
        :type params: dict
        :param clip_areas: 旗上げグループの外接矩形のクリッピング範囲リスト。
        :type clip_areas: list[tuple(tuple(float, float), tuple(float, float))]
        :param result_geometry_list: 旗上げグループの外接矩形のジオメトリリスト。
        :type result_geometry_list: list[shapely.geometry.Polygon]

        :return: 属性の値の辞書と、旗上げグループの外接矩形のFieldSetFile。
        :rtype: tuple(dict, str)

        :raises Exception:
            最終出力の作成中にエラーが発生した場合に例外をスローし、処理が失敗する。
        """
        try:
            # valueをシリアライズしてbase64でエンコード
            serialized_value = pickle.dumps(result_geometry_list)
            base64_serialized_value = base64.b64encode(serialized_value).decode("utf-8")

            # fidsの作成
            fids = [(str(i), float(i)) for i in range(len(result_geometry_list))]
            
            # FIDをシリアライズしてbase64でエンコード
            serialized_fid_value = pickle.dumps(fids)
            base64_serialized_fid_value = base64.b64encode(serialized_fid_value).decode(
                "utf-8"
            )

            # FieldSetFileのValue
            field_value_list = [base64_serialized_value, base64_serialized_fid_value]

            # FieldSetFileの中身を準備
            field_type_list = ["geometry", "string"]
            dwh_file_name_list = [f'{params["dst"]}/geometry', f'{params["dst"]}/FID']

            # FieldSetFile化
            out_fsf = NSP.get_field_set_file(
                field_type_list, field_value_list, dwh_file_name_list
            )

            attribute = {
                "ClipAreas": str(clip_areas),
                "MaxClipAreas": str(len(clip_areas)),
            }

            return attribute, out_fsf

        except Exception as e:
            raise Exception(f"[make_transform_result]: {str(e)}")

    def main_process(self, params, field_set_file):
        """
        メインプロセス。
        インプットデータの整形、メイン処理、返却データの作成を行う。

        :param params: 属性の値とプロパティの設定値を格納した辞書。
        :type params: dict
        :param field_set_file: 入力としてのCSV形式のFieldSetFileのバイトデータ。
        :type field_set_file: bytes

        :return: 旗上げグループの外接矩形のクリッピング範囲リストと、旗上げグループの外接矩形のGeoDataFrame。
        :rtype: tuple(list[tuple(tuple(float, float), tuple(float, float))], list[shapely.geometry.Polygon])

        :raises Exception:
            メインプロセス中にエラーが発生した場合に例外をスローし、処理が失敗する。
        """
        try:

            # インプットデータの整形
            flag_line_gdf, circle_gdf, flag_grp_dwh_val, flag_pair_dwh_val = (
                self.prepare_main_process(params, field_set_file)
            )

            # ペアリング情報から旗上げ線のFIDを取得
            key_flag_fids = list(set(fidx for fidx, _ in flag_pair_dwh_val))

            # 旗上げ情報グループ作成の下準備
            # 関連するサークル情報と旗上げ情報の抽出・整備
            find_flag_fids, find_circle_fids = self.prepare_for_create_flag_grp_data(
                key_flag_fids, flag_grp_dwh_val, flag_pair_dwh_val
            )

            # 旗上げグループ作成
            # 関連するサークル情報と旗上げ情報を結合
            geometries = self.create_flag_grp_data(
                key_flag_fids,
                find_flag_fids,
                find_circle_fids,
                flag_line_gdf,
                circle_gdf,
            )

            # 最終出力データを作成
            clip_areas, result_geometry_list = self.make_result(geometries)

            return clip_areas, result_geometry_list

        except Exception as e:
            raise Exception(f"[main_process]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: 入力としてのCSV形式のFieldSetFileのバイトデータ
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
            # Attributeの値とプロパティの値の辞書
            params = self.get_params(attribute, properties)

            # メインプロセス実行
            clip_areas, result_geometry_list = self.main_process(params, byte_data)

            # 最終出力作成
            attribute, out_fsf = self.make_transform_result(
                params, clip_areas, result_geometry_list
            )

            return out_fsf, attribute

        except Exception as e:
            raise Exception(f"[transform]: {str(e)}")
