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
LineString = getattr(import_module("shapely.geometry"), "LineString")
box = getattr(import_module("shapely.geometry"), "box")

import cad.common.cad_utils as CU


class CreateClipRangePolygonLogic:

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
            all_params["data_name"] = properties["DATA_NAME"]
            all_params["suffix"] = properties["SUFFIX"]
            all_params["polygon_name"] = properties["POLYGON_NAME"]

            return all_params

        except Exception as e:
            raise Exception(f"[get_params Exception]: {str(e)}")

    def get_geometry_list(self, df, all_params):
        """
        DataFrame形式の入力データから、操作対象であるLineStringのジオメトリリストを抽出する。

        :param df: 入力データのFieldSetFileをDataFrameに変換したもの。
        :type df: pandas.DataFrame
        :param all_params: プロパティの設定値を格納した辞書。
        :type all_params: dict

        :return: 入力データから取得したジオメトリ（LineString）リスト。
        :rtype: list[shapely.geometry.LineString]

        :raises Exception:
            プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 操作対象データのジオメトリ情報・FID情報が入力データに存在しているかを確認
            if not (
                df["Dwh"] == f"{all_params['data_name']}/{all_params['suffix']}"
            ).any():
                raise Exception(
                    f"{all_params['data_name']}/{all_params['suffix']} does not exist."
                )

            # ジオメトリデータを取得
            geometry_list = df[
                df["Dwh"] == f"{all_params['data_name']}/{all_params['suffix']}"
            ]["Value"].to_list()[0]

            # 取得したジオメトリがLineStringかどうか確認
            if not all(isinstance(geom, LineString) for geom in geometry_list):
                raise Exception(f"Geometry is not LineString.")

            return geometry_list

        except Exception as e:
            raise Exception(f"[get_geometry_list Exception]: {str(e)}")

    def create_polygon_of_bounding_box(self, linestring_list):
        """
        線分を囲む最小のバウンディングボックス（外接矩形）を生成する。

        :param linestring_list: 入力データから取得したジオメトリ（LineString）リスト。
        :type linestring_list: list[shapely.geometry.LineString]

        :return: 線分の外接矩形（Polygon）のリスト。
        :rtype: list[shapely.geometry.Polygon]

        :raises Exception:
            プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            polygon_list = []  # 外接矩形ポリゴン格納先

            # DataFrameのレコード数分（LineStringの数分）ループ
            for line in linestring_list:
                # 線分を囲む最小のバウンディングボックスの座標を取得
                min_x, min_y, max_x, max_y = line.bounds

                # バウンディングボックスの座標情報から矩形を生成
                polygon = box(min_x, min_y, max_x, max_y)

                # ポリゴンリストに追加
                polygon_list.append(polygon)

            return polygon_list

        except Exception as e:
            raise Exception(f"[create_polygon_of_bounding_box Exception]: {str(e)}")

    def create_df_for_fsf(self, polygon_list, all_params):
        """
        処理結果のDataFrameからFieldSetFile用DataFrameを生成する。

        :param polygon_list: 線分の外接矩形（Polygon）のリスト。
        :type polygon_list: list[shapely.geometry.Polygon]
        :param all_params: プロパティの設定値を格納した辞書。
        :type all_params: dict

        :return: 処理結果を持つFieldSetFile生成用DataFrame。
        :rtype: pandas.DataFrame

        :raises Exception:
            プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            # FIDデータを生成
            fid_findex_list = [
                (str(fidx_int), float(fidx_int))
                for fidx_int in range(len(polygon_list))
            ]

            # FieldSetFile生成用DataFrame作成
            df_for_fsf = pd.DataFrame(
                {
                    "Dwh": [
                        f"{all_params['polygon_name']}/geometry",
                        f"{all_params['polygon_name']}/FID",
                    ],
                    "Type": ["geometry", "string"],
                    "Value": [polygon_list, fid_findex_list],
                }
            )

            return df_for_fsf

        except Exception as e:
            raise Exception(f"[create_df_for_fsf Exception]: {str(e)}")

    def create_clip_range_polygon(self, input_fsf, all_params):
        """
        入力データのFieldSetFileから、線分データを抽出し、
        外接矩形となるポリゴンの情報を持つFieldSetFileを生成する。

        :param input_fsf: バイナリ形式のFieldSetFile。
        :type input_fsf: bytes
        :param all_params: プロパティの値を格納した辞書。
        :type all_params: dist

        :return: 処理結果のFieldSetFile。
        :rtype: str

        :raises Exception:
            プロパティ取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 入力データのFieldSetFileをDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(input_fsf)

            # 入力データからジオメトリデータリスト（LineString）を取得
            linestring_list = self.get_geometry_list(dataframe, all_params)

            # ラインからポリゴンを生成し、DataFrameに格納
            polygon_list = self.create_polygon_of_bounding_box(linestring_list)

            # 処理結果のDataFrameをFieldSetFileに変換
            df_for_fsf = self.create_df_for_fsf(polygon_list, all_params)

            # 抽出したDataFrameからFieldSetFileを生成
            field_set_file = CU.dataframe_to_field_set_file(df_for_fsf)

            return field_set_file

        except Exception as e:
            raise Exception(f"[create_clip_range_polygon Exception]: {str(e)}")

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
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティ値取得
            all_params = self.get_params(properties)

            # メイン処理を実行
            results = self.create_clip_range_polygon(byte_data, all_params)

            return results, attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
