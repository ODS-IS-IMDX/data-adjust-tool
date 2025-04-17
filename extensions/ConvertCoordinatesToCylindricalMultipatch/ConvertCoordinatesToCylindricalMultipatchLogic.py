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
# 共通のIDを所持しているgeometryを1つにまとめた"マルチパッチ"を作成するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import cad.common.cad_utils as CU

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")


class ConvertCoordinatesToCylindricalMultipatchLogic:
    def __init__(self, **kwargs):
        pass

    def get_fsf_values(self, byte_data, circle_radius_dwh_name):
        """
        FieldSetFileのValue列の値を、デコード、デシリアライズしradius_list, coordinates_arrayにする。

        :param byte_data: シリアライズされたFieldSetFile
        :type byte_data: bytes
        :param circle_radius_dwh_name: 円筒の半径のDwhファイル名
        :type circle_radius_dwh_name: str

        :return: 地物IDと半径の値が入ったlist, 座標配列
        :rtype: tuple[list, ndarray]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # CSV形式のFieldSetFileをDataFrameに変換
            field_set_file_data_frame = CU.field_set_file_to_dataframe(byte_data)

            # DataFrameからValue列、Dwh列をリスト化。
            value_list = field_set_file_data_frame["Value"].tolist()
            dwh_list = field_set_file_data_frame["Dwh"].tolist()

            # リストからプロパティで入力した、円筒の半径のDwh名のインデックスを抽出。
            circle_radius_dwh_name_index = dwh_list.index(circle_radius_dwh_name)

            # INPUTのFieldSetFileは半径と座標の2つの行のみであることが前提。半径の行でない方が座標の行として取得
            geometry_index = None
            for index in range(len(dwh_list)):
                if index != circle_radius_dwh_name_index:
                    geometry_index = index

            # Value_listの各要素を該当のインデックスを用いて行を指定して抽出
            radius_list = value_list[circle_radius_dwh_name_index]
            coordinates_array = value_list[geometry_index]

            return radius_list, coordinates_array
        except Exception as e:
            raise Exception(f"[get_fsf_values Exception]: {str(e)}")

    def check_nan_values(self, array):
        """
        np.nanが存在するかを確認する関数。存在する場合はErrorを返す。
        ここでインデックスと属性の数が揃っているかを確認する。（インデックスにIDを用いる）

        :param array: 配列
        :type array: numpy.ndarray

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # arrayの各要素を見て、nanかどうかを判定する。nanの時その要素に対してTrueが付与される。
            nan_target_bool_array = np.isnan(array)

            # nan_target_bool_arrayに1つでもnanが含まれていればraise ValueErrorが処理される。
            if np.any(nan_target_bool_array):
                raise ValueError
        except Exception as e:
            raise Exception(f"[check_nan_values Exception]: {str(e)}")

    def is_LineStringZ_valid(self, coordinates_array):
        """
        LineStringZ（2点を用いて直線の情報となったもの）として成り立っているかを確認する関数。
        ➡coordinates_arrayの要素数は{ID, x成分, y成分, z成分}の4つ。0番目がIDとなる。
        IDが等しい1つものをgeometry1つとし合体したものをマルチパッチと呼ぶが、
        まず線を構成するためには点が2つ必要なので、共通のIDの数が2以上であることを確認する。

        :param coordinates_array: 座標配列
        :type coordinates_array: numpy.ndarray

        :return: 出現順に直したidの配列
        :rtype: numpy.ndarray

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # uniqueなIDを取得する。
            # id_unique_array   : 各点のIDを取得（このとき内部でソートされる）
            # id_indexes_array  : 元の配列で各点（のID）が初めて出てくる番号
            # id_counts_array   : 共通のIDの個数（点のIDが共通のもので線を結ぶため）
            id_unique_array, id_indexes_array, id_counts_array = np.unique(
                coordinates_array[:, 0], return_index=True, return_counts=True
            )

            # 元のnumpy配列の出現順に戻す。
            id_unique_array = id_unique_array[id_indexes_array.argsort()]
            id_counts_array = id_counts_array[id_counts_array.argsort()]

            # 構成点が1つしかない地物が存在するかチェック
            linestring_judge_bool_array = id_counts_array == 1

            if np.any(linestring_judge_bool_array):
                # 構成点が1つしかない地物が存在した場合、エラーを返す。
                raise ValueError
            else:
                # 構成点がすべて2つ以上存在する場合はid_unique_arrayを返す。
                return id_unique_array
        except Exception as e:
            raise Exception(f"[is_LineStringZ_valid Exception]: {str(e)}")

    def convert_np_float64_from_list(self, list):
        """
        listをfloat型の配列に変換する関数

        :param list: 加工するデータ
        :type list: list

        :return: float64型の配列
        :rtype: numpy.ndarray

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # listをfloat型の配列に変換
            array = np.array(list, dtype=np.float64)

            # nanがあればValueErrorを返す
            self.check_nan_values(array)

            # なければそのままarrayを返す
            return array
        except Exception as e:
            raise Exception(f"[convert_np_float64_from_list Exception]: {str(e)}")

    def get_coordinates_by_id(self, id_unique_array, coordinates_array):
        """
        geometryのID1つごとのインデックス取得

        :param id_unique_array: ユニークな地物ID
        :type id_unique_array: numpy.ndarray
        :param coordinates_array: 座標配列
        :type coordinates_array: numpy.ndarray

        :return: IDごとの構成点座標
        :rtype: dict

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # coordinates_arrayには{ID, x成分, y成分, z成分}の4つの要素があるので、IDを除いた座標の列を指定して取得する。
            id_coordinate_dict = {
                id_unique_array[i]: coordinates_array[
                    list(np.where(coordinates_array[:, 0] == id_unique_array[i])[0]),
                    1:4,
                ]
                for i in range(len(id_unique_array))
            }

            return id_coordinate_dict
        except Exception as e:
            raise Exception(f"[get_coordinates_by_id Exception]: {str(e)}")

    def generate_multi_patch(
        self,
        id_unique_array,
        id_coordinate_dict,
        radius_array,
        circle_divisions,
        start_multipatch_flag,
        end_multipatch_flag,
    ):
        """
        マルチパッチを生成する関数

        :param id_unique_array: ユニークな地物ID
        :type id_unique_array: numpy.ndarray
        :param id_coordinate_dict: IDごとの構成点座標
        :type id_coordinate_dict: dict
        :param radius_array: 半径のIDと値の入った配列
        :type radius_array: numpy.ndarray
        :param circle_divisions: 円筒の円を構成する点の数
        :type circle_divisions: int
        :param start_multipatch_flag: 円筒の入口にマルチパッチを作成するかのフラグ
        :type start_multipatch_flag: int
        :param end_multipatch_flag: 円筒の出口にマルチパッチを作成するかのフラグ
        :type end_multipatch_flag: int

        :return: マルチパッチ化の結果格納用リスト
        :rtype: list

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 結果格納用リスト
            multi_patch_list = []

            # geometryのIDでマルチパッチを作成
            for i in range(len(id_unique_array)):

                # geometryのIDが共通するものでマルチパッチを生成する。
                temporary_multipatch_array = NCP.get_cylindrical_multipatch_array(
                    id_coordinate_dict[id_unique_array[i]],
                    radius_array[i, 1],
                    circle_divisions,
                    start_multipatch_flag,
                    end_multipatch_flag,
                )

                # ポリゴンを閉じるために先頭の点を4点目に設定
                multipatch_array = np.concatenate(
                    [temporary_multipatch_array, temporary_multipatch_array[:, 0:1, :]],
                    axis=1,
                ).reshape((len(temporary_multipatch_array) * 4, 6))

                # マルチパッチジオメトリid生成
                multipatch_geometry_id_array = np.repeat(
                    np.arange(0, len(temporary_multipatch_array)), 4
                ).reshape(len(multipatch_array), 1)

                # 地物id生成
                feature_id_array = np.array(
                    len(multipatch_array) * [id_unique_array[i]]
                ).reshape(len(multipatch_array), 1)

                # id+xyz+multi_idの形に結合
                multipatch_array = np.concatenate(
                    [feature_id_array, multipatch_array, multipatch_geometry_id_array],
                    axis=1,
                )

                multi_patch_list.append(multipatch_array)

            return multi_patch_list
        except Exception as e:
            raise Exception(f"[generate_multi_patch Exception]: {str(e)}")

    def create_field_set_file_from_multi_patch(self, multi_patch_list, output_dwh_name):
        """
        output用のFieldSetFileを作る関数

        :param multi_patch_list: マルチパッチ化の結果格納用リスト
        :type multi_patch_list: list
        :param output_dwh_name: プロパティで入力したDwh名
        :type output_dwh_name: str

        :return: 次のプロセッサに送るためのCSV形式のデータ
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # マルチパッチを結合
            coordinates_array = np.concatenate(multi_patch_list, 0)

            # output_field_set_fileのvalueの型
            geometry_type = "geometry"

            # 出力FieldSetFile仕様にリスト化
            output_dwh_name_list = [output_dwh_name]
            geometry_type_list = [geometry_type]
            coordinates_array_list = [coordinates_array]

            # マルチパッチをFieldSetFileに加工
            output_field_set_file = (PBP.set_field_set_file)(
                output_dwh_name_list, geometry_type_list, coordinates_array_list
            )

            return output_field_set_file
        except Exception as e:
            raise Exception(
                f"[create_field_set_file_from_multi_patch Exception]: {str(e)}"
            )

    def __call__(self, byte_data, attributes, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: バイト型のFieldSetFile
        :type byte_data: bytes
        :param attributes: 属性
        :type attributes: dict
        :param properties: プロパティ
        :type properties: dict

        :return: 出力用のFieldSetFile
        :rtype: tuple[str, dict]

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # 作成するなら1そうでないなら0
            start_multipatch_flag = (
                1 if properties["START_MULTIPATCH_FLAG"] == "作成する" else 0
            )
            end_multipatch_flag = (
                1 if properties["END_MULTIPATCH_FLAG"] == "作成する" else 0
            )

            # 円筒の半径の値をstrからintに変換。
            circle_divisions = CU.get_number_from_string(properties["CIRCLE_DIVISIONS"], int)

            # FieldSetFileのValueの値を取得
            radius_list, coordinates_array = self.get_fsf_values(
                byte_data, properties["CIRCLE_RADIUS"]
            )

            # LineStringZ（2点を用いて直線となったもの）として成り立っているかを確認する
            # uniqueなidは元のnumpy配列の出現順に直しておく
            id_unique_array = self.is_LineStringZ_valid(coordinates_array)

            # マルチパッチを作成するために、半径をfloat型に変換
            radius_array = self.convert_np_float64_from_list(radius_list)

            # IDが紐づくジオメトリだけ抽出。IDの絞り込み。
            id_unique_array, radius_array = NSP.get_target_array(
                id_unique_array, radius_array
            )

            # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
            # geometryのID1つごとのインデックス取得（共通のIDを持つものをグループ化）
            id_coordinate_dict = self.get_coordinates_by_id(
                id_unique_array, coordinates_array
            )

            # geometryのID1つごとにマルチパッチを生成する。
            multi_patch_list = self.generate_multi_patch(
                id_unique_array,
                id_coordinate_dict,
                radius_array,
                circle_divisions,
                start_multipatch_flag,
                end_multipatch_flag,
            )

            # FieldSetFileに変換する。
            output_field_set_file = self.create_field_set_file_from_multi_patch(
                multi_patch_list, properties["OUTPUT_DWH_NAME"]
            )

            return output_field_set_file, attributes

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
