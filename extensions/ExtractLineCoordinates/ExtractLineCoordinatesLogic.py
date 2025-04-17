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

import pickle
import math
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import cad.common.cad_utils as CU
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")


class ExtractLineCoordinatesLogic:
    def __init__(self, **kwargs):
        pass

    def convert_line_coordinates_list(self, line, fid_list):
        """
        各LineStringの座標情報と角度情報を文字列形式に変換して、対応するFIDとともにリストとして出力する。

        :param line: LineStringの座標情報
        :type line: list[shapely.geometry.LineString]
        :param fid_list: FID
        :type fid_list: list[float]

        :return: FID、座標情報のリスト、角度情報のリスト
        :rtype: list[tuple(float, str, str)]
        """
        try:
            add_list = []
            fid_list_cnt = 0

            for geom_line in line:
                coords = list(geom_line.coords)
                if len(coords) != 2:
                    raise ValueError("LineString は2点（始点・終点）で構成される必要があります")

                # 座標情報（2点）
                point_list = [[int(p) for p in coords[0]], [int(p) for p in coords[1]]]

                # 角度計算（始点 → 終点）
                dx = coords[1][0] - coords[0][0]
                dy = coords[1][1] - coords[0][1]
                angle = math.degrees(math.atan2(dy, dx))  # ラジアンを度に変換
                angle = round(angle, 2)  # 小数点2桁に丸める

                add_list.append((fid_list[fid_list_cnt], str(pickle.dumps(point_list)), str(pickle.dumps(angle))))
                fid_list_cnt += 1

            return add_list

        except Exception as e:
            raise Exception(f"[convert_line_coordinates_list]: {str(e)}")


    def get_field_set_file(self, dataframe, geometry):
        """
        FieldSetFileのジオメトリ情報を取得する

        :param dataframe: ジオメトリ情報を持つFieldSetFileをDataFrameに変換したもの
        :type dataframe: pandas.DataFrame
        :param geometry: 対象の名前
        :type geometry: str

        :return: ジオメトリデータとFIDデータ
        :rtype: tuple(list[shapely.geometry.LineString], list[tuple(str, int)])

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """

        try:

            if not (dataframe["Dwh"] == f"{geometry}/geometry").any():
                raise ValueError(f"指定された {geometry} が存在しません。")

            # 処理対象のDataFrameを抽出
            dataframe = dataframe[dataframe['Dwh'].str.contains(geometry)]

            # DataFrameからジオメトリデータとFIDデータを抽出
            geometry_list = dataframe[dataframe["Dwh"] == f"{geometry}/geometry"].values[0][2]
            fid_list = dataframe[dataframe['Dwh'] == f"{geometry}/FID"]['Value'].values[0]

            return geometry_list, fid_list

        except Exception as e:
            raise Exception(f"[get_field_set_file]: {str(e)}")

    def extract_line_coordinates(self, byte_data, properties):
        """
        LineStringのジオメトリデータを抽出し、各LineStringの座標情報を文字列形式に変換して、
        対応するFIDとともにリストとして出力

        :param byte_data: FieldSetFileもしくはGeoDataFrameのバイトデータ
        :type byte_data: bytes
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: FIDと座標情報を紐づけた文字列のFieldSetFile
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティに設定された値を取得
            geometry_name = properties['GEOMETRY_NAME']
            suffix = properties['SUFFIX']
            input_data_type = properties['INPUT_DATA_TYPE']

            if input_data_type == "Geo Data Frame":
                # バイトデータからGeoDataFrameを復元する
                dataframe = pickle.loads(byte_data)
                geometry_list = dataframe["geometry"].tolist()
                fid_list = [(item, float(idx)) for idx, item in enumerate(dataframe["FID"].tolist())]
            else:
                # バイナリ形式のFieldSetFileをDataFrameに変換
                dataframe = CU.field_set_file_to_dataframe(byte_data)
                # FieldSetFileをデシリアライズし、座標情報、FIDのリストを取得
                geometry_list, fid_list = self.get_field_set_file(dataframe, geometry_name)

            org_fid_list = fid_list
            fid_list = [float(idx) for _, idx in fid_list]
            # 座標情報を文字列に変換し、FIDと紐づける
            extract_list = self.convert_line_coordinates_list(geometry_list, fid_list)

            # FieldSetFileに格納する値をシリアライズ＆エンコード
            base64_serialized_results = CU.encode_value(extract_list)
            base64_serialized_fid_results = CU.encode_value(org_fid_list)

            # FieldSetFileに格納するデータを作成
            field_type_list = ['string', 'string']
            field_value_list = [base64_serialized_fid_results, base64_serialized_results]
            dwh_file_name_list = [geometry_name + '/FID', geometry_name + '/' + suffix]

            # FieldSetFile化
            field_set_file = NSP.get_field_set_file(field_type_list, field_value_list, dwh_file_name_list)

            return field_set_file

        except Exception as e:
            raise Exception(f"[extract_line_coordinates]: {str(e)}")

    def __call__(self, byte_data, attributes, properties):
        """
        NiFiプロセッサのエントリーポイント関数。FlowFileからジオメトリデータを取得し、
        メイン処理関数に渡して処理を実行する。

        :param byte_data: 入力となるFieldSetFileのバイトデータ
        :type byte_data: bytes
        :param attributes: FlowFileの属性情報を格納する辞書
        :type attributes: dict
        :param properties: 処理に必要なプロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のFieldSetFileと更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # メインの処理関数に渡して実行
            field_set_file = self.extract_line_coordinates(byte_data, properties)
            return field_set_file, attributes

        except Exception as e:
            # 例外発生時、エラーメッセージを記録し、失敗を返す
            raise Exception(f"[transform]:{e}")
