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
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
from cad.common import cad_utils as CU


class ConvertFromFieldSetFileToGeoNdarrayLogic:
    def __init__(self):
        pass

    def convert_geometry_list_to_geo_ndarray(self, field_set_file):
        """
        FieldSetFileが持つジオメトリデータをGeoNdarrayに変換する。

        :param field_set_file: 入力データのFieldSetFile。ジオメトリデータを1レコード持つことを想定。
        :type field_set_file: bytes

        :return: ジオメトリデータをGeoNdarrayに変換したもの。
        :rtype: numpy.ndarray

        :raises Exception: ジオメトリデータが存在しない場合や、処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # FieldSetFileをDataFrameに変換
            dataframe = CU.field_set_file_to_dataframe(field_set_file)

            if not (dataframe["Type"] == "geometry").any():
                # ジオメトリデータが無い場合は例外処理を発生させる
                raise f"Geometry data does not exist."

            # DataFrameから先頭のジオメトリデータを取得
            geometry_value_list = dataframe[dataframe["Type"] == "geometry"][
                "Value"
            ].reset_index(drop=True)[0]

            # ジオメトリデータをGeoNdarrayに変換
            geo_ndarray = NSP.get_geometries_points_numpy(geometry_value_list)

            return geo_ndarray

        except Exception as e:
            raise f"[ERROR convert_geometry_list_to_geo_ndarray]: {str(e)}"

    def __call__(self, byte_data, attribute, properties):
        """
        FieldSetFileからジオメトリデータを受け取り、GeoNdarrayに変換して
        シリアライズされたGeoNdarrayを返す。

        :param byte_data: バイナリ形式のFieldSetFile
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: シリアライズされたGeoNdarrayと、更新された属性情報を含むタプル
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # FieldSetFileのジオメトリデータをGeNdarrayに変換
            geo_ndarray = self.convert_geometry_list_to_geo_ndarray(byte_data)

            # シリアライズ
            output_contents = pickle.dumps(geo_ndarray)

            return output_contents, attribute

        except Exception as e:
            raise f"[ERROR __call__]: {str(e)}"
