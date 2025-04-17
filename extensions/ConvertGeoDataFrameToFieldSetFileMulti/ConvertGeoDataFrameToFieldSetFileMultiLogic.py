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
from io import StringIO
import pickle
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")

import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
from cad.common import cad_utils as CU


class ConvertGeoDataFrameToFieldSetFileMultiLogic:

    def __init__(self):
        pass

    def get_params(self, properties, attribute):
        """
        プロパティの値を取得し、辞書にして返す。

        :param properties: プロパティ情報を格納した辞書。
        :type properties: dict
        :param attribute: 属性情報を格納した辞書。
        :type attribute: dict

        :return: プロパティの値を格納した辞書。
        :rtype: dict

        :raises Exception: プロパティの取得中にエラーが発生した場合に例外をスローする。
        """
        try:
            all_params = {}

            # プロパティ値（Data Definition Delimiter）取得
            all_params["data_definition_delimiter"] = properties["DATA_DEFINITION_DELIMITER"]

            # 属性値（DataDefinition）取得
            all_params["data_definition"] = attribute.get("DataDefinition", None)
            if all_params["data_definition"] is None:
                # 属性にDataDefinitionがない場合例外を発生させる
                raise Exception("'DataDefinition' is not defined.")

            return all_params

        except Exception as e:
            raise Exception(f"[get_params Exception]: {str(e)}")

    def convert_geodataframe_to_field_set_file(self, geo_df, all_params):
        """
        GeoDataFrameから、レイヤごと・ジオメトリタイプごとに別レコードを成すFieldSetFileを生成する。

        :param geo_df: 入力データのGeoDataFrame。
        :type geo_df: geopandas.GeoDataFrame
        :param all_params: プロパティと属性情報を格納した辞書。
        :type all_params: dict

        :return: FieldSetFile形式で分割した結果の文字列。
        :rtype: str

        :raises Exception: ジオメトリデータが存在しない場合や、処理エラーが発生した場合に例外をスローする。
        """
        try:
            # data_definition_streamからフィールド情報を取得
            geometry_type_list, _, _, _, attribute_name_list, _, _, all_attribute_name_list, *_ = NSP.get_data_definition_index(
                StringIO(all_params["data_definition"]),
                data_definition_delimiter=all_params["data_definition_delimiter"],
            )

            # ジオメトリ情報のカラムのみ取得
            __geometry_type_list__ = ["+1", "+2", "+3"]
            geometry_type_bool_array = np.array(
                [
                    geometry_type_list[i] in __geometry_type_list__
                    for i in range(len(geometry_type_list))
                ],
                dtype=np.bool_,
            )
            # ジオメトリデータがない場合は例外を発生させる
            if np.all(geometry_type_bool_array):
                pass
            else:
                raise Exception("Geometry data does not exists.")

            # GeoDataFrameからFieldSetFileを生成
            field_set_file = CU.geodataframe_to_fieldsetfile(geo_df, all_attribute_name_list, layer_column="layer")

            return field_set_file

        except Exception as e:
            raise Exception(f"[convert_geodataframe_to_field_set_file Exception]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        GeoDataFrameから、レイヤごと・ジオメトリタイプごとに別レコードを成すFieldSetFileを生成し、結果として返す。

        :param byte_data: バイナリ形式のGeoDataFrame。
        :type byte_data: bytes
        :param attribute: 属性情報を格納した辞書。
        :type attribute: dict
        :param properties: プロパティ情報を格納した辞書。
        :type properties: dict

        :return: GeoDataFrameをレイヤごと・ジオメトリタイプごとに分割したFieldSetFileと属性情報のタプル。
        :rtype: tuple(str, dict)

        :raises Exception: FieldSetFileの生成中にエラーが発生した場合に例外をスローする。
        """
        try:
            # プロパティ値、属性値を取得
            all_params = self.get_params(properties, attribute)

            # FlowFileのコンテンツをデシリアライズしてGeoDataFrameを取得
            geodataframe = pickle.loads(byte_data)

            # GeoDataFrameからFieldSetFileを生成
            field_set_file = self.convert_geodataframe_to_field_set_file(geodataframe, all_params)

            return field_set_file, attribute

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")
