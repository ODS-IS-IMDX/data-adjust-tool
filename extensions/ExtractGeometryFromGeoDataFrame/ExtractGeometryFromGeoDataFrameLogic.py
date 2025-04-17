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
import pickle
from importlib import import_module

# 外部ライブラリの動的インポート
gpd = import_module("geopandas")
shapely = import_module("shapely")


class ExtractGeometryFromGeoDataFrameLogic:
    def __call__(self, serialized_contents, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータ（シリアライズされたGeoDataFrame）とプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param serialized_contents: 入力としてのシリアライズされたGeoDataFrameのバイトデータ。
        :type serialized_contents: bytes
        :param attribute: FlowFileの属性情報を格納する辞書。
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書。
        :type properties: dict

        :return: 処理結果のFieldSetFileと更新された属性情報を含むタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: 指定されたレイヤが見つからない場合や、処理中にエラーが発生した場合に例外をスローする。
        """

        try:
            selected_layers = properties['LAYER_NAME']

            # 指定レイヤをカンマ区切りでリスト化
            selected_layer_list = [layer.strip() for layer in selected_layers.split(',')] if selected_layers else None
            # FlowFileのLayerName属性値を取得
            layer_name = attribute.get("LayerName", "")

            # レイヤ名が指定されていて、かつ、処理中のGeoDataFrameのレイヤ名が指定レイヤ名リスト内にない場合
            if selected_layer_list is not None and layer_name not in selected_layer_list:
                raise Exception(f'[selected_layer_list]:{selected_layer_list}:{layer_name}')

            # FlowFileをデシリアライズしDataFrameを取得
            geodataframe = pickle.loads(serialized_contents)

            # 対象ジオメトリタイプを取得
            geometry_types = geodataframe.geometry.geom_type
            unique_geom_types = geometry_types.unique()
            unique_geom_types.sort()
            geometry_index = int(attribute.get("copy.index", 0))
            target_geometry = unique_geom_types[geometry_index]

            # 対象ジオメトリタイプのGeoDataFrameを取得
            output_gdf = geodataframe[geodataframe.geometry.geom_type == target_geometry].reset_index(drop=True)

            output_contents = pickle.dumps(output_gdf)

            attribute = {'GeometryType': target_geometry}

            return output_contents, attribute

        except Exception as e:
            raise Exception(f'[transform_Exception]:{e}')
