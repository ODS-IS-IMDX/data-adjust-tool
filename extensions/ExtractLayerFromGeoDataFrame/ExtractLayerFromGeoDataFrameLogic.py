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

class ExtractLayerFromGeoDataFrameLogic:
    def __call__(self, serialized_contents, attribute, properties):
        """
        GeoDataFrameをレイヤごとに抽出する。

        :param serialized_contents: 入力としてのシリアライズされたGeoDataFrameのバイトデータ。
        :type serialized_contents: bytes
        :param attribute: FlowFileの属性情報を格納する辞書。
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書。
        :type properties: dict

        :return: レイヤごとのGeoDataFrameのシリアライズされたバイトデータと、更新された属性情報を含むタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: 指定されたレイヤが見つからない場合や、処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            gdf = pickle.loads(serialized_contents)

            # FlowFileの複製インデックス番号を取得
            layer_index = int(attribute.get('copy.index', 0))

            # レイヤごとのGeoDataFrameを取得
            layer_list = gdf['layer']
            unique_layer_list = layer_list.unique()
            layer_name = unique_layer_list[layer_index]
            sub_gdf = gdf[gdf['layer'] == layer_name]

            # ジオメトリタイプ数を取得
            geometry_types = sub_gdf.geometry.geom_type
            unique_geom_types = geometry_types.unique()
            max_geometry_index = len(unique_geom_types) - 1

            output_contents = pickle.dumps(sub_gdf)

            attribute = {
                'MaxGeometryIndex': str(max_geometry_index),
                'LayerName': layer_name
            }
            return output_contents, attribute

        except Exception as e:
            # エラー発生時のログ出力
            raise Exception(f'[transform_Exception]:{e}')
