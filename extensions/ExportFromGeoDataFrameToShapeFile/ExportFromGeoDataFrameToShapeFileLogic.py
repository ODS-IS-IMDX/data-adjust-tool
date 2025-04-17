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

import os
import pickle

class ExportFromGeoDataFrameToShapeFileLogic:

    def __init__(self, **kwargs):
        pass

    def __call__(self, byte_data, attributes, properties):
        """
        NiFiのFlowFileからGeoDataFrameをデシリアライズし、指定のジオメトリタイプを抽出して
        Shapefileとして保存します。

        :param byte_data: NiFi FlowFileのバイトデータで、ジオメトリ情報を含むGeoDataFrameをデシリアライズする対象。
        :type byte_data: bytes
        :param attributes: FlowFileの属性情報を格納する辞書。ファイル名やレイヤ名などを含む。
        :type attributes: dict
        :param properties: プロパティ情報を含む辞書。出力ディレクトリなどの設定が含まれる。
        :type properties: dict

        :return: 処理したバイトデータと更新された属性情報を含むタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: Shapefileの出力中やディレクトリ作成中にエラーが発生した場合、例外をスローする。
        """
        try:
            output_directory = properties['OUTPUT_DIRECTORY']

            # デシリアライズ
            geodataframe = pickle.loads(byte_data)

            # ジオメトリタイプを取得
            geometry_type = geodataframe.geometry.geom_type.unique()[0]

            target_geodataframe = geodataframe[
                geodataframe.geometry.geom_type == geometry_type
            ]

            # ファイル名から拡張子を除いた名前を取得
            filename = attributes.get('filename').split('.')[0]

            layer_name = attributes.get('LayerName')

            # output_directoryの下にfilenameの名前のディレクトリを作成
            file_directory = os.path.join(output_directory, filename)
            os.makedirs(file_directory, exist_ok=True)

            # ファイル名にジオメトリタイプを結合
            shapefile_name = f"{filename}_{geometry_type.lower()}_{layer_name}.shp"

            # 抽出したジオメトリタイプのGeoDataFrameをShapefileとして保存
            target_geodataframe.to_file(os.path.join(file_directory, shapefile_name), driver='ESRI Shapefile')

            return byte_data, attributes

        except Exception as e:
            raise Exception(f"[transform_Exception]: {str(e)}")
