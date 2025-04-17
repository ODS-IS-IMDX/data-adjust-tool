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
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP

class ConvertFromGeoDataFrameToGeoNdarrayLogic:

    def __call__(self, serialize_dataframe, attribute, properties):
        """
        GeoDataFrameのシリアライズデータからGeoNdarrayを生成し、シリアライズ結果を返す。

        :param serialize_dataframe: シリアライズされたGeoDataFrameデータ。
        :type serialize_dataframe: bytes
        :param attribute: FlowFileの属性情報を格納する辞書。
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書。
        :type properties: dict

        :return: シリアライズされたGeoNdarrayデータと、更新された属性情報を含むタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:

            # デシリアライズ
            geodataframe = pickle.loads(serialize_dataframe)

            # ジオメトリリストを作成
            geometry_list = list(geodataframe.geometry)

            # ジオメトリリストをGeoNdarrayに変換
            geo_ndarray = NSP.get_geometries_points_numpy(geometry_list)

            # シリアライズ
            serialize_contents = pickle.dumps(geo_ndarray)

            return serialize_contents, attribute

        except Exception as e:
            raise Exception(f'[transform_Exception]:{e}')
