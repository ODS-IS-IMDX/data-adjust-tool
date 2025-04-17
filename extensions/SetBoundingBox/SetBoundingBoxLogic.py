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
np = import_module("numpy")
gpd = import_module("geopandas")


class SetBoundingBoxLogic:

    def __call__(self, serialize_data, attribute, properties):
        """
        DXFデータを含むGeoDataFrameからバウンディングボックスを取得し、属性情報を更新する関数。

        :param serialize_data: GeoDataFrameのバイトデータ
        :type serialize_data: bytes

        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict

        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のGeoDataFrameと、更新された属性情報を含むタプル
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # 入力データの読み込み
            gdf = pickle.loads(serialize_data)

            if not gdf.empty:
                min_x, min_y, max_x, max_y = gdf.total_bounds
                bounding_box = ((float(min_x), float(min_y)), (float(max_x), float(max_y)))

                # 属性の作成
                attributes = {'BoundingBox': str(bounding_box)}

                return serialize_data, attributes

        except Exception as e:
            raise Exception(f'[__call__ Exception]:{e}')

