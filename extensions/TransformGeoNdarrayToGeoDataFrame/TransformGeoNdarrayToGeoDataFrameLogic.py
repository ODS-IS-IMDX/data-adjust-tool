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
import cad.common.cad_utils as CU


class TransformGeoNdarrayToGeoDataFrameLogic:

    def __init__(self, **kwargs):
        pass

    def __call__(self, byte_data, attribute, properties):
        """
        NiFiのFlowFileからGeoNdarrayデータをデシリアライズし、GeoDataFrameに変換して
        CRS（座標参照系）を設定したうえでシリアライズされたGeoDataFrameを返します。

        :param byte_data: NiFi FlowFileのバイトデータで、ジオメトリ情報を含むGeoNdarrayをデシリアライズする対象。
        :type byte_data: bytes
        :param attribute: FlowFileの属性情報を格納する辞書。CRS（座標参照系）などの設定が含まれる。
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書。処理中には使用しないが、拡張性のために保持。
        :type properties: dict

        :return: シリアライズされたGeoDataFrameと更新された属性情報を含むタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: GeoDataFrame変換中、またはCRS設定時にエラーが発生した場合、例外をスローする。
        """
        try:
            # CRSを取得
            crs = CU.get_number_from_string(attribute["crs"], int)

            # GeoNdarrayからGeoDataFrameを生成
            geo_ndarray = pickle.loads(byte_data)
            geodataframe = CU.geo_ndarray_to_geodataframe(geo_ndarray)

            # CRSを設定
            geodataframe.set_crs(crs, allow_override=True)

            # シリアライズ
            output_contents = pickle.dumps(geodataframe)

            return output_contents, attribute

        except Exception as e:
            raise Exception(f"[transform_Exception]: {str(e)}")