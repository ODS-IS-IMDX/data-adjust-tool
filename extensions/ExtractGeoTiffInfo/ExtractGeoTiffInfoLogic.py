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
# 読み込まれたGeotiffからワールドファイル情報を取得する
# --------------------------------------------------------------------------------------------
# Python標準ライブラリ
import pickle
import io
from importlib import import_module

# 外部ライブラリの動的インポート
Image = import_module("PIL.Image")
np = import_module("numpy")
MemoryFile = import_module('rasterio.io').MemoryFile

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class ExtractGeoTiffInfoLogic(FlexibleRasterVectorLogic):
    def __init__(self):
        pass

    def __call__(self, flow_data, attributes, properties):
        """
        入力画像をチェックし、ワールドファイル情報を取得します。
        入力画像がない場合、ワールドファイル情報が取得できない場合に例外を発生させます。

        Parameters
        ----------
        flow_data : bytes
            画像データのバイト列です。
        attributes : dict
            画像の属性情報を含む辞書です。
        properties : なし
        
        Returns
        -------
        tuple
            - flow_data : bytes
                入力画像のバイト列です。
            - attributes : dict
                更新された属性情報を含む辞書です。ワールドファイル情報が追加されます。

        Raises
        ------
        Exception
            - 入力画像が設定されていない場合に例外を発生させます。
            - 入力画像がワールドファイル情報を所持していない場合に例外を発生させます。
        """
        if flow_data is None:
            raise Exception("入力データが設定されていません")

        try:
            content = flow_data['content']
            with MemoryFile(content) as memfile:
                with memfile.open() as src:
                    new_byte_data = src.transform

            # ワールドファイル情報をattributesへ格納
            attributes['UnitXPerPixel'] = str(new_byte_data[0])
            attributes['UnitYPerPixel'] = str(new_byte_data[4])
            attributes['UpperLeftX'] = str(new_byte_data[3])
            attributes['UpperLeftY'] = str(new_byte_data[1])
            attributes['PixelToCoordinateX'] = str(new_byte_data[2])
            attributes['PixelToCoordinateY'] = str(new_byte_data[5])
        except Exception as e:
            raise Exception('GeoTiffファイルからワールドファイル情報を取得するのに失敗しました', e)

        try:
            img = Image.open(io.BytesIO(content))
            img_numpy = np.array(img)
            new_content = pickle.dumps(img_numpy)
            flow_data['content'] = new_content
        except Exception as e:
            raise Exception('GeoTiffファイルからワールドファイル情報を取得するのに失敗しました', e)

        return flow_data, attributes
