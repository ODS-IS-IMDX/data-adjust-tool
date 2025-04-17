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

import io
from importlib import import_module


# 外部ライブラリの動的インポート
rasterio = import_module("rasterio")
np = import_module("numpy")


def get_geotiff_information(geotiff_value):
    # -------------------------------------------------------------------------------------------------
    # 概要　 : TIFFデータから標高情報を取得する関数
    # 引数　 : geotiff_value - プロセッサの設定値が格納されているデータ
    # 戻り値 : height_array  - [画像の高さのpixel数]>[画像の幅のpixel数]　要素はZ値を表すfloat64
    # 　　　 : tfw_file_list - tfw情報 List [x_pixelの長さ, 0.0, 0.0, y_pixelの長さ, 左上のpixelの中心X座標, 左上のpixelの中心Y座標]
    # 　　　 : raster_x_size - 幅のpixel数
    # 　　　 : raster_y_size - 高さのpixel数
    # -------------------------------------------------------------------------------------------------

    # GeoTIFF バイナリデータを BytesIO オブジェクトとして読み込む
    geotiff_value = io.BytesIO(geotiff_value)

    # Rasterio を使って GeoTIFF を開く
    with rasterio.open(geotiff_value) as src:

        # バンド 1 からデータを読み込む
        height_array = src.read(1)

        # データの中で -9999 の値を 0 に置き換える（無効値を処理）
        height_array[np.where(height_array == -9999)] = 0

        # ワールドファイルの内容取得
        tfw_file_list = [
            # x_pixelの長さ
            src.transform[0],
            # x_pixelの回転
            src.transform[1],
            # y_pixelの回転
            src.transform[3],
            # y_pixelの長さ
            src.transform[4],
            # 左上のpixelの中心X座標
            src.transform[2],
            # 左上のpixelの中心Y座標
            src.transform[5]
        ]

        # ピクセルの数取得
        raster_x_size = src.width
        raster_y_size = src.height

    return height_array, tfw_file_list, raster_x_size, raster_y_size


def get_grid_index(x_array, y_array, tfw_list):
    # -------------------------------------------------------------------------------------------------
    # 概要　 : 座標がどのxy平面上のグリッドに存在するかインデックスを取得する関数
    # 引数　 : x_array  - x座標 1次元配列
    # 　　　 : y_array  - y座標 1次元配列
    # 　　　 : tfw_list - tfw情報 list
    # 戻り値 : x_index  - x_index 1次元配列 np.int64
    # 　　　 : y_index  - y_index 1次元配列 np.int64
    # -------------------------------------------------------------------------------------------------

    # 各座標から左上ピクセルの中心 X 座標を引き、x ピクセルのサイズで割ることで、グリッドのセル単位のインデックスを計算
    x_index = np.array(
        np.floor((x_array - tfw_list[4]) / tfw_list[0]), dtype=np.int64)
    y_index = np.array(
        np.floor((y_array - tfw_list[5]) / tfw_list[3]), dtype=np.int64)

    return x_index, y_index
