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
import base64
import io
from collections import defaultdict
import pathlib

import datetime
import csv

from importlib import import_module

import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC

# 外部ライブラリの動的インポート
np = import_module("numpy")
jit = import_module("numba").jit
f8 = import_module("numba").f8
prange = import_module("numba").prange
void = import_module("numba").void
b1 = import_module("numba").b1
i8 = import_module("numba").i8
cross2d = import_module("numba.np.extensions").cross2d

pyproj = import_module("pyproj")
pd = import_module("pandas")
tky2jgd = import_module("tky2jgd")
osgeo = import_module("osgeo")
gdal = import_module("osgeo.gdal")
gdalconst = import_module("osgeo.gdalconst")


@jit(i8(i8), nopython=True, cache=True, nogil=True)
def get_max_tile_counts(zoom_level=26):
    # ------------------------------------------------------------------------------
    # 【空間ID】空間IDの計算式資料
    # ------------------------------------------------------------------------------
    # 経緯度→zfxyの変換に関して https://github.com/unvt/zfxy-spec/blob/main/README.md
    # zfxy→経緯度の変換に関して https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    # 経緯度の範囲は明確に どうやら-π＜θ＜π
    # 経度：東経が正、西経が負
    # 緯度：北緯が正、南緯が負
    # ------------------------------------------------------------------------------
    # 【空間ID】ズームレベルごとの1辺当たりのタイル枚数の取得
    # voxelの1辺あたり50cmに該当するzoomlevelは26
    return 2**zoom_level


@jit(i8(i8), nopython=True, cache=True, nogil=True)
def get_max_height(exponent=25):
    # 【空間ID】高さの最大値取得
    # 空間IDは2^25と定義している　33,554,432 地球１周よりも短いくらいの長さ
    return 2**exponent


@jit(i8[:](f8[:], i8), nopython=True, cache=True, nogil=True)
def convert_longitude2xid(longitude, max_tile):
    # 【空間ID】経度をXIDに変換する。
    # 引数1：longitudeの単位は度[degree]
    return np.floor(max_tile * ((longitude + 180) / 360)).astype(np.int64)


@jit(i8[:](f8[:], i8), nopython=True, cache=True, nogil=True)
def convert_latitude2yid(latitude, max_tile):
    # 【空間ID】緯度をYIDに変換する。
    # 引数1：latitudeの単位は度[degree]
    latitude_rad = latitude * np.pi / 180
    return np.floor(max_tile * (1 - np.log(np.tan(latitude_rad) + (1 / np.cos(latitude_rad))) / np.pi) / 2).astype(np.int64)


@jit(i8[:](f8[:], i8, i8), nopython=True, cache=True, nogil=True)
def convert_height2fid(height, max_tile, max_height):
    # 【空間ID】高さをFIDに変換する。
    # 引数1：高さの単位はメートル[m]
    return np.floor(max_tile * height / max_height).astype(np.int64)


@jit(f8[:](i8[:], i8), nopython=True, cache=True, nogil=True)
def convert_xid2longitude(x_index, max_tile):
    # 【空間ID】XIDを左上の経度（単位は度[degree]）に変換する
    return x_index / max_tile * 360.0 - 180.0


@jit(f8[:](i8[:], i8), nopython=True, cache=True, nogil=True)
def convert_yid2latitude(y_index, max_tile):
    # 【空間ID】YIDを左上の緯度（単位は度[degree]）に変換する。
    latitude_rad = np.arctan(np.sinh(np.pi * (1 - 2 * y_index / max_tile)))
    return np.degrees(latitude_rad)


@jit(f8[:](i8[:], i8, i8), nopython=True, cache=True, nogil=True)
def convert_fid2height(f_index, max_tile, max_height):
    # 【空間ID】FIDを左上の高さに変換する。
    return f_index * max_height / max_tile


def calculate_ground_resolution(latitude, max_tile):
    # 【空間ID】緯度から地上解像度
    return (np.cos(latitude * np.pi / 180) * 2 * np.pi * 6378137) / max_tile


def convert_xy_corrdinates_array(target_x_coordinates_array,
                                 target_y_coordinates_array,
                                 from_crs,
                                 to_crs):
    # 【共通】座標参照系の変換
    # 引数1：変換するx座標1次元配列
    # 引数2：変換するy座標1次元配列
    # x,yの概念は、xが経度、yが緯度の方向とする 平面直角座標系の配列を渡すときには注意すること

    # pyprojのインスタンス生成 xy方向を統一
    transform_instance\
        = pyproj.Transformer.from_crs(from_crs,
                                      to_crs,
                                      always_xy=True)

    # 座標系変換
    result_x_array, \
        result_y_array\
        = transform_instance.transform(target_x_coordinates_array,
                                       target_y_coordinates_array)

    return result_x_array, result_y_array


def get_min_max_longitude_latitude_from_id(x_id, y_id, tiles_counts):
    # 【空間ID】
    # XID,YIDからx,y座標のmin,maxを取得する
    # 引数1:範囲抽出対象XID
    # 引数2:範囲抽出対象YID
    # 戻り値1:最小値経度
    # 戻り値2:最大値経度
    # 戻り値3:最小値緯度
    # 戻り値4:最大値緯度

    # 左上の座標
    left_up_lon = convert_xid2longitude(x_id, tiles_counts)
    left_up_lat = convert_yid2latitude(y_id, tiles_counts)

    # 右下の座標
    right_bottom_lon = convert_xid2longitude(x_id + 1, tiles_counts)
    right_bottom_lat = convert_yid2latitude(y_id + 1, tiles_counts)

    return left_up_lon, right_bottom_lon, right_bottom_lat, left_up_lat


@jit(b1[:](f8, f8, f8, f8, f8[:], f8[:]), nopython=True, cache=True, nogil=True)
def get_bool_in_rectangle_area(min_x, max_x, min_y, max_y, x_coordinate, y_coordinate):
    # 矩形範囲内に存在する座標のbool配列取得

    result_bool = (min_x <= x_coordinate) & (x_coordinate <= max_x)\
        & (min_y <= y_coordinate) & (y_coordinate <= max_y)

    return result_bool


def get_coordinates_mask_by_index(id_array, target_unique_id_array):
    # 【共通】idを指定して構成点抽出用maskを返す

    # mask初期値 allFalse
    result_mask = np.zeros(len(id_array), dtype=np.bool_)

    for i in range(len(target_unique_id_array)):

        temp_index = id_array == target_unique_id_array[i]

        result_mask[temp_index] = True

    return result_mask


@jit(b1(f8[:, :]), nopython=True, cache=True, nogil=True)
def is_closed(target_array):
    # 地物が閉じているかそうでないか判定する
    # 引数1：任意の地物のxyz座標を持つ二次元配列（[構成点1, 構成点2, …]、構成点n = [x, y, z]）
    # 概要：bool

    return np.all(target_array[0] == target_array[len(target_array) - 1])


@jit(f8(f8[:, :]), nopython=True, cache=True, nogil=True)
def get_degree_3points(points_array):
    # 3点が構成するのxy平面上における角度を算出する
    # 引数1:[構成点3点]>[x,y]
    # 戻り値：角度（単位：degree）

    # 各点の座標からベクトルを作成
    ba_vector = points_array[0] - points_array[1]
    bc_vector = points_array[2] - points_array[1]

    # ベクトルの内積を計算
    dot_product = np.dot(ba_vector, bc_vector)

    # ベクトルの大きさを計算
    ba_magnitude = np.linalg.norm(ba_vector)
    bc_magnitude = np.linalg.norm(bc_vector)

    # ベクトルの角度を計算
    # 内積=|a||b|cosθ
    # これをaベクトルとbベクトルの大きさの積で割ってやるとcosθが出てくるのでarccosに渡して角度（ラジアン取得）
    # 浮動小数点の誤差で-1より若干小さいcosxが出てくる（そんなものはこの世に存在しない）→arccosがnanを返す
    # 丸目誤差を消すようにする 0への丸目
    cosx = np.trunc(dot_product / (ba_magnitude * bc_magnitude) * 1000) / 1000

    radianx = np.arccos(cosx)

    # ラジアンから度に変換して返す
    return np.degrees(radianx)


def get_angle_value_in_rectangle(target_unique_id_array, target_coordinates_array, target_lon_lat_array, min_lon, max_lon, min_lat, max_lat):
    # 指定範囲内の地物の構成転換角度を計算し最小値を返す

    result_list = []

    # 指定した地物idの数だけループ
    for i in range(len(target_unique_id_array)):

        # 抽出対象インデックス
        temp_index = target_lon_lat_array[:, 0] == target_unique_id_array[i]

        # 角度計算用座標抽出
        temp_coordinates_array = target_coordinates_array[temp_index]

        # 範囲内判定用座標抽出
        temp_lon_lat_array = target_lon_lat_array[temp_index]

        # 閉じた地物の場合終点における角度を考慮する
        if is_closed(temp_coordinates_array[:, 1:]):

            # 角度算出対象の構成点が範囲内かどうか
            temp_bool = get_bool_in_rectangle_area(min_lon, max_lon, min_lat, max_lat, temp_lon_lat_array[0, 1:2], temp_lon_lat_array[0, 2:3]
                                                   )

            # 範囲内の場合は角度計算
            if np.all(temp_bool):
                # 角度
                closed_array = np.stack((temp_coordinates_array[-2, 1:], temp_coordinates_array[0, 1:], temp_coordinates_array[1, 1:]
                                         ))
                unique_temp_closed_array = np.unique(closed_array, axis=0)

                if len(unique_temp_closed_array) != 3:
                    pass
                else:

                    target_degree = get_degree_3points(closed_array)
                    result_list.append(target_degree)
            else:
                pass

        else:
            pass

        for j in range(int(len(temp_coordinates_array)) - 2):

            # 角度算出対象の構成点が範囲内かどうか
            temp_bool = get_bool_in_rectangle_area(min_lon, max_lon, min_lat, max_lat, temp_lon_lat_array[j + 1, 1:2], temp_lon_lat_array[j + 1, 2:3]
                                                   )

            # 範囲内の場合は角度計算
            if np.all(temp_bool):
                unique_temp_coordinate_array = np.unique(
                    temp_coordinates_array[j:j + 3, 1:], axis=0)

                if len(unique_temp_coordinate_array) != 3:
                    continue
                else:

                    # 角度
                    target_degree = get_degree_3points(
                        temp_coordinates_array[j:j + 3, 1:])
                    result_list.append(target_degree)
            else:
                pass

    return np.array(result_list, dtype=np.float64)


def get_min_max_xy_value(xy_array):
    # 最小値と最大値を返す
    # 引数1:xy2次元配列 [構成点数]>[x,y]

    # それぞれ算出
    min_x = np.min(xy_array[:, 0])
    max_x = np.max(xy_array[:, 0])
    min_y = np.min(xy_array[:, 1])
    max_y = np.max(xy_array[:, 1])

    return min_x, max_x, min_y, max_y


def _convert_geometry_crs(coordinates_array, from_crs, to_crs):
    # [id+x+y]の座標配列のCRSを変換する

    result_array = coordinates_array.copy()

    # 変換前後のCRSが同じならそのまま返す
    if from_crs == to_crs:
        return result_array
    else:

        # 座標系変換
        result_x_array, result_y_array\
            = convert_xy_corrdinates_array(result_array[:, 1], result_array[:, 2], from_crs, to_crs
                                           )

        result_array[:, 1] = result_x_array
        result_array[:, 2] = result_y_array

    return result_array


def _get_coordinates_by_tfw_array(point_array, tfw_file_list):
    # pixelの情報とtfwの情報から座標にして配列を返す
    # 戻り値：座標２次元配列 [point]>[id,x,y]

    x_array = point_array[:, 0] * tfw_file_list[0] + tfw_file_list[4]
    y_array = point_array[:, 1] * tfw_file_list[3] + tfw_file_list[5]

    index_array = np.arange(0, len(x_array))

    return np.stack([index_array, x_array, y_array], axis=1)


def write_tfw(tfw_list, output_folder_path, output_file_name):
    # tfwファイルの作成

    # tfwファイル（ラスタデータ用の座標ファイル）を作成しopen
    tfw_open_object_ground_level = open(output_folder_path + '\\' + output_file_name + '.tfw', 'w', encoding='UTF-8'
                                        )

    # 以下openしたtfwファイルに必要な書式を書き込む
    for i in range(len(tfw_list)):
        # 1ピクセルのX方向の長さ
        # 行の回転パラメータ
        # 列の回転パラメータ
        # 1ピクセルのY方向の長さ
        # 左上ピクセルの中心位置のX座標
        # 左上ピクセルの中心位置のY座標
        tfw_open_object_ground_level.write(str(tfw_list[i]) + '\n')

    # tfwファイル書き込み後close
    tfw_open_object_ground_level.close()


def _get_xyid_object_array(coordinates_array, coordinates_crs, voxel_crs, voxel_zoom_level, exponent=25):
    # -----------------------------------------------------------------------------------------------------------
    # 空間IDのXYID配列取得
    # -----------------------------------------------------------------------------------------------------------

    # 編集用配列
    edit_array = coordinates_array.copy()

    # Voxel算出用のCRSに変換
    if coordinates_crs == voxel_crs:
        converted_x_coordinates_array = edit_array[:, 1].copy()
        converted_y_coordinates_array = edit_array[:, 2].copy()

    else:
        converted_x_coordinates_array, converted_y_coordinates_array\
            = convert_xy_corrdinates_array(edit_array[:, 1], edit_array[:, 2], coordinates_crs, voxel_crs
                                           )

    # 空間ID計算用座標配列
    max_tile_counts = get_max_tile_counts(voxel_zoom_level)
    max_height = get_max_height(exponent)

    # Keyごとに座標取り出して近傍結合処理
    x_id_array = convert_longitude2xid(converted_x_coordinates_array, max_tile_counts
                                       )
    y_id_array = convert_latitude2yid(converted_y_coordinates_array, max_tile_counts
                                      )

    # idobject作成
    x_id_array = np.array([str(int(x_id_array[xi]))
                          for xi in range(len(x_id_array))], dtype=object)
    y_id_array = np.array([str(int(y_id_array[yi]))
                          for yi in range(len(y_id_array))], dtype=object)

    xy_string_array = x_id_array + '@' + y_id_array
    unique_xy_id_array = np.unique(xy_string_array)

    mesh_id_default_dict = defaultdict(list)
    [mesh_id_default_dict[xy_string_array[mi]].append(
        mi) for mi in range(len(xy_string_array))]

    return xy_string_array, unique_xy_id_array, mesh_id_default_dict


def _get_nearest_neighbors_array(subjective_array, objective_array, gcp_distance_threshold
                                 ):

    # 結果抽出用mask初期配列
    result_subjective_bool = np.zeros(len(subjective_array), dtype=np.bool_)
    result_objective_bool = np.zeros(len(objective_array), dtype=np.bool_)

    # 座標格納用
    result_subjective_list = []
    result_objective_list = []

    # GCPの数だけループ
    for i in range(len(objective_array)):

        # 全体の構成点との距離を計算し最小値
        temp_distance_array = np.sqrt(
            np.sum(np.power(subjective_array - objective_array[i], 2), axis=1))
        temp_min_index = np.argmin(temp_distance_array)

        # 閾値より大きい場合次のGCPへ
        if temp_distance_array[temp_min_index] > gcp_distance_threshold:
            continue

        else:
            # すでにTrueの場合はスキップ
            if result_subjective_bool[temp_min_index] == True \
                    or result_objective_bool[i] == True:
                pass

            else:
                # すべて閾値に収まる場合は出力対象とする
                result_subjective_bool[temp_min_index] = True
                result_objective_bool[i] = True
                result_subjective_list.append(
                    subjective_array[temp_min_index].copy())
                result_objective_list.append(objective_array[i].copy())

    result_subjective_array = np.array(
        result_subjective_list, dtype=np.float64)
    result_objective_array = np.array(result_objective_list, dtype=np.float64)

    return result_subjective_array, result_objective_array


def _get_nearest_neighbors_array_by_voxel(unique_subjective_xy_array, subjective_default_dict, objective_default_dict, subjective_coordinates_array, objective_coordinates_array, nearest_neighbor_threshold):
    # --------------------------------------------------------------------------
    # VoxelIDごとに近傍処理を行う
    # --------------------------------------------------------------------------
    # 結果格納用
    result_subjective_list = []
    result_objective_list = []

    for i in range(len(unique_subjective_xy_array)):

        temp_subjective_index = subjective_default_dict[unique_subjective_xy_array[i]]

        temp_objective_index = objective_default_dict[unique_subjective_xy_array[i]]

        if len(temp_objective_index) == 0:
            continue
        else:

            temp_subjective_array, temp_objective_array = _get_nearest_neighbors_array(subjective_coordinates_array[temp_subjective_index, 1:3], objective_coordinates_array[temp_objective_index, 1:3], nearest_neighbor_threshold
                                                                                       )
            if len(temp_subjective_array) == 0:
                pass
            else:
                result_subjective_list.append(temp_subjective_array.copy())
                result_objective_list.append(temp_objective_array.copy())

    result_subjective_array = np.concatenate(result_subjective_list)
    result_objective_array = np.concatenate(result_objective_list)

    return result_subjective_array, result_objective_array


@jit('Tuple((i8[:],i8[:]))(f8[:,:],f8[:,:],f8[:,:],f8[:,:],f8,f8)', nopython=True, cache=True, nogil=True)
def snap_and_extract_gcp(query_gcp_array, query_array, train_gcp_array, train_array, snap_threshold, gcp_distance_threshold
                         ):

    # 結果抽出用mask初期配列
    result_query_bool = np.zeros(len(query_array), dtype=np.bool_)
    result_train_bool = np.zeros(len(train_array), dtype=np.bool_)

    # インデックス格納用
    result_query_index_list = []
    result_train_index_list = []

    # GCPの数だけループ
    for i in range(len(query_gcp_array)):

        # 全体の構成点との距離を計算し最小値
        query_distance_array = np.sqrt(
            np.sum(np.power(query_array - query_gcp_array[i], 2), axis=1))
        train_distance_array = np.sqrt(
            np.sum(np.power(train_array - train_gcp_array[i], 2), axis=1))
        q_min_index = np.argmin(query_distance_array)
        t_min_index = np.argmin(train_distance_array)

        # 毎閾値ごとに次のループへ進むようにする
        # 条件にそぐわなかった瞬間に次のGCPへ移行することで処理件数減らす

        # query側距離
        # 閾値より大きいの場合次へ
        if query_distance_array[q_min_index] > snap_threshold\
           or train_distance_array[t_min_index] > snap_threshold:
            continue
        else:

            # min_indexからスナップ先座標取得
            q_snap_array = query_array[q_min_index]
            t_snap_array = train_array[t_min_index]

            # snap先同士の距離計算
            snap_gcp_distance = np.sqrt(
                np.sum(np.power(q_snap_array - t_snap_array, 2)))

            # 閾値より大きい場合次のGCPへ
            if snap_gcp_distance > gcp_distance_threshold:
                continue
            else:
                # すでにTrueの場合はスキップ
                if result_query_bool[q_min_index] == True \
                        or result_train_bool[t_min_index] == True:
                    pass
                else:
                    # すべて閾値に収まる場合は出力対象とする
                    result_query_bool[q_min_index] = True
                    result_train_bool[t_min_index] = True
                    result_query_index_list.append(q_min_index)
                    result_train_index_list.append(t_min_index)

    return np.array(result_query_index_list, dtype=np.int64), np.array(result_train_index_list, dtype=np.int64)


def _extract_and_reset_id(coordinates_array, index_array):
    # 座標配列をインデックス配列で抽出してidを振りなおす

    # 編集用
    result_array = coordinates_array.copy()

    # mask抽出
    result_array = result_array[index_array]

    # 連番でid振りなおし
    id_array = np.arange(0, len(result_array))
    result_array[:, 0] = id_array

    return result_array


def _get_linestring_point_by_gradient_threshold(target_coordinates_array, angle_threshold):
    # ３点がなす角度が閾値より小さい構成点を抽出する
    # 座標配列は[構成点]>[id,xy]の２次元配列

    # id取得
    unique_id_array = np.unique(target_coordinates_array[:, 0])

    # 座標格納用list
    result_list = []

    # 指定した地物idの数だけループ
    for i in range(len(unique_id_array)):

        # 抽出対象mask
        temp_index = np.where(
            target_coordinates_array[:, 0] == unique_id_array[i])[0]

        # 2点しかない場合は次の地物へ
        if len(temp_index) == 2:
            continue
        else:
            pass

        # 角度計算用座標抽出
        temp_coordinates_array = target_coordinates_array[temp_index]

        # 閉じた地物の場合終点における角度を考慮する
        if is_closed(temp_coordinates_array[:, 1:]):

            # 角度計算用
            closed_array = np.stack((temp_coordinates_array[-2, 1:], temp_coordinates_array[0, 1:], temp_coordinates_array[1, 1:]
                                     ))
            # 角度算出
            target_degree = get_degree_3points(closed_array)

            # 角度が閾値未満の構成点であれば構成点を格納
            if target_degree < angle_threshold:

                result_list.append(temp_coordinates_array[0].copy())
            else:
                pass

        # 閉じてなければ中間の構成点ごとの処理へ
        else:
            pass

        # 構成点ごとに角度を判定
        for j in range(int(len(temp_coordinates_array)) - 2):

            unique_temp_coordinates_array = np.unique(
                temp_coordinates_array[j:j + 3, 1:], axis=0)

            if len(unique_temp_coordinates_array) != 3:
                continue
            else:
                pass

            # 角度
            target_degree = get_degree_3points(
                temp_coordinates_array[j:j + 3, 1:])

            # 角度が閾値未満の構成点であれば構成点を格納
            if target_degree < angle_threshold:

                result_list.append(temp_coordinates_array[j + 1].copy())
            else:
                pass
    return np.array(result_list)


def densify_line_segment_given_interval(line_segment_array, interval):
    # 線分の高密度化
    # 線分の距離が間隔よりも短い場合はそのまま返す
    # 線分の配列は要素数2の[x,y]

    # 構成点間距離計算
    # 構成点間距離は地物ごとに一回の計算処理でもとまる
    line_segment_distance = np.sqrt(
        np.sum(np.power(line_segment_array[0] - line_segment_array[1], 2)))

    # 構成点の件数取得
    # interval 0以下の数字ダメ
    coordinates_counts = int(np.ceil(line_segment_distance / interval))

    # 間隔の長さの最大値よりも線分が短ければそのまま返す
    if coordinates_counts == 1:

        return line_segment_array

    else:

        # 線間隔の係数
        vector_coefficient = np.linspace(0, 1, coordinates_counts)
        vector_coefficient = np.stack(
            [vector_coefficient, vector_coefficient], axis=1)

        result_array = line_segment_array[0] + np.multiply(
            vector_coefficient, (line_segment_array[1] - line_segment_array[0]))

        return result_array


def get_densified_points(line_segment_array, interval):
    # 高密度化したときに追加する点を取得
    # 対象ないときはlenghth0配列返す

    # 最初と最後以外を返す
    return densify_line_segment_given_interval(line_segment_array, interval)[1:-1, :]


def _get_image_array_information(coordinates_array, x_unit, y_unit):
    # 画像作成に必要な情報を取得する
    # 引数pixelの数版必要か？→pixel数から1pixel当たりの長さ求めて返す
    # 引数1：座標配列
    # 引数2：xの1pixel当たりの大きさ voxelの１辺や固定値
    # 引数3：yの1pixel当たりの大きさ voxelの１辺や固定値 大きさそのものなので注意 負は考えていない
    # 戻り値1：原点の座標 left up
    # 戻り値2：xのpixel数
    # 戻り値3：yのpixel数
    # 戻り値4：x座標画像配列インデックス（引数の座標配列がどのpixelに存在するか）
    # 戻り値5：y座標画像配列インデックス（引数の座標配列がどのpixelに存在するか）

    # x,yそれぞれの最小値、最大値取得
    min_x_coordinate, max_x_coordinate, min_y_coordinate, max_y_coordinate = get_min_max_xy_value(
        coordinates_array)

    # 横の長さと1pixel当たりの単位から横のpixel数取得
    x_pixel_counts = int(
        np.ceil((max_x_coordinate - min_x_coordinate) / x_unit))

    # 縦の長さと1pixel当たりの単位から縦pixel数取得
    y_pixel_counts = int(
        np.ceil((max_y_coordinate - min_y_coordinate) / y_unit))

    # 構成点画像のどのインデックスに存在するか
    # leftupが原点の条件なので負への無限大の丸め
    x_pixel_index_array = np.floor(
        (coordinates_array[:, 0] - min_x_coordinate) / x_unit).astype(np.int64)
    y_pixel_index_array = np.floor(
        (coordinates_array[:, 1] - max_y_coordinate) / -y_unit).astype(np.int64)

    return min_x_coordinate, max_x_coordinate, min_y_coordinate, max_y_coordinate, x_pixel_counts, y_pixel_counts, x_pixel_index_array, y_pixel_index_array


def _get_voxel_id_array(coordinate_array, zoom_level):
    # 座標から空間IDの配列取得
    # 引数1:座標2次元配列 経緯度 [構成点]>[lon,lat]
    # 引数2:Voxel生成用ZoomLevel
    # 戻り値1:VoxelのXID最小値 Voxelを1pixelとしたときのpixel数を求めるために使用する
    # 戻り値2:VoxelのXID最大値 pixelの四隅の座標を求めるために使用する
    # 戻り値3:VoxelのYID最小値
    # 戻り値4:VoxelのYID最大値
    # 戻り値5:画像の横のpixel数
    # 戻り値6:画像の縦のpixel数
    # 戻り値7:全座標に対するXID
    # 戻り値8:全座標に対するYID
    # 戻り値9:全座標に対するXYID(2次元配列)
    # 戻り値10:座標の存在するXYID(2次元配列) pixel内の色を決定する全対象
    # 戻り値11:座標の存在するXYIDのインデックス(2次元配列) pixel内に存在する地物を取得する用のインデックス
    # 戻り値12:1辺のタイル数

    # 指定zoomlevelでの1辺のタイル枚数取得
    tile_counts = get_max_tile_counts(zoom_level=zoom_level)

    # 存在するid取得
    # 全xy座標id化→unique（ループ対象）
    x_id_array = convert_longitude2xid(coordinate_array[:, 0], tile_counts
                                       )
    y_id_array = convert_latitude2yid(coordinate_array[:, 1], tile_counts
                                      )

    # 出力対象id取得
    xy_id_array = np.stack([x_id_array, y_id_array], axis=1
                           )

    # 更新対象pixel数
    unique_xy_id_array, unique_xy_index = np.unique(xy_id_array, axis=0, return_index=True
                                                    )

    # IDの最大最小
    min_x_id, max_x_id, min_y_id, max_y_id = get_min_max_xy_value(
        unique_xy_id_array)

    # 最大最小のidから出力する画像のpixel数取得
    x_pixel_counts = int(max_x_id - min_x_id + 1)
    y_pixel_counts = int(max_y_id - min_y_id + 1)

    # 座標がどのidに存在するか
    pixel_x_index_array = np.array(
        unique_xy_id_array[:, 0] - min_x_id, dtype=np.int64)
    pixel_y_index_array = np.array(
        unique_xy_id_array[:, 1] - min_y_id, dtype=np.int64)

    return min_x_id, max_x_id, min_y_id, max_y_id, x_pixel_counts, y_pixel_counts, pixel_x_index_array, pixel_y_index_array, xy_id_array, unique_xy_id_array, unique_xy_index, tile_counts


def _update_image_array_by_gradient_in_mesh(unique_xy_id_array, target_coordinates_array, target_lon_lat_array, pixel_x_index_array, pixel_y_index_array, tile_counts, angle_threshold, image_array):
    # 空間ID内の地物を抽出し閾値未満の角度の場合白色255で更新　それ以外の場合は50で更新する
    # 該当のpixelの上下左右も白色で更新

    # 更新用
    result_image_array = image_array.copy()

    # voxelのidごとにループ→これはvoxelの範囲事に出力するときだけ
    for i in range(len(unique_xy_id_array)):

        # idから4隅の座標取得
        min_lon, max_lon, min_lat, max_lat = get_min_max_longitude_latitude_from_id(
            unique_xy_id_array[i, 0], unique_xy_id_array[i, 1], tile_counts)

        # 指定idの範囲内に含まれる構成点取得
        # 線の交差は考慮しない 線だけ交差されていても計算対象の角度がない
        temp_bool = get_bool_in_rectangle_area(min_lon, max_lon, min_lat, max_lat, target_lon_lat_array[:, 1], target_lon_lat_array[:, 2]
                                               )

        # 抽出対象id取得
        temp_id_array = np.unique(target_lon_lat_array[temp_bool, 0])

        # 角度算出
        angle_array = get_angle_value_in_rectangle(temp_id_array, target_coordinates_array[:, :3], target_lon_lat_array[:, :3], min_lon, max_lon, min_lat, max_lat
                                                   )
        if len(angle_array) == 0:
            pass
        else:

            # 2択にしてみる
            target_angle = np.min(angle_array)

            if target_angle < angle_threshold:
                gray_scale_value = 255

                # 該当のvoxel更新
                result_image_array[pixel_y_index_array[i],
                                   pixel_x_index_array[i]] = gray_scale_value
                result_image_array[pixel_y_index_array[i] + 1,
                                   pixel_x_index_array[i]] = gray_scale_value
                result_image_array[pixel_y_index_array[i] - 1,
                                   pixel_x_index_array[i]] = gray_scale_value
                result_image_array[pixel_y_index_array[i],
                                   pixel_x_index_array[i] + 1] = gray_scale_value
                result_image_array[pixel_y_index_array[i],
                                   pixel_x_index_array[i] - 1] = gray_scale_value

            else:
                pass
    return result_image_array


def _update_image_array_by_densified_geometries(coordinates_array, x_unit, y_unit, interval, standard_x, standard_y, image_array):
    # 構成点を高密度化しその構成点が属するpixelを更新する

    # 更新用
    result_image_array = image_array.copy()

    # 原点からの相対座標
    target_coordinates_array = coordinates_array.copy()
    target_coordinates_array[:, 1] = coordinates_array[:, 1] - standard_x
    target_coordinates_array[:, 2] = coordinates_array[:, 2] - standard_y

    # 地物の数
    unique_id_array = np.unique(target_coordinates_array[:, 0])

    # 地物ごと
    for i in range(len(unique_id_array)):

        # 構成点数取得
        temp_coordinates_array = target_coordinates_array[np.where(
            target_coordinates_array[:, 0] == unique_id_array[i])[0]]

        # 線分ごと
        for j in range(len(temp_coordinates_array) - 1):

            # 線分を高密度化
            densified_array = densify_line_segment_given_interval(temp_coordinates_array[j:j + 2, 1:], interval
                                                                  )

            # 高密度化したインデックスを取得
            x_pixel_index_array = np.floor(
                densified_array[:, 0] / x_unit).astype(np.int64)
            y_pixel_index_array = np.floor(
                densified_array[:, 1] / -y_unit).astype(np.int64)

            # インデックスで更新
            result_image_array[y_pixel_index_array, x_pixel_index_array] = 50

    return result_image_array


def extract_gcp_by_random_index_in_voxel(mesh_id_dictionary, output_maximum):
    # voxelごとにデータを分けてsnap_and_extract_gcpを行う

    # --------------------------------------------------------------------------
    # 各Voxelで存在すべきGCPの個数算出
    # --------------------------------------------------------------------------
    # 辞書型配列のValueに設定されているnumpy配列の要素数をlistにして返す
    value_len_array = np.array(
        list(map(lambda x: len(mesh_id_dictionary[x]), mesh_id_dictionary.keys())))

    # keyのみ返却
    key_array = np.array(list(map(lambda x: x, mesh_id_dictionary.keys())))

    # 構成点合計
    len_sum = np.sum(value_len_array)

    # 削除すべき件数
    delete_numbers = len_sum - output_maximum

    # 1グリッドあたりに存在すべき件数
    numbers_per_grid = np.ceil(output_maximum / len(key_array))

    # 1グリッドあたりに存在できる構成点数を上回るグリッド特定
    more_per_grid_mask = value_len_array > numbers_per_grid
    less_per_grid_mask = np.logical_not(more_per_grid_mask)

    # グリッドd特定
    more_per_grid = key_array[more_per_grid_mask]
    less_per_grid = key_array[less_per_grid_mask]

    # 足りないところの不足分取得
    # ほかのグリッドにこれだけ負担してもらおうの数
    add_numbers = int(
        np.sum(np.abs(value_len_array[less_per_grid_mask] - numbers_per_grid)))

    # 負担分を合わせたときの１グリッドあたりに存在できる構成点の数
    # （全体で削除すべき件数）/グリッド数 +（底上げ分）/超過グリッド件数
    new_numbers_per_grid = int(np.ceil(
        numbers_per_grid + add_numbers / len(value_len_array[more_per_grid_mask])))

    # 毎ループごとに削除件数を管理して最後のグリッドで削除後の件数を調整する
    # １，削除後の件数だけランダムで重複のないインデックス生成
    # ２，距離計算して昇順に並び替えそこから削除後件数分スライス

    # 削除後の件数の初期値は
    # 超過していないグリッドの合計
    less_per_grid_sum = int(np.sum(value_len_array[less_per_grid_mask]))
    temp_output = less_per_grid_sum

    # 結果格納用
    result_index_list = []

    # 最終調整用
    # 1グリッドあたりの数より大きく、負担分を合わせたときの１グリッドあたりに存在できる構成点の数より小さい場合は
    # 差を合計して最後のグリッドで調整する
    last_adjust_numbers = 0
    # --------------------------------------------------------------------------
    # 1グリッドあたりに存在すべき数を下回ったグリッド分ループ
    # --------------------------------------------------------------------------
    # 最後の配列だけ最大出力件数からこれまでの出力件数の和を引いた分だけ出力
    for i in range(len(more_per_grid) - 1):

        # 辞書型配列からインデックス取得
        temp_index_array = np.array(mesh_id_dictionary[more_per_grid[i]])

        # 出力すべき件数と実際のグリッドの件数の差
        temp_difference_output = len(temp_index_array) - new_numbers_per_grid

        if temp_difference_output < 0:
            last_adjust_numbers = last_adjust_numbers - temp_difference_output
            result_index = temp_index_array.copy()
        else:
            # このVoxelで調整する件数算出
            # 全部受け入れられなければ受け入れられる分だけ）追加
            # 全部可能の場合調整件数0にしする

            # 仮の出力件数
            temp_voxel_numbers = new_numbers_per_grid + last_adjust_numbers

            # Voxel内全件より多く選ぼうとするのであれば全件追加して調整件数を再算出
            if temp_voxel_numbers > len(temp_index_array):
                last_adjust_numbers = last_adjust_numbers - \
                    (temp_voxel_numbers - len(temp_index_array))
                result_index = temp_index_array.copy()

            else:
                # ランダムで重複なくインデックスを選択
                result_index = np.random.choice(temp_index_array, new_numbers_per_grid + last_adjust_numbers                                                # , new_numbers_per_grid
                                                , replace=False
                                                )

                # 調整分加算したらリセット
                last_adjust_numbers = 0

            # ランダム選択後のインデックスで座標を抽出
        result_index_list.append(result_index.copy())

        # 削除後の件数カウントアップ
        temp_output = temp_output + len(result_index)

    # 最終ridで件数調整
    # 足りない件数取得
    rest_numbers = int(output_maximum - temp_output)

    # 最後のグリッドからランダムに取得
    # 辞書型配列からインデックス取得
    temp_index_array = np.array(
        mesh_id_dictionary[more_per_grid[int(len(more_per_grid) - 1)]])

    result_index = np.random.choice(temp_index_array, rest_numbers, replace=False
                                    )

    result_index_list.append(result_index.copy())

    # --------------------------------------------------------------------------
    # 1グリッドあたりに存在すべき数を下回ったグリッドのインデックスはそのままListに追加する
    # --------------------------------------------------------------------------
    [result_index_list.append(mesh_id_dictionary[less_per_grid[i]])
     for i in range(len(less_per_grid))]

    return np.concatenate(result_index_list)


def _get_pixel_unit_and_unique_id(target_coordinate_array, x_pixel_counts, y_pixel_counts, left_up_min_x, left_up_max_y, right_down_min_y, right_down_max_x, unique_xy_index):
    # 座標と原点の情報、ピクセルの数から1pixelあたりの大きさと更新対象pixelのインデックスを取得
    # 引数1:座標2次元配列 経緯度 [構成点]>[lon,lat]
    # 引数2:横のpixel数
    # 引数3:縦のpixel数
    # 引数4:矩形範囲の左上のx座標
    # 引数5:矩形範囲の左上のy座標
    # 引数6:矩形範囲の右下のy座標
    # 引数7:矩形範囲の右下のx座標
    # 引数8:pixel内の色を決定する対象座標のインデックス
    # 戻り値1:1pixelの横の長さ
    # 戻り値2:1pixelの縦の長さ
    # 戻り値3:座標の存在するXインデックス
    # 戻り値4:座標の存在するYインデックス

    # 矩形範囲の縦と横の長さをpixel数で割ることで1pixel当たりの単位を算出
    x_unit = (right_down_max_x - left_up_min_x) / x_pixel_counts
    y_unit = (left_up_max_y - right_down_min_y) / y_pixel_counts

    temp_coordinate_array = target_coordinate_array.copy()

    # 基準値からの相対距離を算出し1pixel当たりの単位で割る
    # →pixelのインデックスが求まる
    temp_coordinate_array[:, 1] = temp_coordinate_array[:, 1] - left_up_min_x
    temp_coordinate_array[:, 2] = temp_coordinate_array[:, 2] - left_up_max_y
    pixel_x_index_array = np.floor(
        temp_coordinate_array[:, 1] / x_unit).astype(np.int64)
    pixel_y_index_array = np.floor(
        temp_coordinate_array[:, 2] / -y_unit).astype(np.int64)

    unique_pixel_x_index_array = pixel_x_index_array[unique_xy_index]
    unique_pixel_y_index_array = pixel_y_index_array[unique_xy_index]

    return x_unit, y_unit, unique_pixel_x_index_array, unique_pixel_y_index_array


def convert_flowfile_to_field_set_file_dataframe(flowfile):
    # flowfileをフィールド集合ファイルのdataframeへ変換
    # 引数:NifiFlowファイルオブジェクト

    # FlowFileからデータを取得プロパティから取得
    input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

    # FlowFileのデータをfile_objectへ変換
    field_set_file_stringio = io.StringIO(input_field_set_file)

    # Dataframeに変換。
    field_set_file_dataframe = pd.read_csv(field_set_file_stringio)

    return field_set_file_dataframe


def get_value_dwh_list_from_field_set_file_dataframe(field_set_file_dataframe, dwh_column_name='Dwh', type_column_name='Type', value_column_name='Value'):
    # フィールド集合ファイルのdataframeからValue列、Type列、Dwh列のListを取得

    # フィールド集合ファイルのdataframeからValue列、Dwh列のList取得
    value_list = field_set_file_dataframe[value_column_name].tolist()
    type_list = field_set_file_dataframe[type_column_name].tolist()
    dwh_list = field_set_file_dataframe[dwh_column_name].tolist()

    return value_list, type_list, dwh_list


def get_value_field_from_value_dwh_list(value_list, dwh_list, target_dwh_name):
    # 指定Dwh名のValueをValue列、Dwh列のListから取得

    # 指定したDwh名の行数インデックス取得
    target_dwh_index = dwh_list.index(target_dwh_name)

    # value_listの各要素をデコード、デシリアライズし、list or arrayにする。
    target_value = pickle.loads(base64.b64decode(value_list[target_dwh_index]))

    return target_value


def set_list_to_field_set_file(dwh_list, type_list, value_list, dwh_column_name='Dwh', type_column_name='Type', value_column_name='Value'):
    # FieldSetFile作成

    # field_set_file用のdata_dictを作成。
    data_dict = {dwh_column_name: dwh_list, type_column_name: type_list, value_column_name: value_list
                 }

    # data_dictをfield_set_file(CSV形式)に加工。
    result_df = pd.DataFrame(data_dict)
    field_set_file = result_df.to_csv(index=False)

    return field_set_file


def convert_list_to_value(target_list):
    # ValueのListの要素となる文字列をlistから作成する

    serialized_and_encoded_value = base64.b64encode(
        pickle.dumps(target_list)).decode("utf-8")

    return serialized_and_encoded_value


def add_record_to_field_set_file_list(dwh_list, type_list, value_list, add_target_dwh_name_string, add_target_type_string, add_target_value_list):

    # Value列、Type列、Dwh列のListに要素を追加
    serialized_and_encoded_value = convert_list_to_value(add_target_value_list)

    dwh_list.append(add_target_dwh_name_string)
    type_list.append(add_target_type_string)
    value_list.append(serialized_and_encoded_value)

    return dwh_list, type_list, value_list


def reset_coordinate_index(coordinates_array):
    # 座標のidを0から連番で付与する
    # xyzのみの配列（[[x1, y1, z1], [x2, y2, z2], ...]）

    # 座標のIDを付与する配列を引数で初期化
    reset_array = coordinates_array.copy()

    # 新しいIDの配列を作成
    # 例：[[x1, y1, z1], [x2, y2, z2]]→[[0], [1]]
    geometry_id_array = np.arange(
        len(reset_array)).reshape((len(reset_array), 1))

    # 新しいIDの配列を結合
    # 例：[[0], [1]] + [[x1, y1, z1], [x2, y2, z2]]→[[0, x1, y1, z1], [1, x2, y2, z2]]
    reset_array = np.concatenate([geometry_id_array, reset_array], axis=1)

    return reset_array


def add_coordinates_record_to_field_set_file_list(dwh_list, type_list, value_list, add_target_dwh_name_string, add_target_type_string, add_target_value_list):
    # 座標用Value列、Type列、Dwh列のListに要素を追加
    # 追加する要素としては３種類
    # 座標、属性に登録する連番のID、地物ID（FID）
    # Dwh名に関しては以下の通り
    # ・属性に登録する連番のID：[ファイル名]_ATTRIBUTE_ID
    # ・地物ID：[ファイル名]_FID

    # 座標配列追加
    dwh_list, type_list, value_list\
        = add_record_to_field_set_file_list(dwh_list, type_list, value_list, add_target_dwh_name_string, add_target_type_string, add_target_value_list
                                            )

    coordinates_array = np.array(add_target_value_list)
    # 一意な地物のidを取得
    unique_id_array = np.unique(coordinates_array[:, 0:1]).astype(np.int64)

    # データ流通基盤のidの配列生成
    feature_id_list = [(str(i), i) for i in range(len(unique_id_array))]

    # データ流通基盤の属性へidの配列生成
    feature_attribute_id_list = [(i, i) for i in range(len(unique_id_array))]

    # 連番属性配列追加
    dwh_list, type_list, value_list\
        = add_record_to_field_set_file_list(dwh_list, type_list, value_list, add_target_dwh_name_string + '_ATTRIBUTE_ID', 'float64', feature_attribute_id_list
                                            )

    # 地物ID(FID)配列追加
    dwh_list, type_list, value_list\
        = add_record_to_field_set_file_list(dwh_list, type_list, value_list, add_target_dwh_name_string + '_FID', 'string', feature_id_list
                                            )

    return dwh_list, type_list, value_list


def get_unique_count_index_array(target_array, axis_number=0):
    # numpy.uniqueのオプション、return_index=True, return_counts=Trueにする
    # numbaに対応していないためこの関数で呼び出してからnumba対応関数に渡す
    # 引数は1次元配列想定なのでaxisのデフォルトは0としている

    unique_array, unique_index_array, unique_counts_array\
        = np.unique(target_array, return_index=True, return_counts=True, axis=axis_number
                    )

    return unique_array, unique_index_array, unique_counts_array


@jit(i8(f8[:, :], f8), nopython=True, cache=True, nogil=True)
def get_line_segment_splitting_counts(line_segment_array, interval):
    # 線分の分割回数算出 始点終点含む

    line_segment_distance\
        = np.sqrt(np.sum(np.power(line_segment_array[0]
                                  - line_segment_array[1], 2)))

    # 構成点の件数取得
    # interval 0以下の数字ダメ
    coordinates_counts = int(np.ceil(line_segment_distance / interval))

    return coordinates_counts


def densify_line_segment_given_number(line_segment_array, coordinates_counts):
    # 距離計算用次元数指定

    # 間隔の長さの最大値よりも線分が短ければそのまま返す
    if coordinates_counts == 1:

        return line_segment_array

    else:

        # 線間隔の係数
        vector_coefficient = np.linspace(0, 1, coordinates_counts)

        # xy, xyzで場合分け
        if line_segment_array.shape[1] == 2:
            vector_coefficient = np.stack(
                [vector_coefficient, vector_coefficient], axis=1)
        else:
            vector_coefficient = np.stack(
                [vector_coefficient, vector_coefficient, vector_coefficient], axis=1)

        result_array = line_segment_array[0] + np.multiply(
            vector_coefficient, (line_segment_array[1] - line_segment_array[0]))

        return result_array


@jit(i8[:](i8[:]), nopython=True, cache=True, nogil=True)
def level_up_id(target_array):
    # 偶数なら2で割る、奇数なら1引いて2で割る

    result_id_array\
        = np.where(target_array % 2 == 0, target_array / 2, (target_array - 1) / 2
                   )

    return result_id_array.astype(np.int64)


@jit('Tuple((i8[:],i8[:],i8[:]))(i8[:],i8[:],i8[:],i8,i8)', nopython=True, cache=True, nogil=True)
def calculate_level_up_voxel_id(x_id_array, y_id_array, f_id_array, from_zoom_level, to_zoom_level):
    # fromとtoのzoomlevelを指定して空間IDをレベルアップさせる

    # レベルアップ回数
    level_up_counts = int(from_zoom_level - to_zoom_level)

    result_x_id_array = x_id_array.copy()
    result_y_id_array = y_id_array.copy()
    result_f_id_array = f_id_array.copy()

    for _ in prange(level_up_counts):

        result_x_id_array = level_up_id(result_x_id_array)
        result_y_id_array = level_up_id(result_y_id_array)
        result_f_id_array = level_up_id(result_f_id_array)

    return result_x_id_array, result_y_id_array, result_f_id_array


def get_voxel_id_by_densifying_feature(coordinates_array, interval, zoom_level=26, exponent=25):
    # 1つの地物から重複しない空間IDを取得する
    # なお取得する際は地物を高密度化してから空間IDにする
    # 経緯度,標高すべて1次元配列 1個だけだと高密度化できないのでNG

    # 与えられた座標数を取得
    # 1つならそのまま変換して返す
    # 2つ以上なら線分ごとに高密度化+変換

    # 結果格納用List
    result_x_id_list = []
    result_y_id_list = []
    result_f_id_list = []
    result_fxy_id_list = []

    # 線分ごとに処理
    for ti in range(len(coordinates_array) - 1):

        # 間隔のながさで構成点数を決定 xy座標のみ設定
        coordinates_counts\
            = get_line_segment_splitting_counts(coordinates_array[ti:ti + 2, :2], interval
                                                )

        # 線分を高密度化
        densified_array\
            = densify_line_segment_given_number(coordinates_array[ti:ti + 2, :], coordinates_counts
                                                )

        # fxy取得
        x_id_array, y_id_array, f_id_array\
            = get_voxel_fxy_id(densified_array[:, 0], densified_array[:, 1], densified_array[:, 2], zoom_level, exponent
                               )

        # 重複削除用オブジェクト配列生成
        fxy_id_object_array\
            = get_voxel_id_object_array(f_id_array, x_id_array, y_id_array
                                        )

        # 結果格納
        result_x_id_list.append(x_id_array.copy())
        result_y_id_list.append(y_id_array.copy())
        result_f_id_list.append(f_id_array.copy())
        result_fxy_id_list.append(fxy_id_object_array.copy())

    # 結果結合
    result_x_id_array = np.concatenate(result_x_id_list)
    result_y_id_array = np.concatenate(result_y_id_list)
    result_f_id_array = np.concatenate(result_f_id_list)
    result_fxy_id_array = np.concatenate(result_fxy_id_list)

    # fxyidの重複削除
    unique_array, unique_index_array, unique_counts_array\
        = get_unique_count_index_array(result_fxy_id_array)

    # 重複削除結果を返す
    return result_x_id_array[unique_index_array], result_y_id_array[unique_index_array], result_f_id_array[unique_index_array]


@jit('Tuple((i8[:],i8[:],i8[:]))(f8[:],f8[:],f8[:],i8,i8)', nopython=True, cache=True, nogil=True)
def get_voxel_fxy_id(longtitude_array, latitude_array, altitude_array, zoom_level=26, exponent=25):
    # 経緯度,標高すべて1次元配列

    # ZoomLevelを指定してタイルの枚数を決定する
    # デフォルトの値(26)は１辺が約50cmになるVoxelになっている
    max_tile_counts = get_max_tile_counts(zoom_level)

    # 高さの最大値取得
    max_height = get_max_height(exponent)

    # 経度をXIDへ変換
    x_id_array\
        = convert_longitude2xid(longtitude_array, max_tile_counts
                                )

    # 緯度をYIDへ変換
    y_id_array\
        = convert_latitude2yid(latitude_array, max_tile_counts
                               )

    # 標高をFIDへ変換
    f_id_array\
        = convert_height2fid(altitude_array, max_tile_counts, max_height
                             )

    return x_id_array, y_id_array, f_id_array


def get_voxel_id_object_array(f_id_array, x_id_array, y_id_array, delimiter='@'):
    # そのまま変換してオブジェクト配列→区切り文字でIDを結合
    # このnumpy配列をuniqueすることにより重複するidを削除する
    # 辞書型配列のKeyやValueに設定できるようにするために使用

    # オブジェクト型配列へ変換
    f_id_object_array = get_object_array(f_id_array)
    x_id_object_array = get_object_array(x_id_array)
    y_id_object_array = get_object_array(y_id_array)

    # 区切り文字とともに結合
    fxy_id_object_array = f_id_object_array + delimiter\
        + x_id_object_array + delimiter\
        + y_id_object_array

    return fxy_id_object_array


def get_object_array(target_array):
    # データ型がobject型のnumpy配列を取得する
    # 複数列のuniqueを取得するときに使用する
    # 引数のnumpy配列は１次元配列

    # 各要素をstrをかけてからオブジェクト型でnumpy配列生成
    return np.array(list(map(str, target_array)), dtype=object)


def get_voxel_edge_degree(lontitude_array, max_tile_counts):
    # 経度全体から1辺あたりの度を算出する

    min_lon = np.min(lontitude_array)
    max_lon = np.max(lontitude_array)

    minmax_xid = convert_longitude2xid(
        np.array([min_lon, max_lon]), max_tile_counts)
    edge_degree = (max_lon - min_lon) / (minmax_xid[1] - minmax_xid[0] + 1)

    return edge_degree


def get_data_definition_index(data_definition_path, data_definition_delimiter=DDC.DELIMITER_COMMA, data_definition_encoding='shift-jis'):
    # 引数1:製品データ定義ファイルパス(.csv or .tsv) 文字列
    # 引数2:製品データ定義ファイルパスの区切り文字種別 1:タブ[\t](.tsv) それ以外:[,]カンマ(.csv) デフォルト値を0設定して何も指定がなければCSVとして読み込むものとする
    # 引数3:製品データ定義ファイルのencoding 日本語を含むので基本的に'shift-jis' デフォルト値に設定しておく 文字列
    # 戻り値1:ジオメトリのファイルタイプList
    # 戻り値2:ジオメトリのDWHファイル名List
    # 戻り値3:ジオメトリの流通項目名List
    # 戻り値4:属性のファイルタイプList
    # 戻り値5:属性のファイル名List
    # 戻り値6:属性の流通項目名List
    # 戻り値7:属性の属性値List

    # 製品データ定義ファイルをすべて文字列として読み込み
    # 区切り文字が[\t](タブ)の時
    if data_definition_delimiter == DDC.DELIMITER_TAB:
        data_definition_dataframe = pd.read_csv(
            data_definition_path, sep='\t', encoding=data_definition_encoding, dtype=str)
    # それ以外の場合、区切り文字をカンマとして読み込む
    else:
        data_definition_dataframe = pd.read_csv(
            data_definition_path, encoding=data_definition_encoding, dtype=str)

    # -----------------------------------------------------------------------------------------------------------
    # ジオメトリ、属性のインデックス取得
    # -----------------------------------------------------------------------------------------------------------
    # ファイルタイプの列取得
    file_type_array = data_definition_dataframe[DDC.DATA_DEFINITION_FILE_TYPE_COLUMN_NAME].to_numpy(
    )

    # 属性項目のファイル名のインデックス
    attribute_name_index = (file_type_array == DDC.ATTRIBUTE_FILE_TYPE) | (
        file_type_array == DDC.CONST_ATTRIBUTE_FILE_TYPE)

    # ジオメトリ項目ファイルのインデックス
    # 先頭の文字列が'+'ならジオメトリタイプとする
    geometry_name_index = np.array(
        [file_type_array[i][0] == '+' for i in range(len(file_type_array))], dtype=np.bool_)
    # -----------------------------------------------------------------------------------------------------------

    # ジオメトリ項目ファイルのジオメトリタイプ取得
    geometry_type_list = list(file_type_array[geometry_name_index])

    # 出力時ジオメトリ項目ファイル名List
    geometry_file_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME][geometry_name_index])

    # 出力時ジオメトリ項目ファイル名List
    geometry_distribution_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME][geometry_name_index])

    # 出力時属性項目ファイル名LIst
    dwh_file_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME][attribute_name_index])

    # 入力ファイル元の属性名List取得
    attribute_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME][attribute_name_index])

    # 属性値List
    attribute_const_value_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME][attribute_name_index])

    # ファイルタイプリスト
    attribute_file_type_list = list(file_type_array[attribute_name_index])

    # 流通項目名List
    all_attribute_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME])

    # DWHファイル名List
    all_dwh_file_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME])
    # -----------------------------------------------------------------------------------------------------------

    return geometry_type_list, geometry_file_name_list, geometry_distribution_name_list, dwh_file_name_list, attribute_name_list, attribute_const_value_list, attribute_file_type_list, all_attribute_name_list, all_dwh_file_name_list


def create_attribute_dataframe(field_set_data_frame, dwh_file_name_list, attribute_name_list, attribute_const_value_list, attribute_file_type_list, geometry_number, encoding='UTF-8', input_file_type=0, feature_id_column_name='地物ID'):
    # データ定義ファイルから属性のdataframeを作成する

    # 属性ファイル名(-1)かき集める ファイル名List→カラム名List
    all_attribute_list = []

    temp_attribute_dataframe = pd.DataFrame()

    for i in range(len(dwh_file_name_list)):

        # ファイルタイプが固定値の場合
        if attribute_file_type_list[i] == '-2':

            index_list = np.arange(0, geometry_number)
            temp_attribute_dataframe[feature_id_column_name] = index_list
            temp_attribute_dataframe[attribute_name_list[i]
                                     ] = attribute_const_value_list[i]
        else:
            # listのdumpを取得する場合
            if input_file_type == 1:

                # field_set_fileから特定のDwh名のある行のValueの値を読み込み、デコード、デシリアライズをし、listを取得
                temp_attribute_list = pickle.loads(base64.b64decode(
                    field_set_data_frame.loc[field_set_data_frame['Dwh'] == dwh_file_name_list[i], 'Value'].values[0]))

                # 属性のリストからdataframeを作成する
                temp_attribute_dataframe = pd.DataFrame(temp_attribute_list, columns=[
                                                        feature_id_column_name, attribute_name_list[i]], dtype=str)

        # 初回の場合の地物IDを属性項目の追加対象にしておく
        # 全ファイルが同じ順序で地物IDを持つとは限らないという前提
        all_attribute_list.append(
            temp_attribute_dataframe[attribute_name_list[i]])

    # 属性のdataframe作成
    all_attribute_dataframe = pd.concat(
        all_attribute_list, axis=1).fillna('None')

    return all_attribute_dataframe


@jit('Tuple((i8[:], i8[:]))(f8[:,:])', nopython=True, cache=True, nogil=True)
def get_start_index_and_end_index(feature_array):
    # 引数の1列目にあるID列を元に始点と終点のインデックスを取得する。
    # 引数:id+xyz座標の2次元numpy配列
    # 戻り値:始点インデックス、終点インデックスの1次元numpy配列

    # 始点と終点のインデックスをそれぞれ取得
    # 判定にはジオメトリごとのidを用いている
    # geom_idの列取得（例：①[0, 0, 0, 1, 1, 2, 2, 2]）
    geom_id_array = feature_array[:, 0:1]

    # 地物が一つしかない場合は始点と終点のインデックスを返す(0と配列の長さ-1)
    if len(np.unique(geom_id_array)) == 1:
        return np.array([0]), np.array([len(geom_id_array) - 1])

    # 始点インデックス取得用配列（例：②[2, 0, 0, 0, 1, 1, 2, 2]）
    geom_id_roll_array1 = np.roll(geom_id_array, 1)

    # 終点インデックス取得用配列（例：③[0, 0, 1, 1, 2, 2, 2, 0]）
    geom_id_roll_array2 = np.roll(geom_id_array, -1)

    # ID列を比較し異なるインデックスリストを取得→始点のインデックス
    # 具体例：①[0, 0, 0, 1, 1, 2, 2, 2]
    # 　　　　②[2, 0, 0, 0, 1, 1, 2, 2]
    # 　　　　→[0, 3, 5]が始点のインデックス
    start_index_array = np.where(
        (geom_id_array == geom_id_roll_array1) == False)[0]

    # ID列を比較し異なるインデックスリストを取得→終点のインデックス
    # 具体例：①[0, 0, 0, 1, 1, 2, 2, 2]
    # 　　　　③[0, 0, 1, 1, 2, 2, 2, 0]
    # 　　　　→[2, 4, 7]が終点のインデックス
    end_index_array = np.where(
        (geom_id_array == geom_id_roll_array2) == False)[0]

    return start_index_array, end_index_array


def _create_voxel_dict(unique_left_values_length,
                       all_attribute_dataframe,
                       target_zoom_level,
                       output_zoom_level,
                       target_f_id_array,
                       target_x_id_array,
                       target_y_id_array):

    # 結果格納用defaultdict
    result_default_dict = defaultdict(list)

    for i in range(len(unique_left_values_length)):
        # 1属性取得
        temp_attribute_list = all_attribute_dataframe.iloc[i].values.tolist()

        # -----------------------------------------------------------------------------------------
        # Key作成
        # 出力対象のZoomLevel26の空間IDを14に変換
        # -----------------------------------------------------------------------------------------
        output_x_id_array, output_y_id_array, output_f_id_array\
            = calculate_level_up_voxel_id(target_x_id_array, target_y_id_array, target_f_id_array, target_zoom_level, output_zoom_level
                                          )

        temp_output_key\
            = get_voxel_id_object_array(output_x_id_array, output_y_id_array, output_f_id_array
                                        )
        # -----------------------------------------------------------------------------------------

        # 出力単位IDをKeyとして空間ID+属性をlistに追加
        [result_default_dict[temp_output_key[ti]].append([target_zoom_level, target_x_id_array[ti], target_y_id_array[ti], target_f_id_array[ti]
                                                          ] + temp_attribute_list) for ti in range(len(temp_output_key))]

    return result_default_dict, output_x_id_array, output_f_id_array, temp_output_key


def get_voxel_id_by_coordinates(coordinates_array, zoom_level=26, exponent=25):
    # 構成点を参照して空間IDを生成する（１地物単位）

    # fxy取得
    x_id_array, y_id_array, f_id_array\
        = get_voxel_fxy_id(coordinates_array[:, 0], coordinates_array[:, 1], coordinates_array[:, 2], zoom_level, exponent
                           )

    # 重複削除用オブジェクト配列生成
    fxy_id_object_array\
        = get_voxel_id_object_array(f_id_array, x_id_array, y_id_array
                                    )

    # fxyidの重複削除
    unique_array, unique_index_array, unique_counts_array\
        = get_unique_count_index_array(fxy_id_object_array)

    # 重複削除結果を返す
    return x_id_array[unique_index_array], y_id_array[unique_index_array], f_id_array[unique_index_array]


def _create_voxel_dict_from_point(target_coordinates_array, start_index_array, end_index_array, all_attribute_dataframe, target_zoom_level, output_zoom_level):
    # 1地物ごとの構成点に対して空間IDを生成する

    # 結果格納用defaultdict
    result_default_dict = defaultdict(list)

    for i in range(len(start_index_array)):
        # 1属性取得
        temp_attribute_list = all_attribute_dataframe.iloc[i].values.tolist()

        temp_start_index = start_index_array[i]
        temp_end_index = end_index_array[i]

        # -----------------------------------------------------------------------------------------
        # 1地物の構成点から空間ID取得
        # -----------------------------------------------------------------------------------------
        target_x_id_array, target_y_id_array, target_f_id_array\
            = get_voxel_id_by_coordinates(target_coordinates_array[temp_start_index:temp_end_index + 1, 1:4],
                                          zoom_level=target_zoom_level)
        # -----------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------------------
        # Key作成
        # 出力対象のZoomLevel26の空間IDを14に変換
        # -----------------------------------------------------------------------------------------
        output_x_id_array, output_y_id_array, output_f_id_array\
            = calculate_level_up_voxel_id(target_x_id_array, target_y_id_array, target_f_id_array, target_zoom_level, output_zoom_level
                                          )
        temp_output_key\
            = get_voxel_id_object_array(output_x_id_array, output_y_id_array, output_f_id_array
                                        )
        # -----------------------------------------------------------------------------------------

        # 出力単位IDをKeyとして空間ID+属性をlistに追加
        [result_default_dict[temp_output_key[ti]].append([target_zoom_level, target_x_id_array[ti], target_y_id_array[ti], target_f_id_array[ti]
                                                          ] + temp_attribute_list) for ti in range(len(temp_output_key))]

    return result_default_dict


def transform_coordinate_using_parameter_file(target_lon_lat_coordinates_array,
                                              par_file_path):

    tky2jgd.load_parameter(par_file_path)
    result_coordinates_array = target_lon_lat_coordinates_array.copy()
    par_result_list = []

    for i in range(len(target_lon_lat_coordinates_array)):

        dB, dL = tky2jgd.bilinear(target_lon_lat_coordinates_array[i, 1], target_lon_lat_coordinates_array[i, 0]
                                  )
        par_result_list.append([dL, dB])

    par_array = np.array(par_result_list)
    par_array = par_array / 3600
    result_coordinates_array[:,
                             0] = target_lon_lat_coordinates_array[:, 0] + par_array[:, 0]
    result_coordinates_array[:,
                             1] = target_lon_lat_coordinates_array[:, 1] + par_array[:, 1]

    return result_coordinates_array


def get_file_path(folder_path, extent):

    pathlib_object = pathlib.Path(folder_path)
    path_list = list(pathlib_object.glob('*.' + extent))

    return path_list


def get_geotiff_coordinates_information_array(geotiff_file_path):

    # 戻り値格納用
    min_x_list = []
    max_x_list = []
    min_y_list = []
    max_y_list = []
    file_name_list = []

    # ファイルごとに処理
    for temp_path in geotiff_file_path:

        # ファイル名格納
        file_name_list.append(temp_path.name)

        # Geotiff読み込む
        gdal_object = gdal.Open(str(temp_path), gdalconst.GA_ReadOnly)

        # 座標の情報取得
        tfw_file_tuple = gdal_object.GetGeoTransform()

        # 値取得
        min_x = tfw_file_tuple[0]
        max_y = tfw_file_tuple[3]

        # ラスタのピクセル数取得
        raster_x_size = gdal_object.RasterXSize
        raster_y_size = gdal_object.RasterYSize

        # 原点からのx移動量を加算
        max_x = min_x + raster_x_size * tfw_file_tuple[1]

        # 原点からのy移動量を加算
        min_y = max_y + raster_y_size * tfw_file_tuple[5]

        # 値格納
        min_x_list.append(min_x)
        max_x_list.append(max_x)
        min_y_list.append(min_y)
        max_y_list.append(max_y)

        # 明示的メモリの解放
        gdal_object = None

    return np.array(min_x_list), np.array(max_x_list), np.array(min_y_list), np.array(max_y_list), np.array(file_name_list)


@jit(b1(f8[:], f8[:, :]), nopython=True, cache=True, nogil=True)
def is_perpendicular_point_segment(point_array, segment_array):
    # 点から線分に向かって垂線が下せるかチェック
    # 旗上げの検知に使えるか調査したい
    # 引数1:ポイント 1次元配列 [x,y,z]
    # 引数2:ライン 2次元配列 [[x1,y1,z2],[x2,y2,z2]]

    # 垂線が下せるか判定
    # 線分の両端点:A,B 垂線の始点:C
    # CA・ABの内積とCB・BAの内積の積の正負によって垂線が引けるか判定する
    inter_product1 = np.dot(
        segment_array[0]-point_array, segment_array[1]-segment_array[0])
    inter_product2 = np.dot(
        segment_array[1]-point_array, segment_array[0]-segment_array[1])

    return inter_product1*inter_product2 >= 0


@jit(f8(f8[:], f8[:]), nopython=True, cache=True, nogil=True)
def get_distance_points(point1, point2):
    # 2点間の距離を算出
    # どちらも１次元配列、xy,xyz問わない

    # 距離計算
    distance = np.sqrt(np.sum(np.power(point1-point2, 2)))

    return distance


@jit(f8(f8[:], f8[:, :]), nopython=True, cache=True, nogil=True)
def get_distance_point_segment_xy(point_array, segment_array):
    # 点から線分までの距離
    # 垂線が下せるかどうか関係なく線分が存在する直線にたいして垂線をおろしてその長さを求める
    # 引数1:ポイント 1次元配列 [x,y,z]
    # 引数2:ライン 1次元配列 [[x1,y1][x2,y2]]

    # 外積の大きさ→平行四辺形の面積
    cross_value = np.abs(cross2d(segment_array[1]-segment_array[0], point_array - segment_array[0]
                                 ))

    # 線分の距離
    line_length = get_distance_points(segment_array[0], segment_array[1])

    # 線分を底辺として割ると高さ（点と直線の距離が出る）
    return cross_value / line_length


@jit(f8(f8[:], f8[:, :]), nopython=True, cache=True, nogil=True)
def get_distance_point_segment_xyz(point_array, segment_array):
    # 点から線分までの距離
    # 垂線が下せるかどうか関係なく線分が存在する直線にたいして垂線をおろしてその長さを求める
    # 引数1:ポイント 1次元配列 [x,y,z]
    # 引数2:ライン 1次元配列 [[x1,y1][x2,y2]]

    # 外積の大きさ→平行四辺形の面積
    cross_value = np.linalg.norm(
        np.cross(segment_array[1]-segment_array[0], point_array - segment_array[0]))

    # 線分の距離
    line_length = get_distance_points(segment_array[0], segment_array[1])

    # 線分を底辺として割ると高さ（点と直線の距離が出る）
    return cross_value / line_length


@jit(f8(f8[:], f8[:, :]), nopython=True, cache=True, nogil=True)
def get_min_distance(point_array, segment_array):
    # 点と直線の最小距離取得

    # 垂線が引けるか判定
    if is_perpendicular_point_segment(point_array, segment_array
                                      ):

        # xy座標の場合
        if len(point_array) == 2:

            # 垂線を下した時の距離取得
            min_distance = get_distance_point_segment_xy(point_array, segment_array
                                                         )

        # それ以外（xyz座標の場合）
        else:
            # 垂線を下した時の距離取得
            min_distance = get_distance_point_segment_xyz(point_array, segment_array
                                                          )

    else:

        # 線分の両端点との距離計算
        # 小さいほうを返す
        distance1 = get_distance_points(point_array, segment_array[0])
        distance2 = get_distance_points(point_array, segment_array[1])

        if distance1 < distance2:
            min_distance = distance1
        else:
            min_distance = distance2

    return min_distance


@jit(f8(f8[:], f8[:, :]), nopython=True, cache=True, nogil=True)
def get_min_distance_point_linestring(point_array, linestring_array):
    # 構成するすべての線分との距離を求める

    # 線分の数
    segment_counts = len(linestring_array)-1

    # 最短距離計算結果格納用配列
    min_distance_array = np.zeros(segment_counts, dtype=np.float64)

    # 線分ごとに最短距離を計算
    for si in prange(segment_counts):

        temp_distance = get_min_distance(
            point_array, linestring_array[si:si+2, :])
        min_distance_array[si] = temp_distance

    return np.min(min_distance_array)


@jit('Tuple((i8[:],i8[:]))(f8[:,:], f8[:,:], i8[:], i8[:])', nopython=True, cache=True, nogil=True)
def get_min_distance_points2linestrings(points_array, linestring_array, start_index_array, end_index_array):
    # 複数のポイントに対して複数のラインとの最短距離をもとめ、最も短い距離を持つラインをペアとする
    # ポイントごとにどのラインをペアとするのか決定する

    # 1点に対してどのラインを紐づけるかのインデックス格納するList
    result_point_index_list = []
    result_linestring_index_list = []

    for pi in prange(len(points_array)):

        # 1点に対して計算される距離のnumpy配列
        # 点に対して複数紐づく場合あるかも（管路同士のつなぎ目が最短距離）
        temp_distance_array = np.zeros(
            len(start_index_array), dtype=np.float64)

        # 1点に対する
        for li in prange(len(start_index_array)):

            temp_min_distance\
                = get_min_distance_point_linestring(points_array[pi], linestring_array[start_index_array[li]:end_index_array[li]+1, :]
                                                    )
            temp_distance_array[li] = temp_min_distance

        # 最短距離を持つラインのインデックス取得
        temp_min = np.min(temp_distance_array)
        temp_min_index = np.where(temp_distance_array == temp_min)[0]

        # インデックス格納
        [result_point_index_list.append(pi)
         for _ in range(len(temp_min_index))]
        [result_linestring_index_list.append(
            temp_min_index[mi]) for mi in range(len(temp_min_index))]

    # 初期配列
    result_point_index_array = np.zeros(
        len(result_point_index_list), dtype=np.int64)
    result_linestring_index_array = np.zeros(
        len(result_linestring_index_list), dtype=np.int64)

    # リストに格納したポイントとラインのインデックスを設定
    for rp in prange(len(result_point_index_list)):
        result_point_index_array[rp] = result_point_index_list[rp]

    for rl in prange(len(result_linestring_index_list)):
        result_linestring_index_array[rl] = result_linestring_index_list[rl]

    return result_point_index_array, result_linestring_index_array


def convert_xy_coordinates_array(target_x_coordinates_array, target_y_coordinates_array, from_crs, to_crs
                                 ):

    # pyprojのインスタンス生成 xy方向を統一
    transform_instance = pyproj.Transformer.from_crs(from_crs, to_crs, always_xy=True
                                                     )

    # 座標系変換
    result_x_array, result_y_array = transform_instance.transform(target_x_coordinates_array, target_y_coordinates_array
                                                                  )

    return result_x_array, result_y_array


def _get_xy_string_id_dict(xy_id_array, unique_xy_index
                           ):

    x_id_object_array = np.array(
        [str(int(xy_id_array[xi, 0])) for xi in range(len(xy_id_array))], dtype=object)
    y_id_object_array = np.array(
        [str(int(xy_id_array[yi, 1])) for yi in range(len(xy_id_array))], dtype=object)
    xy_string_array = x_id_object_array+'@'+y_id_object_array
    unique_xy_string_array = xy_string_array[unique_xy_index]

    xy_id_default_dict = defaultdict(list)
    [xy_id_default_dict[xy_string_array[mi]].append(
        mi) for mi in range(len(xy_string_array))]

    return unique_xy_string_array, xy_id_default_dict


@jit(b1[:](f8[:, :], i8[:], i8[:], f8, f8, f8, f8), nopython=True, cache=True, nogil=True)
def _get_feature_id_around_unit(coordinates_array, start_index_array, end_index_array, unit_min_x, unit_max_x, unit_min_y, unit_max_y):
    # 対象矩形付近に構成点が存在する地物を抽出する
    # 少なくとも1点は図郭の最小から最大の範囲内に構成点が存在する地物のこと
    # 図郭の線上は含める

    # 地物の数だけの判定を設定する
    result_index_bool_array = np.ones(len(start_index_array), dtype=np.bool_)

    # 地物ごとに処理を行う
    for i in prange(len(start_index_array)):

        # 1地物分取得
        temp_coordinates_array = coordinates_array[start_index_array[i]:end_index_array[i]+1, :]

        # x座標 全部図郭の最小値未満の場合、検索対象外とし次の地物へ
        if np.all(temp_coordinates_array[:, 0] < unit_min_x):
            result_index_bool_array[i] = False
            continue

        # x座標 全部図郭の最大値より大きい場合、検索対象外とし次の地物へ
        elif np.all(unit_max_x < temp_coordinates_array[:, 0]):
            result_index_bool_array[i] = False
            continue

        # y座標 全部図郭の最小値未満の場合、検索対象外とし次の地物へ
        elif np.all(temp_coordinates_array[:, 1] < unit_min_y):
            result_index_bool_array[i] = False
            continue

        # y座標 全部図郭の最大値より大きい場合、検索対象外とし次の地物へ
        elif np.all(unit_max_y < temp_coordinates_array[:, 1]):
            result_index_bool_array[i] = False
            continue

        # 上記以外の場合検索対象とする（何もしない）
        else:
            pass

    return result_index_bool_array


def _get_index_array_dict_around_unit(line_array, start_index_array, end_index_array, unique_xy_string_array, left_up_x_line, right_bottom_x_line, right_bottom_y_line, left_up_y_line):
    # 図郭付近に存在する地物のIDを取得

    linestring_id_dict = {}

    for ui in range(len(unique_xy_string_array)):

        # 図郭付近の地物取得
        temp_result_bool\
            = _get_feature_id_around_unit(line_array[:, 1:], start_index_array, end_index_array, left_up_x_line[ui], right_bottom_x_line[ui], right_bottom_y_line[ui], left_up_y_line[ui]
                                          )

        linestring_id_dict[unique_xy_string_array[ui]]\
            = np.where(temp_result_bool)[0]

    return linestring_id_dict


def _get_index_array_by_unit(point_array, line_array, unique_xy_string_array, start_index_array, end_index_array, linestring_id_dict, point_id_array, line_id_array, xy_id_default_dict):
    # 空間IDごとに最近傍結合を行い、検索側のインデックス配列と検索される側のインデックス配列を返却する

    # 各IDごとで最近傍処理を行いポイントに対するラインのIDを取得する
    result_point_index = []
    result_linestring_index = []

    #
    for i in range(len(unique_xy_string_array)):

        # 対象の空間ID
        temp_xy_id = unique_xy_string_array[i]

        # 対象空間ID内に存在する地物のID
        temp_line_id_array = line_id_array[start_index_array[linestring_id_dict[temp_xy_id]]]
        temp_point_id_array = point_id_array[xy_id_default_dict[temp_xy_id]]

        if len(temp_line_id_array) == 0:
            continue
        else:
            pass

        result_point_index_array, result_linestring_index_array\
            = get_min_distance_points2linestrings(point_array[xy_id_default_dict[temp_xy_id], 1:], line_array[:, 1:], start_index_array[linestring_id_dict[temp_xy_id]], end_index_array[linestring_id_dict[temp_xy_id]]
                                                  )

        result_point_index.append(
            temp_point_id_array[result_point_index_array])
        result_linestring_index.append(
            temp_line_id_array[result_linestring_index_array])

    # 結果を結合し1つのインデックスへ
    result_point_index_array = np.concatenate(result_point_index)
    result_linestring_index_array = np.concatenate(result_linestring_index)

    return result_point_index_array, result_linestring_index_array


def get_spatial_id_center_point_from_spatial_id(f_index_array,
                                                x_index_array,
                                                y_index_array,
                                                zoom_level,
                                                exponent=25):
    # 空間IDの配列から中心座標を取得
    # 引数1:fインデックス1次元配列
    # 引数2:xインデックス1次元配列
    # 引数3:yインデックス1次元配列
    # 引数4:ズームレベル
    # 戻り値1:ボクセルの中心Z座標
    # 戻り値2:ボクセルの中心X座標
    # 戻り値3:ボクセルの中心Y座標

    # タイル枚数と高さ上限取得
    max_tile = get_max_tile_counts(zoom_level)
    max_height = get_max_height(exponent)

    # 空間IDのボクセルにおける北西下部と南東上部取得
    north_west_height_array\
        = convert_fid2height(f_index_array,
                             max_tile,
                             max_height)
    south_east_height_array\
        = convert_fid2height(f_index_array+1,
                             max_tile,
                             max_height)

    # idを経緯度座標へ変換 北西の経緯度と南東の経緯度を取得する
    # 北西x座標、南東x座標
    north_west_longtitude_array\
        = convert_xid2longitude(x_index_array,
                                max_tile)
    south_east_longtitude_array\
        = convert_xid2longitude(x_index_array+1,
                                max_tile)

    # 北西y座標、南東y座標
    north_west_latitude_array\
        = convert_yid2latitude(y_index_array,
                               max_tile)
    south_east_latitude_array\
        = convert_yid2latitude(y_index_array+1,
                               max_tile)

    # 中心の座標取得
    center_z = (north_west_height_array+south_east_height_array)/2
    center_lon = (north_west_longtitude_array+south_east_longtitude_array)/2
    center_lat = (north_west_latitude_array+south_east_latitude_array)/2

    return center_z, \
        center_lon, \
        center_lat


def split_string_list(string_list,
                      delimiter):
    # Listに格納された文字列を区切り文字指定でsplitし、numpy配列にする

    result_array\
        = np.array([temp_str.split(delimiter) for temp_str in string_list], dtype=np.float64)

    return result_array


def get_attribute_list(attribute_array):

    # ---------------------------------------------------------------------
    # 属性の１次元配列から2次元配列を作成
    # （フィールド集合ファイルへ設定する用の配列）
    # 引数の想定はdataframe[target_column].to_numpy()の戻り値
    # ---------------------------------------------------------------------

    # データ流通基盤の属性へidの配列生成
    attribute_list = [(i, attribute_array[i])
                      for i in range(len(attribute_array))]

    return attribute_list


def add_attribute_records_to_field_set_file_list(target_dwh_list,
                                                 target_type_list,
                                                 target_value_list,
                                                 target_dataframe,
                                                 target_column_name_list,
                                                 prefix_name=''):

    # ---------------------------------------------------------------------
    # dataframeから指定のカラムを取得しフィールド集合ファイルに設定する
    # ---------------------------------------------------------------------

    # 出力対象のカラムごとに追加
    for temp_column_name in target_column_name_list:

        # 属性の2次元配列取得
        temp_list = get_attribute_list(
            target_dataframe[temp_column_name].to_numpy())

        # DWH名の設定がなければカラム名を設定
        # DWH名の設定があればカラム名を結合して設定
        if prefix_name == '':
            temp_name = temp_column_name
        else:
            temp_name = prefix_name+'_'+temp_column_name

        # 引数で指定したDWH名に属性のカラム名を結合してフィールド集合ファイルへ設定
        target_dwh_list, \
            target_type_list, \
            target_value_list\
            = add_record_to_field_set_file_list(target_dwh_list,
                                                target_type_list,
                                                target_value_list,
                                                temp_name,
                                                'object',
                                                temp_list)

    return target_dwh_list, \
        target_type_list, \
        target_value_list


def get_start_end_duplicated_points_index(target_array):
    # 複数地物の重複点抽出
    # id+xyの２次元配列

    # 始点と終点を取得
    start_index, end_index = get_start_index_and_end_index(target_array)

    # 処理対象のインデックスをすべて取得
    # 全体から見たインデックス
    all_index_array = np.stack((start_index, end_index), axis=1).flatten()
    all_array = target_array[all_index_array]

    # id含めた一意の座標を取得
    unique_array, unique_index_array, unique_counts_array\
        = get_unique_count_index_array(all_array)

    # id含めた一意な座標が全体の配列から見てどこに存在するかのインデックス
    all_index_array = all_index_array[unique_index_array]

    # 同一ID内重複地物を排除
    # 異なるIDは1回しかカウントされない
    temp_unique_array = unique_array[unique_counts_array == 1]
    all_index_array = all_index_array[unique_counts_array == 1]

    # 座標値のみでunique
    temp_unique, temp_index, temp_count\
        = get_unique_count_index_array(temp_unique_array[:, 1:])

    all_index_array = all_index_array[temp_index]

    return all_index_array[temp_count >= 2]


def get_start_end_duplicated_points(target_array):
    # 複数地物の重複点抽出
    # id+xyの２次元配列

    # 異なる地物で始点、終点を共有している点を抽出する
    duplicated_index_array\
        = get_start_end_duplicated_points_index(target_array)

    # 重複する座標抽出
    result_unique_array = target_array[duplicated_index_array, 1:]

    reset_id_array = np.arange(len(result_unique_array)).reshape(
        (len(result_unique_array), 1))

    # 新しいIDの配列を結合
    result_unique_array = np.concatenate(
        [reset_id_array, result_unique_array], axis=1)

    return result_unique_array


def _update_image_array_by_duplicated_points(image_array, target_lon_lat_array, x_unit, y_unit, left_up_min_lon, left_up_max_lat
                                             ):
    # 2点以上の重複する点に対して白色を付ける

    # 更新対象画像配列
    result_image_array = image_array.copy()
    temp_lon_lat_array = target_lon_lat_array.copy()

    # 更新対象座標
    temp_lon_lat_array = get_start_end_duplicated_points(temp_lon_lat_array)

    # 座標から画像に対するインデックス算出
    temp_lon_lat_array[:, 1] = temp_lon_lat_array[:, 1]-left_up_min_lon
    temp_lon_lat_array[:, 2] = temp_lon_lat_array[:, 2]-left_up_max_lat
    pixel_x_index_array = np.floor(
        temp_lon_lat_array[:, 1]/x_unit).astype(np.int64)
    pixel_y_index_array = np.floor(
        temp_lon_lat_array[:, 2]/-y_unit).astype(np.int64)

    # 対象のインデックスを白で更新
    result_image_array[pixel_y_index_array, pixel_x_index_array] = 255
    result_image_array[pixel_y_index_array+1, pixel_x_index_array] = 255
    result_image_array[pixel_y_index_array-1, pixel_x_index_array] = 255
    result_image_array[pixel_y_index_array, pixel_x_index_array+1] = 255
    result_image_array[pixel_y_index_array, pixel_x_index_array-1] = 255

    return result_image_array
