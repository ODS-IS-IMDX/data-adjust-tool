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

# 標準ライブラリ
import datetime
import math

# 外部ライブラリの動的インポート
from importlib import import_module

tripy = import_module("tripy")
np = import_module("numpy")
jit = import_module("numba").jit
f8 = import_module("numba").f8
prange = import_module("numba").prange
void = import_module("numba").void
b1 = import_module("numba").b1
i8 = import_module("numba").i8
Delaunay = import_module("scipy.spatial").Delaunay
pd = import_module("pandas")
cross2d=import_module("numba.np.extensions").cross2d

@jit(f8[:,:](f8[:,:], f8),nopython=True,cache=True,nogil=True)
def rotate_coordinates_around_y_axis(xyz_coordinates_array, rotation_radian):
    """
    概要   : Y軸周りの反時計回りでxz座標を回転させる
    引数1  : 回転対象xyz座標 2次元配列 回転対象ポイント>xyz座標
    引数2  : 回転角(単位:ラジアン)float64
    戻り値 : 回転後xyz座標 2次元配列 回転対象ポイント>xyz座標
    """
    # 回転行列作成
    rotation_matrix = np.array([[ np.cos(rotation_radian),         0.0, np.sin(rotation_radian)]
                              , [                     0.0,         1.0,                     0.0]
                              , [-np.sin(rotation_radian),         0.0, np.cos(rotation_radian)]])

    # xyz座標に適用
    return rotation_matrix.dot(xyz_coordinates_array.T).T

@jit(f8[:,:](f8[:,:], f8),nopython=True,cache=True,nogil=True)
def rotate_coordinates_around_z_axis(xyz_coordinates_array, rotation_radian):
    """
    概要   : Z軸周りの反時計回りでxy座標を回転させる
            Y軸をX軸に向ける方向
    引数   : 回転対象xyz座標 2次元配列 回転対象ポイント>xyz座標
    引数   : 回転角(単位:ラジアン)float64
    戻り値 : 回転後xyz座標 2次元配列 回転対象ポイント>xyz座標
    """
    # 回転行列作成
    rotation_matrix = np.array([[ np.cos(rotation_radian), -np.sin(rotation_radian),       0.0]
                              , [ np.sin(rotation_radian),  np.cos(rotation_radian),       0.0]
                              , [                     0.0,                      0.0,       1.0]])
    
    # xyz座標に適用
    return rotation_matrix.dot(xyz_coordinates_array.T).T

@jit(f8[:](f8[:, :]), nopython=True, cache=True, nogil=True)
def get_distance(target_array):
    """
    ---------------------------------------------------------------------------------------------------------------
    複数の構成点に対する汎用処理
    ---------------------------------------------------------------------------------------------------------------
    概要   : 構成点間の距離を計算する 連続する同一構成点は考慮していない
    引数   ：xy座標 or xyz座標（[構成点1, 構成点2, …, 構成点n]、構成点n = [x, y] あるいは[x, y, z]）
    戻り値 ： 構成点間の距離一次元配列（[距離1, 距離2, … 距離n]、距離n = 構成点n-1から構成点nまでの距離）
    """
    # 次の構成点の配列（最後の要素は始点）
    # 次点表現用のroll数
    roll_number = -target_array.shape[1]
    target_array_next = np.roll(target_array, roll_number)
    
    # xy間の距離を算出
    # 算出ロジック：xy座標の差→差の2乗→和→平方根
    target_distance_array = np.sqrt(np.sum(np.power(target_array_next - target_array,2),axis = 1))
    
    # 最後の要素は始点と終点の距離になっているため不要 最後の要素以外をスライスで抽出
    target_distance_array = target_distance_array[0:target_distance_array.size - 1]
    
    return target_distance_array

@jit(f8[:](f8[:, :], i8), nopython=True, cache=True, nogil=True)
def get_distance_to_start_end_point(target_array, index):
    """
    概要   : 指定したインデックスから始点、終点までの距離の和を計算する
    例　   ：点Aから点Dの配列で点Bを指定する 戻り値は以下のようになる
            [-(点B→点Aまでの距離),0(点B→点B),点B→点Cまでの距離,点B→点C→点Dまでの距離]
            [-1 ,0 ,3 ,7]
    引数1  ：xy座標 or xyz座標（[構成点1, 構成点2, …, 構成点n]、構成点n = [x, y] あるいは[x, y, z]）
    戻り値 ：構成点間の距離一次元配列（[距離1, 距離2, … 距離n]、距離n = 構成点n-1から構成点nまでの距離）
    """
    # 戻り値初期配列 構成点の数分距離の和が発生する
    result_distance_to_start_end_point_array = np.zeros(len(target_array))

    # 構成点間の距離取得
    target_distance_array = get_distance(target_array)

    # 始点から指定インデックスまでの距離の総和→★こちらはすべて負
    distance_from_start_to_index_array = -np.array([np.sum(target_distance_array[i:index]) for i in range(0,index)])

    # 戻り値用配列に更新
    result_distance_to_start_end_point_array[0:index] = distance_from_start_to_index_array

    # 指定インデックスから終点までの距離の総和
    distance_from_index_to_end_array =  np.array([np.sum(target_distance_array[index:i]) for i in range(index,len(target_array))])

    # 戻り値用配列に更新
    result_distance_to_start_end_point_array[index:] = distance_from_index_to_end_array

    return result_distance_to_start_end_point_array

@jit(f8[:,:,:](f8[:,:], f8[:,:]), nopython=True, cache=True, nogil=True)
def get_side_tinz_coordinates(wire_frame_array1, wire_frame_array2):
    '''
    ワイヤーフレーム2本からTINZを返す処理
    引数1:ワイヤーフレーム1 2次元配列 [ワイヤーフレームの構成点]>[xyz座標] np.float64
    引数2:ワイヤーフレーム2 2次元配列 [ワイヤーフレームの構成点]>[xyz座標] np.float64
    　　　※※※注意※※※本管の始点から見て左を0、右を1とする 「0,1,次の点」を繰り返すことで作成する三角形の回転方向は左回りとなり外積は外側に向く
    戻り値:TINZを表すnumpy配列 3次元配列 [TINZ]>[TINZの構成点]>[構成点のxyz座標] np.float64
    '''
    # ワイヤーフレーム1の始点から各構成点までの総距離を求める
    distance_array1 = get_distance_to_start_end_point(wire_frame_array1, 0)[1:]

    # ワイヤーフレーム2の始点から各構成点までの総距離を求める
    distance_array2 = get_distance_to_start_end_point(wire_frame_array2, 0)[1:]

    # 総距離比較インデックス
    index1 = 0
    index2 = 0

    # ★インデックス最大値
    max_index1 = len(wire_frame_array1)-1
    max_index2 = len(wire_frame_array2)-1

    # TINの数、ループの回数
    tin_counts = len(wire_frame_array1) + len(wire_frame_array2) - 2

    # 結果格納用3次元配列 [TINの数]>[TINの構成点3点]>[xyz座標]
    result_array = np.zeros((tin_counts, 3, 3), dtype = np.float64)

    # 構成点-2回ループする 1回の操作ごとに始点を終点に向かって必ず動かすから作成されるTINの数も決定する
    for i in range(tin_counts):

        # 次点までの距離の総和を比較
        # ワイヤーフレーム1が大きい場合
        if distance_array1[index1] > distance_array2[index2]:

            # 1の始点、2の始点、2の次のインデックスの構成点を結果に格納
            # ★少し冗長になるが共有辺を一致させるためにif文の内部へ入れる
            # ★前後のTINにおいて共有辺を明らかにすることでTRIANGLE_STRIPの形になる Multipatchへの変換に必須
            result_array[i,0,] = wire_frame_array1[index1]
            result_array[i,1,] = wire_frame_array2[index2]

            # 短い方のインデックスを次に進める
            if index2 + 1 > max_index2:

                # 反対側のワイヤーフレームの進める
                # 反対側のワイヤーフレームのインデックスを進めたときに最大のインデックスは超えることはないのでチェックの必要なし
                # なぜならワイヤーフレーム両方がインデックスの最大値に達しているときでありループの終了であるから
                result_array[i,2,] = wire_frame_array1[index1 + 1]
                index1 = index1 + 1

            else:

                # 短い方のインデックスを進める
                result_array[i,2,] = wire_frame_array2[index2 + 1]
                index2 = index2 + 1

        # ワイヤーフレーム1がワイヤーフレーム2以下の場合
        else:

            # 1の始点、2の始点、1の次のインデックスの構成点を結果に格納
            result_array[i,0,] = wire_frame_array1[index1]
            result_array[i,1,] = wire_frame_array2[index2]

            # 短い方のインデックスを次に進める
            if index1 + 1 > max_index1:

                # 反対側のワイヤーフレームの進める
                # 反対側のワイヤーフレームのインデックスを進めたときに最大のインデックスは超えることはないのでチェックの必要なし
                # なぜならワイヤーフレーム両方がインデックスの最大値に達しているときでありループの終了であるから
                result_array[i,2,] = wire_frame_array2[index2 + 1]
                index2 = index2 + 1

            else:

                # 短い方のインデックスを進める
                result_array[i,2,] = wire_frame_array1[index1 + 1]
                index1 = index1 + 1

    return result_array

@jit(f8[:,:,:,:](f8[:,:], f8, f8), nopython=True, cache=True, nogil=True)
def get_wire_frame_parts_coordinates(xyz_array, width, depth):

    '''
    ワイヤーフレームパーツ作成関数
    引数1:地物のxyz座標 2次元配列 np.float64
    引数2:幅(単位は比較元の線分と合わせること) np.float64
    引数3:深さ(単位は比較元の線分と合わせること) np.float64
    戻り値：ワイヤーフレームパーツ 4次元配列 [ワイヤーフレームパーツ]>[パーツの線分]>[線分の2点]>[xyz座標] np.float64
    '''
    # 結果格納用配列
    # [ワイヤーフレームパーツ](1地物あたり4本)
    # >[パーツの線分](1地物の構成点-1)
    # >[線分の2点](2)
    # >[xyz座標](3)
    result_array = np.zeros((4,len(xyz_array)-1,2,3), )

    # 地物の線分の数だけループ
    # li:LineStringのindexのこと
    for linestring_index in range(len(xyz_array)-1):

        # 線分取得
        temp_linestring_array = xyz_array[linestring_index:linestring_index+2,:]

        # 始点を原点とした位置に線分を平行移動
        translation_amount = temp_linestring_array[0].copy()
        temp_linestring_array = temp_linestring_array - translation_amount

        # xy平面上から見た、線分とx軸がなす角度算出
        # 始点に対して終点のX座標が0の場合 Z軸上に検索対象が存在することになる
        radian1 = np.arctan2(temp_linestring_array[1,1] ,temp_linestring_array[1,0])

        # 検索対象の終点、検索条件を、Z軸で回転させXZ平面上に移動する
        # Y軸をX軸に向ける方向
        temp_linestring_array = rotate_coordinates_around_z_axis(temp_linestring_array, -radian1)

        # xz平面上から見た、線分とx軸がなす角度算出
        # 始点に対して終点のX座標が0の場合回転処理はスキップ
        radian2 = np.arctan2((temp_linestring_array[1,2] - temp_linestring_array[0,2]) ,(temp_linestring_array[1,0] - temp_linestring_array[0,0]))

        # 検索対象の終点、検索条件を、Y軸で回転させX軸上に移動する
        # Z軸をX軸に向ける方向
        temp_linestring_array = rotate_coordinates_around_y_axis(temp_linestring_array, radian2)

        # 始点終点からそれぞれ4点ずつ生成
        # [0:4,:]→始点から生成した4点
        # [4:,:]→終点から生成した4点
        coordinate_array = np.zeros((8,3), dtype = np.float64)

        # 始点終点平行移動用配列
        array1 = np.array([0,  width/2,  depth/2] ,dtype = np.float64)
        array2 = np.array([0,  width/2, -depth/2] ,dtype = np.float64)
        array3 = np.array([0, -width/2, -depth/2] ,dtype = np.float64)
        array4 = np.array([0, -width/2,  depth/2] ,dtype = np.float64)

        # 1本目の始点終点
        coordinate_array[0] = temp_linestring_array[0] + array1
        coordinate_array[4] = temp_linestring_array[1] + array1

        # 2本目の始点終点
        coordinate_array[1] = temp_linestring_array[0] + array2
        coordinate_array[5] = temp_linestring_array[1] + array2

        # 3本目の始点終点
        coordinate_array[2] = temp_linestring_array[0] + array3
        coordinate_array[6] = temp_linestring_array[1] + array3

        # 4⃣本目の始点終点
        coordinate_array[3] = temp_linestring_array[0] + array4
        coordinate_array[7] = temp_linestring_array[1] + array4

        # 元の位置に戻す
        # -radian2 Y軸回転
        coordinate_array = rotate_coordinates_around_y_axis(coordinate_array, -radian2)

        # radian1 Z軸回転
        coordinate_array = rotate_coordinates_around_z_axis(coordinate_array, radian1)

        # 平行移動量を戻す
        coordinate_array = translation_amount + coordinate_array

        # 結果格納用配列に更新
        # 1本目>線分番目>始点[0]と終点[1]
        result_array[0, linestring_index, 0] = coordinate_array[0]
        result_array[0, linestring_index, 1] = coordinate_array[4]

        # 2本目>線分番目>始点[0]と終点[1]
        result_array[1, linestring_index, 0] = coordinate_array[1]
        result_array[1, linestring_index, 1] = coordinate_array[5]

        # 3本目>線分番目>始点[0]と終点[1]
        result_array[2, linestring_index, 0] = coordinate_array[2]
        result_array[2, linestring_index, 1] = coordinate_array[6]

        # 4本目>線分番目>始点[0]と終点[1]
        result_array[3, linestring_index, 0] = coordinate_array[3]
        result_array[3, linestring_index, 1] = coordinate_array[7]

    return result_array

@jit(f8[:](f8[:,:], f8[:], f8), nopython=True, cache=True, nogil=True)
def get_particular_length_perpendicular(linestring_array, point_array, length):

    '''
    概要  : 点から線分に向かって特定の長さで垂線の足を延ばした座標を取得する
    引数  :垂線を下す対象の線分 2次元配列 [線分を表す2点]>[xyz座標] np.float64
    引数  :垂線の始点 1次元配列 [xyz座標] np.float64
    引数  :垂線の長さ np.float64
    戻り値:垂線の終点 1次元配列 [xyz座標] np.float64
    '''
    # 垂線は線分上の点Pを媒介変数表示を用いて求める
    # 線分の方向ベクトル
    direction_vector_array = linestring_array[1] - linestring_array[0]

    # 媒介変数にかかる係数の合計(左辺)
    constant_sum = (linestring_array[0,0]-point_array[0]) * direction_vector_array[0] + (linestring_array[0,1]-point_array[1]) * direction_vector_array[1] + (linestring_array[0,2]-point_array[2]) * direction_vector_array[2]

    # 定数の合計(右辺)
    coefficient_sum = -np.sum(np.power(direction_vector_array,2))

    # 暫定対処 小数点の精度判定をもってzerodivisionerrorは回避する
    if coefficient_sum == 0:

        return linestring_array[1]

    else:
        # 媒介変数取得
        parameter = constant_sum / coefficient_sum

        # 点Pの座標から垂線の足の座標取得
        perpendicular =  linestring_array[0] + direction_vector_array * parameter

        # 垂線の方向ベクトル
        perpendicular_direction_vector_array = perpendicular - point_array

        # 方向ベクトルの長さ
        perpendicular_direction_vector_length = np.sqrt(np.sum(np.power(perpendicular_direction_vector_array,2)))

        # 点 + 方向ベクトル * (引数で指定した長さ/方向ベクトルの長さ)←1より大きいはず
        result_point = point_array + perpendicular_direction_vector_array * (length / perpendicular_direction_vector_length)

    return result_point

@jit(f8[:,:](f8[:,:,:], f8[:,:], f8), nopython=True, cache=True, nogil=True)
def interpolate_wire_frame_parts(wire_frame_parts_array, center_linestring_array, interpolation_distance):
    '''
    #ワイヤーフレームパーツ補間関数 (1本ずつ)
    引数1:ワイヤーフレームのパーツ 3次元配列 [ワイヤーフレームパーツの数]>[線分を表す2点]>[xyz座標] np.float64
    引数2:中心線のxyz座標 2次元配列 [中心線構成点]>[xyz座標] np.float64
    引数3:補間距離 np.float64
    戻り値:ワイヤーフレーム 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    ★このワイヤーフレームは2次元のほうが使い勝手が良いのではないか ワイヤーフレームパーツは3次元で表す方がよい 点の組み合わせだからどちらかというとMultiLineStringZの概念になる
    '''

    # 結果格納用List
    result_list = []

    # 垂線を下す対象線分の座標 初期配列 [線分を表す2点]>[xyz座標]
    target_linestring_array = np.zeros((2,3), dtype = np.float64)

    # 格納対象座標配列 初期配列 [線分を表す2点]>[xyz座標]

    # 始点追加
    result_list.append(wire_frame_parts_array[0,0].copy())

    # 中心線の隣接線分の組み合わせ分処理（構成点-2）
    # ci:組み合わせのインデックス
    for ci in range(len(center_linestring_array)-2):

        # 垂線を下す対象線分の座標
        target_linestring_array[0] = wire_frame_parts_array[ci,1,]
        target_linestring_array[1] = wire_frame_parts_array[ci+1,0,]

        # 終点1と始点2を結ぶ線に中心線の節点から垂線を補間距離の長さだけ下す
        # その足を線分補間構成点とする ここは関数化
        interpolate_point = get_particular_length_perpendicular(target_linestring_array, center_linestring_array[ci+1], interpolation_distance)

        # 終点1と補完点が近い場合補完点のみ追加
        # それ以外の場合は終点と補完点
        distance = np.sqrt(np.sum(np.power(wire_frame_parts_array[ci,1,] - interpolate_point, 2)))

        if distance < 1:

            result_list.append(interpolate_point.copy())
        else:
            result_list.append(wire_frame_parts_array[ci,1].copy())
            result_list.append(interpolate_point.copy())

        distance2 = np.sqrt(np.sum(np.power(wire_frame_parts_array[ci+1,0] - interpolate_point, 2)))

        # 補完点と始点2が近い場合終点2のみ追加
        # それ以外の場合は始点2終点2追加
        if distance2 < 1:
            pass

        else:
            result_list.append(wire_frame_parts_array[ci+1,0].copy())

    # ★終点追加
    result_list.append(wire_frame_parts_array[-1,1].copy())


    # 返却用配列

    result_array = np.zeros((len(result_list),3), np.float64)

    # 結果格納用Listから線分の座標arrayを取り出して一つずつnumpy配列更新
    for rli in range(len(result_list)):
        result_array[rli] = result_list[rli]

    return result_array

@jit(f8(f8[:], f8[:], f8[:]), nopython=True, cache=True, nogil=True)
def perpendicular_distance_3d(point, line_start, line_end):
    """
    3次元空間における点から直線までの垂直距離を計算する。

    :param point: 点の座標を表す長さ3の配列 [x, y, z]。
    :type point: numpy.ndarray
    :param line_start: 直線の始点を表す長さ3の配列 [x, y, z]。
    :type line_start: numpy.ndarray
    :param line_end: 直線の終点を表す長さ3の配列 [x, y, z]。
    :type line_end: numpy.ndarray

    :return: 点から直線までの垂直距離。
    :rtype: float
    """
    # 始点から終点へのベクトルを計算
    line_vec = line_end - line_start

    # 始点から点へのベクトルを計算
    point_vec = point - line_start

    # 直線の長さを計算
    line_len = np.sqrt(line_vec[0]**2 + line_vec[1]**2 + line_vec[2]**2)

    # 直線が点の場合（長さが0）の処理
    if line_len == 0:
        # 始点と点の間のユークリッド距離を返す
        return np.sqrt((point[0] - line_start[0])**2 +
                       (point[1] - line_start[1])**2 +
                       (point[2] - line_start[2])**2)

    # 直線の単位ベクトルを計算
    line_unit_vec = line_vec / line_len

    # 点のベクトルを直線に射影したスカラー値を計算
    projection = point_vec[0] * line_unit_vec[0] + \
                 point_vec[1] * line_unit_vec[1] + \
                 point_vec[2] * line_unit_vec[2]

    # 直線上の点（最も近い点）の座標を計算
    closest_point = line_start + projection * line_unit_vec

    # 点と直線上の最も近い点の間の距離を計算
    distance = np.sqrt((point[0] - closest_point[0])**2 +
                       (point[1] - closest_point[1])**2 +
                       (point[2] - closest_point[2])**2)

    return distance


@jit(f8[:, :](f8[:, :], f8), nopython=True, cache=True, nogil=True)
def douglas_peucker_3d(points, tolerance):
    """
    3次元空間におけるDouglas-Peuckerアルゴリズムを適用し、線の簡略化を行う。

    :param points: 3次元座標のリスト。形状は(N, 3)で、各行が[x, y, z]形式の座標を表す。
    :type points: numpy.ndarray
    :param tolerance: 簡略化の許容誤差。この値よりも大きい偏差を持つ点のみが保持される。
    :type tolerance: float

    :return: 簡略化された3次元座標のリスト。形状は(N', 3)で、N'は保持された点の数。
    :rtype: numpy.ndarray
    """

    # セグメント間の距離を計算してリストに格納
    segment_distances = []
    for i in range(len(points) - 1):
        dx = points[i + 1, 0] - points[i, 0]
        dy = points[i + 1, 1] - points[i, 1]
        dz = points[i + 1, 2] - points[i, 2]
        distance = np.sqrt(dx ** 2 + dy ** 2 + dz ** 2)
        segment_distances.append(distance)

    # スタックに開始点と終了点のインデックスを追加
    stack = [(0, len(points) - 1)]

    # 保持する点を示すフラグ配列（0: 削除, 1: 保持）
    keep = np.zeros(points.shape[0], dtype=np.int8)
    keep[0] = 1  # 開始点を保持
    keep[-1] = 1  # 終了点を保持

    # スタックが空になるまでループ
    while stack:
        # スタックから区間のインデックスを取得
        start, end = stack.pop()
        max_distance = 0.0  # 最大距離
        index_of_max = -1  # 最大距離を持つ点のインデックス

        # 開始点と終了点の間のすべての点について距離を計算
        for i in range(start + 1, end):
            # 点iと開始点および終了点との垂直距離を計算
            distance = perpendicular_distance_3d(points[i], points[start], points[end])

            # 最大距離を更新
            if distance > max_distance:
                max_distance = distance
                index_of_max = i

        # 最大距離が許容誤差を超える場合、その点を保持
        if max_distance > tolerance:
            keep[index_of_max] = 1  # 点を保持
            # 新しい区間をスタックに追加
            stack.append((start, index_of_max))
            stack.append((index_of_max, end))

    # 保持する点のみを抽出して結果を返す
    result = points[keep == 1]
    return result


@jit(f8[:, :](f8[::1], f8[::1], f8), nopython=True, cache=True, nogil=True)
def calculate_parallel_coordinates(p1, p2, distance):
    """
    与えられた2点から平行線の座標を計算する。

    :param p1: 始点の座標
    :type p1: numpy.ndarray
    :param p2: 終点の座標
    :type p2: numpy.ndarray
    :param distance: 平行線との距離
    :type distance: float

    :return: 平行線の始点と終点 (2, 3)
    :rtype: numpy.ndarray
    """
    dx, dy = p2[0] - p1[0], p2[1] - p1[1]
    length = math.sqrt(dx ** 2 + dy ** 2)

    # 方向ベクトルの正規化
    dx, dy = dx / length, dy / length

    # 垂直ベクトルを計算（90度回転）
    perp_dx, perp_dy = -dy, dx

    # 平行線の座標を計算
    result = np.zeros((2, 3), dtype=np.float64)
    result[0] = np.ascontiguousarray(p1 + np.array([perp_dx * distance, perp_dy * distance, 0]))
    result[1] = np.ascontiguousarray(p2 + np.array([perp_dx * distance, perp_dy * distance, 0]))

    return result


@jit(f8[:, :](f8[:]), nopython=True, cache=True, nogil=True)
def create_rotation_matrix_to_align_x(normal):
    """
    任意の法線ベクトルをX軸に整列させる回転行列を生成する。

    :param normal: 3Dの法線ベクトル。
    :type normal: numpy.ndarray

    :return: 回転行列
    :rtype: numpy.ndarray
    """
    x_axis = np.array([1.0, 0.0, 0.0])  # X軸
    v = np.array([normal[1] * x_axis[2] - normal[2] * x_axis[1],
                  normal[2] * x_axis[0] - normal[0] * x_axis[2],
                  normal[0] * x_axis[1] - normal[1] * x_axis[0]], dtype=np.float64)  # 外積
    s = np.sqrt(v[0]**2 + v[1]**2 + v[2]**2)  # 外積の長さ
    c = normal[0] * x_axis[0] + normal[1] * x_axis[1] + normal[2] * x_axis[2]  # 内積

    if s == 0.0:
        return np.eye(3, dtype=np.float64)  # すでにX軸と一致している場合

    vx = np.array([[0.0, -v[2], v[1]],
                   [v[2], 0.0, -v[0]],
                   [-v[1], v[0], 0.0]], dtype=np.float64)  # 外積行列

    return np.eye(3, dtype=np.float64) + vx + (vx @ vx) * ((1 - c) / (s ** 2))


@jit(f8[:, :](f8[:]), nopython=True, cache=True, nogil=True)
def create_rotation_matrix_to_align_z(normal):
    """
    任意の法線ベクトルをZ軸に整列させる回転行列を生成する。

    :param normal: 3Dの法線ベクトル
    :type normal: numpy.ndarray

    :return: 回転行列
    :rtype: numpy.ndarray
    """
    z_axis = np.array([0.0, 0.0, 1.0], dtype=np.float64)  # Z軸のベクトル
    v = np.array([
        normal[1] * z_axis[2] - normal[2] * z_axis[1],
        normal[2] * z_axis[0] - normal[0] * z_axis[2],
        normal[0] * z_axis[1] - normal[1] * z_axis[0]
    ], dtype=np.float64)  # 外積

    s = np.sqrt(v[0]**2 + v[1]**2 + v[2]**2)  # 外積の長さ
    c = normal[0] * z_axis[0] + normal[1] * z_axis[1] + normal[2] * z_axis[2]  # 内積

    if s == 0.0:
        return np.eye(3, dtype=np.float64)  # すでにZ軸と一致している場合

    vx = np.array([[0.0, -v[2], v[1]],
                   [v[2], 0.0, -v[0]],
                   [-v[1], v[0], 0.0]], dtype=np.float64)  # 外積行列

    return np.eye(3, dtype=np.float64) + vx + (vx @ vx) * ((1 - c) / (s ** 2))


@jit(f8[:, :](f8[:, :], f8), nopython=True, cache=True, nogil=True)
def parallel_offset(line_coords, distance):
    """
    指定された線分に平行な新しい線分を作成する。
    Z座標のみが変化している場合は、軸を回転させて平行線を生成し、元の座標系に戻す。

    :param line_coords: ２点で構成される線分の座標 [[x1, y1, z1], [x2, y2, z2]]
    :type line_coords: numpy.ndarray
    :param distance: 平行線と線分の距離
    :type distance: float

    :return: 新しい平行線の座標 [[x1', y1', z1], [x2', y2', z2]]
    :rtype: numpy.ndarray
    """
    # 座標を抽出(メモリ上のX,Y,Zの並びをascontiguousarrayで整列)
    p1 = np.ascontiguousarray(line_coords[0])
    p2 = np.ascontiguousarray(line_coords[1])

    # XY座標が同一かどうか判定
    if abs(p1[0] - p2[0]) < 1e-8 and abs(p1[1] - p2[1]) < 1e-8:  # Zだけが変化している場合
        direction = p2 - p1
        rotation_matrix = np.ascontiguousarray(create_rotation_matrix_to_align_x(direction))
        inverse_rotation_matrix = np.ascontiguousarray(np.linalg.inv(rotation_matrix))

        # 座標を回転して処理
        p1_rotated = np.dot(rotation_matrix, np.ascontiguousarray(p1))
        p2_rotated = np.dot(rotation_matrix, np.ascontiguousarray(p2))
        offsets = calculate_parallel_coordinates(p1_rotated, p2_rotated, distance)

        # 元の座標系に戻す
        result = np.empty((2, 3), dtype=np.float64)
        result[0] = np.dot(inverse_rotation_matrix, np.ascontiguousarray(offsets[0]))
        result[1] = np.dot(inverse_rotation_matrix, np.ascontiguousarray(offsets[1]))
    else:
        # 通常の処理
        result = calculate_parallel_coordinates(p1, p2, distance)

    return result


@jit(f8[:, :, :](f8[:, :], f8), nopython=True, cache=True, nogil=True)
def create_parallel_lines(lines, horizontal):
    """
    線分群に基づき、指定距離の右側または左側に平行線を生成する関数。

    :param lines: 元の3次元座標リスト（形状: N×3）。各行が[x, y, z]形式の座標を表す。
    :type lines: numpy.ndarray
    :param horizontal: 平行線のオフセット距離（正: 右側, 負: 左側）。
    :type horizontal: float

    :return: 平行線のリスト（形状: M×2×3）
    :rtype: numpy.ndarray
    """
    n_segments = len(lines) - 1
    result_lines = np.zeros((n_segments, 2, 3), dtype=np.float64)

    for i in prange(n_segments):
        # 現在のセグメントの始点と終点を取得
        segment = np.empty((2, 3), dtype=np.float64)
        segment[0] = lines[i]
        segment[1] = lines[i + 1]

        # parallel_offsetを使用して平行線を計算
        result_lines[i] = parallel_offset(segment, horizontal)

    return result_lines


@jit(b1(f8[:, :], f8), nopython=True, cache=True, nogil=True)
def is_z_only_variation(coords_array, tol):
    """
    XY座標がほぼ変化せず、Z座標だけが変化している場合に True を返す。

    :param coords_array: 中心線を構成する3D座標の配列 (N, 3)
    :param tol: float, XY座標の変化を許容する誤差の範囲
    :return: bool, Zのみ変化していれば True
    """
    n_points = coords_array.shape[0]
    xy_first_x, xy_first_y = coords_array[0, 0], coords_array[0, 1]
    z_first = coords_array[0, 2]

    xy_consistent = True  # XYが一定かどうか
    z_different = False   # Zが変化しているかどうか

    for i in range(1, n_points):
        xy_current_x, xy_current_y = coords_array[i, 0], coords_array[i, 1]
        z_current = coords_array[i, 2]

        # XY座標の誤差範囲内の変化を許容
        if abs(xy_current_x - xy_first_x) > tol or abs(xy_current_y - xy_first_y) > tol:
            xy_consistent = False
            break  # XYが変化した時点で終了

        # Z座標が異なる場合を検出
        if abs(z_current - z_first) > tol:
            z_different = True

    return xy_consistent and z_different


@jit((f8[:, :], f8), nopython=True, cache=True, nogil=True)
def generate_offset_lines(coords_array, distance):
    """
    座標列に基づき、上下方向のオフセットラインを生成する。
    Z座標のみが変化している場合はZ軸に沿ったオフセットを適用し、
    それ以外の場合は進行方向に基づいて上下方向を計算して適用する。

    :param coords_array: 中心線を構成する3D座標の配列。
    :type coords_array: numpy.ndarray
    :param distance: 上下の座標を生成するオフセット距離。
    :type distance: float

    :return: 真上の座標リスト, 真下の座標リスト
    :rtype: tuple[numpy.ndarray, numpy.ndarray]
    """

    n_points = len(coords_array)

    if n_points < 2:
        raise ValueError("少なくとも2つの座標が必要です。")

    # Z軸方向のみにオフセットを適用すべきか判定
    # Z座標以外に変化がある場合は基本的にこっち
    if not is_z_only_variation(coords_array, 1e-8):
        # Z軸方向にオフセットを適用
        upper_array = coords_array.copy()
        lower_array = coords_array.copy()
        upper_array[:, 2] += distance
        lower_array[:, 2] -= distance
    else:
        # 進行方向に基づいて上下方向を計算してオフセットを適用
        upper_array = np.zeros_like(coords_array)
        lower_array = np.zeros_like(coords_array)

        for i in range(n_points - 1):
            p1, p2 = coords_array[i], coords_array[i + 1]
            direction = p2 - p1
            direction /= np.linalg.norm(direction)  # 正規化

            # 上下方向のベクトルを計算
            z_axis = np.array([0.0, 0.0, 1.0], dtype=np.float64)
            upward_vector = np.cross(direction, z_axis)

            if np.linalg.norm(upward_vector) < 1e-6:  # 方向ベクトルがZ軸にほぼ平行
                upward_vector = np.array([1.0, 0.0, 0.0], dtype=np.float64)  # X方向にオフセット
            else:
                upward_vector /= np.linalg.norm(upward_vector)  # 正規化

            # 始点のオフセットを計算
            upper_array[i] = p1 + upward_vector * distance
            lower_array[i] = p1 - upward_vector * distance

        # 最後の点のオフセット
        upper_array[-1] = coords_array[-1] + upward_vector * distance
        lower_array[-1] = coords_array[-1] - upward_vector * distance

    return upper_array, lower_array


@jit(f8(f8[:], f8[:], f8[:]), nopython=True, cache=True)
def interpolate_z(p1, p2, px):
    """
    線分 (p1, p2) 上で点 px の Z 座標を線形補間する。

    :param p1: 3次元空間の始点座標 [x1, y1, z1]。
    :type p1: numpy.ndarray
    :param p2: 3次元空間の終点座標 [x2, y2, z2]。
    :type p2: numpy.ndarray
    :param px: 線分上の補間対象点の座標 [x, y]（2次元）。
    :type px: numpy.ndarray

    :return: 補間された Z 座標。
    :rtype: float

    :raises ZeroDivisionError: p1 と p2 が同一の座標の場合。
    """
    # 線分の2次元長さを基準に補間の重み t を計算
    denominator = np.linalg.norm(p2[:2] - p1[:2])
    if denominator == 0:
        raise ZeroDivisionError("p1 と p2 が同一の座標であるため、補間を実行できません。")
    t = np.linalg.norm(px - p1[:2]) / denominator

    # Z 座標の補間計算
    return p1[2] + t * (p2[2] - p1[2])


@jit(f8[:](f8[:], f8[:], f8[:]), nopython=True, cache=True)
def calculate_normal(p1, p2, p3):
    """
    3点から法線ベクトルを計算する。

    :param p1: 3次元空間の点1（始点の座標）。
    :type p1: numpy.ndarray
    :param p2: 3次元空間の点2（中間点の座標）。
    :type p2: numpy.ndarray
    :param p3: 3次元空間の点3（終点の座標）。
    :type p3: numpy.ndarray

    :return: 法線ベクトル（正規化されたベクトル）。
    :rtype: numpy.ndarray

    :raises ZeroDivisionError: 入力点が直線上に並んでいる場合。
    """
    # p1 → p2 ベクトル
    v1 = p2 - p1

    # p2 → p3 ベクトル
    v2 = p3 - p2

    # 外積を計算して法線ベクトルを取得
    normal = np.cross(v1, v2)

    # 外積の結果を正規化
    norm = np.linalg.norm(normal)
    if norm == 0:
        raise ZeroDivisionError("法線ベクトルを計算できません。入力点が直線上に並んでいる可能性があります。")

    return normal / norm


@jit(f8(f8[:, :], f8[:, :]), nopython=True, cache=True, nogil=True)
def calculate_determinant(segment1, segment2):
    """
    2D線分の行列式（determinant）を計算する。

    :param segment1: 2D線分1の始点と終点。
    :type segment1: numpy.ndarray
    :param segment2: 2D線分2の始点と終点。
    :type segment2: numpy.ndarray

    :return: determinantの値。
    :rtype: float
    """
    p1, p2 = segment1[0], segment1[1]
    p3, p4 = segment2[0], segment2[1]

    A1 = p2[1] - p1[1]
    B1 = p1[0] - p2[0]

    A2 = p4[1] - p3[1]
    B2 = p3[0] - p4[0]

    determinant = A1 * B2 - A2 * B1
    return determinant


@jit(f8[:](f8[:, :], f8[:, :], f8), nopython=True, cache=True, nogil=True)
def calculate_intersection_point(segment1, segment2, determinant):
    """
    線分間の交点（x, y）を計算する。

    :param segment1: 2D線分1の始点と終点。
    :type segment1: numpy.ndarray
    :param segment2: 2D線分2の始点と終点。
    :type segment2: numpy.ndarray
    :param determinant: 行列式の値。
    :type determinant: float

    :return: 交点の座標（x, y）。
    :rtype: numpy.ndarray
    """
    p1, p2 = segment1[0], segment1[1]
    p3, p4 = segment2[0], segment2[1]

    A1 = p2[1] - p1[1]
    B1 = p1[0] - p2[0]
    C1 = A1 * p1[0] + B1 * p1[1]

    A2 = p4[1] - p3[1]
    B2 = p3[0] - p4[0]
    C2 = A2 * p3[0] + B2 * p3[1]

    x = (B2 * C1 - B1 * C2) / determinant
    y = (A1 * C2 - A2 * C1) / determinant

    return np.array([x, y])


@jit(b1(f8[:, :], f8[:, :], f8[:], f8), nopython=True, cache=True, nogil=True)
def is_point_on_segments(segment1, segment2, point, tolerance):
    """
    交点が両方の線分上にあるかを判定する。

    :param segment1: 2D線分1の始点と終点。
    :type segment1: numpy.ndarray
    :param segment2: 2D線分2の始点と終点。
    :type segment2: numpy.ndarray
    :param point: 交点の座標。
    :type point: numpy.ndarray
    :param tolerance: 座標が一致とみなす誤差範囲。
    :type tolerance: float

    :return: 線分上にある場合はTrue。
    :rtype: bool
    """
    p1, p2 = segment1[0], segment1[1]
    p3, p4 = segment2[0], segment2[1]
    x, y = point[0], point[1]

    return (
        (min(p1[0], p2[0]) - tolerance <= x <= max(p1[0], p2[0]) + tolerance)
        and (min(p1[1], p2[1]) - tolerance <= y <= max(p1[1], p2[1]) + tolerance)
        and (min(p3[0], p4[0]) - tolerance <= x <= max(p3[0], p4[0]) + tolerance)
        and (min(p3[1], p4[1]) - tolerance <= y <= max(p3[1], p4[1]) + tolerance)
    )


@jit(f8[:, :](f8[:, :], f8[:, :]), nopython=True, cache=True, nogil=True)
def find_3d_intersection(segment1, segment2):
    """
    3D空間上の2つの線分において、交差判定を行い、
    それぞれの線分上の交点を1つずつ返す。
    交差しない場合はNumPyのNaNを返す。

    :param segment1: 第1の線分（始点と終点の配列）。例: [[x1, y1, z1], [x2, y2, z2]]
    :type segment1: numpy.ndarray
    :param segment2: 第2の線分（始点と終点の配列）。例: [[x3, y3, z3], [x4, y4, z4]]
    :type segment2: numpy.ndarray

    :return: 交差点を3Dで表現する配列 (2×3)。
        - [0]: 線分1上の交点（または [np.nan, np.nan, np.nan]）
        - [1]: 線分2上の交点（または [np.nan, np.nan, np.nan]）
    :rtype: numpy.ndarray
    """
    # 定数としての閾値を定義（2つの値が近いかどうかを判定するための基準値）
    tolerance = np.float64(1e-5)

    # 戻り値としての初期化（交点が存在しない場合のデフォルト値）
    result = np.full((2, 3), np.nan, dtype=np.float64)

    # 線分の方向ベクトルを計算
    # segment1の方向ベクトル: p2 - p1
    # segment2の方向ベクトル: p4 - p3
    v1 = segment1[1] - segment1[0]
    v2 = segment2[1] - segment2[0]

    # 法線ベクトルを計算
    # 線分v1とv2の外積が平面の法線を定義する
    normal = np.cross(v1, v2)
    norm = np.linalg.norm(normal)

    # 法線がほぼゼロの場合、線分は平行であり交差しない
    if norm < tolerance:
        return result

    # 法線を正規化
    normal /= norm

    # Z軸との回転を計算する
    z_axis = np.array([0.0, 0.0, 1.0])  # Z軸の単位ベクトル
    cross_product = np.cross(normal, z_axis)  # Z軸との外積
    sin_theta = np.linalg.norm(cross_product)  # 回転角の正弦
    cos_theta = np.dot(normal, z_axis)        # 回転角の余弦

    # 回転行列を計算
    if sin_theta == 0.0:  # 法線が既にZ軸と一致している場合
        rotation_matrix = np.eye(3)  # 単位行列を使用
    else:
        # 回転行列を構築（ロドリゲスの回転公式を使用）
        cross_product /= sin_theta
        skew_symmetric = np.array([
            [0, -cross_product[2], cross_product[1]],
            [cross_product[2], 0, -cross_product[0]],
            [-cross_product[1], cross_product[0], 0]
        ])
        rotation_matrix = (
            np.eye(3) +
            sin_theta * skew_symmetric +
            (1 - cos_theta) * np.dot(skew_symmetric, skew_symmetric)
        )

    # 線分を回転して平面に投影
    seg1_rotated = np.dot(rotation_matrix, segment1.T).T  # segment1を回転
    seg2_rotated = np.dot(rotation_matrix, segment2.T).T  # segment2を回転

    # 回転後の2D平面上の線分を計算
    seg1_2d = seg1_rotated[:, :2]  # XY平面の情報を抽出
    seg2_2d = seg2_rotated[:, :2]

    # 2D平面上で行列式を用いて交差判定
    det = (seg1_2d[1, 0] - seg1_2d[0, 0]) * (seg2_2d[1, 1] - seg2_2d[0, 1]) - \
          (seg1_2d[1, 1] - seg1_2d[0, 1]) * (seg2_2d[1, 0] - seg2_2d[0, 0])

    # 行列式が閾値以下の場合、線分は平行または重なるため交差しない
    if abs(det) < tolerance:
        return result

    # パラメトリック係数 t1, t2 を計算
    t1 = ((seg2_2d[0, 0] - seg1_2d[0, 0]) * (seg2_2d[1, 1] - seg2_2d[0, 1]) -
          (seg2_2d[0, 1] - seg1_2d[0, 1]) * (seg2_2d[1, 0] - seg2_2d[0, 0])) / det
    t2 = ((seg2_2d[0, 0] - seg1_2d[0, 0]) * (seg1_2d[1, 1] - seg1_2d[0, 1]) -
          (seg2_2d[0, 1] - seg1_2d[0, 1]) * (seg1_2d[1, 0] - seg1_2d[0, 0])) / det

    # t1, t2が[0, 1]の範囲内の場合、線分上に交差点が存在
    if 0 <= t1 <= 1 and 0 <= t2 <= 1:
        # 交差点の2D座標を計算
        intersection_2d = seg1_2d[0] + t1 * (seg1_2d[1] - seg1_2d[0])

        # 各線分のZ座標を線形補間で計算
        z1 = seg1_rotated[0, 2] + t1 * (seg1_rotated[1, 2] - seg1_rotated[0, 2])
        z2 = seg2_rotated[0, 2] + t2 * (seg2_rotated[1, 2] - seg2_rotated[0, 2])

        # 交差点を3D座標として構成
        intersection_3d_seg1 = np.dot(rotation_matrix.T, np.array([*intersection_2d, z1]))
        intersection_3d_seg2 = np.dot(rotation_matrix.T, np.array([*intersection_2d, z2]))

        # 結果に交差点を設定
        result[0] = intersection_3d_seg1
        result[1] = intersection_3d_seg2

    # 最終的な結果を返す
    return result


def generate_arc(P, A, B, degree_step=6, clockwise=False, shortest_arc=True):
    """
    点Pを中心とし、点Aと点Bの間に円弧を生成する。

    :param P: 中心点
    :type P: numpy.ndarray
    :param A: np.ndarray, 始点
    :type A: numpy.ndarray
    :param B: np.ndarray, 終点
    :type B: numpy.ndarray
    :param degree_step: 円弧の角度ステップ（単位：度）
    :type degree_step: int
    :param clockwise: 時計回りで生成する場合はTrue（デフォルトはFalse＝反時計回り）
    :type clockwise: bool
    :param shortest_arc: 短い方の円弧を生成する場合はTrue（デフォルトはTrue）
    :type shortest_arc: bool

    :return: 円弧上の点群
    :rtype: numpy.ndarray
    """
    try:
        vec_A = A - P
        vec_B = B - P
        radius = np.linalg.norm(vec_A)

        if radius == 0:
            raise ValueError("中心点Pと点Aが同じ座標です。円弧を生成できません。")

        # 角度を計算
        dot_product = np.dot(vec_A, vec_B)
        norm_product = radius * np.linalg.norm(vec_B)
        if norm_product == 0:
            raise ValueError("点Aまたは点Bが中心点Pと同じ座標です。円弧を生成できません。")

        angle_rad = np.arccos(np.clip(dot_product / norm_product, -1.0, 1.0))  # 安全な角度計算

        # 角度を補正（短い方の円弧 or 長い方の円弧）
        if not shortest_arc:
            angle_rad = 2 * np.pi - angle_rad

        # 時計回りなら法線を反転
        normal = np.cross(vec_A, vec_B)
        if np.linalg.norm(normal) == 0:
            #raise ValueError("点A、点B、点Pが同一線上にあります。法線を計算できません。")
            return None

        if clockwise:
            normal = -normal
        normal = normal / np.linalg.norm(normal)

        # 分割数を計算
        angle_deg = np.degrees(angle_rad)
        if angle_deg <= degree_step:
            # 角度が degree_step 以下の場合は何も作成しない
            return None
        else:
            # 通常の分割
            num_divisions = int(np.ceil(angle_deg / degree_step))
            adjusted_step = angle_rad / num_divisions

        # 円弧上の点を生成
        arc_points = [A]
        for i in range(1, num_divisions):
            t = adjusted_step * i
            rotation_matrix = (
                np.cos(t) * np.eye(3) +
                np.sin(t) * np.array([[0, -normal[2], normal[1]],
                                      [normal[2], 0, -normal[0]],
                                      [-normal[1], normal[0], 0]]) +
                (1 - np.cos(t)) * np.outer(normal, normal)
            )
            rotated_vec = np.dot(rotation_matrix, vec_A)
            arc_points.append(P + rotated_vec)

        arc_points.append(B)  # 終点を追加
        return np.array(arc_points)

    except Exception as e:
        raise RuntimeError(f"[generate_arc_Exception]: {str(e)}")


def create_wire_line(target_line_coords, side_line, other_side_line):
    """
    中心線と側線を基に調整された線分と円弧部分を生成する。

    この関数では、中心線を基に左右の側線の交差を考慮して線分を調整し、
    必要に応じて円弧を生成します。円弧は、交差がない場合にのみ作成されます。

    :param target_line_coords: 中心線を構成する3D座標のリスト。
    :type target_line_coords: numpy.ndarray
    :param side_line: 側線の線分リスト。各要素は[[x1, y1, z1], [x2, y2, z2]]の形式。
    :type side_line: numpy.ndarray
    :param other_side_line: 対応する反対側の側線の線分リスト。各要素は[[x1, y1, z1], [x2, y2, z2]]の形式。
    :type other_side_line: numpy.ndarray

    :return: 以下2つの処理結果を返す。
        - 調整された側線の座標リスト。
        - 円弧部分の線分リスト。各要素は中心線の点と円弧上の点を結ぶ線分。
    :rtype: tuple[numpy.ndarray, numpy.ndarray]
    """
    # 調整済みの座標リストを格納するリスト
    coords = [np.array(side_line[0][0], dtype=float)]
    arc_lines = []  # 円弧部分の線分を格納するリスト

    for i in range(1, len(side_line)):
        # 現在の側線セグメントと次のセグメントの始点と終点を取得
        p1, p2 = np.array(side_line[i - 1][0], dtype=float), np.array(side_line[i - 1][1], dtype=float)
        p3, p4 = np.array(side_line[i][0], dtype=float), np.array(side_line[i][1], dtype=float)

        # 反対側の側線の対応するセグメントの始点と終点を取得
        o1, o2 = np.array(other_side_line[i - 1][0], dtype=float), np.array(other_side_line[i - 1][1], dtype=float)
        o3, o4 = np.array(other_side_line[i][0], dtype=float), np.array(other_side_line[i][1], dtype=float)

        # 中心線の対応する点を取得
        P = np.array(target_line_coords[i], dtype=float)

        # 側線同士が交差しているかどうかを判定
        intersection1, intersection2 = find_3d_intersection(np.array([p1, p2]), np.array([p3, p4]))

        # 側線間に交差がない場合
        if np.isnan(intersection1).any() or np.isnan(intersection2).any():

            # 内側に巻き込むような交差がある場合は円弧を作成しない
            intersection1, intersection2 = find_3d_intersection(np.array([p2, o2]), np.array([p4, o4]))
            intersection3, intersection4 = find_3d_intersection(np.array([p1, o1]), np.array([p3, o3]))

            # 全ての交差がない場合に円弧を生成
            if (np.isnan(intersection1).all() and
                np.isnan(intersection2).all() and
                np.isnan(intersection3).all() and
                np.isnan(intersection4).all()):

                # 中心線の点Pを中心にp2からp3への円弧を生成
                arc = generate_arc(P, p2, p3)

                if arc is not None:
                    # 円弧の全点を調整済み座標リストに追加
                    coords.extend(arc)

                    # 円弧の先頭と末尾を除いた点と中心線の点Pを結ぶ線分をarc_linesに追加
                    for point in arc[1:-1]:
                        arc_lines.append((P, np.array(point, dtype=float)))
                else:
                    # 円弧が生成されない場合は現在の点p2を追加
                    coords.append(p2)
        else:
            # 側線間に交差がある場合は交差点の中間点を計算して調整済み座標リストに追加
            midpoint = (np.array(intersection1) + np.array(intersection2)) / 2
            coords.append(midpoint)

    # 側線の最後の終点を調整済み座標リストに追加
    coords.append(np.array(side_line[-1][1], dtype=float))

    # 調整済みの座標リストと円弧部分の線分リストを返却
    return np.array(coords), np.array(arc_lines)


def create_wire_info(original_line, vertical, horizontal, coefficient=0.0):
    """
    中心線に対する平行線、円弧、統合線を生成する。外側がARC、内側が統合線になる。

    :param original_line: 元の中心線の座標リスト。
    :type original_line: numpy.ndarray
    :param vertical: 垂直方向の平行線の距離。
    :type vertical: float
    :param horizontal: 水平方向の平行線の距離。
    :type horizontal: float
    :param coefficient: 平行線生成時の許容誤差の計算に使用する係数 (デフォルトは0.0)。
    :type coefficient: float

    :return: 以下２つの処理結果を返す。
        - right_info:
            右側の平行線、調整済み線分、円弧部分の情報。
            各要素は以下を含む:
            1. right_wire_line: ndarray, 調整済みの右側の統合線。
            2. right_parallel_lines: ndarray, 右側の平行線。
            3. right_arc_lines: ndarray, 右側の円弧部分の線分。
        - left_info:
            左側の平行線、調整済み線分、円弧部分の情報。
            各要素は以下を含む:
            1. left_wire_line: ndarray, 調整済みの左側の統合線。
            2. left_parallel_lines: ndarray, 左側の平行線。
            3. left_arc_lines: ndarray, 左側の円弧部分の線分。
    :rtype: tuple[list[numpy.ndarray, numpy.ndarray, numpy.ndarray], list[numpy.ndarray, numpy.ndarray, numpy.ndarray]]
    """

    # 係数を計算する
    tolerrance = max(vertical, horizontal) * coefficient

    # 入力座標をDouglas-Peuckerアルゴリズムで簡略化
    if tolerrance != 0.0:
        target_line = douglas_peucker_3d(original_line, tolerrance)
    else:
        target_line = original_line

    # 平行線を生成
    right_parallel_lines = create_parallel_lines(target_line, horizontal)
    left_parallel_lines = create_parallel_lines(target_line, -horizontal)

    # 右側のワイヤーと円弧部分の線分を構築
    right_wire_line, right_arc_lines = create_wire_line(target_line, right_parallel_lines , left_parallel_lines)

    # 左側のワイヤーと円弧部分の線分を構築
    left_wire_line, left_arc_lines = create_wire_line(target_line, left_parallel_lines, right_parallel_lines)

    # 戻り値の順序を修正
    right_info = [right_wire_line, right_parallel_lines, right_arc_lines]
    left_info = [left_wire_line, left_parallel_lines, left_arc_lines]

    return right_info, left_info


@jit(f8[:, :, :](f8[:, :, :], i8), nopython=True, cache=True, nogil=True)
def resample_arc(arc, target_count):
    """
    円弧の点の数を target_count にリサンプリングする。

    :param arc: np.ndarray, (N, 2, 3) の形状を持つ円弧の座標配列
    :param target_count: int, リサンプリング後の点数
    :return: np.ndarray, (target_count, 2, 3) の形状を持つリサンプリング後の円弧座標
    """
    current_count = arc.shape[0]
    if current_count == target_count:
        return arc  # すでに点数が一致している場合は変更なし

    # 元のインデックス
    original_indices = np.linspace(0.0, 1.0, current_count)

    # 新しいインデックス（等間隔に target_count 分割）
    new_indices = np.linspace(0.0, 1.0, target_count)

    # 新しい配列を作成
    new_arc = np.zeros((target_count, 2, 3), dtype=np.float64)

    for i in range(2):  # 始点・終点の2点セットを補間
        for j in range(3):  # X, Y, Z 座標
            for k in range(target_count):
                t = new_indices[k]
                # 線形補間
                idx_low = 0
                idx_high = 1
                for n in range(current_count - 1):
                    if original_indices[n] <= t <= original_indices[n + 1]:
                        idx_low = n
                        idx_high = n + 1
                        break

                x0, x1 = original_indices[idx_low], original_indices[idx_high]
                y0, y1 = arc[idx_low, i, j], arc[idx_high, i, j]

                if x1 - x0 == 0:
                    new_arc[k, i, j] = y0
                else:
                    new_arc[k, i, j] = y0 + (t - x0) * (y1 - y0) / (x1 - x0)

    return new_arc


def adjust_arc_pair(upper_arc, lower_arc):
    """
    2つの円弧の点数を揃える関数
    点数が異なる場合、多い方を少ない方に合わせる。

    :param upper_arc: np.ndarray, 上側の円弧 (N1, 2, 3)
    :param lower_arc: np.ndarray, 下側の円弧 (N2, 2, 3)
    :return: (np.ndarray, np.ndarray), 揃えた upper_arc, lower_arc
    """
    num_upper = upper_arc.shape[0]
    num_lower = lower_arc.shape[0]

    if num_upper > num_lower:
        upper_arc = resample_arc(upper_arc, num_lower)
    elif num_lower > num_upper:
        lower_arc = resample_arc(lower_arc, num_upper)

    return upper_arc, lower_arc


def split_arcs_by_center(arc):
    """
    円弧の配列を、異なる中心点ごとに分割する。

    :param arc: np.ndarray, (N, 2, 3) の形状を持つ円弧の座標配列
    :return: np.ndarray, (M, N, 2, 3) の形状を持つ分割された円弧のリスト
    """
    n = arc.shape[0]

    # データなしの場合
    if n == 0:
        return None

    # グループの開始インデックスを記録
    group_starts = [0]  # 最初のデータはグループの開始点

    # 異なる中心点ごとにグループを判定
    for i in range(1, n):
        if not np.array_equal(arc[i, 0], arc[i - 1, 0]):
            group_starts.append(i)

    unique_centers = len(group_starts)

    # グループが 1 つしかない場合、そのまま返す
    if unique_centers == 1:
        return arc.reshape(1, n, 2, 3)

    # 各グループの長さを計算し、最大長を取得
    group_lengths = np.diff(group_starts + [n])  # 各グループの長さ
    max_count = max(group_lengths)

    # グループごとにデータを格納
    arc_groups = []

    for i, start_idx in enumerate(group_starts):
        end_idx = group_starts[i + 1] if i + 1 < unique_centers else n
        arc_groups.append(arc[start_idx:end_idx])

    return arc_groups


def adjust_all_arcs(upper_arc, lower_arc):
    """
    upper_arc, lower_arc を円弧ごとに分割し、それぞれの円弧の点数を揃える。

    :param upper_arc: np.ndarray, 上側の円弧 (N1, 2, 3)
    :param lower_arc: np.ndarray, 下側の円弧 (N2, 2, 3)
    :return: (np.ndarray, np.ndarray), 揃えた upper_arc, lower_arc
    """
    # 各円弧ごとに分割
    upper_arc_groups = split_arcs_by_center(upper_arc)
    lower_arc_groups = split_arcs_by_center(lower_arc)

    if(upper_arc_groups is None or lower_arc_groups is None):
        return upper_arc , lower_arc

    if len(upper_arc_groups) != len(lower_arc_groups):
        raise ValueError("upper_arc と lower_arc の円弧のグループ数が一致しません")

    for i in range(len(upper_arc_groups)):
        adjusted_upper_groups = []
        adjusted_lower_groups = []

        if len(upper_arc_groups[i]) != len(lower_arc_groups[i]):
            raise ValueError("upper_arc と lower_arc の円弧座標数が一致しません")

        adjusted_u_arc, adjusted_l_arc = adjust_arc_pair(upper_arc_groups[i], lower_arc_groups[i])
        adjusted_upper_groups.append(adjusted_u_arc)
        adjusted_lower_groups.append(adjusted_l_arc)

    # 分割した円弧を再結合
    return np.vstack(adjusted_upper_groups), np.vstack(adjusted_lower_groups)


def get_wire_frame_info(xyz_array, width, depth, coefficient=np.array(0.0)):
    """
    3D座標配列からワイヤーフレームの情報を生成する。

    :param xyz_array: 中心線を構成する3D座標の配列。例: [[x1, y1, z1], [x2, y2, z2], ...]
    :type xyz_array: numpy.ndarray
    :param width: ワイヤーフレームの横方向の幅。
    :type width: float
    :param depth: ワイヤーフレームの縦方向の深さ。
    :type depth: float
    :param coefficient: 平行線生成時の許容誤差に用いる係数。
    :type coefficient: float

    :return: 各ワイヤーフレームの情報を含むタプル。形式: (上右ワイヤー, 下右ワイヤー, 下左ワイヤー, 上左ワイヤー)
    :rtype: tuple[
        list[numpy.ndarray, numpy.ndarray, numpy.ndarray],
        list[numpy.ndarray, numpy.ndarray, numpy.ndarray],
        list[numpy.ndarray, numpy.ndarray, numpy.ndarray],
        list[numpy.ndarray, numpy.ndarray, numpy.ndarray]
    ]
    """

    # 中心線を基準に上下方向にオフセットしたラインを生成
    upper_line, lower_line = generate_offset_lines(xyz_array, depth)

    # 上側の2本のワイヤー情報を生成
    # 右側と左側のワイヤー情報をそれぞれ取得
    upper_right_info, upper_left_info = create_wire_info(upper_line, depth, width, coefficient)

    # 下側の2本のワイヤー情報を生成
    # 右側と左側のワイヤー情報をそれぞれ取得
    lower_right_info, lower_left_info = create_wire_info(lower_line, depth, width, coefficient)

    # 上下4本のワイヤーフレーム情報を返却
    return upper_right_info, lower_right_info, lower_left_info, upper_left_info

@jit('Tuple((f8[:,:],f8[:,:],f8[:,:],f8[:,:]))(f8[:,:],f8,f8)', nopython=True, cache=True, nogil=True)
def get_wire_frame(xyz_array, width, depth):
    '''
    ワイヤーフレーム作成関数
    引数1:中心線のxyz座標 2次元配列 [中心線構成点]>[xyz座標] np.float64
    引数2:幅 np.float64
    引数3:深さ np.float64
    戻り値:ワイヤーフレーム1 2次元配列 [ワイヤーフレームの構成点]>[xyz座標] np.float64
    戻り値:ワイヤーフレーム2 2次元配列 [ワイヤーフレームの構成点]>[xyz座標] np.float64
    戻り値:ワイヤーフレーム3 2次元配列 [ワイヤーフレームの構成点]>[xyz座標] np.float64
    戻り値:ワイヤーフレーム4 2次元配列 [ワイヤーフレームの構成点]>[xyz座標] np.float64
    '''

    target_xyz_array = xyz_array.copy()

    # ワイヤーフレームパーツ取得
    wire_frame_parts_array = get_wire_frame_parts_coordinates(target_xyz_array, width, depth)

    # ワイヤーフレームパーツ補間用の長さ
    interpolation_distance = np.sqrt(width**2 + depth**2) / 2

    # 結果格納用List
    result_list = []

    # 構成点が2点の場合は補間の必要なし
    if len(target_xyz_array) != 2:

        # ワイヤーフレームパーツを補間してワイヤーフレーム取得
        for i in range(len(wire_frame_parts_array)):

            # ワイヤーフレームパーツ1本ごとにワイヤーフレームを生成して結果を格納
            result_array = interpolate_wire_frame_parts(wire_frame_parts_array[i], target_xyz_array, interpolation_distance)
            result_list.append(result_array.copy())

    # 構成点2点の場合
    else:

        # ワイヤーフレームパーツをそのまま格納
        [result_list.append(wire_frame_parts_array[i,0,].copy()) for i in range(len(wire_frame_parts_array))]

    # 4本分返却
    return result_list[0], result_list[1], result_list[2], result_list[3]

@jit(f8[:,:,:](f8[:,:], f8[:,:], f8[:,:], f8[:,:], i8, i8), nopython=True, cache=True, nogil=True)
def get_tinz_coordinates_from_wire_frames(wire_frame_array1
                                        , wire_frame_array2
                                        , wire_frame_array3
                                        , wire_frame_array4
                                        , start_point_flag
                                        , end_point_flag):
    '''
    概要 :2本のワイヤーフレームからTINZのインデックス取得
    引数1:ワイヤーフレーム1 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    引数2:ワイヤーフレーム2 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    引数3:ワイヤーフレーム3 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    引数4:ワイヤーフレーム4 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    引数5:始点TINZ生成flg 0:生成しない 1:生成する
    引数6:終点TINZ生成flg 0:生成しない 1:生成する
    戻り値:TINZを表すnumpy配列 [TINZ]>[TINZの構成点]>[構成点のxyz座標] np.float64
    '''
    # 4つの面を作成
    # [1,2], [2,3], [3,4], [4,1]の順番でワイヤーフレームを選んで処理を行う
    tinz_array1 = get_side_tinz_coordinates(wire_frame_array2.copy(),wire_frame_array1.copy())
    tinz_array2 = get_side_tinz_coordinates(wire_frame_array3.copy(),wire_frame_array2.copy())
    tinz_array3 = get_side_tinz_coordinates(wire_frame_array4.copy(),wire_frame_array3.copy())
    tinz_array4 = get_side_tinz_coordinates(wire_frame_array1.copy(),wire_frame_array4.copy())

    # 結果結合
    result_array = np.concatenate((tinz_array1, tinz_array2, tinz_array3, tinz_array4))

    # 始点TINZ生成flgが1の場合、入口のTINZ生成
    # 始点4つ集めて決まったインデックスでTINZを生成
    if start_point_flag == 1:

        # 始点TINZ初期配列 [TINZ2つ]>[構成点3点]>[xyz座標]
        start_tinz_array = np.zeros((2,3,3), dtype = np.float64)

        # TINZの各構成点を更新 np.arrayでオブジェクトを発生させるよりnumpy配列を指定して更新のほうが早そう
        # ★回転方向を反時計回りに統一
        start_tinz_array[0,0] = wire_frame_array4.copy()[0]
        start_tinz_array[0,1] = wire_frame_array1.copy()[0]
        start_tinz_array[0,2] = wire_frame_array2.copy()[0]
        start_tinz_array[1,0] = wire_frame_array4.copy()[0]
        start_tinz_array[1,1] = wire_frame_array2.copy()[0]
        start_tinz_array[1,2] = wire_frame_array3.copy()[0]

        # 側面のワイヤーフレームに追加
        result_array = np.concatenate((result_array, start_tinz_array))
    else:
        pass

    # 終点TINZ生成flgが1の場合、出口のTINZ生成
    # 終点4つ集めて決まったインデックスでTINZを生成
    if end_point_flag == 1:

        # 終点TINZ初期配列 [TINZ2つ]>[構成点3点]>[xyz座標]
        end_tinz_array = np.zeros((2,3,3), dtype = np.float64)

        # TINZの各構成点を更新 np.arrayでオブジェクトを発生させるよりnumpy配列を指定して更新のほうが早そう
        #  ★回転方向を反時計回りに統一

        end_tinz_array[0,0] = wire_frame_array1.copy()[-1]
        end_tinz_array[0,1] = wire_frame_array4.copy()[-1]
        end_tinz_array[0,2] = wire_frame_array3.copy()[-1]
        end_tinz_array[1,0] = wire_frame_array1.copy()[-1]
        end_tinz_array[1,1] = wire_frame_array3.copy()[-1]
        end_tinz_array[1,2] = wire_frame_array2.copy()[-1]

        # 側面のワイヤーフレームに追加
        result_array = np.concatenate((result_array, end_tinz_array))

    else:
        pass

    return result_array

@jit(f8[:,:,:](f8[:,:,:]), nopython=True, cache=True, nogil=True)
def add_vertex_normal(multipatch_array):
    '''
    概要  :三角形の座標から外積を計算し各頂点に対し単位法線ベクトルを設定する
    引数1 :[multipatch]>[頂点組み合わせ3点]>[頂点xyz座標] 3次元配列 np.float64
    戻り値:[multipatch]>[頂点組み合わせ3点]>[頂点xyz座標+ジオメトリ頂点法線xyz座標] 3次元配列 np.float64
    '''
    target_multipatch_array = multipatch_array.copy()

    # 頂点法線ベクトル格納用配列
    result_vertex_normal_array = np.zeros((len(multipatch_array), 3, 3))

    for i in range(len(multipatch_array)):

        # ベクトル計算用配列
        multipatch_array_roll = np.roll(target_multipatch_array[i],3)
        temp_array = target_multipatch_array[i] - multipatch_array_roll

        # 外積算出
        vertex_normal = np.cross(temp_array[0],temp_array[1])

        # 外積を単位ベクトルへ
        unit_vertex_normal = vertex_normal / (np.sqrt(np.sum(np.power(vertex_normal,2))))

        # 頂点法線ベクトル設定
        result_vertex_normal_array[i,0] = unit_vertex_normal
        result_vertex_normal_array[i,1] = unit_vertex_normal
        result_vertex_normal_array[i,2] = unit_vertex_normal

    # 各頂点の座標と頂点法線ベクトルを結合
    target_multipatch_array = np.concatenate((target_multipatch_array,result_vertex_normal_array), axis=2)

    return target_multipatch_array

def get_multipatch_array(xyz_array, width, depth, start_point_flag, end_point_flag, coefficient=np.array(0.0)):
    """
    3D座標配列からマルチパッチ形式のデータを生成する関数。

    この関数は指定された中心線（3D座標配列）から、指定された幅と深さを持つ
    ワイヤーフレームを生成し、三角形メッシュ（TIN: Triangulated Irregular Network）を作成する。
    また、各三角形の法線ベクトルを計算してマルチパッチ形式のデータを返却する。

    :param xyz_array: 中心線を構成する3D座標の配列。例: [[x1, y1, z1], [x2, y2, z2], ...]
    :type xyz_array: numpy.ndarray
    :param width: ワイヤーフレームの横方向の幅。
    :type width: float
    :param depth: ワイヤーフレームの縦方向の深さ。
    :type depth
    :param start_point_flag: 開始点の特別処理を行うかを示すフラグ。Trueの場合、開始点での調整を行う。
    :type start_point_flag: bool
    :param end_point_flag: 終了点の特別処理を行うかを示すフラグ。Trueの場合、終了点での調整を行う。
    :type end_point_flag: bool
    :param coefficient: ワイヤーフレーム生成時に使用される係数。
    :type coefficient: float

    :return: マルチパッチ形式のデータ。各三角形の頂点座標と法線ベクトルを含む配列。
        例: [[x1, y1, z1, nx1, ny1, nz1], [x2, y2, z2, nx2, ny2, nz2], ...]
    :rtype: numpy.ndarray
    """

    # 1. ワイヤーフレーム情報を生成
    #    上右、下右、下左、上左の各ワイヤーフレームの情報を取得
    ur_inf, lr_inf, ll_inf, ul_inf = get_wire_frame_info(
        xyz_array, np.float64(width), np.float64(depth), coefficient
    )

    # 2. 各ワイヤーフレームから線分情報を取得
    #    [0]は調整済みのワイヤーフレーム線分を指す
    a1 = ur_inf[0]  # 上右ワイヤーフレーム
    a2 = lr_inf[0]  # 下右ワイヤーフレーム
    a3 = ll_inf[0]  # 下左ワイヤーフレーム
    a4 = ul_inf[0]  # 上左ワイヤーフレーム

    # 3. ワイヤーフレーム間を接続して三角形メッシュ（TINZ）を生成
    #    各ワイヤーフレーム間の接続を考慮し三角形の頂点座標を作成
    b = get_tinz_coordinates_from_wire_frames(
        a1, a2, a3, a4, start_point_flag, end_point_flag
    )

    # 4. 各三角形の法線ベクトルを計算し頂点データに追加
    #    各三角形の外積を利用して法線ベクトルを計算
    c = add_vertex_normal(b)

    # 5. マルチパッチ形式のデータを返却
    #    マルチパッチ形式: 各三角形の頂点座標 + 法線ベクトル
    return c

@jit('Tuple((f8[:,:],f8[:,:]))(f8[:,:])', nopython=True, cache=True, nogil=True)
def create_wire_frame_parts_from_polygon(polygon_array):
    """
    概要   :polygonのnumpyから下面のワイヤーフレームを作成する
    引数1  :座標配列 2次元配列 [構成点]>[xyz] np.float64
    戻り値1:ワイヤーフレーム1 2次元配列 [構成点]>[xyz] polygonの最初のインデックスから区切り位置※までの構成点
    戻り値2:ワイヤーフレーム2 2次元配列 [構成点]>[xyz] polygonの最初の区切り位置※-1から最後までの構成点
    ※構成点数/2の切り上げした整数値
    """
    # 区切り位置を決めるインデックスを取得する
    # 構成点数/2の切り上げ整数値
    split_index = int(np.ceil(len(polygon_array) / 2))

    # ワイヤーフレーム1作成
    # 0から区切り位置まで取得
    wire_frame_1 = polygon_array[0:split_index-1,:]

    # ワイヤーフレーム2作成
    # 区切り位置から最後の構成点まで取得し逆順に並び替え
    # numbaはflipのaxis受け付けない
    wire_frame_2 = np.flipud(polygon_array[split_index-1:len(polygon_array)-1,:])

    return wire_frame_1, wire_frame_2

@jit('Tuple((f8[:,:],f8[:,:],f8[:,:],f8[:,:]))(f8[:,:],f8)', nopython=True, cache=True, nogil=True)
def create_wire_frame_from_polygon(polygon_array, height):
    """
    概要 :polygonのnumpyからワイヤーフレームを作成する
    引数1:座標配列 2次元配列 [構成点]>[xyz] np.float64
    引数2:高さ
    """
    # polygonから下面のワイヤーフレーム作成
    wire_frame_1, wire_frame_2 = create_wire_frame_parts_from_polygon(polygon_array)

    # 上面のワイヤーフレーム作成
    wire_frame_3 = wire_frame_1.copy()
    wire_frame_3[:,2] = wire_frame_3[:,2] + height

    wire_frame_4 = wire_frame_2.copy()
    wire_frame_4[:,2] = wire_frame_4[:,2] + height

    # 底面、上面の順に返す
    return wire_frame_1, wire_frame_2, wire_frame_3, wire_frame_4

@jit(f8[:,:,:](f8[:,:], f8[:,:], f8[:,:], f8[:,:], i8, i8), nopython=True, cache=True, nogil=True)
def get_tinz_coordinates_from_wire_frames_polygon( wire_frame_array1
                                                 , wire_frame_array2
                                                 , wire_frame_array3
                                                 , wire_frame_array4
                                                 , start_point_flag
                                                 , end_point_flag):
    '''
    引数1:ワイヤーフレーム1 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    引数2:ワイヤーフレーム2 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    引数3:ワイヤーフレーム3 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    引数4:ワイヤーフレーム4 3次元配列 [ワイヤーフレームの線分]>[線分を表す2点]>[xyz座標] np.float64
    引数5:始点TINZ生成flg 0:生成しない 1:生成する
    引数6:終点TINZ生成flg 0:生成しない 1:生成する
    戻り値:TINZを表すnumpy配列 [TINZ]>[TINZの構成点]>[構成点のxyz座標] np.float64
    上面と底面のマルチパッチはtripyを用いて作成
    '''
    target_wire_frame_array1 = wire_frame_array1.copy()
    target_wire_frame_array2 = wire_frame_array2.copy()
    target_wire_frame_array3 = wire_frame_array3.copy()
    target_wire_frame_array4 = wire_frame_array4.copy()

    # 側面を作成
    tinz_array1 = get_side_tinz_coordinates(target_wire_frame_array1, target_wire_frame_array3)
    tinz_array2 = get_side_tinz_coordinates(target_wire_frame_array2, target_wire_frame_array4)

    # 結果結合
    result_array = np.concatenate((tinz_array1, tinz_array2))

    # 始点TINZ生成flgが1の場合、入口のTINZ生成
    # 始点4つ集めて決まったインデックスでTINZを生成
    if start_point_flag == 1:

        # 始点TINZ初期配列 [TINZ2つ]>[構成点3点]>[xyz座標]
        start_tinz_array = np.zeros((2,3,3), dtype = np.float64)

        # TINZの各構成点を更新 np.arrayでオブジェクトを発生させるよりnumpy配列を指定して更新のほうが早そう
        start_tinz_array[0,0] = target_wire_frame_array4[0]
        start_tinz_array[0,1] = target_wire_frame_array1[0]
        start_tinz_array[0,2] = target_wire_frame_array2[0]
        start_tinz_array[1,0] = target_wire_frame_array4[0]
        start_tinz_array[1,1] = target_wire_frame_array3[0]
        start_tinz_array[1,2] = target_wire_frame_array1[0]

        # 側面のワイヤーフレームに追加
        result_array = np.concatenate((result_array, start_tinz_array))
    else:
        pass

    # 終点TINZ生成flgが1の場合、出口のTINZ生成
    # 終点4つ集めて決まったインデックスでTINZを生成
    if end_point_flag == 1:

        # 終点TINZ初期配列 [TINZ2つ]>[構成点3点]>[xyz座標]
        end_tinz_array = np.zeros((2,3,3), dtype = np.float64)

        # TINZの各構成点を更新 np.arrayでオブジェクトを発生させるよりnumpy配列を指定して更新のほうが早そう
        # ★回転方向を反時計回りに統一
        end_tinz_array[0,0] = target_wire_frame_array3[-1]
        end_tinz_array[0,1] = target_wire_frame_array2[-1]
        end_tinz_array[0,2] = target_wire_frame_array1[-1]
        end_tinz_array[1,0] = target_wire_frame_array3[-1]
        end_tinz_array[1,1] = target_wire_frame_array4[-1]
        end_tinz_array[1,2] = target_wire_frame_array2[-1]

        # 側面のワイヤーフレームに追加
        result_array = np.concatenate((result_array, end_tinz_array))

    else:
        pass

    return result_array

def get_tinz_coordinates_by_tripy(xyz_array, height):
    '''
    上面と底面を三角形で分割する
    引数1:polygonの構成点 2次元配列 [構成点]>[xyz座標] np.float64
    引数2:高さ float
    戻り値:[multipatch]>[3点]>[xyz座標] 3次元配列 np.float64
    '''
    # xy座標のみスライスして三角形分割を行う
    # polygonの始点終点は一致しているので始点以外を抽出
    triangles_array = np.array(tripy.earclip(xyz_array[1:,:2]))

    bottom_array = triangles_array.copy()
    top_array = triangles_array.copy()
    # 底面のz値を列追加
    bottom_z = xyz_array[0,2].copy()
    bottom_z_array = np.full((len(triangles_array), 3, 1), bottom_z)
    bottom_array = np.concatenate((bottom_array, bottom_z_array), axis = 2)

    # 底面から見て反時計回りの形に構成点を入れ替える
    bottom_array = bottom_array[:,[0,2,1],:]

    # 上面のz値を列追加
    top_z = bottom_z + height
    top_z_array = np.full((len(triangles_array), 3, 1), top_z)
    top_array = np.concatenate((top_array, top_z_array), axis = 2)

    #上面底面を集約して返す
    result_array = np.concatenate((bottom_array, top_array))

    return result_array

def get_multipatch_array_from_polygon(xyz_array, height, start_point_flag, end_point_flag):
    '''
    高級関数
    PolygonZを角柱化
    引数1:polygonの構成点 2次元配列 [構成点]>[xyz座標] np.float64
    引数2:高さ np.float64
    引数3:角柱の入口にマルチパッチを作成するかのフラグ int64
    引数4:角柱の出口にマルチパッチを作成するかのフラグ int64
    戻り値:[multipatch]>[3点]>[xyz座標] 3次元配列 np.float64
    '''
    # polygonからワイヤーフレーム作成
    a1, a2, a3, a4 = create_wire_frame_from_polygon(xyz_array, height)

    # ワイヤーフレームからTINZ作成関数呼び出し
    # 属面と出入り口のみ作成
    b = get_tinz_coordinates_from_wire_frames_polygon(a1, a2, a3, a4, start_point_flag, end_point_flag)

    # tripyを用いて上面、底面を三角形で分割
    c = get_tinz_coordinates_by_tripy(xyz_array, height)

    # 結果を集約
    d = np.concatenate((b, c))

    # 三角形ごとの外積を計算し列追加
    e = add_vertex_normal(d)

    return e

# ----------------------------------------------------------------
# 定数
# ----------------------------------------------------------------
# 座標平面
__coordinate_plane_xy__ = 2
__coordinate_plane_xz__ = 3

# TINの生成（create_TIN）結果のインデックス
__create_TIN_result_index_target_tin__   = 0
__create_TIN_result_index_standard_tin__ = 1

# アフィン変換（execute_affine_transformation）結果のインデックス
__affine_result_index_target_feature__ = 0
__affine_result_index_target_tin__     = 1

# z値の補正用定数
# TINのデータフレームのカラム名
__tin_dataframe_column_list__ = ['id', 'x', 'y', 'z']

# TINの点のインデックス[点A、点B、点C]
__tin_point_index_A__ = 0
__tin_point_index_B__ = 1
__tin_point_index_C__ = 2

# TINの座標のインデックス[[id, x, y, z], [id, x, y, z], [id, x, y, z]]
__point_coordinate_index_x__ = 1
__point_coordinate_index_z__ = 3

# 左右
__position_left__ = 0
__position_right__ = 1

# 移動量算出用計算式のインデックス（z' = ax + b、のaとb）
__diff_formula_index_a__ = 0
__diff_formula_index_b__ = 1

@jit(i8(f8[:],f8[:,:,:]),nopython=True,cache=True,nogil=True)
def get_tin_index(correction_target_feature,correction_target_tin_numpy_array):
    """
    ----------------------------------------------------------------
    ・構成点を内包する補正対象TINのインデックス取得処理
    補正対象地物の構成点数がどのTINに内包されているか判定する
    ----------------------------------------------------------------
    【引数】
    ①:補正対象地物の構成点1点 (1次元NumPy配列([x,y]または[x,y,z]))
    ②:補正対象TINの3次元NumPy配列 (TIN>構成点の組み合わせ>座標)
    ※①と②の座標数は合わせること
    【戻り値】
    ①TINのインデックス
    """
    # 補正対象TINごと
    # ti:TINのインデックス
    for ti in prange(len(correction_target_tin_numpy_array)):

        # TINを構成する3点取得
        temp_correction_target_tin_numpy_array = correction_target_tin_numpy_array[ti]
        
        # 内外判定初期化
        judgment_boolean = False

        # 外積計算
        ab_vector_0 = temp_correction_target_tin_numpy_array[1][0] - temp_correction_target_tin_numpy_array[0][0]
        ab_vector_1 = temp_correction_target_tin_numpy_array[1][1] - temp_correction_target_tin_numpy_array[0][1]
        bp_vector_0 = correction_target_feature[0]  - temp_correction_target_tin_numpy_array[1][0]
        bp_vector_1 = correction_target_feature[1]  - temp_correction_target_tin_numpy_array[1][1]

        bc_vector_0 = temp_correction_target_tin_numpy_array[2][0] - temp_correction_target_tin_numpy_array[1][0]
        bc_vector_1 = temp_correction_target_tin_numpy_array[2][1] - temp_correction_target_tin_numpy_array[1][1]
        cp_vector_0 = correction_target_feature[0]  - temp_correction_target_tin_numpy_array[2][0]
        cp_vector_1 = correction_target_feature[1]  - temp_correction_target_tin_numpy_array[2][1]

        ca_vector_0 = temp_correction_target_tin_numpy_array[0][0] - temp_correction_target_tin_numpy_array[2][0]
        ca_vector_1 = temp_correction_target_tin_numpy_array[0][1] - temp_correction_target_tin_numpy_array[2][1]
        ap_vector_0 = correction_target_feature[0] - temp_correction_target_tin_numpy_array[0][0]
        ap_vector_1 = correction_target_feature[1] - temp_correction_target_tin_numpy_array[0][1]

        cross_product_ab_bp = ab_vector_0 * bp_vector_1 - ab_vector_1 * bp_vector_0
        cross_product_bc_cp = bc_vector_0 * cp_vector_1 - bc_vector_1 * cp_vector_0
        cross_product_ca_ap = ca_vector_0 * ap_vector_1 - ca_vector_1 * ap_vector_0

        # 外積の正負がすべて一致していれば三角形内部に点が存在する。
        if (cross_product_ab_bp >= 0 and cross_product_bc_cp >= 0 and cross_product_ca_ap >= 0) or (cross_product_ab_bp <= 0 and cross_product_bc_cp <= 0 and cross_product_ca_ap <= 0):
            judgment_boolean = True
        else:
            judgment_boolean = False

        # 構成点がTINに内包されているならTINのインデックスを返して終了
        if judgment_boolean == True:
            return ti

        # 内包していない場合次のTINへ
        # 内包しているTINが存在しない場合は初期値のままとなる
        else:
            continue

    # 一度も内包判定されない場合-1を返して終了
    return -1

def create_TIN( correction_target_gcp_array
              , position_standard_gcp_array):
    """
    ----------------------------------------------------------------
    【概要】
    TIN生成　GCPからTINを生成
    【引数】
    ①:補正対象GCPのNumpy配列 GCP>GCPのid + xy(z)座標
    ②:位置基準GCPのNumpy配列 GCP>GCPのid + xy(z)座標
    【戻り値】
    ①:引数①で構成されるTINのNumpy配列 TIN>GCP>GCPのid + xy(z)座標
    ②:引数②で構成されるTINのNumpy配列 TIN>GCP>GCPのid + xy(z)座標
    ----------------------------------------------------------------
    """

    # -----------------------------------------------------------------
    # TINの生成
    # -----------------------------------------------------------------
    # delaunayでGCPからTINの配列を作成
    # ドロネーのインデックス取得
    delaunay_index_object = Delaunay(position_standard_gcp_array[:, [1, 2]])
    delaunay_index = delaunay_index_object.simplices

    # 組み合わせから補正対象TINの３次元配列取得 TIN>GCP>GCPのid + coordinate
    correction_target_tin_array = correction_target_gcp_array[delaunay_index]

    # 組み合わせから位置基準TINの３次元配列取得 TIN>GCP>GCPのid + coordinate
    position_standard_tin_array = position_standard_gcp_array[delaunay_index]

    return correction_target_tin_array, position_standard_tin_array

@jit('Tuple((f8[:],f8[:],f8[:,:],f8,f8,f8,f8[:]))(f8[:,:], f8[:,:])', nopython = True, cache = True, nogil = True)
def create_formula_to_correct_feature(points_of_correction_target_tin_numpy_array ,points_of_position_standard_tin_numpy_array):
    """
    ----------------------------------------------------------------
    変換用計算式作成関数
    ----------------------------------------------------------------
    【引数】
    ①:補正対象TINのNumPy配列
    ②:位置基準TINのNumPy配列
    【戻り値】
    ①:平行移動量
    ②:原点扱い
    ③:回転行列
    ④:拡縮倍率（第２段階）
    ⑤:位置基準角度
    ⑥:拡縮倍率（第３段階）
    ⑦:せん断角度（0：補正対象、1：位置基準）
    """
    # ≪TINの構成点１を一致させる処理（平行移動）≫
    # 補正対象TINの構成点１を、位置基準TINの構成点１まで平行移動
    # 平行移動量 = 引数②[0] - 引数①[0]
    parallel_translation_amount = points_of_position_standard_tin_numpy_array[0] - points_of_correction_target_tin_numpy_array[0]

    # 編集用引数① = 引数① + 平行移動量
    edit_argument = points_of_correction_target_tin_numpy_array + parallel_translation_amount

    # 補正対象TINの構成点１を、以降の回転等の処理時の原点として扱う
    # 原点扱い = 編集用引数①[0]
    starting_point = edit_argument[0].copy()

    # 戻り値の設定
    # 戻り値① = 平行移動量
    return_value1 = parallel_translation_amount

    # 戻り値② = 原点扱い
    return_value2 = starting_point

    # ≪TINの構成点２を一致させる処理（回転＆拡縮）≫
    # 引数①の構成点１と構成点２が作る線分の距離と、原点に対する角度を算出
    # 補正対象座標差 = 引数①[1] - 引数①[0]
    correction_target_coordinate_difference = points_of_correction_target_tin_numpy_array[1] - points_of_correction_target_tin_numpy_array[0]

    # 補正対象距離 = np.linalg.norm(補正対象座標差)
    correction_target_distance = np.linalg.norm(correction_target_coordinate_difference)

    # 補正対象角度 = np.degrees(np.arctan2(補正対象座標差[1], 補正対象座標差[0]))
    correction_target_angle = np.degrees(np.arctan2(correction_target_coordinate_difference[1], correction_target_coordinate_difference[0]))

    # 位置基準座標差 = 引数②[1] - 引数②[0]
    position_standard_coordinate_difference = points_of_position_standard_tin_numpy_array[1] - points_of_position_standard_tin_numpy_array[0]

    # 位置基準距離 = np.linalg.norm(位置基準座標差)
    position_standard_distance = np.linalg.norm(position_standard_coordinate_difference)

    # 位置基準角度 = np.degrees(np.arctan2(位置基準座標差[1], 位置基準座標差[0]))
    position_standard_angle = np.degrees(np.arctan2(position_standard_coordinate_difference[1], position_standard_coordinate_difference[0]))

    # 線分を揃えるための回転角度を算出
    # 回転角度 = np.radians(位置基準角度 - 補正対象角度)
    rotation_angle = np.radians(position_standard_angle - correction_target_angle)

    # 回転行列 = np.array([np.cos(回転角度), -np.sin(回転角度)], [np.sin(回転角度), np.cos(回転角度)])
    rotation_matrix = np.array([[np.cos(rotation_angle), -np.sin(rotation_angle)]
                              , [np.sin(rotation_angle),  np.cos(rotation_angle)]])

    # 引数①と引数②の構成点２が一致するための倍率を算出
    # 倍率 = 基準距離 / 補正対象距離
    magnification = position_standard_distance / correction_target_distance

    # 補正対象TINに反映
    # 編集用引数① = np.array([list(np.dot(回転行列, 編集用引数①[i] - 原点扱い)) for i in range(len(編集用引数①))])
    for i in range(len(edit_argument)):
        edit_argument[i] = np.dot(rotation_matrix, edit_argument[i] - starting_point) 

    # 編集用引数① = 編集用引数① * 倍率
    edit_argument = edit_argument * magnification

    # 戻り値の設定
    # 戻り値③ = 回転行列
    return_value3 = rotation_matrix

    # 戻り値④ = 倍率
    return_value4 = magnification

    # ≪TINの構成点３を一致させる処理（高さの拡縮＆せん断）≫
    # 補正対象TINの構成点１を原点とした位置基準TINの構成点３を取得
    # 位置基準TIN構成点３ = 引数②[2] - 原点扱い
    position_standard_tin_constituent_point3 = points_of_position_standard_tin_numpy_array[2] - starting_point

    # 補正対象TINの構成点１～２が作る線分がx軸と重なる様に回転
    # 回転行列 = np.array([np.cos(-位置基準角度), -np.sin(-位置基準角度)], [np.sin(-位置基準角度), np.cos(-位置基準角度)])
    position_standard_angle_rad = np.radians(position_standard_angle)
    rotation_matrix = np.array([[np.cos(-position_standard_angle_rad), -np.sin(-position_standard_angle_rad)], [np.sin(-position_standard_angle_rad), np.cos(-position_standard_angle_rad)]])

    # 編集用引数① = np.array([list(np.dot(回転行列, 編集用引数①[i])) for i in range(len(編集用引数①))])
    for i in range(len(edit_argument)):
        edit_argument[i] = np.dot(rotation_matrix, edit_argument[i])

    # 位置基準TIN構成点３ = np.dot(回転行列, 位置基準TIN構成点３)
    position_standard_tin_constituent_point3 = np.dot(rotation_matrix, position_standard_tin_constituent_point3)

    # 補正対象TINの構成点３のy座標を、位置基準TIN構成点３に合わせるように全体をy軸方向に拡縮
    # 倍率 =  位置基準TIN構成点③[1] / 編集用引数①[2][1]
    # zero divide考慮（補正対象TINが直線になっている事を意味するので基本的にあり得ない想定）
    if edit_argument[2][1] == 0:
        magnification = 0
    else:
        magnification = position_standard_tin_constituent_point3[1] / edit_argument[2][1]

    # 編集用引数① = 編集用引数① * [1, 倍率]
    edit_argument = edit_argument * np.array([1,magnification])

    # 補正対象TINの構成点３のx座標を、位置基準TIN構成点３に合わせるように全体をx軸方向にせん断
    # 補正対象せん断角度 = np.arctan2(編集用引数①[2][0], 編集用引数①[2][1])
    correction_target_shear_angle = np.arctan2(edit_argument[2][0], edit_argument[2][1])

    # 位置基準せん断角度 = np.arctan2(位置基準TIN構成点３[0], 位置基準TIN構成点３[1])
    position_standard_shear_angle = np.arctan2(position_standard_tin_constituent_point3[0], position_standard_tin_constituent_point3[1])

    # 戻り値の設定
    # 戻り値⑤ = 位置基準角度
    return_value5 = position_standard_angle_rad

    # 戻り値⑥ = 倍率
    return_value6 = magnification

    # 戻り値⑦ = [補正対象せん断角度, 位置基準せん断角度]
    return_value7 = np.array([correction_target_shear_angle,position_standard_shear_angle])

    # 処理を終了する
    return return_value1, return_value2, return_value3, return_value4, return_value5, return_value6, return_value7

@jit(f8[:](f8[:], f8[:], f8[:], f8[:, :], f8, f8, f8, f8[:]), nopython = True, cache = True, nogil = True)
def calculate_geometry_coordinate_by_formula( correction_target_feature           #①:補正対象地物
                                            , parallel_translation_amount_list    #③:平行移動量配列
                                            , starting_point_list                 #④:原点扱い配列
                                            , rotation_matrix_list                #⑤:回転行列配列
                                            , magnification_second_step           #⑥:拡縮倍率（第２段階）
                                            , position_standard_angle             #⑦:位置基準角度
                                            , magnification_third_step            #⑧:拡縮倍率（第３段階）
                                            , shear_angle_list                    #⑨:せん断角度（0：補正対象、1：位置基準）配列
                                            ):
    """
    ----------------------------------------------------------------
    計算式による地物座標計算 内包判定済想定版
    ----------------------------------------------------------------
    【引数】
    ①補正対象地物　　　　（構成点のx、y座標）
    ③平行移動量配列　　　（TINの点1を合わせるためのx、y方向の並行移動量）
    ④原点扱い配列　　　　（TINの点1のx、y座標）
    ⑤回転行列配列　　　　（TINの線分12の向きを一致させるために原点を中心として回転させる角度）
    ⑥拡縮倍率（第２段階）（TINの点2が一致するように全体を拡縮する倍率）
    ⑦位置基準角度　　　　（TINの線分12をx軸と一致させるために原典を中心として回転させる角度）
    ⑧拡縮倍率（第３段階）（TINの点3の高さを一致させるためにY方向へ実施する拡縮の倍率）
    ⑨せん断角度（0：補正対象、1：位置基準）配列
    【戻り値】
    ①変換用計算式で更新された補正対象地物のFeatureオブジェクト
    """
    # ★ジオメトリ座標 = ジオメトリ座標 + 引数③[n]
    geometry_coordinate = correction_target_feature + parallel_translation_amount_list

    # ジオメトリ座標 = np.dot(引数⑤[n], ジオメトリ座標 - 引数④[n]) *引数⑥[n]
    geometry_coordinate = np.dot(rotation_matrix_list, geometry_coordinate - starting_point_list) * magnification_second_step

    # 回転行列 = np.array([[np.cos(-引数⑦), -np.sin(-引数⑦)],[np.sin(-引数⑦), np.cos(-引数⑦)]])
    rotation_matrix = np.array([[np.cos(-position_standard_angle),-np.sin(-position_standard_angle)],[np.sin(-position_standard_angle), np.cos(-position_standard_angle)]])

    # ジオメトリ座標 = np.dot(回転行列, ジオメトリ座標)
    geometry_coordinate = np.dot(rotation_matrix,geometry_coordinate)

    # ジオメトリ座標 = ジオメトリ座標 * [1, 引数⑧]
    geometry_coordinate = geometry_coordinate * np.array([1, magnification_third_step])

    # ジオメトリ座標[0] = ジオメトリ座標[0] + ジオメトリ座標[1]×(np.tan(引数⑨[1]) - np.tan(引数⑨[0]))
    geometry_coordinate[0] = geometry_coordinate[0] + geometry_coordinate[1] * (np.tan(shear_angle_list[1]) - np.tan(shear_angle_list[0]))

    # 回転行列 = np.array([[np.cos(引数⑦), -np.sin(引数⑦)], [np.sin(引数⑦), np.cos(引数⑦)]])
    rotation_matrix = np.array([[np.cos(position_standard_angle),-np.sin(position_standard_angle)],[np.sin(position_standard_angle), np.cos(position_standard_angle)]])

    # ジオメトリ座標 = np.dot(回転行列, ジオメトリ座標)
    geometry_coordinate = np.dot(rotation_matrix, geometry_coordinate)

    # ジオメトリ座標 = ジオメトリ座標 + 原点扱い[n]
    geometry_coordinate = geometry_coordinate + starting_point_list

    # ★補正対象地物のジオメトリを更新
    correction_target_feature = geometry_coordinate

    # 戻り値①：変換用計算式で更新された補正対象地物のFeatureオブジェクト
    return correction_target_feature

@jit(i8[:, :](f8[:, :],f8[:, :, :]), nopython = True, cache = True, nogil = True)
def get_point_id_and_tin_index(correction_target_feature_array, correction_target_tin_array):
    """
    ----------------------------------------------------------------
    【概要】
    補正対象地物の各構成点を内包するTINのインデックスリスト作成処理
    【引数】
    ①:補正対象TINの3次元NumPy配列 (TIN>構成点の組み合わせ>座標)
    ②:補正対象地物のNumPy配列 id + xy(z)座標 + 構成点id
    【戻り値】
    ①構成点id+tinの2次元配列インデックス
    ----------------------------------------------------------------
    """
    # 構成点数取得
    correction_target_feature_number = len(correction_target_feature_array)

    # 構成点id + TINのインデックス格納用配列
    id_tin_index_array = np.full((correction_target_feature_number, 2), -1)

    # 1列目を構成点id（最後の要素）で更新
    id_tin_index_array[:, 0] = correction_target_feature_array[:, -1]

    # 補正対象TINと補正対象地物のxy座標を取得
    correction_target_feature_xy_array = correction_target_feature_array[:, 1:3].copy()
    correction_target_tin_xy_array = correction_target_tin_array[:, :, 1:3].copy()

    # 構成点数の数だけ内包判定処理
    tin_index_array = np.array([get_tin_index(correction_target_feature_xy_array[i], correction_target_tin_xy_array) for i in range(correction_target_feature_number)])
    tin_index_array = tin_index_array.reshape(correction_target_feature_number, 1)

    # 2列目をTINのインデックスで更新
    id_tin_index_array[:, 1:2] = tin_index_array

    return id_tin_index_array

def judge_inclusion( correction_target_tin_array
                   , position_standard_tin_array
                   , correction_target_feature_array
                   , split_unit_number
                   , logger):
    """
    ----------------------------------------------------------------
    【概要】
    内包判定 xy平面上で補正対象地物の構成点がいづれのTINに内包されるかを判定
    【引数】
    ①:補正対象GCPで構成されるTINのNumpy配列 TIN>GCP>GCPのid + xy(z)座標
    ②:位置基準GCPで構成されるTINのNumpy配列 TIN>GCP>GCPのid + xy(z)座標
    ③:補正対象地物のNumPy配列 id + xyz座標
    ④:並列処理最大スレッド数 int
    ⑤:分割単位 int
    ⑥:LOGGERオブジェクト
    【戻り値】
    ①:内包判定結果（引数③の各構成点を内包する引数①のインデックスのndarray）
    ----------------------------------------------------------------
    """
    #-----------------------------------------------------------------
    # 内包判定の並列処理用に構成点を分割
    #-----------------------------------------------------------------
    # 構成点件数
    correction_target_feature_number = len(correction_target_feature_array)

    # 構成点idを連番で生成
    constituent_point_id_array = np.arange(correction_target_feature_number)

    # 構成点idを結合用に2次元配列に形状変換
    constituent_point_id_array = constituent_point_id_array.reshape(correction_target_feature_number, 1)

    # 補正対象地物に構成点idを付与する
    correction_target_feature_point_id_array = np.concatenate([correction_target_feature_array, constituent_point_id_array], 1)

    logger.info(str(datetime.datetime.now()) + ' 補正対象地物分割開始')
    # 分割回数取得
    split_times = int(np.ceil(correction_target_feature_number / split_unit_number))

    # 分割結果データ格納用list
    split_geometry_list = []

    # 補正対象地物を分割する
    for i in range(split_times):
        split_geometry_list.append(correction_target_feature_point_id_array[i * split_unit_number : (i + 1) * split_unit_number, :])
    logger.info(str(datetime.datetime.now()) + ' 補正対象地物分割終了')

    #-----------------------------------------------------------------
    # 内包判定
    #-----------------------------------------------------------------

    # 内包判定結果格納用リスト
    tin_index_array = []

    logger.info(str(datetime.datetime.now()) + ' 内包判定処理開始')

    # 分割して処理
    for i in range(split_times):
        result = get_point_id_and_tin_index(split_geometry_list[i], correction_target_tin_array)
        tin_index_array.append(result)

    # 結果を結合
    tin_index_array = np.concatenate(tin_index_array)

    # 構成点idで並び替え
    tin_index_array = tin_index_array[np.argsort(tin_index_array[:, 0])]
    logger.info(str(datetime.datetime.now()) + ' 内包判定処理終了')

    return tin_index_array

@jit('Tuple((f8[:,:], f8[:,:,:]))(f8[:,:,:], f8[:,:,:], f8[:,:], i8, i8[:,:], i8)', nopython = True, cache = True, nogil = True)
def execute_affine_transformation( correction_target_tin_array
                                 , position_standard_tin_array
                                 , correction_target_feature_array
                                 , split_unit_number
                                 , tin_index_array
                                 , coordinate_plane):
    """
    ----------------------------------------------------------------
    【概要】
    アフィン変換実行 指定された座標平面でアフィン変換を実行する
    【引数】
    ①補正対象GCPで構成されるTINのNumpy配列 id + xyz座標
    ②位置基準GCPで構成されるTINのNumpy配列 id + xyz座標
    ③補正対象地物のNumPy配列 id + xyz座標
    ④分割単位 int
    ⑤内包判定結果（構成点id（全地物を通した先頭から連番） + TINのインデックス）
    ⑥座標平面の指定（2：xy平面、3：xz平面）
    【戻り値】
    ①補正後の補正対象地物
    ----------------------------------------------------------------
    """

    # 指定された座標平面に応じて取得する座標のインデックスを設定
    # XY平面
    if coordinate_plane == __coordinate_plane_xy__:
        target_coordinate_index = np.array([1, 2])
    # XZ平面
    elif coordinate_plane == __coordinate_plane_xz__:
        target_coordinate_index = np.array([1, 3])

    # GCPの要素数を取得（id, x, y(, z)で3 or 4想定）
    gcp_element_count = len(correction_target_tin_array[0, 0])

    #-----------------------------------------------------------------
    #変数の初期化
    #-----------------------------------------------------------------
    # 同値のTINは処理を行わないようにするため、あらかじめTINの数の0で初期化する
    correction_target_tin_count = len(correction_target_tin_array)

    # 1点目を一致させるための移動関連
    parallel_translation_amount_list = np.zeros((correction_target_tin_count,2), dtype = np.float64) #平行移動量

    # 2点目を一致させるための移動関連
    starting_point_list              = np.zeros((correction_target_tin_count,2), dtype = np.float64)    #原点扱い
    rotation_matrix_list             = np.zeros((correction_target_tin_count,2,2), dtype = np.float64)  #回転行列
    magnification_second_step_list   = np.zeros(correction_target_tin_count, dtype = np.float64)        #拡縮倍率(2点目を一致させるための拡縮)

    # 3点目を一致させるための移動関連
    position_standard_angle_list     = np.zeros(correction_target_tin_count, dtype = np.float64)        #位置基準角度（1点目と2点目で構成される線分をx軸に合わせるための回転）
    magnification_third_step_list    = np.zeros(correction_target_tin_count, dtype = np.float64)        #拡縮倍率(3点目のy座標を一致させるための拡縮)
    shear_angle_list                 = np.zeros((correction_target_tin_count,2), dtype = np.float64)    #せん断角度（0：補正対象、1:位置基準）

    #-----------------------------------------------------------------
    # 計算式作成
    #-----------------------------------------------------------------
    # 変換対象とする座標のみを抽出
    correction_target_tin_extract_array = correction_target_tin_array[:, :, target_coordinate_index].copy()
    position_standard_tin_extract_array = position_standard_tin_array[:, :, target_coordinate_index].copy()

    # GCPで構成されるTINの数だけ計算式を作成する。
    correction_target_tin_range = range(len(correction_target_tin_array))
    for i in correction_target_tin_range:

        # 補正対象TINと位置基準TINの座標が同値の場合は移動する必要が無いので計算式を作成しない
        if np.all(correction_target_tin_extract_array[i] == position_standard_tin_extract_array[i]):
            continue

        # 対応するTIN同士で計算式を作成する。
        formula_list = create_formula_to_correct_feature( correction_target_tin_extract_array[i]
                                                        , position_standard_tin_extract_array[i])
        # 1点目を一致させるための移動関連
        parallel_translation_amount_list[i] = formula_list[0]  #平行移動量

        # 2点目を一致させるための移動関連
        starting_point_list[i]              = formula_list[1]  #原点扱い
        rotation_matrix_list[i]             = formula_list[2]  #回転行列
        magnification_second_step_list[i]   = formula_list[3]  #拡縮倍率(2点目を一致させるための拡縮)

        # 3点目を一致させるための移動関連
        position_standard_angle_list[i]     = formula_list[4]  #位置基準角度（1点目と2点目で構成される線分をx軸に合わせるための回転）
        magnification_third_step_list[i]    = formula_list[5]  #拡縮倍率(3点目のy座標を一致させるための拡縮)
        shear_angle_list[i]                 = formula_list[6].copy()  #せん断角度（0：補正対象、1:位置基準）

    #-----------------------------------------------------------------
    # アフィン変換による座標変換
    #-----------------------------------------------------------------
    # 補正対象地物の全件の座標を、TINごとに作成した計算式で更新する
    correction_target_feature_number = len(correction_target_feature_array)
    correction_target_feature_range = range(len(correction_target_feature_array))

    for fi in correction_target_feature_range:

        # tinのインデックス取得
        temp_tin_index = tin_index_array[fi][1]

        # TINに内包されていない点ならば次の構成点へ
        if temp_tin_index == -1:
            continue

        # 補正対象TINと位置基準TINの座標が同値の場合は移動する必要が無いので補正しない
        elif np.all(correction_target_tin_extract_array[temp_tin_index] == position_standard_tin_extract_array[temp_tin_index]):
            continue

        else:

            if coordinate_plane == __coordinate_plane_xy__:
                correction_target_feature_array[fi, 1:3] = \
                    calculate_geometry_coordinate_by_formula( correction_target_feature_array[fi, 1:3]
                                                            , parallel_translation_amount_list[temp_tin_index]  #平行移動量
                                                            , starting_point_list[temp_tin_index]               #原点扱い
                                                            , rotation_matrix_list[temp_tin_index]              #回転行列
                                                            , magnification_second_step_list[temp_tin_index]    #拡縮倍率(2点目を一致させるための拡縮)
                                                            , position_standard_angle_list[temp_tin_index]      #位置基準度（1点目と2点目で構成される線分をx軸に合わせるための回転）
                                                            , magnification_third_step_list[temp_tin_index]     #拡縮倍率(3点目のy座標を一致させるための拡縮)
                                                            , shear_angle_list[temp_tin_index]                  #せん断角度（0：補正対象、1:位置基準）
                                                            )
            elif coordinate_plane == __coordinate_plane_xz__:

                correction_target_feature_array[fi, 1:4:2] = \
                    calculate_geometry_coordinate_by_formula( correction_target_feature_array[fi, 1:4:2]
                                                            , parallel_translation_amount_list[temp_tin_index]  #平行移動量
                                                            , starting_point_list[temp_tin_index]               #原点扱い
                                                            , rotation_matrix_list[temp_tin_index]              #回転行列
                                                            , magnification_second_step_list[temp_tin_index]    #拡縮倍率(2点目を一致させるための拡縮)
                                                            , position_standard_angle_list[temp_tin_index]      #位置基準度（1点目と2点目で構成される線分をx軸に合わせるための回転）
                                                            , magnification_third_step_list[temp_tin_index]     #拡縮倍率(3点目のy座標を一致させるための拡縮)
                                                            , shear_angle_list[temp_tin_index]                  #せん断角度（0：補正対象、1:位置基準）
                                                            )

    # TINの個数を取得
    TIN_count = len(correction_target_tin_array)

    # return_tin_arrayの初期値は補正対象TIN
    return_tin_array = correction_target_tin_array.copy()

    # Z座標のみ活かして更新して戻り値とする
    return_tin_array[:,:,1:3] = position_standard_tin_array[:,:,1:3]

    # 戻り値の設定と返却
    return correction_target_feature_array, return_tin_array


def calculate_geometry_coordinate_by_vector_affine_2D( correction_target_gcp_array
                                                     , position_standard_gcp_array
                                                     , correction_target_feature_array
                                                     , split_unit_number
                                                     , logger):
    """
    ----------------------------------------------------------------
    【概要】
    2Dベクトルアフィン xy平面上でベクトルアフィン処理を行う
    【引数】
    ①:補正対象GCPのNumpy配列 GCP>GCPのid + xy(z)座標
    ②:位置基準GCPのNumpy配列 GCP>GCPのid + xy(z)座標
    ③:補正対象地物のNumPy配列 地物>地物のid + xy(z)座標
    ④:並列処理最大スレッド数 int
    ⑤:分割単位 int
    ⑥:LOGGERオブジェクト
    【戻り値】
    ①補正後の補正対象地物
    ----------------------------------------------------------------
    """
    #-------------------------------------------------------
    # TINの生成
    #-------------------------------------------------------
    create_TIN_result = create_TIN( correction_target_gcp_array
                                  , position_standard_gcp_array)

    # 戻り値取得（0：補正対象TIN、1：位置基準TIN）
    correction_target_tin_array = create_TIN_result[__create_TIN_result_index_target_tin__]
    position_standard_tin_array = create_TIN_result[__create_TIN_result_index_standard_tin__]

    #-------------------------------------------------------
    # 内包判定
    #-------------------------------------------------------
    tin_index_array = judge_inclusion( correction_target_tin_array
                                     , position_standard_tin_array
                                     , correction_target_feature_array
                                     , split_unit_number
                                     , logger)

    #-------------------------------------------------------
    # アフィン変換(xy座標)
    #-------------------------------------------------------
    affine_result = execute_affine_transformation( correction_target_tin_array
                                                , position_standard_tin_array
                                                , correction_target_feature_array
                                                , split_unit_number
                                                , tin_index_array
                                                , __coordinate_plane_xy__)

    # 戻り値取得（0：補正対象地物、1：補正対象TIN（変換後））
    correction_target_feature_array = affine_result[__affine_result_index_target_feature__]

    return correction_target_feature_array


def correct_z_value( correction_target_tin_array
                   , position_standard_tin_array
                   , correction_target_feature_array
                   , tin_index_array):
    """
    ----------------------------------------------------------------
    3Dベクトルアフィンにおけるz値の補正
    【概要】
    位置基準に利用するGCPは基本的にマンホールや道路縁で構成されているため
    xz平面上でGCPの内包判定を行うと地表面付近の物体しか位置補正できない。
    そのため、xy平面上で内包はんていを行った上でxz平面で補正を行っていたが
    xz平面上ではGCPで構成されるTINが反転する場合があり、補正結果のz値が
    異常値となる現象が確認されたため、xz平面上の1次関数を用いた移動量にて補正する。
    【引数】
    ①補正対象GCPで構成されるTINのNumpy配列 id + xyz座標([[[id, x, y, z], [id, x, y, z], [id, x, y, z]], ...])
    ②位置基準GCPで構成されるTINのNumpy配列 id + xyz座標
    ③補正対象地物のNumPy配列 id + xyz座標（[[id, x, y, z], ...]）
    ④内包判定結果（構成点id（全地物を通した先頭から連番） + TINのインデックス）
    【戻り値】
    ①補正後の補正対象地物
    ----------------------------------------------------------------
    """
    # 計算用にTINを複写
    temp_target_tin_array = correction_target_tin_array.copy()
    temp_standard_tin_array = position_standard_tin_array.copy()

    # TIN別の左右の変化量を算出する計算式の配列初期化（[[[a, b], [a, b]], ...]）※一次関数（z' = ax + b）の係数aと切片b
    tin_diff_array = []

    # 全てのTINを対象に処理を行う
    for tin_index in range(len(temp_target_tin_array)):
        # 補正対象TINの構成点をx座標の昇順に整列（以下、整列後のTINの構成点を点A、B、Cとする）
        tin_dataframe = pd.DataFrame(data = temp_target_tin_array[tin_index], columns = __tin_dataframe_column_list__, copy = True)
        temp_target_tin_array[tin_index] = tin_dataframe.sort_values(__tin_dataframe_column_list__[__point_coordinate_index_x__]).to_numpy(copy = True)

        # 位置基準TINの構成点をx座標の昇順に整列
        tin_dataframe = pd.DataFrame(data = temp_standard_tin_array[tin_index], columns = __tin_dataframe_column_list__, copy = True)
        temp_standard_tin_array[tin_index] = tin_dataframe.sort_values(__tin_dataframe_column_list__[__point_coordinate_index_x__]).to_numpy(copy = True)

        # TINごとに、左側（0：点A～B）、右側（1：点B～C）の範囲内にある
        # 任意のxにおけるzの移動量を算出するための一次関数（ax + b = z）を算出

        # 補正対象TIN
        # 各点の取得
        pointA = temp_target_tin_array[tin_index][__tin_point_index_A__]
        pointB = temp_target_tin_array[tin_index][__tin_point_index_B__]
        pointC = temp_target_tin_array[tin_index][__tin_point_index_C__]

        # 線分AB(x * a + 1 * b = z、の連立でa, bを算出)
        target_AB = np.linalg.solve(
            [
                [pointA[__point_coordinate_index_x__], 1],
                [pointB[__point_coordinate_index_x__], 1]
            ],
            [
                pointA[__point_coordinate_index_z__],
                pointB[__point_coordinate_index_z__]
            ]
        )

        # 線分BC(x * a + 1 * b = z、の連立でa, bを算出)
        target_BC = np.linalg.solve(
            [
                [pointB[__point_coordinate_index_x__], 1],
                [pointC[__point_coordinate_index_x__], 1]
            ],
            [
                pointB[__point_coordinate_index_z__],
                pointC[__point_coordinate_index_z__]
            ]
        )

        # 位置基準TIN
        # 各点の取得
        pointA = temp_standard_tin_array[tin_index][__tin_point_index_A__]
        pointB = temp_standard_tin_array[tin_index][__tin_point_index_B__]
        pointC = temp_standard_tin_array[tin_index][__tin_point_index_C__]

        # 線分AB(x * a + 1 * b = z、の連立でa, bを算出)
        standard_AB = np.linalg.solve(
            [
                [pointA[__point_coordinate_index_x__], 1],
                [pointB[__point_coordinate_index_x__], 1]
            ],
            [
                pointA[__point_coordinate_index_z__],
                pointB[__point_coordinate_index_z__]
            ]
        )

        # 線分BC(x * a + 1 * b = z、の連立でa, bを算出)
        standard_BC = np.linalg.solve(
            [
                [pointB[__point_coordinate_index_x__], 1],
                [pointC[__point_coordinate_index_x__], 1]
            ],
            [
                pointB[__point_coordinate_index_z__],
                pointC[__point_coordinate_index_z__]
            ]
        )

        # 左右の差分の計算式を取得
        diff_AB = standard_AB - target_AB
        diff_BC = standard_BC - target_BC


        # 左側（0：点A～B）、右側（1：点B～C）として追加
        tin_diff_array.append([diff_AB, diff_BC])


    # 補正対象地物の構成点（以下、補正対象点とする）数分、処理を繰り返す
    for point_index in range(len(correction_target_feature_array)):
        # 補正対象点をxy平面上で内包する補正対象TINの座標を取得[[id, x, y, z], [id, x, y, z], [id, x, y, z]]
        target_tin = temp_target_tin_array[tin_index_array[point_index][1]]

        # 補正対象点のx座標と、点Bのx座標を比較し、左側（0：点A～B）、右側（1：点B～C）を判定する
        # 左側（A～Bと同値含む）
        if target_tin[__tin_point_index_B__][__point_coordinate_index_x__] >= correction_target_feature_array[point_index][__point_coordinate_index_x__]:
            position_index = __position_left__
        # 右側（B～C）
        else:
            position_index = __position_right__

        # 判定結果の式から移動量を算出
        diff_formula = tin_diff_array[tin_index_array[point_index][1]][position_index]
        diff_z = diff_formula[__diff_formula_index_a__] * correction_target_feature_array[point_index][__point_coordinate_index_x__] + diff_formula[__diff_formula_index_b__]

        # 元のz値に移動量を加算して補正後のz値とする
        correction_target_feature_array[point_index][__point_coordinate_index_z__] = correction_target_feature_array[point_index][__point_coordinate_index_z__] + diff_z

    # 戻り値の返却
    return correction_target_feature_array


def calculate_geometry_coordinate_by_vector_affine_3D( correction_target_gcp_array
                                                     , position_standard_gcp_array
                                                     , correction_target_feature_array
                                                     , split_unit_number
                                                     , logger):
    """
    ----------------------------------------------------------------
    【概要】
    3Dベクトルアフィン xy平面上でベクトルアフィン処理を行う
    【引数】
    ①:補正対象GCPのNumpy配列 id + xyz座標
    ②:位置基準GCPのNumpy配列 id + xyz座標
    ③:補正対象地物のNumPy配列 id + xyz座標
    ④:並列処理最大スレッド数 int
    ⑤:分割単位 int
    ⑥:LOGGERオブジェクト
    【戻り値】
    ①補正後の補正対象地物
    ----------------------------------------------------------------
    """
    # -------------------------------------------------------
    # TINの生成
    # -------------------------------------------------------
    create_TIN_result = create_TIN( correction_target_gcp_array
                                  , position_standard_gcp_array)

    # 戻り値取得（0：補正対象TIN、1：位置基準TIN）
    correction_target_tin_array = create_TIN_result[__create_TIN_result_index_target_tin__]
    position_standard_tin_array = create_TIN_result[__create_TIN_result_index_standard_tin__]

    # -------------------------------------------------------
    # 内包判定
    # -------------------------------------------------------
    tin_index_array = judge_inclusion( correction_target_tin_array
                                     , position_standard_tin_array
                                     , correction_target_feature_array
                                     , split_unit_number
                                     , logger)

    # -------------------------------------------------------
    # アフィン変換(xy座標)
    # -------------------------------------------------------
    affine_result = execute_affine_transformation( correction_target_tin_array
                                                , position_standard_tin_array
                                                , correction_target_feature_array
                                                , split_unit_number
                                                , tin_index_array
                                                , __coordinate_plane_xy__)

    # 戻り値取得（0：補正対象地物、1：補正対象TIN（変換後））
    correction_target_feature_array = affine_result[__affine_result_index_target_feature__]
    correction_target_tin_array     = affine_result[__affine_result_index_target_tin__]

    # -------------------------------------------------------
    # z値の補正
    # -------------------------------------------------------
    correction_target_feature_array = correct_z_value( correction_target_tin_array
                                                     , position_standard_tin_array
                                                     , correction_target_feature_array
                                                     , tin_index_array)

    # 戻り値取得（0：補正対象地物、1：補正対象TIN（変換後））

    return correction_target_feature_array


@jit(f8[:,:](f8[:,:], f8[:,:]), nopython=True, cache=True, nogil=True)
def get_bottom_polygon_coordinates_from_wire_frames(wire_frame_array2
                                               , wire_frame_array3
                                               ):

    # 片方のワイヤーフレームを逆順に入れ替え
    reverse_wire_frame_array3=wire_frame_array3[::-1, :]

    # 底面を作成 ワイヤーフレーム1+逆順ワイヤーフレーム2+ワイヤーフレーム1の始点（ポリゴン閉じるため）
    bottom_polygon_array=np.concatenate(( wire_frame_array2
                                        , reverse_wire_frame_array3
                                        , wire_frame_array2[0:1,:]))

    return bottom_polygon_array

def get_bottom_polygon_array(xyz_array, width, depth,coefficient=np.array(0.0)):

    # ワイヤーフレーム関連情報を生成
    # 上右、下右、下左、上左の各ワイヤーフレーム情報を取得
    ur_inf, lr_inf, ll_inf, ul_inf = get_wire_frame_info(
        xyz_array, np.float64(width), np.float64(depth) , coefficient
    )

    # ワイヤーフレームから線分情報を取得
    # [0]は調整済みのワイヤーフレーム線分を指す
    a2 = lr_inf[0]  # 下右ワイヤーフレーム
    a3 = ll_inf[0]  # 下左ワイヤーフレーム

    # 2,3が底面のワイヤーフレーム
    # ワイヤーフレームからTINZ作成関数呼び出し
    b = get_bottom_polygon_coordinates_from_wire_frames(a2, a3)
    
    return b


def _get_smoothing_coordinates_array_by_distance( coordinates_array
                                                , epsilon=0.25
                                                ):
    """
    短い距離削除用 id+xyz デフォルト値25cm未満なら削除
    """
    id_unique_array=np.unique(coordinates_array[:,0])
    result_list=[]

    for i in range(len(id_unique_array)):
        temp_coodinates_array\
            =coordinates_array[coordinates_array[:,0]==id_unique_array[i]]
        # 2点の場合そのまま追加
        if len(temp_coodinates_array)==2:
            result_list.append(temp_coodinates_array.copy())
        else:

            # 構成点間距離
            temp_distance\
                =get_distance(temp_coodinates_array[:,1:3])

            # 出力対象bool
            distance_bool_array=temp_distance>epsilon

            temp_bool_array=np.ones(len(temp_coodinates_array), dtype=np.bool_)

            temp_bool_array[1:]=distance_bool_array.copy()

            if temp_bool_array[-1]==False:
                temp_bool_array[-1]=True
                temp_bool_array[-2]=False
            else:
                pass
            result_list.append(temp_coodinates_array[temp_bool_array].copy())

    return np.concatenate(result_list,0)


@jit('Tuple((i8[:], i8[:]))(f8[:,:])', nopython=True, cache=True, nogil=True)
def get_start_index_and_end_index(feature_array):
    """
    引数の1列目にあるID列を元に始点と終点のインデックスを取得する。
    引数:id+xyz座標の2次元numpy配列
    戻り値:始点インデックス、終点インデックスの1次元numpy配列
    """
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
    start_index_array = np.where((geom_id_array == geom_id_roll_array1) == False)[0]

    # ID列を比較し異なるインデックスリストを取得→終点のインデックス
    # 具体例：①[0, 0, 0, 1, 1, 2, 2, 2]
    # 　　　　③[0, 0, 1, 1, 2, 2, 2, 0]
    # 　　　　→[2, 4, 7]が終点のインデックス
    end_index_array = np.where((geom_id_array == geom_id_roll_array2) == False)[0]

    return start_index_array, end_index_array


@jit(i8(i8), nopython=True, cache=True, nogil=True)
def get_max_tile_counts(zoom_level=26):
    """
    ------------------------------------------------------------------------------
    【空間ID】空間IDの計算式資料
    ------------------------------------------------------------------------------
    経緯度→zfxyの変換に関して https://github.com/unvt/zfxy-spec/blob/main/README.md
    zfxy→経緯度の変換に関して https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    経緯度の範囲は明確に どうやら-π＜θ＜π
    経度：東経が正、西経が負
    緯度：北緯が正、南緯が負
    ------------------------------------------------------------------------------
    【空間ID】ズームレベルごとの1辺当たりのタイル枚数の取得
    voxelの1辺あたり50cmに該当するzoomlevelは26
    """
    return 2**zoom_level


@jit(i8(i8), nopython=True, cache=True, nogil=True)
def get_max_height(exponent=25):
    """
    【空間ID】高さの最大値取得
    まだ決まっていないが空間IDは2^25と定義している　33,554,432 地球１周よりも短いくらいの長さ
    """
    return 2**exponent


@jit(i8[:](f8[:], i8), nopython=True, cache=True, nogil=True)
def convert_longitude2xid(longitude, max_tile
                          ):
    """
    【空間ID】経度をXIDに変換する。
    引数1：longitudeの単位は度[degree]
    """
    return np.floor(max_tile * ((longitude + 180) / 360)).astype(np.int64)


@jit(i8[:](f8[:], i8), nopython=True, cache=True, nogil=True)
def convert_latitude2yid(latitude, max_tile
                         ):
    """
    【空間ID】緯度をYIDに変換する。
    引数1：latitudeの単位は度[degree]
    """
    latitude_rad = latitude * np.pi / 180
    return np.floor(max_tile * (1 - np.log(np.tan(latitude_rad) + (1 / np.cos(latitude_rad))) / np.pi) / 2).astype(np.int64)


@jit(i8[:](f8[:], i8, i8), nopython=True, cache=True, nogil=True)
def convert_height2fid(height, max_tile, max_height
                       ):
    """
    【空間ID】高さをFIDに変換する。
    引数1：高さの単位はメートル[m]
    """
    return np.floor(max_tile * height / max_height).astype(np.int64)


@jit(f8[:](i8[:], i8), nopython=True, cache=True, nogil=True)
def convert_xid2longitude(x_index, max_tile
                          ):
    """
    【空間ID】XIDを左上の経度（単位は度[degree]）に変換する
    """
    return x_index / max_tile * 360.0 - 180.0


@jit(f8[:](i8[:], i8), nopython=True, cache=True, nogil=True)
def convert_yid2latitude(y_index, max_tile
                         ):
    """
    【空間ID】YIDを左上の緯度（単位は度[degree]）に変換する。
    """
    latitude_rad = np.arctan(np.sinh(np.pi * (1 - 2 * y_index / max_tile)))
    return np.degrees(latitude_rad)


@jit(f8[:](i8[:], i8, i8), nopython=True, cache=True, nogil=True)
def convert_fid2height(f_index, max_tile, max_height
                       ):
    """
    【空間ID】FIDを左上の高さに変換する。
    """
    return f_index * max_height / max_tile


def get_spatial_id_center_point_from_spatial_id(f_index_array,
                                           x_index_array,
                                           y_index_array,
                                           zoom_level,
                                           exponent=25
                                           ):
    """
    空間IDの配列から中心座標を取得

    引数1:fインデックス1次元配列
    引数2:xインデックス1次元配列
    引数3:yインデックス1次元配列
    引数4:ズームレベル
    戻り値1:ボクセルの中心Z座標
    戻り値2:ボクセルの中心X座標
    戻り値3:ボクセルの中心Y座標
    """
    # タイル枚数と高さ上限取得
    max_tile=get_max_tile_counts(zoom_level)
    max_height=get_max_height(exponent)

    # 空間IDのボクセルにおける北西下部と南東上部取得
    north_west_height_array\
        =convert_fid2height(f_index_array,
                            max_tile,
                            max_height)
    south_east_height_array\
        =convert_fid2height(f_index_array+1,
                            max_tile,
                            max_height)

    # idを経緯度座標へ変換 北西の経緯度と南東の経緯度を取得する
    # 北西x座標、南東x座標
    north_west_longtitude_array\
        =convert_xid2longitude(x_index_array,
                               max_tile)
    south_east_longtitude_array\
        =convert_xid2longitude(x_index_array+1,
                               max_tile)

    # 北西y座標、南東y座標
    north_west_latitude_array\
        =convert_yid2latitude(y_index_array,
                              max_tile)
    south_east_latitude_array\
        =convert_yid2latitude(y_index_array+1,
                              max_tile)

    # 中心の座標取得
    center_z=(north_west_height_array+south_east_height_array)/2
    center_lon=(north_west_longtitude_array+south_east_longtitude_array)/2
    center_lat=(north_west_latitude_array+south_east_latitude_array)/2

    return center_z,\
           center_lon,\
           center_lat


@jit(b1(f8[:], f8[:,:]),nopython=True,cache=True,nogil=True)
def is_perpendicular_point_segment(point_array, segment_array):
    """
    点から線分に向かって垂線が下せるかチェック
    旗上げの検知に使えるか調査したい
    引数1:ポイント 1次元配列 [x,y,z]
    引数2:ライン 2次元配列 [[x1,y1,z2],[x2,y2,z2]]
    """
    # 垂線が下せるか判定
    # 参考：http://marupeke296.com/COL_3D_No27_CapsuleCapsule.html
    # 線分の両端点:A,B 垂線の始点:C
    # CA・ABの内積とCB・BAの内積の積の正負によって垂線が引けるか判定する
    inter_product1 = np.dot(segment_array[0]-point_array, segment_array[1]-segment_array[0])
    inter_product2 = np.dot(segment_array[1]-point_array, segment_array[0]-segment_array[1])

    return inter_product1*inter_product2 >= 0


@jit(f8(f8[:], f8[:]),nopython=True,cache=True,nogil=True)
def get_distance_points(point1, point2):
    """
    2点間の距離を算出
    どちらも1次元配列、xy,xyz問わない
    """
    # 距離計算
    distance=np.sqrt(np.sum(np.power(point1-point2,2)))

    return distance


@jit(f8(f8[:], f8[:,:]),nopython=True,cache=True,nogil=True)
def get_distance_point_segment_xy(point_array, segment_array):
    """
    点から線分までの距離
    垂線が下せるかどうか関係なく線分が存在する直線にたいして垂線をおろしてその長さを求める
    引数1:ポイント 1次元配列 [x,y,z]
    引数2:ライン 1次元配列 [[x1,y1][x2,y2]]
    """
    # 外積の大きさ→平行四辺形の面積
    cross_value = np.abs(cross2d( segment_array[1]-segment_array[0]
                                , point_array - segment_array[0]
                                ))

    # 線分の距離
    line_length = get_distance_points(segment_array[0],segment_array[1])

    # 線分を底辺として割ると高さ（点と直線の距離が出る）
    return cross_value / line_length


@jit(f8[:](f8[:], f8[:,:]),nopython=True,cache=True,nogil=True)
def get_perpendicular_foot_array(point_array,
                                 segment_array):
    """
    垂線の足の座標取得
    """

    # 直線の方向を表す単位ベクトル
    normal_vector = (segment_array[1] - segment_array[0]) /\
                     np.linalg.norm(segment_array[1] - segment_array[0])

    # 点から直線に対して下ろした垂線の足の座標
    perpendicular_foot_array = np.dot((point_array - segment_array[0]),normal_vector) * normal_vector + segment_array[0]

    return perpendicular_foot_array


@jit('Tuple((f8,f8[:]))(f8[:], f8[:,:])', nopython=True, cache=True, nogil=True)
def get_min_distance_coordinates(point_array,
                                 segment_array):
    """
    点と直線の最小距離とその距離をなす座標を取得
    """

    # 線分が同一構成点なら最初の点との距離を返す
    segment_distance=get_distance_points(segment_array[0],segment_array[1])

    if segment_distance==0:
        min_distance=get_distance_points(point_array,segment_array[0])
        result_array=segment_array[0].copy()
    else:

        # 垂線が引けるか判定
        if is_perpendicular_point_segment(point_array,
                                          segment_array):
            # 垂線を下した時の距離取得
            min_distance\
                =get_distance_point_segment_xy(point_array,
                                               segment_array)

            # 垂線の足の座標取得
            result_array\
                =get_perpendicular_foot_array(point_array,
                                              segment_array)

        else:

            # 線分の両端点との距離計算
            # 小さいほうを返す
            distance1=get_distance_points(point_array,segment_array[0])
            distance2=get_distance_points(point_array,segment_array[1])

            if distance1<distance2:
                min_distance=distance1
                result_array=segment_array[0].copy()

            else:
                min_distance=distance2
                result_array=segment_array[1].copy()

    return min_distance,result_array


@jit('Tuple((f8[:],f8[:,:]))(f8[:], f8[:,:])', nopython=True, cache=True, nogil=True)
def get_min_distance_coordinates_array(point_array,
                                       linestring_array):
    """
    1点と複数のラインの最小距離とその距離をなす座標を取得する
    """

    # 線分の数
    segment_count=len(linestring_array)-1

    # 最小距離格納用配列（要素数は線分の数）
    result_distance_array\
        =np.zeros(segment_count, dtype=np.float64)

    # 最小距離をなす座標格納用初期配列（要素数は線分の数）
    result_coordinates_array\
        =np.zeros((segment_count,2), dtype=np.float64)

    for i in prange(segment_count):
        min_distance,result_array\
            =get_min_distance_coordinates(point_array,
                                          linestring_array[i:i+2,:])
        result_distance_array[i]=min_distance
        result_coordinates_array[i]=result_array

    return result_distance_array,\
           result_coordinates_array


@jit('Tuple((f8[:],f8[:,:], i8[:]))(f8[:,:], f8[:,:])', nopython=True, cache=True, nogil=True)
def get_snapped_point_to_linestring(point_array,
                                    linestring_array):
    """
    ポイントをラインに最近傍でスナップしてその座標を取得する
    スナップはxy平面上での最短距離で判定する
    引数１：ポイント座標配列（２次元配列）[ラインの構成点]＞[id+xy(z)]
    引数２：ライン座標配列（２次元配列）[ラインの構成点]＞[id+xy(z)]
    戻り値：ポイントごとの最近傍のスナップ座標
    戻り値：ポイントごとの最近傍距離配列
    戻り値：ポイントごとのスナップ先ラインのID
    """
    # ポイントの数
    point_count=len(point_array)

    # ラインの始点終点インデックスを取得する
    # lsi:ラインの始点インデックス
    # lei:ラインの終点インデックス
    lsi,lei=get_start_index_and_end_index(linestring_array)

    # ラインの地物数
    linestring_count=len(lsi)

    # 最小距離格納用配列（要素数はポイントの数）
    result_distance_array\
        =np.zeros(point_count, dtype=np.float64)

    # 最小距離をなす座標格納用初期配列（要素数はポイントの数）

    result_coordinates_array=point_array.copy()

    # 最小距離をなすラインのインデックス
    result_linestring_index_array\
            =np.zeros(point_count, dtype=np.int64)

    # ポイントの数だけループ
    for pi in prange(point_count):

        # 1つのポイントに対する全ラインの情報
        temp_point_distance_array\
            =np.zeros(linestring_count, dtype=np.float64)
        temp_point_coordinates_array\
            =np.zeros((linestring_count, 2), dtype=np.float64)
        temp_point_linestring_index_array\
            =np.zeros(linestring_count, dtype=np.int64)

        # ラインごとに処理
        for li in prange(linestring_count):

            # 1つのラインの線分に対する距離と最小距離取得
            temp_distance_array,\
            temp_coordinates_array\
                =get_min_distance_coordinates_array(point_array[pi,1:3],
                                                    linestring_array[lsi[li]:lei[li]+1,1:3])

            # 最小値のインデックス取得
            temp_argmin=np.argmin(temp_distance_array)
            temp_point_distance_array[li]=temp_distance_array[temp_argmin]
            temp_point_coordinates_array[li]=temp_coordinates_array[temp_argmin]
            temp_point_linestring_index_array[li]=li

        # 最小距離を持つ座標を結果の配列に設定
        temp_point_argmin=np.argmin(temp_point_distance_array)

        # 結果格納用配列更新
        result_distance_array[pi]=temp_point_distance_array[temp_point_argmin]
        result_coordinates_array[pi,1:3]=temp_point_coordinates_array[temp_point_argmin]
        result_linestring_index_array[pi]=temp_point_linestring_index_array[temp_point_argmin]

    return result_distance_array,\
           result_coordinates_array,\
           result_linestring_index_array


@jit(f8[:](f8[:],f8[:,:]),nopython=True,cache=True,nogil=True)
def get_distance_one_point_to_points(point_array,
                                     distance_points_array):
    """
    1つの点から多数の点に向かって距離を計算する
    引数1:座標1次元配列 [x,y]
    引数2:距離計測座標対象座標2次元配列 [point...]>[x,y]
    戻り値:距離1次元配列 [disance...]
    """
    distance_array\
        =np.sqrt(np.sum(np.power(distance_points_array-point_array,2), axis=1))

    return distance_array


def add_point_to_linestring(points_array,
                            linestring_array,
                            minimum_thresod,
                            snap_maximum_distance):
    """
    ポイントをラインに追加
    ポイントの座標
    追加対象のラインID
    同一構成点とする距離の閾値
    """

    # 更新用ライン配列
    result_linestring_array=linestring_array.copy()

    # 追加対象ポイントごとに更新処理
    for pi in range(len(points_array)):
        # 同一構成点がある場合
        # まずはラインの構成点ごとの距離算出
        # その構成点に対して追加対象ポイントの座標を設定する
        points_distance_array\
            =get_distance_one_point_to_points(points_array[pi,:2],
                                              result_linestring_array[:,:2])
        points_distance_bool=points_distance_array<=minimum_thresod

        if np.any(points_distance_bool):
            result_linestring_array[points_distance_bool]=points_array[pi]
            continue

        else:

            # 同一構成点がない場合
            # 線分に対する距離の最小値を計算
            temp_distance_array,\
            temp_coordinates_array\
               =get_min_distance_coordinates_array(points_array[pi,:2],
                                                   result_linestring_array[:,:2])

            segment_index=np.argmin(temp_distance_array)

            # 最小の距離がスナップ上限を超える場合
            if temp_distance_array[segment_index]>snap_maximum_distance:

                # 次の点へ
                continue
            else:
                # 線分のインデックスを取得して元のラインを更新
                result_linestring_array=np.insert(result_linestring_array,
                                                  segment_index+1,
                                                  points_array[pi],
                                                  axis=0)
    return result_linestring_array


def get_snapped_point_to_linestring_multi(point_array,
                                         linestring_array,
                                         minimum_threthold):
    """
    ポイントをラインに最近傍でスナップしてその座標を取得する
    スナップはxy平面上での最短距離で判定する
    引数１：ポイント座標配列（２次元配列）[ラインの構成点]＞[id+xy(z)]
    引数２：ライン座標配列（２次元配列）[ラインの構成点]＞[id+xy(z)]
    戻り値：ポイントごとの最近傍のスナップ座標
    戻り値：ポイントごとの最近傍距離配列
    戻り値：ポイントごとのスナップ先ラインのID
    """
    # ポイントの数
    point_count=len(point_array)

    # ラインの始点終点インデックスを取得する
    # lsi:ラインの始点インデックス
    # lei:ラインの終点インデックス
    lsi,lei=get_start_index_and_end_index(linestring_array)

    # ラインの地物数
    linestring_count=len(lsi)

    result_distance_list=[]
    result_coordinates_list=[]
    result_linestring_list=[]

    # ポイントの数だけループ
    for pi in prange(point_count):

        # 1つのポイントに対する全ラインの情報
        temp_point_distance_array\
            =np.zeros(linestring_count, dtype=np.float64)
        temp_point_coordinates_array\
            =np.zeros((linestring_count, 2), dtype=np.float64)
        temp_point_linestring_index_array\
            =np.zeros(linestring_count, dtype=np.int64)

        # ラインごとに処理
        for li in prange(linestring_count):

            # 1つのラインの線分に対する距離と最小距離取得
            temp_distance_array,\
            temp_coordinates_array\
                =get_min_distance_coordinates_array(point_array[pi,1:3],
                                                    linestring_array[lsi[li]:lei[li]+1,1:3])

            # 最小値のインデックス取得
            temp_argmin=np.argmin(temp_distance_array)
            temp_point_distance_array[li]=temp_distance_array[temp_argmin]
            temp_point_coordinates_array[li]=temp_coordinates_array[temp_argmin]
            temp_point_linestring_index_array[li]=li

        # 最小距離を持つ座標を結果の配列に設定
        temp_point_min_index_array=temp_point_distance_array<minimum_threthold
        temp_point_array=temp_point_coordinates_array[temp_point_min_index_array]
        result_point_array=np.tile(point_array[pi,1:],(len(temp_point_array),1))
        result_point_array[:,:2]=temp_point_array

        # これを複製してpoint_array[pi,1:]

        # 結果格納用配列更新
        result_distance_list.append(temp_point_distance_array[temp_point_min_index_array])
        result_coordinates_list.append(result_point_array)
        result_linestring_list.append(temp_point_linestring_index_array[temp_point_min_index_array])

    return np.concatenate(result_distance_list),\
           np.concatenate(result_coordinates_list),\
           np.concatenate(result_linestring_list)


def _add_snapped_point_to_linestring(linestring_array,
                                     point_array,
                                     point_link_index_array,
                                     minimum_threshold,
                                     snap_maximum_distance):
    """
    ラインに対して紐づいたポイントを構成点に追加する
    """

    si,ei=get_start_index_and_end_index(linestring_array)

    # 構成点追加後格納用List
    result_list=[]

    # ラインごとに処理
    for li in range(len(si)):

        # ラインのid取得
        temp_id=int(linestring_array[si[li],0])

        #ラインのidと等しい点を取得
        target_point_bool=point_link_index_array==temp_id

        # 一つも対象がなければ辞書に設定して次のラインへ
        if np.any(target_point_bool)==False:
            result_list.append(linestring_array[si[li]:ei[li]+1,:])
            continue

        else:
            # 追加更新対象ポイント
            temp_added_linestring_array\
                =add_point_to_linestring(point_array[target_point_bool,1:],
                                         linestring_array[si[li]:ei[li]+1,1:],
                                         minimum_threshold,
                                         snap_maximum_distance)

            # id付きにして出力
            added_linestring_array=np.zeros((temp_added_linestring_array.shape[0],
                                             temp_added_linestring_array.shape[1]+1),
                                            dtype=np.float64)

            added_linestring_array[:,1:]=temp_added_linestring_array
            added_linestring_array[:,0]=temp_id
            result_list.append(added_linestring_array)

    return np.concatenate(result_list)


@jit("Tuple((f8[:,:,:], f8[:,:,:]))(f8[:,:], f8[:,:])", nopython=True, cache=True, nogil=True, parallel=True)
def connect_circles(circle1, circle2):
    """
    2つの円周の点を三角形メッシュに接続する（円柱の側面）。
    さらに、各対応する頂点をつないだ線分情報も取得する。

    :param circle1: 下側の円の点 (N, 3)
    :type circle1: numpy.ndarray
    :param circle2: 上側の円の点 (N, 3)
    :type circle2: numpy.ndarray

    :return: 三角形メッシュのリスト(triangles), 線分リスト(edges)
        - triangles: 側面の三角形メッシュ (N*2, 3, 3)
        - edges: 対応する頂点をつないだ線分 (N, 2, 3)
    :rtype: tuple[numpy.ndarray, numpy.ndarray]
    """

    num_points = len(circle1)
    triangles = np.zeros((num_points * 2, 3, 3), dtype=np.float64)  # 三角形メッシュ
    edges = np.zeros((num_points, 2, 3), dtype=np.float64)  # 頂点間の線分

    for i in prange(num_points):
        next_i = (i + 1) % num_points  # 次の頂点（円周なのでループする）

        # **三角形メッシュの作成（円柱の側面）**
        triangles[i * 2] = np.vstack((circle1[i], circle2[i], circle2[next_i]))
        triangles[(i * 2) + 1] = np.vstack((circle1[i], circle2[next_i], circle1[next_i]))

        # **各頂点を直線でつなぐ線分情報を追加**
        edges[i] = np.vstack((circle1[i], circle2[i]))  # 下側の点 → 上側の点

    return triangles, edges


@jit(f8[:,:,:](f8[:,:]), nopython=True, cache=True, nogil=True, parallel=True)
def triangulate_circle(circle_points):
    """
    円周の点を基に三角形分割を行い、円のメッシュを作成する。

    :param circle_points: 円周の点 (N, 3)
    :type circle_points: numpy.ndarray

    :return: 三角形メッシュ (N, 3, 3)
    :rtype: numpy.ndarray
    """

    center = np.sum(circle_points, axis=0) / circle_points.shape[0]
    triangles = np.zeros((len(circle_points), 3, 3), dtype=np.float64)

    for i in prange(len(circle_points)):
        next_i = (i + 1) % len(circle_points)
        triangles[i] = np.vstack((center, circle_points[i], circle_points[next_i]))

    return triangles


@jit(f8[:](f8[:, :]), nopython=True, cache=True, nogil=True, parallel=True)
def calculate_row_norms(arr):
    """
    2次元配列の NumPy 配列に対し、行ごとのノルムを算出する。
    JITコンパイラはnumpy.linalg.norm関数のaxisパラメータをサポートしていないため、
    当関数でnumpy.linalg.norm(array, axis=1)と同等の処理を行う。

    :param arr: 処理対象の NumPy 配列 (N, 3)
    :type arr: numpy.ndarray

    :return: 行ごとのノルムを持つ結果 (N)
    :rtype: numpy.ndarray
    """

    # 行ごとのノルムを計算
    norms = np.empty(arr.shape[0], dtype=np.float64)  # 結果を格納する配列
    for i in prange(arr.shape[0]):
        norms[i] = np.sqrt(np.sum(arr[i] ** 2))  # 各行のL2ノルムを計算
    return norms


@jit(f8[:, :, :](f8[:, :, :], f8), nopython=True, cache=True, nogil=True)
def split_segment_edges(segment_edges, interval):
    """
    各セグメントの分割点リストを生成する。

    :param segment_edges: 複数のエッジを持つセグメント (N, 2, 3)
    :type segment_edges: numpy.ndarray
    :param interval: 線分を分割する間隔
    :type interval: float

    :return: セグメントの分割点リスト (N, M, 3)
    :rtype: numpy.ndarray
    """

    # **セグメント内の最長線分の長さを計算**
    lengths = calculate_row_norms(segment_edges[:, 1, :] - segment_edges[:, 0, :])
    longest_edge = np.max(lengths)

    # **統一された分割数を決定**
    split_num = max(1, int(np.ceil(longest_edge / interval)))

    segment_split_edges = np.zeros((len(segment_edges), split_num + 1, 3), dtype=np.float64)

    # **各線分を統一された分割数で分割**
    for i in range(len(segment_edges)):
        start_point, end_point = segment_edges[i]

        # **分割点を統一された `split_num` 個にする**
        t_values = np.linspace(0, 1, split_num + 1)[:, None]  # 0 から 1 の等間隔な値
        split_points = (1 - t_values) * start_point + t_values * end_point  # 線形補間

        segment_split_edges[i] = split_points

    return segment_split_edges


def split_edges_by_interval(edge_coordinates, interval=0.5):
    """
    各セグメントごとの線分を、セグメント内の最長線分の長さを基準にした統一分割数で分割する。

    :param edge_coordinates: [セグメント数, 頂点数, 2点（始点・終点）, 3座標]の形で構成されるセグメントデータ (N, M, 2, 3)
    :type edge_coordinates: numpy.ndarray
    :param interval: 線分を分割する間隔 (デフォルト: 0.5)
    :type interval: float

    :return: 各セグメントごとのNumPy配列 (M, X, 3) をN個持つリスト
    :rtype: list[numpy.ndarray]
    """

    split_edge_coordinates = []  # セグメントごとのリスト

    for segment_edges in edge_coordinates:
        segment_split_edges = split_segment_edges(segment_edges, interval)
        split_edge_coordinates.append(segment_split_edges)  # **NumPy 配列に変換してセグメントごとに保存**

    return split_edge_coordinates  # **リストのリストを返す**


@jit(f8[:, :, :, :](f8[:, :, :]), nopython=True, cache=True, nogil=True, parallel=True)
def process_segment_triangulation(segment):
    """
    各セグメントに対し三角形分割を行う。

    :param segment: 三角形分割処理対象のセグメント (N, M, 3)
    :type segment: numpy.ndarray

    :return: セグメントの三角形分割処理後の結果 (M, N, 3, 3)
    :rtype: numpy.ndarray
    """

    num_vertices = segment.shape[0]  # 頂点数 (例: 16)
    num_splits = segment.shape[1]

    triangulated_segments = np.zeros((num_splits, num_vertices, 3, 3), dtype=np.float64)

    # **分割位置ごとに 16頂点を集めて三角形分割**
    for x in prange(num_splits):  # 各分割位置
        circle_coords = segment[:, x, :]  # (num_vertices, 3)

        # **分割位置の円を三角形に分割**
        triangles = triangulate_circle(circle_coords)

        triangulated_segments[x] = triangles

    return triangulated_segments


def generate_segment_triangulation(split_edge_coordinates):
    """
    分割された線分から、各分割位置に対応する円の頂点を集め、
    triangulate_circle() を適用して三角形メッシュを生成する。

    :param split_edge_coordinates: [セグメント数, 分割数, X, 3座標]の形で構成される分割後の線分座標 (N, M, X, 3) を持つリスト
    :type split_edge_coordinates: list[numpy.ndarray]

    :return: NumPy配列の各セグメント (N, M, 3, 3) を持つリスト
    :rtype: list[numpy.ndarray]
    """

    segment_triangulations = []  # 各セグメントごとのメッシュリスト

    # **セグメント単位で処理**
    for seg in split_edge_coordinates:  # (num_vertices, X, 3)
        # **分割位置ごとに 16頂点を集めて三角形分割**
        triangulated_segments = process_segment_triangulation(seg)

        segment_triangulations.append(triangulated_segments)

    return segment_triangulations


@jit(f8[:](f8, f8, i8), nopython=True, cache=True, nogil=True, parallel=True)
def linspace_except_stop(start, stop, num):
    """
    指定された範囲内に等間隔の値を生成し、numpy配列として返す。但し、終了点は含めない。
    JITコンパイラはnumpy.linspace関数のendpointパラメータをサポートしていないため、
    当関数でnumpy.linspace(start, end, num, endpoint=False)と同等の処理を行う。

    :param start: 開始点
    :type start: float
    :param stop: 終了点
    :type stop: float
    :param num: 分割数
    :type num: int

    :return: 開始点から終了点まで、終了点を含まずに等間隔に分割数分の値を持つ NumPy 配列 (N)
    :rtype: numpy.ndarray
    """

    step = (stop - start) / num  # ステップサイズ
    result = np.empty(num)
    for i in prange(num):
        result[i] = start + i * step
    return result


@jit(f8[:, :](f8[:], f8, f8[:], i8), nopython=True, cache=True, nogil=True, parallel=True)
def generate_circle_points(center, radius, direction, circle_divisions=12):
    """
    指定した中心座標とセグメントの方向ベクトルに基づいて、円周上の座標を生成する。

    :param center: 円の中心座標　[x, y, z]
    :type center: numpy.ndarray
    :param radius: 円の半径
    :type radius: float
    :param direction: 円の方向ベクトル（セグメントの進行方向） [x, y, z]
    :type direction: numpy.ndarray
    :param circle_divisions: 円周上の分割数
    :type circle_divisions: int

    :return: 円周上の点のリスト (N, 3)
    :rtype: numpy.ndarray
    """

    # **円周の分割数が3未満の場合(不正な値の場合)は12に変更**
    circle_divisions = 12 if circle_divisions < 3 else circle_divisions

    # 方向ベクトルの正規化
    direction = direction / np.linalg.norm(direction)

    # **direction と平行でない基準ベクトルを選択**
    if np.all(np.isclose(direction, np.array([1, 0, 0]))):  # X 軸正方向
        arbitrary_vector = np.array([0, 1, 0])
    elif np.all(np.isclose(direction, np.array([-1, 0, 0]))):  # X 軸負方向
        arbitrary_vector = np.array([0, -1, 0])
    elif np.all(np.isclose(direction, np.array([0, 1, 0]))):  # Y 軸正方向
        arbitrary_vector = np.array([1, 0, 0])
    elif np.all(np.isclose(direction, np.array([0, -1, 0]))):  # Y 軸負方向
        arbitrary_vector = np.array([-1, 0, 0])
    elif np.all(np.isclose(direction, np.array([0, 0, 1]))):  # Z 軸正方向
        arbitrary_vector = np.array([1, 0, 0])
    elif np.all(np.isclose(direction, np.array([0, 0, -1]))):  # Z 軸負方向
        arbitrary_vector = np.array([0, 1, 0])  # **修正: Y 軸を基準に選択**
    else:  # 斜め方向の場合
        arbitrary_vector = np.array([1, 0, 0]) if np.abs(direction[2]) < 0.9 else np.array([0, 1, 0])

    # 方向ベクトルに直交する2つのベクトルを求める（円の平面を作るため）
    u = np.cross(direction, arbitrary_vector)
    u /= np.linalg.norm(u)
    v = np.cross(direction, u)
    v /= np.linalg.norm(v)

    circle_points = np.zeros((circle_divisions, 3), dtype=np.float64)

    # 円周上の点を計算
    angles = linspace_except_stop(0, 2 * np.pi, circle_divisions)
    for i in prange(len(angles)):
        angle = angles[i]
        circle_points[i] = center + radius * (np.cos(angle) * u + np.sin(angle) * v)

    return circle_points


@jit(i8[:](f8[:, :], f8[:, :]), nopython=True, cache=True, nogil=True, parallel=True)
def match_offset_order(reference_offsets, target_offsets):
    """
    `reference_offsets` の順番に従い、`target_offsets` の点を最適に並べ替える。
    ただし、円周の連続性を保つようにする。

    :param reference_offsets: 基準となるオフセット (N, 3)
    :type reference_offsets: numpy.ndarray
    :param target_offsets: 並び替えたいオフセット (N, 3)
    :type target_offsets: numpy.ndarray

    :return: 並び替えのインデックス (N)
    :rtype: numpy.ndarray
    """

    num_points = reference_offsets.shape[0]

    # **距離行列を作成** (各 `reference_offsets` と `target_offsets` のユークリッド距離)
    dist_matrix = np.zeros((num_points, num_points), dtype=np.float64)
    for i in prange(num_points):
        for j in prange(num_points):
            diff = reference_offsets[i] - target_offsets[j]
            dist_matrix[i, j] = np.sqrt(diff[0]**2 + diff[1]**2 + diff[2]**2)

    # **最初のマッチング点を決定**
    best_start = 0
    min_distance = dist_matrix[0, 0]
    for j in prange(num_points):
        if dist_matrix[0, j] < min_distance:
            min_distance = dist_matrix[0, j]
            best_start = j  # `reference_offsets[0]` に最も近い `target_offsets` の点を探す

    # **best_start を基準に順番を決定（Numbaでは np.roll() が使えないので手動で回転）**
    indices = np.zeros(num_points, dtype=np.int64)
    for i in prange(num_points):
        indices[i] = (best_start + i) % num_points  # 円周の順番を維持

    return indices


@jit(f8[:, :](f8[:], f8[:], f8[:, :]), nopython=True, cache=True, nogil=True)
def rotate_vector_to_match(reference_direction, target_direction, vectors):
    """
    `target_direction` の向きを `reference_direction` に合わせるための回転を `vectors` に適用する。

    :param reference_direction: 基準となる方向ベクトル [x, y, z]
    :type reference_direction: numpy.ndarray
    :param target_direction: 回転前の方向ベクトル [x, y, z]
    :type target_direction: numpy.ndarray
    :param vectors: 回転させるオフセットベクトル (N, 3)
    :type vectors: numpy.ndarray

    :return: 回転後のオフセットベクトル (N, 3)
    :rtype: numpy.ndarray
    """

    # 外積（回転軸）を計算
    axis = np.array([
        target_direction[1] * reference_direction[2] - target_direction[2] * reference_direction[1],
        target_direction[2] * reference_direction[0] - target_direction[0] * reference_direction[2],
        target_direction[0] * reference_direction[1] - target_direction[1] * reference_direction[0]
    ])

    # 軸のノルムを計算
    axis_norm = np.sqrt(axis[0]**2 + axis[1]**2 + axis[2]**2)

    if axis_norm < 1e-8:  # ほぼ同じ方向なら回転不要
        return vectors

    # 正規化
    axis /= axis_norm

    # 角度を計算
    dot_product = target_direction[0] * reference_direction[0] + target_direction[1] * reference_direction[1] + target_direction[2] * reference_direction[2]
    angle = np.arccos(max(-1.0, min(1.0, dot_product)))  # np.clip の代用

    # ロドリゲスの回転公式の行列を作成
    K = np.zeros((3, 3))
    K[0, 1] = -axis[2]
    K[0, 2] = axis[1]
    K[1, 0] = axis[2]
    K[1, 2] = -axis[0]
    K[2, 0] = -axis[1]
    K[2, 1] = axis[0]

    # 回転行列 R = I + sin(angle) * K + (1 - cos(angle)) * (K @ K)
    R = np.eye(3) + np.sin(angle) * K + (1 - np.cos(angle)) * (K @ K)

    # ベクトルを回転
    return np.dot(vectors, R.T)


@jit(f8[:, :, :](f8[:, :], f8, i8), nopython=True, cache=True, nogil=True, parallel=True)
def get_cylinder_frame_info(xyz_array, radius, circle_divisions=12):
    """
    中心線に沿って円柱のワイヤーフレーム情報を生成する（セグメント間の底面を共有）。

    :param xyz_array: 中心線の座標リスト (N, 3)
    :type xyz_array: numpy.ndarray
    :param radius: 円柱の半径
    :type radius: float
    :param circle_divisions: 円周の分割数
    :type circle_divisions: int

    :return: [セグメント数, 分割数, 3座標]の形で構成される各セグメントの円周座標リスト (N, M, 3)
    :rtype: numpy.ndarray
    """

    num_points = len(xyz_array)

    # **円周の分割数が3未満の場合(不正な値の場合)は12に変更**
    circle_divisions = 12 if circle_divisions < 3 else circle_divisions

    xyz_array = xyz_array.astype(np.float64)  # **型変換を追加**
    circles = np.zeros((num_points, circle_divisions, 3))
    offsets_list = np.zeros((num_points, circle_divisions, 3))
    direction_list = np.zeros((num_points, 3))

    for i in prange(num_points):  # 各ポイントごとに円を生成
        center = xyz_array[i]

        if i == 0:
            direction = xyz_array[i + 1] - xyz_array[i]
        elif i == num_points - 1:
            direction = xyz_array[i] - xyz_array[i - 1]
        else:
            # **L字型のケースでも滑らかに補間する方法**
            dir1 = xyz_array[i] - xyz_array[i - 1]  # 前の点への方向
            dir2 = xyz_array[i + 1] - xyz_array[i]  # 次の点への方向

            # A == C なら `dir1 + dir2` はゼロになる
            if np.allclose(dir1 + dir2, 0):
                direction = dir1  # dir1 を選択
            else:
                direction = dir1 + dir2  # 進行方向ベクトルを補間
                direction /= np.linalg.norm(direction)  # 正規化

        direction = direction.astype(np.float64)  # **float64 に明示的に変換**
        direction /= np.linalg.norm(direction)  # 正規化

        # 方向ベクトルを記憶
        direction_list[i] = direction

        # 円の生成
        circle = generate_circle_points(center, radius, direction, circle_divisions)

        # `center` からの相対オフセットを計算
        offsets = circle - center
        offsets_list[i] = offsets
        circles[i] = circle

    # **基準となる最初の `direction` に揃える**
    reference_direction = direction_list[0]
    reference_offsets = offsets_list[0]  # 基準となる最初の `offsets`

    for i in prange(1, len(circles)):
        current_direction = direction_list[i]
        current_offsets = offsets_list[i]

        # 1. `reference_direction` に合わせて `current_offsets` を回転
        adjusted_offsets = rotate_vector_to_match(reference_direction, current_direction, current_offsets)

        # 2. 並び順を `reference_offsets` に統一
        indices = match_offset_order(reference_offsets, adjusted_offsets)

        # 並び順を揃える
        circles[i] = circles[i][indices]

    return circles


@jit(f8[:, :, :, :](f8[:, :], f8, i8), nopython=True, cache=True, nogil=True, parallel=True)
def extract_edge_coordinates(xyz_array, radius, circle_divisions=12):
    """
    マルチパッチのエッジ座標を取得する。

    :param xyz_array: 中心線の座標 (N, 3)
    :type xyz_array: numpy.ndarray
    :param radius: 円柱の半径
    :type radius: float
    :param circle_divisions: 円周の分割数
    :type circle_divisions: int

    :return: [セグメント数, 頂点数, 2点（始点・終点）, 3座標]の形で構成されるエッジ座標 (N, M, 2, 3)
    :rtype: numpy.ndarray
    """

    # **円周の分割数が3未満の場合(不正な値の場合)は12に変更**
    circle_divisions = 12 if circle_divisions < 3 else circle_divisions

    num_segments_total = len(xyz_array) - 1  # 総セグメント数
    edge_coordinates = np.zeros((num_segments_total, circle_divisions, 2, 3))  # **(セグメント数, 頂点数, 2点, 3座標)**

    # **各ポイントごとに円を生成**
    circles = get_cylinder_frame_info(xyz_array, radius, circle_divisions)

    # **側面メッシュと線分を作成**
    for i in prange(num_segments_total):
        _, edges = connect_circles(circles[i], circles[i + 1])
        edge_coordinates[i] = edges  # (num_segments, 2, 3) の形

    return edge_coordinates


def generate_cylindrical_multipatch_split_triangles(xyz_array, radius, circle_divisions=12, interval=0.5):
    """
    セグメントごとに円を分割し、三角形メッシュを作成する。

    :param xyz_array: 中心線の座標 (N, 3)
    :type xyz_array: numpy.ndarray
    :param radius: 円柱の半径
    :type radius: float
    :param circle_divisions: 円周の分割数
    :type circle_divisions: int
    :param interval: 線分を分割する間隔
    :type interval: float

    :return: 分割された円を基にした三角形メッシュ (N, M, 3, 3) を持つリスト
    :rtype: list[numpy.ndarray]
    """

    # **円周の分割数が3未満の場合(不正な値の場合)は12に変更**
    circle_divisions = 12 if circle_divisions < 3 else circle_divisions

    # **エッジを取得（円の頂点をつなぐ線分）**
    edge_coordinates = extract_edge_coordinates(xyz_array, radius, circle_divisions)

    # **エッジを分割して新しい円を生成**
    split_edge_coordinates = split_edges_by_interval(edge_coordinates, interval)

    # **分割した円を三角形メッシュ化**
    split_circle_triangles = generate_segment_triangulation(split_edge_coordinates)

    return split_circle_triangles


@jit(f8[:, :, :](f8[:, :], f8, i8, i8, i8), nopython=True, cache=True, nogil=True)
def generate_multipatch_mesh(xyz_array, radius, circle_divisions=12, start_flag=1, end_flag=1):
    """
    マルチパッチ（三角形メッシュ）を作成する関数。

    :param xyz_array: 中心線の座標 (N, 3)
    :type xyz_array: numpy.ndarray
    :param radius: 円柱の半径
    :type radius: float
    :param circle_divisions: 円周の分割数
    :type circle_divisions: int
    :param start_flag: 始点TINZを生成するか (0 or 1)
    :type start_flag: int
    :param end_flag: 終点TINZを生成するか (0 or 1)
    :type end_flag: int

    :return: 三角形メッシュデータ (N, M, 3)
    :rtype: numpy.ndarray
    """

    # **円周の分割数が3未満の場合(不正な値の場合)は12に変更**
    circle_divisions = 12 if circle_divisions < 3 else circle_divisions

    num_segments_total = len(xyz_array) - 1  # 総セグメント数

    # **各ポイントごとに円を生成（底面を共有する形）**
    circles = get_cylinder_frame_info(xyz_array, radius, circle_divisions)

    # 三角形メッシュの要素数を算出
    mesh_num = (circle_divisions * 2) * num_segments_total + (circle_divisions * (start_flag + end_flag))
    tinz_coordinates = np.zeros((mesh_num, 3, 3), np.float64)  # 三角形メッシュ

    start_index = 0  # 始点・終点の要素を格納する開始インデックス

    # **側面メッシュを作成**
    for i in range(num_segments_total):
        triangles, _ = connect_circles(circles[i], circles[i + 1])
        tinz_coordinates[i * triangles.shape[0]:(i + 1) * triangles.shape[0]] = triangles
        start_index = (i + 1) * triangles.shape[0]

    # **始点・終点の円をメッシュ化**
    if start_flag:
        # 要素を格納するインデックスを算出
        tinz_coordinates[start_index:start_index + circle_divisions] = triangulate_circle(circles[0])
        # 開始インデックスを更新
        start_index += circle_divisions
    if end_flag:
        tinz_coordinates[start_index:start_index + circle_divisions] = triangulate_circle(circles[-1])

    return tinz_coordinates


#@jit(f8[:, :, :](f8[:, :], f8, i8, i8, i8), nopython=True, cache=True, nogil=True)
def get_cylindrical_multipatch_array(xyz_array, radius, circle_divisions=12, start_flag=1, end_flag=1):
    """
    円柱状のマルチパッチデータを生成する関数（セグメント間の底面を共有）。

    :param xyz_array: 中心線の座標 (N, 3)
    :type xyz_array: numpy.ndarray
    :param radius: 円柱の半径
    :type radius: float
    :param circle_divisions: 円周の分割数
    :type circle_divisions: int
    :param start_flag: 始点TINZを生成するか (0 or 1)
    :type start_flag: int
    :param end_flag: 終点TINZを生成するか (0 or 1)
    :type end_flag: int

    :return: マルチパッチ（三角形メッシュ） (N, M, 3)
    :rtype: numpy.ndarray
    """

    # **円周の分割数が3未満の場合(不正な値の場合)は12に変更**
    circle_divisions = 12 if circle_divisions < 3 else circle_divisions

    # **マルチパッチ（三角形メッシュ）を作成**
    tinz_coordinates = generate_multipatch_mesh(xyz_array, radius, circle_divisions, start_flag, end_flag)

    # 法線ベクトルを追加
    multipatch_array = add_vertex_normal(tinz_coordinates)

    return multipatch_array


@jit('Tuple((b1[:],b1[:]))(f8[:],f8)', nopython=True, cache=True, nogil=True)
def get_target_and_not_target_bool_array(target_array,
                                         target_value):
    """
    1次元配列を特定の値で検索し、対象と対象外のboolを取得する
    """

    # 対象のbool配列
    target_bool=target_array==target_value
    not_target_bool=np.logical_not(target_bool)

    return target_bool,\
           not_target_bool


@jit('Tuple((i8[:],i8[:]))(f8[:],f8)', nopython=True, cache=True, nogil=True)
def get_target_and_not_target_index_array(target_array,
                                          target_value):
    """
    1次元配列を特定の値で検索し、対象インデックスと対象外インデックスを取得する
    """
    # 対象のbool配列
    target_bool,\
    not_target_bool\
        =get_target_and_not_target_bool_array(target_array,
                                              target_value)

    target_index=np.where(target_bool)[0]
    not_target_index=np.where(not_target_bool)[0]

    return target_index,\
           not_target_index


@jit(f8[:,:](f8[:,:], i8[:], i8[:]), nopython=True, cache=True, nogil=True)
def get_estimation_Z_numba(target_ndarray,
                           gradient_indices,
                           estimate_target_indices):
    """
    推定標高付与処理→デフォルトのまま使用すると始点終点
    引数1：推定標高対象地物の2次元配列 xyz座標
    引数2：傾きと切片を計算する2点のインデックス配列（要素数は2）
    引数3：推定標高対象構成点のインデックス配列
    戻り値：推定標高対象地物の2次元配列
    """
    # 処理結果返却用配列
    result_array=target_ndarray.copy()


    # 2点の地物の場合推定標高せず（できない）そのまま返す
    if len(result_array) == 2:

        return result_array

    else:

        # xyの長さは構成点間の距離の合計
        distance = np.sum(get_distance(target_ndarray[:,:2]))
        # 始点と終点のZ値の差を割って傾きを算出
        gradient = (result_array[gradient_indices[1],2] - result_array[gradient_indices[0],2]) / distance

        # 切片を引数2の1点目のz座標とする この点を基準点とする
        # 基準点のインデックス
        standard_point_index = gradient_indices[0]

        # 基準点のz座標
        slice = result_array[standard_point_index][2]

        # 推定標高処理対象構成点が基準点とどれだけ距離があるか算出する
        distance_array = get_distance_to_start_end_point(result_array[:,:2], standard_point_index)

        # 傾きをかけ切片を加えてZ値とする
        if len(estimate_target_indices) == 0:

            # 推定標高対象を指定していない場合、中間点（始点終点以外）を更新する
            estimate_z_value_array = distance_array[1:-1] * gradient + slice

            # 更新用に配列を変換
            estimate_z_value_array = estimate_z_value_array.reshape(len(estimate_z_value_array),1)

            #Z を更新
            result_array[1:-1,2:] = estimate_z_value_array
        else:

            # 推定標高対象を指定している場合、引数で指定された構成点に対して推定標高を行う
            estimate_z_value_array = distance_array[estimate_target_indices] * gradient + slice

            # 更新用に配列を変換
            estimate_z_value_array = estimate_z_value_array.reshape(len(estimate_z_value_array),1)

            # Zを更新
            result_array[estimate_target_indices,2:] = estimate_z_value_array

    return result_array


@jit(f8[:,:](f8[:,:], f8[:,:]), nopython=True, cache=True, nogil=True)
def get_end_point_z_2points(point1topoint2_array,
                            point2totarget_array):
    """
    2点の座標間から傾きを算出し端点を推定標高する
    引数1:Z値が設定されているポイント1からポイント2までの座標配列 2次元配列[構成点]>[x,y,z]
    引数2:ポイントの2から推定標高Zを設定するポイントまでの座標配列 1次元配列[x,y,z]
    注意：構成点はポイント1，ポイント2、推定標高対象ポイントの順に並んでいるものとする
    """
    # 返却用ポイント座標配列
    result_array=point2totarget_array.copy()

    # Z値のある2点の傾き算出
    xy_distance=np.sum(get_distance(point1topoint2_array[:,:2]))
    difference_z=point1topoint2_array[-1,2]-point1topoint2_array[0,2]
    gradient=difference_z/xy_distance

    # ポイント2から推定標高対象までの距離
    target_distance=np.sum(get_distance(point2totarget_array[:,:2]))

    # 距離と傾きからZ値算出
    result_array[-1,2]=point2totarget_array[0,2]+target_distance*gradient

    return result_array


@jit(f8[:,:](f8[:,:], f8), nopython=True, cache=True, nogil=True)
def update_z_value_by_estimation(linestring_array,
                                 default_value):
    """
    ラインのZ値に対して推定標高を行う
    Z値が割り与えられていない点は固定値となっているものとする
    固定値で構成点を検索しその点に向かって推定標高を行う
    最小単位の更新用メソッド
    引数1:ラインの構成点２次元座標配列 [構成点]>[x,y,z]
    """
    # 返却用推定標高対象座標配列
    result_linestring_array=linestring_array.copy()

    # 推定標高対象のインデックス取得
    # 推定標高するために利用するZ値のインデックス取得
    estimation_target_index_array,\
    z_value_index_array\
        =get_target_and_not_target_index_array(linestring_array[:,2],
                                               default_value)

    # 推定標高用のZ値が1件ならすべてのZ値にその値を設定
    if len(z_value_index_array)==1:

        result_linestring_array[:,2]=linestring_array[z_value_index_array[0],2]

    # 0件の場合はそのまま返却
    elif len(z_value_index_array)==0:
        pass
    else:

        # スライスした座標の始点終点をもとに構成点間を推定標高処理を行うための固定引数
        # 推定標高に使用するインデックス
        gradient_indices=np.array([0,-1], dtype=np.int64)

        # 推定標高対象のインデックス（長さ0なら構成点間すべて対象になる）
        estimate_target_indices=np.zeros(0, dtype=np.int64)

        # 始点が規定値の場合
        if estimation_target_index_array[0]==0:
            # 2点の傾きから始点のZ値を取得する
            # 傾き判定用
            temp_array=result_linestring_array[z_value_index_array[0]:z_value_index_array[1]+1,:]

            # 推定標高Z設定用
            temp_target_array=result_linestring_array[0:z_value_index_array[0]+1,:]

            temp_linestring_array\
                =get_end_point_z_2points(temp_array[::-1],
                                         temp_target_array[::-1])

            result_linestring_array[0:z_value_index_array[0]+1,:]=temp_linestring_array[::-1]

        else:
            pass

        # 終点が規定値の場合
        if estimation_target_index_array[-1]==len(result_linestring_array)-1:

            # 2点の傾きから終点のZ値を取得する
            result_linestring_array[z_value_index_array[-1]:,:]\
                =get_end_point_z_2points(result_linestring_array[z_value_index_array[-2]:z_value_index_array[-1]+1,:],
                                         result_linestring_array[z_value_index_array[-1]:,:])
        else:
            pass

        # 推定標高対象のインデックス再取得
        # 推定標高するために利用するZ値のインデックス再取得
        estimation_target_index_array2,\
        z_value_index_array2\
            =get_target_and_not_target_index_array(result_linestring_array[:,2],
                                                   default_value)

        # 推定標高用のZ区間ごとに処理
        for i in range(len(z_value_index_array2)-1):
            
            # Z値が規定値以外の構成点間に推定標高対象がなければ次へ
            #（規定値以外のインデックスが隣なら差は1）
            if z_value_index_array2[i+1]-z_value_index_array2[i]==1:
                continue
            else:
                pass
            
            # Z値更新
            result_linestring_array[z_value_index_array2[i]:z_value_index_array2[i+1]+1,:]\
                =get_estimation_Z_numba(result_linestring_array[z_value_index_array2[i]:z_value_index_array2[i+1]+1,:],
                                        gradient_indices,
                                        estimate_target_indices)
                
    return result_linestring_array


@jit(f8[:,:](f8[:,:], f8), nopython=True, cache=True, nogil=True)
def get_z_updated_array_by_estimation(linestring_array,
                                      default_value):
    """
    全ジオメトリごとに推定標高を行う 推定標高を行う条件は以下の3つである
    １，ジオメトリ内に規定値（Z値がない）しかない場合、そのまま返す
    ２，ジオメトリ内に規定値以外が1つしかない場合、すべてのZ値をその値で更新する
    ３，ジオメトリ内に規定値以外が2つ以上ある場合、規定値の構成点は直近の2つの点から推定標高を行う
    """
    # 結果格納用配列
    result_linestring_array=linestring_array.copy()

    # 始点終点のインデックスを取得する
    si,ei=get_start_index_and_end_index(result_linestring_array)

    # 地物ごとに処理
    for i in range(len(si)):

        result_linestring_array[si[i]:ei[i]+1,1:]\
            =update_z_value_by_estimation(result_linestring_array[si[i]:ei[i]+1,1:],
                                          default_value)

    return result_linestring_array


@jit(f8[:](f8[:,:],f8[:,:],f8[:,:]),nopython=True,cache=True,nogil=True)
def get_degree_3points_array( before_array
                            , center_array
                            , after_array
                            ):
    """
    3点間のxy平面上における角度を算出する
    引数1:[前点]>[x,y]
    引数2:[中点]>[x,y]
    引数3:[次点]>[x,y]
    """
    # 各点の座標からベクトルを作成
    ba_vector = before_array-center_array
    bc_vector = after_array-center_array

    # 計算結果角度設定用初期配列
    result_degree_array=np.zeros(len(ba_vector), dtype=np.float64)

    for i in range(len(ba_vector)):

        # ベクトルの内積を計算
        dot_product = np.dot(ba_vector[i], bc_vector[i])

        # ベクトルの大きさを計算
        ba_magnitude = np.linalg.norm(ba_vector[i])
        bc_magnitude = np.linalg.norm(bc_vector[i])

        if ba_magnitude * bc_magnitude==0:
            result_degree_array[i]=180

        else:
            # ベクトルの角度を計算
            # 内積=|a||b|cosθ
            # これをaベクトルとbベクトルの大きさの積で割ってやるとcosθが出てくるのでarccosに渡して角度（ラジアン取得）
            # 浮動小数点の誤差で-1より若干小さいcosxが出てくる（そんなものはこの世に存在しない）→arccosがnanを返す
            # 丸目誤差を消すようにする 0への丸目
            cosx=np.trunc(dot_product / (ba_magnitude * bc_magnitude)*1000)/1000

            temp_degree = np.degrees(np.arccos(cosx))
            result_degree_array[i]=temp_degree

    return result_degree_array


@jit(f8[:](f8[:,:],f8[:,:]),nopython=True,cache=True,nogil=True)
def get_2points_degree_array_x_axis( standard_point_array
                                   , next_point_array
                                   ):
    """
    x軸と直線がなす角度を求める 隣接点から基準点に向かうときの角度
    基準点とその隣の（基準点に向かう）点の2点を引数に渡す [構成点]>[2次元配列]
    """
    # 隣接点から基準点に向かうベクトル
    vector_array=standard_point_array-next_point_array

    # radian
    radians_array=np.arctan2( vector_array[:,1]
                            , vector_array[:,0]
                            )

    # randian→degree
    degrees_array=np.rad2deg(radians_array)

    return degrees_array


@jit('Tuple((i8,f8,f8[:]))(f8[:],f8[:,:])', nopython=True, cache=True, nogil=True)
def get_min_distance_index( point_array
                          , distance_points_array
                          ):
    """
    1つの点から多数の点に向かって距離を計算し、最小の距離を持つ点を取得する
    """
    distance_array\
        =get_distance_one_point_to_points( point_array
                                         , distance_points_array
                                         )
    # 最も短い距離のインデックス取得
    min_index=np.argmin(distance_array)

    return min_index\
         , distance_array[min_index]\
         , distance_points_array[min_index]


@jit('Tuple((f8[:,:],f8[:],f8[:],i8[:]))(f8[:,:],f8[:,:],f8[:],f8[:])', nopython=True, cache=True, nogil=True)
def _get_subjective_points_degree_array( subjective_array
                                       , all_subjective_linestring_center_array
                                       , all_subjective_degrees_array1
                                       , all_subjective_degrees_array2
                                       ):

    # --------------------------------------------------------
    # 主たる地物におけるスナップ先の中で最も近い点を取得
    # --------------------------------------------------------
    temp_subjective_degrees_array1\
        =np.zeros(len(subjective_array))
    temp_subjective_degrees_array2\
        =np.zeros(len(subjective_array))
    temp_coordinates_array\
        =np.zeros(subjective_array.shape)
    temp_argmin_array\
        =np.zeros(len(subjective_array), dtype=np.int64)

    # 距離の閾値内で紐づいた複数のGCPに関して必要な値を算出
    # 角度+それぞれのインデックス
    for ti in prange(len(subjective_array)):

        temp_subjective_min_index\
        ,_\
        ,_\
            =get_min_distance_index( subjective_array[ti]
                                   , all_subjective_linestring_center_array
                                   )

        # スナップした先の点も取得
        temp_subjective_degrees_array1[ti]\
            =all_subjective_degrees_array1[temp_subjective_min_index]
        temp_subjective_degrees_array2[ti]\
            =all_subjective_degrees_array2[temp_subjective_min_index]
        temp_coordinates_array[ti]\
            =all_subjective_linestring_center_array[temp_subjective_min_index]
        temp_argmin_array[ti]\
            =temp_subjective_min_index
    # --------------------------------------------------------

    return temp_coordinates_array\
         , temp_subjective_degrees_array1\
         , temp_subjective_degrees_array2\
         , temp_argmin_array


@jit(b1[:](f8[:],f8,f8),nopython=True,cache=True,nogil=True)
def get_bool_array_absolute_difference_threshold( values_array
                                                , target_value
                                                , threshold
                                                ):
    """
    差の絶対値が閾値未満か判定する
    """
    result_bool_array=np.abs(values_array-target_value)<threshold

    return result_bool_array

@jit(b1[:](f8,f8,f8[:],f8[:],f8),nopython=True,cache=True,nogil=True)
def _get_degrees_difference_bool_array( temp_objective_degree1
                                      , temp_objective_degree2
                                      , temp_subjective_degrees_array1
                                      , temp_subjective_degrees_array2
                                      , degrees_difference_threshold
                                      ):

    difference_1_1_bool\
        =get_bool_array_absolute_difference_threshold( temp_subjective_degrees_array1
                                                     , temp_objective_degree1
                                                     , degrees_difference_threshold
                                                     )
    difference_2_2_bool\
        =get_bool_array_absolute_difference_threshold( temp_subjective_degrees_array2
                                                     , temp_objective_degree2
                                                     , degrees_difference_threshold
                                                     )
    difference_1_2_bool\
        =get_bool_array_absolute_difference_threshold( temp_subjective_degrees_array1
                                                     , temp_objective_degree2
                                                     , degrees_difference_threshold
                                                     )
    difference_2_1_bool\
        =get_bool_array_absolute_difference_threshold( temp_subjective_degrees_array2
                                                     , temp_objective_degree1
                                                     , degrees_difference_threshold
                                                     )

    result_degree_bool=(difference_1_1_bool & difference_2_2_bool)\
                     | (difference_1_2_bool & difference_2_1_bool)

    return result_degree_bool


def _get_nearest_degrees_neighbors_array( subjective_array
                                         , objective_array
                                         , subjective_linestring_center_array
                                         , subjective_degrees_array1
                                         , subjective_degrees_array2
                                         , objective_linestring_center_array
                                         , objective_degrees_array1
                                         , objective_degrees_array2
                                         , gcp_distance_threshold
                                         , degrees_difference_threshold
                                         ):
    """
    --------------------------------------------------------------------------
    全部xy座標のみ
    角度の閾値と近傍によるGCP
    「前点を原点としたときの中点がなす角度」、「次点を原点としたときの中点がなす角度」の2つの角度を必要とする
    それぞれ主たる地物と従たる地物の２種類分必要
    全通りの差を閾値と比較するため、角度を引数に設定する順番はない
    --------------------------------------------------------------------------
    """

    # スナップ対象用地物の中点とそれに紐づく角度（主たる地物側）
    all_subjective_linestring_center_array=subjective_linestring_center_array.copy()
    all_subjective_degrees_array1=subjective_degrees_array1.copy()
    all_subjective_degrees_array2=subjective_degrees_array2.copy()

    # スナップ対象用地物の中点とそれに紐づく角度（従たる地物側）
    all_objective_linestring_center_array=objective_linestring_center_array.copy()
    all_objective_degrees_array1=objective_degrees_array1.copy()
    all_objective_degrees_array2=objective_degrees_array2.copy()

    # スナップ対象mask初期配列
    result_subjective_bool=np.ones(len(subjective_linestring_center_array), dtype=np.bool_)
    result_objective_bool=np.ones(len(objective_linestring_center_array), dtype=np.bool_)

    # スナップ対象のmask処理する前の元のインデックス
    subjective_center_index_array=np.arange(len(subjective_linestring_center_array), dtype=np.int64)
    objective_center_index_array=np.arange(len(objective_linestring_center_array), dtype=np.int64)

    # マッチング結果座標格納用
    result_subjective_list=[]
    result_objective_list=[]

    # GCPの数だけループ
    for i in range(len(objective_array)):
        # --------------------------------------------------------
        # ①スナップ先の構成点から指定範囲内のGCPを取得
        # 主→従 距離
        # --------------------------------------------------------
        temp_distance_array\
            =get_distance_one_point_to_points( objective_array[i]
                                             , subjective_array
                                             )

        # 近隣にGCPが存在しない場合次のGCPへ
        temp_distance_bool=temp_distance_array<=gcp_distance_threshold
        if  np.any(temp_distance_bool):
            pass
        else:
            continue
        # --------------------------------------------------------

        # --------------------------------------------------------
        # ②従たる地物におけるスナップ先の中で最も近い点を取得
        # 主→従地物 距離
        # --------------------------------------------------------
        temp_min_index\
        ,temp_snap_distance_array\
        ,temp_objective_linestring_center_array\
            =get_min_distance_index( objective_array[i]
                                   , all_objective_linestring_center_array[result_objective_bool]
                                   )
        # --------------------------------------------------------

        # --------------------------------------------------------
        # 主たる地物におけるスナップ先の中で最も近い点を取得（複数）
        # --------------------------------------------------------
        temp_coordinates_array\
        ,temp_subjective_degrees_array1\
        ,temp_subjective_degrees_array2\
        ,temp_argmin_array\
            =_get_subjective_points_degree_array( subjective_array[temp_distance_bool]
                                                , all_subjective_linestring_center_array[result_subjective_bool]
                                                , all_subjective_degrees_array1[result_subjective_bool]
                                                , all_subjective_degrees_array2[result_subjective_bool]
                                                )

        # --------------------------------------------------------

        # --------------------------------------------------------
        # 角度の組み合わせで差が閾値位以内の対象を抽出
        # スナップ先が持つ角度取得（従たる地物側）
        # --------------------------------------------------------
        result_degree_bool\
            =_get_degrees_difference_bool_array( all_objective_degrees_array1[result_objective_bool][temp_min_index]
                                               , all_objective_degrees_array2[result_objective_bool][temp_min_index]
                                               , temp_subjective_degrees_array1
                                               , temp_subjective_degrees_array2
                                               , degrees_difference_threshold
                                               )

        # --------------------------------------------------------

        # 閾値未満の組み合わせがあるならその結果を抽出対象とする
        if np.any(result_degree_bool):
            pass
        else:
            continue
        # -----------------------------------------------------------------

        # --------------------------------------------------------
        target_distance_argmin_index\
        ,temp_snap_result_distance\
        ,temp_subjective_coordinates_array\
            =get_min_distance_index( temp_objective_linestring_center_array
                                   , temp_coordinates_array[result_degree_bool]
                                   )

        # --------------------------------------------------------

        # 距離が閾値以上なら出力しない
        if temp_snap_result_distance>gcp_distance_threshold:
            continue
        else:
            pass

        result_subjective_list.append(temp_subjective_coordinates_array.copy())
        result_objective_list.append(temp_objective_linestring_center_array.copy())
        # -----------------------------------------------------------------

        # 結果を格納した後は検索対象として外す
        # 全体から見た時のインデックス取得
        target_argmin_index\
            =temp_argmin_array[result_degree_bool][target_distance_argmin_index]

        # マスク更新
        # 以降スナップ先の構成点の座標、角度を用いる場合はマスクで抽出している
        # 更新対象インデックス
        temp_subjective_index=subjective_center_index_array[result_subjective_bool]
        temp_objective_index=objective_center_index_array[result_objective_bool]

        result_subjective_bool[temp_subjective_index[target_argmin_index]]=False
        result_objective_bool[temp_objective_index[temp_min_index]]=False

        # 主たる地物、従たる地物のどちらか一方に対象がなければ終了
        if np.any(result_subjective_bool)==False:
            break
        elif np.any(result_objective_bool)==False:
            break
        else:
            pass

    result_subjective_array=np.array(result_subjective_list, dtype=np.float64)
    result_objective_array=np.array(result_objective_list, dtype=np.float64)

    return result_subjective_array\
         , result_objective_array


def _get_nearest_degrees_neighbors_array_by_voxel( unique_subjective_xy_array
                                                 , subjective_default_dict
                                                 , objective_default_dict
                                                 , subjective_coordinates_array
                                                 , objective_coordinates_array
                                                 , subjective_center_default_dict
                                                 , objective_center_default_dict
                                                 , subjective_linestring_center_array
                                                 , subjective_degrees_array1
                                                 , subjective_degrees_array2
                                                 , objective_linestring_center_array\
                                                 , objective_degrees_array1
                                                 , objective_degrees_array2
                                                 , nearest_neighbor_threshold
                                                 , degrees_difference_threshold
                                                 ):
    """
    # --------------------------------------------------------------------------
    # VoxelIDごとに近傍+角度の差の閾値でマッチング処理を行う
    # --------------------------------------------------------------------------
    """
    # 結果格納用
    result_subjective_list=[]
    result_objective_list=[]
    for i in range(len(unique_subjective_xy_array)):

        # Voxelidごとに座標のインデックスを取り出す
        temp_subjective_index=subjective_default_dict[unique_subjective_xy_array[i]]
        temp_objective_index=objective_default_dict[unique_subjective_xy_array[i]]

        # Voxelidごとにスナップ対象のインデックスを取り出す
        temp_subjective_center_index=subjective_center_default_dict[unique_subjective_xy_array[i]]
        temp_objective_center_index=objective_center_default_dict[unique_subjective_xy_array[i]]

        # 対象がなければ次へ
        if len(temp_objective_index)==0:
            continue
        elif len(temp_subjective_center_index)==0:
            continue
        elif len(temp_objective_center_index)==0:
            continue
        else:

            # 閾値以内に存在する最も近い距離のペアリングを取得
            temp_subjective_array\
            , temp_objective_array\
                =_get_nearest_degrees_neighbors_array( subjective_coordinates_array[temp_subjective_index,1:3]
                                                      , objective_coordinates_array[temp_objective_index,1:3]
                                                      , subjective_linestring_center_array[temp_subjective_center_index,1:3]
                                                      , subjective_degrees_array1[temp_subjective_center_index]
                                                      , subjective_degrees_array2[temp_subjective_center_index]
                                                      , objective_linestring_center_array[temp_objective_center_index,1:3]
                                                      , objective_degrees_array1[temp_objective_center_index]
                                                      , objective_degrees_array2[temp_objective_center_index]
                                                      , nearest_neighbor_threshold
                                                      , degrees_difference_threshold
                                                      )

            # ペアがなければ次へ
            if len(temp_subjective_array)==0:
                pass
            else:
                result_subjective_list.append(temp_subjective_array.copy())
                result_objective_list.append(temp_objective_array.copy())

    result_subjective_array=np.concatenate(result_subjective_list)
    result_objective_array=np.concatenate(result_objective_list)

    return result_subjective_array\
         , result_objective_array


@jit(b1[:](f8[:,:],f8[:,:]), nopython=True, cache=True, nogil=True)
def is_closed_bool_array( start_coordinates_array
                        , end_coordinates_array
                        ):
    """
    地物が閉じているかそうでないか判定する
    配列の要素に地物IDを持つ複数地物を判定する想定
    引数1：任意の地物のxy(xyz)座標を持つ二次元配列[地物の全構成点]>[id, x, y...]
    概要：bool配列 地物ごとに閉じているかそうでないか取得
    """

    # 始点と終点のインデックスを取得
    # 要素ごとにTrueの数を集計
    is_closed_bool=np.sum(start_coordinates_array==end_coordinates_array, axis=1)

    # 要素の数だけTrue、つまり同じ要素であればTrueを返す
    is_closed_bool=is_closed_bool==start_coordinates_array.shape[1]

    return is_closed_bool


def get_serial_3points_array( start_index_array
                            , end_index_array
                            , is_closed_bool
                            ):
    """
    角度計算用連続3点取得
    """

    before_index_list=[]
    center_index_list=[]
    after_index_list=[]

    # 地物ごとの構成点数
    coordinates_counts_array=end_index_array-start_index_array+1

    # 地物の数だけ処理
    for bi in range(len(is_closed_bool)):
        # 2点の場合は次へ
        if int(coordinates_counts_array[bi])==2:
            continue
        else:
            # 前点、中点、次点のインデックスを取得
            temp_center_index_array=np.arange( start_index_array[bi]+1
                                             , end_index_array[bi]+1
                                             )
            temp_before_index_array=temp_center_index_array-1

            temp_after_index_array=temp_center_index_array+1

            # 閉じている場合は次点の最後のインデックスをその地物の2点目（始終点の次の点）とする
            if is_closed_bool[bi]:

                temp_after_index_array[-1]=temp_center_index_array[0]

            # 閉じていない場合最後以外をスライスして取得
            else:

                temp_end_index=len(temp_before_index_array)-1

                temp_before_index_array=temp_before_index_array[:temp_end_index]
                temp_center_index_array=temp_center_index_array[:temp_end_index]
                temp_after_index_array=temp_after_index_array[:temp_end_index]

            before_index_list.append(temp_before_index_array.copy())
            center_index_list.append(temp_center_index_array.copy())
            after_index_list.append(temp_after_index_array.copy())

    # 格納したインデックスを結合
    before_index_array=np.concatenate(before_index_list)
    center_index_array=np.concatenate(center_index_list)
    after_index_array=np.concatenate(after_index_list)

    return before_index_array\
         , center_index_array\
         , after_index_array\


def get_serial_3points_index_array(coordinates_array):
    """
    角度計算用の中点と、それに紐づく前点、次点を取得する
    閉じたラインの場合、始点を中点としてインデックスを生成している
    """
    # 始点と終点のインデックス取得
    start_index_array\
    , end_index_array\
        =get_start_index_and_end_index(coordinates_array)

    # 座標全体に対する処理
    # 始点終点のインデックス取得→スライス位置、閉じられているか判定 np.unique使用
    is_closed_bool\
        =is_closed_bool_array( coordinates_array[start_index_array]
                             , coordinates_array[end_index_array]
                             )

    # 前点、中点、次点のインデックス取得
    before_index_array\
    ,center_index_array\
    ,after_index_array\
        =get_serial_3points_array( start_index_array
                                 , end_index_array
                                 , is_closed_bool
                                 )

    return before_index_array\
         , center_index_array\
         , after_index_array


@jit(f8(f8[:], f8[:, :]), nopython=True, cache=True, nogil=True)
def get_distance_point_segment_xyz(point_array, segment_array):
    """
    点から線分までの距離
    垂線が下せるかどうか関係なく線分が存在する直線にたいして垂線をおろしてその長さを求める
    引数1:ポイント 1次元配列 [x,y,z]
    引数2:ライン 1次元配列 [[x1,y1][x2,y2]]
    """
    # 外積の大きさ→平行四辺形の面積
    cross_value = np.linalg.norm(
        np.cross(segment_array[1]-segment_array[0], point_array - segment_array[0]))

    # 線分の距離
    line_length = get_distance_points(segment_array[0], segment_array[1])

    # 線分を底辺として割ると高さ（点と直線の距離が出る）
    return cross_value / line_length


@jit(f8(f8[:], f8[:, :]), nopython=True, cache=True, nogil=True)
def get_min_distance(point_array, segment_array):
    """
    点と直線の最小距離取得
    """
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
    """
    構成するすべての線分との距離を求める
    """
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
def get_min_distance_points2linestrings(points_array, linestring_array, start_index_array, end_index_array
                                        ):
    """
    複数のポイントに対して複数のラインとの最短距離をもとめ、最も短い距離を持つラインをペアとする
    ポイントごとにどのラインをペアとするのか決定する
    """
    # 1点に対してどのラインを紐づけるかのインデックス格納するList
    result_point_index_list = []
    result_linestring_index_list = []

    for pi in prange(len(points_array)):

        # 1点に対して計算される距離のnumpy配列
        # ★点に対して複数紐づく場合あるかも（管路同士のつなぎ目が最短距離）
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


def _get_index_array_by_unit(point_array, line_array, unique_xy_string_array, start_index_array, end_index_array, linestring_id_dict, point_id_array, line_id_array, xy_id_default_dict
                             ):
    """
    空間IDごとに最近傍結合を行い、検索側のインデックス配列と検索される側のインデックス配列を返却する
    """
    # 各IDごとで最近傍処理を行いポイントに対するラインのIDを取得する
    result_point_index = []
    result_linestring_index = []

    for i in range(len(unique_xy_string_array)):

        # 対象の空間ID
        temp_xy_id = unique_xy_string_array[i]

        # 対象空間ID内に存在する地物のID
        temp_line_id_array = line_id_array[start_index_array[linestring_id_dict[temp_xy_id]]]
        temp_point_id_array = point_id_array[xy_id_default_dict[temp_xy_id]]

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


@jit(b1[:](f8[:, :], i8[:], i8[:], f8, f8, f8, f8), nopython=True, cache=True, nogil=True)
def _get_feature_id_around_unit(coordinates_array, start_index_array, end_index_array, unit_min_x, unit_max_x, unit_min_y, unit_max_y
                                ):
    """
    対象矩形付近に構成点が存在する地物を抽出する
    少なくとも1点は図郭の最小から最大の範囲内に構成点が存在する地物のこと
    図郭の線上は含める
    """
    # 地物の数だけの判定を設定するを
    result_index_bool_array = np.ones(len(start_index_array), dtype=np.bool_)

    # 地物ごとに処理を行う
    for i in prange(len(start_index_array)):

        # 1地物分取得
        temp_coordinates_array = coordinates_array[start_index_array[i]
            :end_index_array[i]+1, :]

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


def _get_index_array_dict_around_unit(line_array, start_index_array, end_index_array, unique_xy_string_array, left_up_x_line, right_bottom_x_line, right_bottom_y_line, left_up_y_line
                                      ):
    """
    図郭付近に存在する地物のIDを取得
    """

    linestring_id_dict = {}

    for ui in range(len(unique_xy_string_array)):

        # 図郭付近の地物取得
        temp_result_bool\
            = _get_feature_id_around_unit(line_array[:, 1:], start_index_array, end_index_array, left_up_x_line[ui], right_bottom_x_line[ui], right_bottom_y_line[ui], left_up_y_line[ui]
                                          )

        linestring_id_dict[unique_xy_string_array[ui]]\
            = np.where(temp_result_bool)[0]

    return linestring_id_dict
