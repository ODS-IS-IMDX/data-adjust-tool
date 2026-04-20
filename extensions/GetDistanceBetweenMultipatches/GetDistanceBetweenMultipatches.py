# MIT License
# 
# Copyright (c) 2026 NTT InfraNet
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
# マルチパッチの座標配列同士の離隔距離と角度を比較し、最大距離閾値を超えないペアのみ抽出する。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback
import io

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
np = import_module("numpy")
jit = import_module("numba").jit
f8 = import_module("numba").f8
b1 = import_module("numba").b1
i8 = import_module("numba").i8
cross2d = import_module("numba.np.extensions").cross2d
prange = import_module("numba").prange

FACE_ONLY = 0
FULL_COMBINATIONS = 1

PROCESS_MODE_MAP = {
    "面 × 面 のみ": FACE_ONLY,
    "点・線・面すべて（9パターン）": FULL_COMBINATIONS
}

@jit(f8[:,:,:](f8[:,:]),nopython=True, cache=True, nogil=True, parallel=True)
def get_bisector_array(target_array):
    # ----------------------------------------------------------
    # 三角形の各頂点から対辺へ引いた角の2等分線生成
    # 引数1:マルチパッチ線分座標3次元配列 [点の組み合わせ>線分の2点>xyz座標]
    # 引数2:マルチパッチ線分座標3次元配列 [点の組み合わせ>線分の2点>xyz座標]
    # 戻り値1:2等分線の両端点 3次元配列 [[[x,y,z][x,y,z]], [[x,y,z][x,y,z]] .....]
    # ----------------------------------------------------------

    # 処理結果初期配列
    result_array = np.zeros((target_array.shape[1],2,3), dtype = np.float64)

    # 頂点毎に処理
    for i in range(target_array.shape[1]):

        # 参考:https://xn--48s96ub7b0z5f.net/bekutoru-nitoubunsen/
        # 1頂点から他の頂点に向かった時の距離
        distance_array = np.sqrt(np.sum(np.power(target_array - target_array[i],2),axis = 1))

        # A:二等分線の始点, BおよびC:三角形を構成するA以外の2点, D:二等分線と辺の交差する点
        # Dの位置ベクトルを求めに行く
        # OD = (|OC|/(|OB|+|OC|)) * OB + (|OB|/(|OB|+|OC|)) * OC
        position_vector_array = distance_array[i-1]/(distance_array[i-2] + distance_array[i-1]) * target_array[i-2] + distance_array[i-2]/(distance_array[i-2] + distance_array[i-1]) * target_array[i-1]

        # 処理結果更新
        result_array[i,0] = target_array[i]
        result_array[i,1] = position_vector_array

    return result_array


@jit(f8[:,:,:](f8[:,:], f8[:,:]),nopython=True,cache=True,nogil=True)
def calculate_coordinates_between_points(target_point_array1, target_point_array2):
    # ----------------------------------------------------------
    # 点と点の座標
    # わざわざ最小値返す必要ない 3点の組み合わせ返すだけで大丈夫
    # 引数1:マルチパッチ座標2次元配列 [三角形の3点>xyz座標]
    # 引数2:マルチパッチ座標2次元配列 [三角形の3点>xyz座標]
    # 戻り値:3点同士の組み合わせ 3次元配列 [点の組み合わせ>2点の座標>xyz座標]
    # ----------------------------------------------------------

    # 検索条件と検索対象の線分の組み合わせ 9通り
    line_index = np.array([0,1,2])
    combinations = np.array([(li,lj) for li in range(len(line_index)) for lj in range(len(line_index))], dtype = np.int64)

    # 返却用配列
    result_array = np.zeros((len(combinations),2,3), dtype = np.float64)

    # 結果格納用Listから線分の座標arrayを取り出して一つずつnumpy配列更新
    result_array[:,0] = target_point_array1[combinations[:,0]]
    result_array[:,1] = target_point_array2[combinations[:,1]]

    return result_array


@jit(f8(f8[:,:]),nopython=True,cache=True,nogil=True)
def get_angle_between_xy_plane_and_line(target_array):
    # ----------------------------------------------------------
    # xy平面と直線のなす角度を算出するメソッド
    # 引数1:線分を表す2点のxyz座標 2次元配列 float64 [線分の始点終点>xyz座標]
    # 戻り値:xy平面と線分の為す角度 float64 単位はdegree
    # ----------------------------------------------------------
    # 線分方向ベクトル
    direction_vector_array = target_array[1] - target_array[0]

    # 線分の長さ
    direction_vector_distance = np.sqrt(np.sum(np.power(direction_vector_array,2)))

    # xy平面に対する法線単位ベクトル
    normal_vector_array = np.array([0,0,1] , dtype = np.float64)

    # 内積の公式よりxy平面と共通垂線のなす角度を求める
    # 共通垂線のベクトル:dv
    # xy平面の法線単位ベクトル:nv(0,0,1)
    # dv・nv = |d||n|cosθ
    # cosθ= dv・nv / (|d||n|)
    # 法線ベクトルが単位ベクトルなので
    # cosθ= dv・nv / |d|
    # θ=arccos(dv・nv / |d|)(単位ラジアン)
    # これは法線ベクトルと共通垂線ベクトルのなす角度なので90°から引く
    # →cosθが負の時は計算結果を90°で引かないといけないのでcosθの結果に絶対値必要
    result_degree = np.rad2deg(np.pi/2 - np.arccos(np.divide(np.dot(direction_vector_array, normal_vector_array.T),direction_vector_distance)))
    return result_degree


@jit(f8[:,:,:](f8[:,:], f8[:,:,:]),nopython=True,cache=True,nogil=True)
def calculate_coordinates_between_point_and_line(target_point_array, target_line_array):
    # ----------------------------------------------------------
    # 点と線の離隔となる線分の座標を算出する 点から線分に向けて垂線が下せない場合はスキップ
    # 引数1:マルチパッチ座標2次元配列 [三角形の3点>xyz座標]
    # 引数2:マルチパッチ線分座標3次元配列 [点の組み合わせ>線分の2点>xyz座標]
    # 戻り値:て 3次元配列 [点の組み合わせ>2点の座標>xyz座標]
    # ----------------------------------------------------------
    # 検索条件と検索対象の線分の組み合わせ 9通り
    line_index = np.array([0,1,2])
    combinations = np.array([(li,lj) for li in range(len(line_index)) for lj in range(len(line_index))])

    # 結果格納用List
    result_list = []

    # 点と線分の組み合わせごとに処理
    for cmi in range(len(combinations)):

        # 距離計算に使用した2点の初期配列
        points_array = np.zeros((2, 3), dtype = np.float64)

        # 比較対象の点と線分の座標取得
        temp_target_point_array = target_point_array[combinations[cmi,0]]
        temp_target_line_array = target_line_array[combinations[cmi,1]]

        # 垂線が下せるか判定
        # 参考：http://marupeke296.com/COL_3D_No27_CapsuleCapsule.html
        # 線分の両端点:A,B 垂線の始点:C
        # CA・ABの内積とCB・BAの内積の積の正負によって垂線が引けるか判定する
        inter_product1 = np.dot((temp_target_line_array[0]-temp_target_point_array), (temp_target_line_array[1]-temp_target_line_array[0]))
        inter_product2 = np.dot((temp_target_line_array[1]-temp_target_point_array), (temp_target_line_array[0]-temp_target_line_array[1]))

        # 点から線分に向かって垂線を下せない場合、片方の内積は負となる
        # 下せる場合は点と直線の距離を返す
        if inter_product1*inter_product2> 0:

            # A(0,0,0),B(2,0,0),C(1,1,1)
            # P = A + (B-A)s = (0 + 2s, 0 + 0s,0 + 0s)
            # c:定数,s:媒介変数の係数
            # Pxc = (A[0]-C[0]) * (B-A)[0]
            # Pxs = (B-A)[0] * (B-A)[0]
            # Pyc = (A[1]-C[1]) * (B-A)[1]
            # Pys = (B-A)[1] * (B-A)[1]
            # Pzc = (A[2]-C[2]) * (B-A)[2]
            # Pzs = (B-A)[2] * (B-A)[2]
            # CP = P - C = (-1 + 2s, -1 + 0s,-1 + 0s)
            # AB・CP = 0
            # (B-A)・CP = (-1 + 2s)*2 + ( -1 + 0s)*0 + (-1 + 0s)*0 = 0
            # 定数の和　(A[0]-C[0]) * (B-A)[0] + (A[1]-C[1]) * (B-A)[1] + (A[2]-C[2]) * (B-A)[2]
            # 係数の和　(B-A)[0]**2 + (B-A)[1]**2 + (B-A)[2]**2 = -np.sum(np.power(B-A,2))
            # 線分上の点Pを媒介変数表示で表す
            # 2線分の内積が0であることから、媒介変数を求める

            # 線分の方向ベクトル
            direction_vector_array = temp_target_line_array[1] - temp_target_line_array[0]

            # 媒介変数にかかる係数の合計(左辺)
            constant_sum = (temp_target_line_array[0,0]-temp_target_point_array[0]) * direction_vector_array[0] + (temp_target_line_array[0,1]-temp_target_point_array[1]) * direction_vector_array[1] + (temp_target_line_array[0,2]-temp_target_point_array[2]) * direction_vector_array[2]

            # 定数の合計(右辺)
            coefficient_sum = -np.sum(np.power(direction_vector_array,2))

            # 媒介変数取得
            parameter = constant_sum / coefficient_sum

            # 点Pの座標から垂線の足の座標取得
            perpendicular =  temp_target_line_array[0] + direction_vector_array * parameter

            # 初期配列に対して垂線の両端点を更新
            points_array[0] = temp_target_point_array
            points_array[1] = perpendicular

            # 結果格納用Listに点から線分に下した垂線の座標を追加
            result_list.append(points_array)

        #  垂線を下せない場合スキップ 次の組み合わせへ
        else:
            continue

    #  返却用配列
    result_array = np.zeros((len(result_list),2,3), dtype = np.float64)

    #  結果格納用Listから線分の座標arrayを取り出して一つずつnumpy配列更新
    for rli in range(len(result_list)):
        result_array[rli] = result_list[rli]

    return result_array


@jit(f8[:,:](f8[:,:], f8),nopython=True,cache=True,nogil=True)
def ratate_coordinates_around_x_axis(xyz_coordinates_array, rotation_radian):
    #  ----------------------------------------------------------
    #  X軸周りの反時計回りでyz座標を回転させる
    #  引数1:回転対象xyz座標 2次元配列 回転対象ポイント>xyz座標
    #  引数2:回転角(単位:ラジアン)float64
    #  戻り値:回転後xyz座標 2次元配列 回転対象ポイント>xyz座標
    #  ----------------------------------------------------------

    #  回転行列作成
    rotation_matrix = np.array([[       1.0,                     0.0,                      0.0]
                            , [       0.0, np.cos(rotation_radian), -np.sin(rotation_radian)]
                            , [       0.0, np.sin(rotation_radian),  np.cos(rotation_radian)]])

    #  xyz座標に適用
    return rotation_matrix.dot(xyz_coordinates_array.T).T


@jit(b1(f8[:,:], f8[:,:]),nopython=True,cache=True,nogil=True)
def is_intersect(condition_array, target_array):
    #  ----------------------------------------------------------
    #  交差判定
    #  引数1:検索条件（建設設計）のマルチパッチを構成する線分 [マルチパッチの線分>xy座標]
    # 引数2:検索対象（地下埋設物）のマルチパッチを構成する線分 [マルチパッチの線分>xy座標]
    # 戻り値:交差判定
    # ----------------------------------------------------------
    # a = ce - cs 検索条件始点から検索条件終点のベクトル
    # b = ts - cs 検索条件始点から検索対象始点のベクトル
    # c = te - cs 検索条件始点から検索対象終点のベクトル
    # s = np.cross(a,b) 外積
    # t = np.cross(a,c) 外積

    # 検索条件をABベクトル、検索対象をCDベクトルとする
    ab_vector = condition_array[1] - condition_array[0]
    ac_vector = target_array[0] - condition_array[0]
    ad_vector = target_array[1] - condition_array[0]

    cd_vector = target_array[1] - target_array[0]
    ca_vector = condition_array[0] - target_array[0]
    cb_vector = condition_array[1] - target_array[0]

    return (cross2d(ab_vector,ac_vector)*cross2d(ab_vector,ad_vector)<0) and (cross2d(cd_vector,ca_vector)*cross2d(cd_vector,cb_vector)<0)


@jit(f8[:,:](f8[:,:], f8[:,:]),nopython=True,cache=True,nogil=True)
def get_common_perpendicular_coordinates(condition_array, target_array):
    # ----------------------------------------------------------
    # 2線分の共通垂線の座標取得
    # ----------------------------------------------------------

    # 参考：https://math-juken.com/kijutu/kyoutusuisen/
    # 共通垂線の(s,t,u)係数
    # A(0,0,0),B(1,2,3)

    # A[0] 0
    # A[1] 0
    # A[2] 0
    # AB上にあるP
    # (B-A)[0] xの係数 1
    # (B-A)[1] yの係数 2
    # (B-A)[2] zの係数 3


    # C(4,5,-6),D(7,8,9)
    # C[0] 4
    # C[1] 5
    # C[2] -6
    # CD上にあるQ
    # (D-C)[0] xの係数 3
    # (D-C)[1] yの係数 3
    # (D-C)[2] zの係数 15
    # PQ = Q - P
    # AB・PQ = 0
    # CD・PQ = 0
    # numpy.linalg.solve()

    # condition_array→AB
    # target_array→CD

    # B-A
    ba_array = condition_array[1] - condition_array[0]

    # D-C
    dc_array = target_array[1] - target_array[0]

    # (B-A)・((D-C)[0],(D-C)[1],(D-C)[2])→s
    s1 = np.dot(ba_array, np.array([dc_array[0], dc_array[1], dc_array[2]]))

    # (D-C)・((D-C)[0],(D-C)[1],(D-C)[2])→s
    s2 = np.dot(dc_array, np.array([dc_array[0], dc_array[1], dc_array[2]]))

    # (B-A)・(-(B-A)[0],-(B-A)[1],-(B-A)[2])→t
    t1 = np.dot(ba_array, np.array([-ba_array[0], -ba_array[1], -ba_array[2]]))

    # (D-C)・(-(B-A)[0],-(B-A)[1],-(B-A)[2])→t
    t2 = np.dot(dc_array, np.array([-ba_array[0], -ba_array[1], -ba_array[2]]))

    # 媒介変数にかかる係数の配列（左辺）
    a_array = np.array([[s1,t1]
                    ,[s2,t2]])

    # ((C[0]-A[0]),(C[1]-A[1]),(C[2]-A[2]))
    u0 = np.array([target_array[0,0]-condition_array[0,0], target_array[0,1]-condition_array[0,1], target_array[0,2]-condition_array[0,2]], dtype = np.float64)

    # (B-A)・((C[0]-A[0]),(C[1]-A[1]),(C[2]-A[2]))→u
    u1 = np.dot(ba_array, u0)

    # (D-C)・((C[0]-A[0]),(C[1]-A[1]),(C[2]-A[2]))→u
    u2 = np.dot(dc_array, u0)

    # 定数の配列（右辺）
    b_array = np.array([-u1,-u2])

    # 媒介変数s,tの係数を
    result_array = np.linalg.solve(a_array, b_array)

    # A+(A-B)t, C+(C-D)s
    abt = condition_array[0] + (condition_array[1] - condition_array[0]) * result_array[1]
    cds = target_array[0] + (target_array[1] - target_array[0]) * result_array[0]

    # 結果格納用初期配列
    result_coordinates_array = np.zeros((2,3))

    # 共通垂線の始点終点を更新
    result_coordinates_array[0] = abt
    result_coordinates_array[1] = cds

    return result_coordinates_array


@jit(f8[:](f8[:,:,:]),nopython=True,cache=True,nogil=True)
def calculate_distance_between_points_array(target_point_array1):
    # ----------------------------------------------------------
    # 点と点の距離 複数の2点の組み合わせ対応版
    # 引数1:点のxyz座標 3次元配列 float64 [2点の組み合わせ>2点それぞれの座標>xyz座標]
    # 戻り値:2点間の距離 1次元配列 float64
    # ----------------------------------------------------------

    # 2点の座標差を2乗→合計→平方根
    distance_array = np.sqrt(np.sum(np.power(target_point_array1[:,1,:] - target_point_array1[:,0,:],2), axis = 1))

    return distance_array


@jit(f8[:](f8[:,:,:]),nopython=True,cache=True,nogil=True)
def get_angles_between_xy_plane_and_lines(target_array):
    # ----------------------------------------------------------
    # xy平面と直線のなす角度を算出するメソッド
    # 引数1:線分を表す2点のxyz座標 3次元配列 float64 [線分>線分の始点終点>xyz座標]
    # 戻り値:xy平面と線分の為す角度 float64 単位はdegree
    # ----------------------------------------------------------

    # 線分方向ベクトル
    direction_vector_array = target_array[:,1,:] - target_array[:,0,:]

    # 線分の長さ
    direction_vector_distance = np.sqrt(np.sum(np.power(direction_vector_array,2), axis = 1))

    # xy平面に対する法線単位ベクトル
    normal_vector_array = np.array([0,0,1] , dtype = np.float64)

    # 内積の公式よりxy平面と共通垂線のなす角度を求める
    # 共通垂線のベクトル:dv
    # xy平面の法線単位ベクトル:nv(0,0,1)
    # dv・nv = |d||n|cosθ
    # cosθ= dv・nv / (|d||n|)
    # 法線ベクトルが単位ベクトルなので
    # cosθ= dv・nv / |d|
    # θ=arccos(dv・nv / |d|)(単位ラジアン)
    # これは法線ベクトルと共通垂線ベクトルのなす角度なので90°から引く
    # →cosθが負の時は計算結果を90°で引かないといけないのでcosθの結果に絶対値必要
    result_degree = np.rad2deg(np.pi/2 - np.arccos(np.abs(np.divide(np.dot(direction_vector_array, normal_vector_array.T),direction_vector_distance))))

    return result_degree


@jit(f8[:](f8[:,:],i8),nopython=True,cache=True,nogil=True)
def get_minmax(line_array, target_index):
# ----------------------------------------------------------
# 検索条件側から検索対象に向けて垂線を下す
# 検索条件がx軸上に存在するのでそこから検索対象に向けて垂線をおろす
# x値の共通範囲を求め最小最大値における検索対象のy座標を計算する
# なお検索対象は始点終点共に同じZ値を持っているものとする
# ----------------------------------------------------------

    return np.array([np.min(line_array[:,target_index]), np.max(line_array[:,target_index])])


@jit(b1(f8[:], f8[:]),nopython=True,cache=True,nogil=True)
def is_perpendicular(target_x_minmax, condition_minmax):
# ----------------------------------------------------------
# 条件から対象に向けて垂線を下すことが出来るか
# ----------------------------------------------------------

    if target_x_minmax[1] < condition_minmax[0] or condition_minmax[1] < target_x_minmax[0]:
        return False
    else:
        return True


@jit(f8[:](f8[:],f8[:]),nopython=True,cache=True,nogil=True)
def get_commmon_area(minmax_array1, minmax_array2):

    # 大きい方の最小値から小さい方の最大値が範囲となる
    return np.array([np.fmax(minmax_array1[0], minmax_array2[0])
                    ,np.fmin(minmax_array1[1], minmax_array2[1])])


@jit(f8[:,:](f8[:,:],f8[:,:],f8[:]),nopython=True,cache=True,nogil=True)
def get_perpendicular_array(target_line_array, condition_line_array, common_area):
    # 条件から対象に向かって垂線を引き短い方を採用する

    # 傾きが計算できない場合
    if (target_line_array[0,0]-target_line_array[1,0]) == 0.0:
        y_coordinate_array = np.array([target_line_array[0,1],target_line_array[1,1]])
    else:
        gradient = ((target_line_array[0,1]-target_line_array[1,1]) / (target_line_array[0,0]-target_line_array[1,0]))
        y_coordinate_array = np.array([gradient*(common_area[i]-target_line_array[0,0]) + target_line_array[0,1] for i in range(len(common_area))])

    y_index = np.argmin(np.abs(y_coordinate_array - condition_line_array[0,1]))
    # 条件→対象へ向かって垂線を引く
    perpendicular_array = np.array([[common_area[y_index], condition_line_array[0,1], condition_line_array[0,2]]
                                   ,[common_area[y_index], y_coordinate_array[y_index], target_line_array[0,2]]], dtype = np.float64)

    return perpendicular_array


@jit(f8[:,:,:](f8[:,:,:], f8[:,:,:]),nopython=True, cache=True, nogil=True, parallel=True)
def calculate_coordinates_between_lines(condition_array, target_array):
    # ----------------------------------------------------------
    # 線同士の離隔となる線分の座標を算出する
    # 引数1:マルチパッチ線分座標3次元配列 [点の組み合わせ>線分の2点>xyz座標]
    # 引数2:マルチパッチ線分座標3次元配列 [点の組み合わせ>線分の2点>xyz座標]
    # 戻り値1:離隔となる線分の両端点 3次元配列 [[[x,y,z][x,y,z]], [[x,y,z][x,y,z]] .....]
    # ----------------------------------------------------------
    # condition_array[i,0]検索条件始点 cs
    # condition_array[i,1]検索条件終点 ce
    # target_array[i,0]検索対象始点 ts
    # target_array[i,1]検索対象終点 te
    # numbaサポートメモ
    # https://numba.pydata.org/numba-doc/dev/reference/numpysupported.html
    # np.dot() 3次元以降の引数は対応していない→不要なのでOK
    # np.arctan2(),np.rad2deg() OK
    # np.linalg.solve() 3次元以降の引数は対応していない→不要なのでOK
    # np.sin,np.cosの引数はすべて単位がラジアン
    # 角度に関する単位はラジアンか度かはっきりさせること
    # この関数はマルチパッチの三角形の組み合わせが1:1で入ってくる
    # 呼び出し元のアプリでマルチパッチの組み合わせインデックス作成+マルチプロセス処理を行う
    # 点と線の組み合わせに関しては以下で網羅できそう
    # http://marupeke296.com/COL_3D_No27_CapsuleCapsule.html
    # ----------------------------------------------------------

    # 検索条件と検索対象の線分の組み合わせ 9通り
    line_index = np.array([0,1,2])
    combinations = np.array([(li,lj) for li in range(len(line_index)) for lj in range(len(line_index))])

    # 結果格納用List
    result_list = []

    # 線分の組み合わせごとに処理
    for cmi in range(len(combinations)):

        # 比較する線分取得
        temp_target_array = target_array[combinations[cmi,0],:].copy()
        temp_condition_array = condition_array[combinations[cmi,1],:].copy()

        # 検索対象の始点を原点とした位置に検索条件、検索対象を平行移動
        translation_amount = temp_target_array[0].copy()
        temp_condition_array = temp_condition_array - translation_amount
        temp_target_array = temp_target_array - translation_amount

        # xy平面上から見た、検索対象とx軸がなす角度算出
        # 始点に対して終点のX座標が0の場合 Z軸上に検索対象が存在することになる
        # tanθ=y1/x1 θ=np.rad2deg(np.arctan(y1/x1))
        radian1 = np.arctan2(temp_target_array[1,1] ,temp_target_array[1,0])

        # 検索対象の終点、検索条件を、Z軸で回転させXZ平面上に移動する
        # Y軸をX軸に向ける方向
        temp_target_array = NCP.rotate_coordinates_around_z_axis(temp_target_array, -radian1)

        # xz平面上から見た、検索対象とx軸がなす角度算出
        # 始点に対して終点のX座標が0の場合回転処理はスキップ
        # tanθ=z1/x1 θ=np.rad2deg(np.arctan(z1/x1))
        radian2 = np.arctan2((temp_target_array[1,2] - temp_target_array[0,2]) ,(temp_target_array[1,0] - temp_target_array[0,0]))

        # 検索対象の終点、検索条件を、Y軸で回転させX軸上に移動する
        # Z軸をX軸に向ける方向
        temp_target_array = NCP.rotate_coordinates_around_y_axis(temp_target_array, radian2)

        # 検索条件を検索対象と同様の回転で移動する
        # 検索条件Z軸回転
        temp_condition_array = NCP.rotate_coordinates_around_z_axis(temp_condition_array, -radian1)

        # 検索条件Y軸回転
        temp_condition_array = NCP.rotate_coordinates_around_y_axis(temp_condition_array, radian2)

        # 立体交差判定 xy平面上で交差判定を行えるようX軸で検索条件を回転させる
        # 検索条件の始点終点のZ座標が等しくなるまで回転させる
        # θ= np.arctan((cs.z - ce.z) / (ce.y - cs.y))
        radian3 = np.arctan2((temp_condition_array[0,2] - temp_condition_array[1,2]), (temp_condition_array[1,1] - temp_condition_array[0,1]))

        # 検索条件のZ値が同じ値となる位置までX軸で回転させる
        temp_condition_array = ratate_coordinates_around_x_axis(temp_condition_array, radian3)

        # xy座標のみ取り出して交差判定を行う
        target_xy_array = np.trunc(temp_target_array[:,0:2] * 1000)/1000
        condition_xy_array = np.trunc(temp_condition_array[:,0:2] * 1000)/1000

        # 交差判定取得
        is_intersect_bool = is_intersect(target_xy_array, condition_xy_array)

        # 交差している場合
        if is_intersect_bool:

            # 垂線の始点終点取得
            coordinate_array = get_common_perpendicular_coordinates(temp_condition_array, temp_target_array)

        else:
            target_x_minmax = get_minmax(temp_condition_array, 0)
            condition_x_minmax = get_minmax(temp_target_array, 0)

            # 垂線引けるか
            perpendicular_bool = is_perpendicular(target_x_minmax, condition_x_minmax)

            if perpendicular_bool:

                # xの範囲
                common_area = get_commmon_area(target_x_minmax, condition_x_minmax)

                coordinate_array = get_perpendicular_array(temp_condition_array, temp_target_array, common_area)

            else:
                continue

        # X,Y,Zの逆順で垂線を回転させる
        # -radian3 X軸回転
        coordinate_array = ratate_coordinates_around_x_axis(coordinate_array, -radian3)

        # -radian2 Y軸回転
        coordinate_array = NCP.rotate_coordinates_around_y_axis(coordinate_array, -radian2)

        # radian1 Z軸回転
        coordinate_array = NCP.rotate_coordinates_around_z_axis(coordinate_array, radian1)

        # 平行移動量を戻す
        coordinate_array = translation_amount + coordinate_array

        # 結果格納
        # 交差している線分の組み合わせは一定でないのでlistに線分の座標を格納
        # 格納後np.arrayでnumpy配列に変換しようとしたがnumbaの制約で失敗
        # 結果格納用Listのlenの数でnumpy配列の初期配列を作成しインデックス指定でarrayを更新するとした
        result_list.append(coordinate_array)

    # 返却用配列
    result_array = np.zeros((len(result_list),2,3), np.float64)

    # 結果格納用Listから線分の座標arrayを取り出して一つずつnumpy配列更新
    for rli in range(len(result_list)):
        result_array[rli] = result_list[rli]

    return result_array


@jit(b1(f8[:],f8[:,:]),nopython=True,cache=True,nogil=True)
def is_include(correction_target_feature,correction_target_tin_numpy_array):
    # ----------------------------------------------------------------
    #   点に対する三角形の内包判定(線上は含む)
    # ----------------------------------------------------------------
    # 【引数】
    #  ①:補正対象地物の構成点1点 (1次元NumPy配列([x,y]または[x,y,z]))
    #  ②:補正対象TINの2次元NumPy配列 (構成点の組み合わせ>座標)
    #  ※①と②の座標数は合わせること
    # 【戻り値
    #  ①判定結果

    # 外積計算
    ab_vector_0 = correction_target_tin_numpy_array[1][0] - correction_target_tin_numpy_array[0][0]
    ab_vector_1 = correction_target_tin_numpy_array[1][1] - correction_target_tin_numpy_array[0][1]
    bp_vector_0 = correction_target_feature[0]  - correction_target_tin_numpy_array[1][0]
    bp_vector_1 = correction_target_feature[1]  - correction_target_tin_numpy_array[1][1]

    bc_vector_0 = correction_target_tin_numpy_array[2][0] - correction_target_tin_numpy_array[1][0]
    bc_vector_1 = correction_target_tin_numpy_array[2][1] - correction_target_tin_numpy_array[1][1]
    cp_vector_0 = correction_target_feature[0]  - correction_target_tin_numpy_array[2][0]
    cp_vector_1 = correction_target_feature[1]  - correction_target_tin_numpy_array[2][1]

    ca_vector_0 = correction_target_tin_numpy_array[0][0] - correction_target_tin_numpy_array[2][0]
    ca_vector_1 = correction_target_tin_numpy_array[0][1] - correction_target_tin_numpy_array[2][1]
    ap_vector_0 = correction_target_feature[0] - correction_target_tin_numpy_array[0][0]
    ap_vector_1 = correction_target_feature[1] - correction_target_tin_numpy_array[0][1]

    cross_product_ab_bp = ab_vector_0 * bp_vector_1 - ab_vector_1 * bp_vector_0
    cross_product_bc_cp = bc_vector_0 * cp_vector_1 - bc_vector_1 * cp_vector_0
    cross_product_ca_ap = ca_vector_0 * ap_vector_1 - ca_vector_1 * ap_vector_0

    # 外積の正負がすべて一致していれば三角形内部に点が存在する。
    if (cross_product_ab_bp >= 0 and cross_product_bc_cp >= 0 and cross_product_ca_ap >= 0) or (cross_product_ab_bp <= 0 and cross_product_bc_cp <= 0 and cross_product_ca_ap <= 0):
        return True
    else:
        return False



@jit(f8[:,:,:](f8[:,:],f8[:,:],f8),nopython=True,cache=True,nogil=True)
def get_ph_array(cde, ab, distance):
    # 参考https://mathscience-teach.com/koukoumath-kuukanbekutoru3-5/

    # 3点から平面の方程式を求める
    cd = cde[1] - cde[0]
    ce = cde[2] - cde[0]

    # 外積
    cde_cross = np.cross(cd, ce)
    # 平面の公式におけるa,b,c,d
    linea = cde_cross[0]
    lineb = cde_cross[1]
    linec = cde_cross[2]
    lined = (-cde_cross[0] * cde[0,0]) + (-cde_cross[1] * cde[0,1]) + (-cde_cross[2] * cde[0,2]) 

    # AB上の点を媒介変数表示 各座標の係数整理
    ab_v = ab[1] - ab[0]

    # 点と平面との距離の公式 両辺2乗しておく しなくても符号を変えてできる
    # 離隔**2*(a**2+b**2+c**2)
    # 平面と点の距離の公式から離隔を固定したときの座標を求める
    left = distance*np.sqrt(linea**2+lineb**2+linec**2)

    # 右辺はt, 定数を整理
    t_number = linea * ab_v[0] + lineb * ab_v[1] + linec * ab_v[2]
    c_number = linea * ab[0,0] + lineb * ab[0,1] + linec * ab[0,2] + lined

    # 面と線が平行
    if t_number == 0.0:
        return np.zeros((0,2,3), dtype = np.float64)

    else:

        # ab上の点
        # 垂線の足
        # 平面の方程式がありp(p,q,r)からおろした垂線の足をhとする
        t1 =  (left - c_number) / t_number
        p1 = ab[0] + ab_v * t1
        coef1 = -((linea * p1[0] + lineb * p1[1] +linec * p1[2] + lined) / (linea**2+lineb**2+linec**2))
        h1 = np.array([linea*coef1 , lineb*coef1, linec*coef1])+p1

        t2 =  (left - (c_number*-1)) / (t_number*-1)
        p2 = ab[0] + ab_v * t2
        coef2 = -((linea * p2[0] + lineb * p2[1] +linec * p2[2] + lined) / (linea**2+lineb**2+linec**2))
        h2 = np.array([linea*coef2 , lineb*coef2, linec*coef2])+p2

        # 垂線を格納する
        result_array = np.zeros((2,2,3), dtype = np.float64)
        result_array[0,0,] = h1
        result_array[0,1,] = p1
        result_array[1,0,] = h2
        result_array[1,1,] = p2

        # 垂線がそれぞれ検索対象の線上にあり検索条件のマルチパッチ上に存在することをチェック
        result_bool = np.zeros(2, dtype = np.bool_)
        result_bool[0] = (is_include(result_array[0,0],cde)) and (t1>=0.0) and (t1<=1.0)
        result_bool[1] = (is_include(result_array[1,0],cde)) and (t2>=0.0) and (t2<=1.0)

        return result_array[result_bool]


@jit('Tuple((f8[:,:],f8,f8))(f8[:,:],f8[:,:])',nopython=True, cache=True, nogil=True)
def get_coordinates_between_multipatches(condition_array, target_array):
    # ----------------------------------------------------------
    # 本当の高級関数
    # 三角形2つ渡して9パターンの離隔すべて返す
    # この関数では座標のみ返す
    # 面=角の2等分線
    # ----------------------------------------------------------
    # 線分の組み合わせ配列へ変換
    # →変換する目的は角の二等分線と配列の形を合わせるため 角の二等分線は構成点にすると6点存在し三角形で表せない
    # line：三角形の構成点の組み合わせ3通り
    # 3次元配列 [[[x,y,z],[x,y,z]]
    #           ,[[x,y,z],[x,y,z]]
    #           ,[[x,y,z],[x,y,z]]]
    condition_line_array = np.stack((condition_array, np.roll(condition_array,3)), axis = 1)
    target_line_array = np.stack((target_array, np.roll(target_array,3)), axis = 1)

    # 面情報（角の2等分線3本）生成
    # bisector：角の二等分線の構成点の組み合わせ3通り
    # 3次元配列 [[[x,y,z],[x,y,z]]
    #           ,[[x,y,z],[x,y,z]]
    #           ,[[x,y,z],[x,y,z]]]
    condition_bisector_array = get_bisector_array(condition_array)
    target_bisector_array = get_bisector_array(target_array)

    # パターン1,条件点と対象点の組み合わせ
    # 点と点の座標だけ欲しければ3×3の組み合わせを返すだけでよい
    pattern1_coordinates_array = calculate_coordinates_between_points(condition_array, target_array)

    # パターン2,条件点と対象線の組み合わせ
    pattern2_coordinates_array = calculate_coordinates_between_point_and_line(condition_array, target_line_array)

    # パターン3,条件点と対称面の組み合わせ
    pattern3_coordinates_array = calculate_coordinates_between_point_and_line(condition_array, target_bisector_array)

    # パターン4,条件線と対象点の組み合わせ パターン2の引数の条件入れ替え
    pattern4_coordinates_array = calculate_coordinates_between_point_and_line(target_array, condition_line_array)

    # パターン5,条件線と対象線の組み合わせ
    pattern5_coordinates_array = calculate_coordinates_between_lines(condition_line_array, target_line_array)

    # パターン6,条件線と対称面の組み合わせ
    pattern6_coordinates_array = calculate_coordinates_between_lines(condition_line_array, target_bisector_array)

    # パターン7,条件面と対象点の組み合わせ パターン3と引数の条件入れ替え
    pattern7_coordinates_array = calculate_coordinates_between_point_and_line(target_array, condition_bisector_array)

    # パターン8,条件面と対象線の組み合わせ パターン6と引数の条件入れ替え
    pattern8_coordinates_array = calculate_coordinates_between_lines(condition_bisector_array, target_line_array)

    # パターン9,条件面と対称面の組み合わせ
    pattern9_coordinates_array = calculate_coordinates_between_lines(condition_bisector_array, target_bisector_array)

    # すべての戻り値（離隔の両端点座標）を集めて3次元配列へ
    result_coordinates_array = np.concatenate((pattern1_coordinates_array
                                                ,pattern2_coordinates_array
                                                ,pattern3_coordinates_array
                                                ,pattern4_coordinates_array
                                                ,pattern5_coordinates_array
                                                ,pattern6_coordinates_array
                                                ,pattern7_coordinates_array
                                                ,pattern8_coordinates_array
                                                ,pattern9_coordinates_array))

    # 丸め誤差切り捨て
    result_coordinates_array = np.floor(result_coordinates_array*1000)/1000

    # 結果距離計算
    # すべての離隔座標3次元配列をループせず処理可能
    result_distance_array = calculate_distance_between_points_array(result_coordinates_array)

    # 距離が最小値のインデックス
    minimum_index = np.argmin(result_distance_array)

    # 結果をxy平面との角度計算へ
    # すべての離隔座標3次元配列をループせず処理可能
    result_degree = get_angle_between_xy_plane_and_line(result_coordinates_array[minimum_index])

    return result_coordinates_array[minimum_index], result_distance_array[minimum_index], result_degree


@jit('Tuple((f8[:,:,:],f8[:],f8[:]))(f8[:,:],f8[:,:],f8)',nopython=True, cache=True, nogil=True)
def get_coordinates_between_multipatches_face_only(condition_array, target_array, maximum_distance_value):
    # ----------------------------------------------------------
    # 本当の高級関数
    # 三角形2つ渡して9パターンの離隔すべて返す
    # この関数では座標のみ返す
    # 面=角の2等分線
    # ----------------------------------------------------------

    # 面情報（角の2等分線3本）生成
    # bisector：角の二等分線の構成点の組み合わせ3通り
    # 3次元配列 [[[x,y,z],[x,y,z]]
    #           ,[[x,y,z],[x,y,z]]
    #           ,[[x,y,z],[x,y,z]]]
    target_bisector_array = get_bisector_array(target_array)

    pattern11_coordinates_array1 = get_ph_array(condition_array,target_bisector_array[0],maximum_distance_value)
    pattern11_coordinates_array2 = get_ph_array(condition_array,target_bisector_array[1],maximum_distance_value)
    pattern11_coordinates_array3 = get_ph_array(condition_array,target_bisector_array[2],maximum_distance_value)

    result_coordinates_array = np.concatenate((pattern11_coordinates_array1
                                              ,pattern11_coordinates_array2
                                              ,pattern11_coordinates_array3))

    # 丸め誤差切り捨て
    result_coordinates_array = np.floor(result_coordinates_array*1000)/1000

    # 結果距離計算
    # すべての離隔座標3次元配列をループせず処理可能
    result_distance_array = calculate_distance_between_points_array(result_coordinates_array)

    # 結果をxy平面との角度計算へ
    # すべての離隔座標3次元配列をループせず処理可能
    result_degree = np.array([get_angle_between_xy_plane_and_line(temp_array) for temp_array in result_coordinates_array])

    target_bool = result_degree <=0

    return result_coordinates_array[target_bool], result_distance_array[target_bool], result_degree[target_bool]

@jit(b1(f8[:,:],f8[:,:]),nopython=True,cache=True,nogil=True)
def is_intersect_or_include(triangle_array, square_array):

    #########################################################################
    #三角形が、四角形の頂点のいずれかを内包していれば重複していると判定する
    #########################################################################
    # 四角形の構成点のいずれかが三角形の内側に存在する場合、内包していると判定
    if np.any(np.array([is_include(square_array[si], triangle_array) for si in range(len(square_array))])):
        return True

    # 四角形の構成点が内包されていない場合後続の交差判定へ処理が移る
    else:
        pass

    #########################################################################
    #四角形の構成点が三角形に内包されていないが、辺同士が交差している場合は、三角形に対して四角形が重複していると判定
    #########################################################################
    # 三角形の辺をインデックスで指定するための配列
    triangle_roll_array = np.roll(triangle_array, -2)

    # 四角形の辺をインデックスで指定するための配列
    square_roll_array = np.roll(square_array, -2)

    # 三角形の辺ごとのループ
    for i in prange(len(triangle_array)):

        # 四角形の辺ごとのループ
        for j in prange(len(square_roll_array)):

            # 交差判定
            tc1 = (triangle_array[i,0] - triangle_roll_array[i,0]) * (square_array[j,1] - triangle_array[i,1]) + (triangle_array[i,1] - triangle_roll_array[i,1]) * (triangle_array[i,0] - square_array[j,0])
            tc2 = (triangle_array[i,0] - triangle_roll_array[i,0]) * (square_roll_array[j,1] - triangle_array[i,1]) + (triangle_array[i,1] - triangle_roll_array[i,1]) * (triangle_array[i,0] - square_roll_array[j,0])
            td1 = (square_array[j,0] - square_roll_array[j,0]) * (triangle_array[i,1] - square_array[j,1]) + (square_array[j,1] - square_roll_array[j,1]) * (square_array[j,0] - triangle_array[i,0])
            td2 = (square_array[j,0] - square_roll_array[j,0]) * (triangle_roll_array[i,1] - square_array[j,1]) + (square_array[j,1] - square_roll_array[j,1]) * (square_array[j,0] - triangle_roll_array[i,0])

            # 一度でも辺が交差されていると判定されたら結果を返す
            if tc1*tc2<0 and td1*td2<0:
                return True
            else:
                continue

    #########################################################################
    #四角形が三角形を完全に覆いつくす場合重複していると判定する
    #########################################################################
    square_min_x = np.min(square_array[:, 0])
    square_min_y = np.min(square_array[:, 1])
    square_max_x = np.max(square_array[:, 0])
    square_max_y = np.max(square_array[:, 1])

    # 三角形の点A→B→Cの１点ずつに対し
    triangle_contained_flag_array = np.array([False, False, False])
    for k in prange(len(triangle_array)):

        # 座標値を個別に取得
        triangle_x = triangle_array[k, 0]
        triangle_y = triangle_array[k, 1]

        # 双方、範囲内なら内側
        if (square_min_x <= triangle_x and triangle_x <= square_max_x and 
            square_min_y <= triangle_y and triangle_y <= square_max_y):
            triangle_contained_flag_array[k] = True

    if np.all(triangle_contained_flag_array == True):
        return True

    # どの判定も通らなければ重複対象外
    return False

# 座標値の絞り込み
def extract_array(target_array, extract_array, maximum_distance_value):


    square_array = np.array([[np.max(extract_array[:,:,0]+maximum_distance_value), np.max(extract_array[:,:,1]+maximum_distance_value)]
                            ,[np.min(extract_array[:,:,0]-maximum_distance_value), np.max(extract_array[:,:,1]+maximum_distance_value)]
                            ,[np.min(extract_array[:,:,0]-maximum_distance_value), np.min(extract_array[:,:,1]-maximum_distance_value)]
                            ,[np.max(extract_array[:,:,0]+maximum_distance_value), np.min(extract_array[:,:,1]-maximum_distance_value)]])

    intersect_include_boolean = [is_intersect_or_include(target_array[i,:,0:2], square_array) for i in range(len(target_array))]

    return target_array[intersect_include_boolean]


class GetDistanceBetweenMultipatches(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        マルチパッチの座標配列同士の離隔距離と角度を比較し、最大距離閾値を超えないペアのみ抽出する。
                        ①input: 検索元（基準）および検索対象、抽出範囲指定用のマルチパッチ座標配列を持つ3行のFFieldSetFile。
                        ②output: 条件を満たす三角形ペアの座標組み合わせ、距離、角度を持つ3行のFieldSetFile。
                      """

        tags = ["Multipatch", "Distance", "Python"]

    SEARCH_BASE_DWH_NAME = PropertyDescriptor(
        name="Search Base DWH Name",
        description="検索条件マルチパッチのDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    SEARCH_TARGET_DWH_NAME = PropertyDescriptor(
        name="Search Target DWH Name",
        description="検索対象マルチパッチのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    EXTRACTION_RANGE_DWH_NAME = PropertyDescriptor(
        name="Extraction Range DWH Name",
        description="抽出範囲指定用マルチパッチのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    DISTANCE_CALCULATION_OPTION = PropertyDescriptor(
        name="Distance Calculation Option",
        description="""
                    離隔距離の計算方法
                    「面 × 面 のみ」は検索条件面から垂線を下すことのできる面のみで離隔距離を計算、
                    「点・線・面すべて」は全ての頂点・辺・面の組み合わせから離隔距離を計算。
                    """,
        allowable_values=["面 × 面 のみ", "点・線・面すべて（9パターン）"],
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
        sensitive=False
    )

    MAXIMUM_DISTANCE_VALUE = PropertyDescriptor(
        name="Maximum Distance Value",
        description="抽出する離隔の最大値",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    OUTPUT_COORDINATES_DWH_NAME = PropertyDescriptor(
        name="Output Coordinates DWH Name",
        description=" 出力データの座標配列のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    OUTPUT_DISTANCE_DWH_NAME = PropertyDescriptor(
        name="Output Distance DWH Name",
        description="出力データの距離のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    OUTPUT_DEGREE_DWH_NAME = PropertyDescriptor(
        name="Output Degree DWH Name",
        description="出力データの角度のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [SEARCH_BASE_DWH_NAME,
                            SEARCH_TARGET_DWH_NAME,
                            EXTRACTION_RANGE_DWH_NAME,
                            DISTANCE_CALCULATION_OPTION,
                            MAXIMUM_DISTANCE_VALUE,
                            OUTPUT_COORDINATES_DWH_NAME,
                            OUTPUT_DISTANCE_DWH_NAME,
                            OUTPUT_DEGREE_DWH_NAME
                            ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        プロパティで設定した値を取得

        Parameters:
            context: プロセッサの設定値
            flowfile: プロセッサに入るデータ

        Returns:
            search_base_dwh_name: 検索条件用マルチパッチのDWH名
            search_target_dwh_name: 検索対象マルチパッチのDWH名
            extraction_range_dwh_name: 抽出範囲指定用マルチパッチのDWH名
            distance_calculation_option: 離隔距離の計算方法
            maximum_distance_value: 抽出離隔最大値
            output_coordinates_dwh_name: 出力データの座標配列のDWH名
            output_distance_dwh_name: 出力データの距離のDWH名
            output_degree_dwh_name: 出力データの角度のDWH名

        """

        # 検索条件用マルチパッチのDWH名
        search_base_dwh_name = context.getProperty(
            self.SEARCH_BASE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 検索対象マルチパッチのDWH名
        search_target_dwh_name = context.getProperty(
            self.SEARCH_TARGET_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 抽出範囲指定用マルチパッチのDWH名
        extraction_range_dwh_name = context.getProperty(
            self.EXTRACTION_RANGE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 離隔距離の計算方法
        distance_calculation_option = context.getProperty(self.DISTANCE_CALCULATION_OPTION).getValue()

        # 抽出離隔最大値
        maximum_distance_value = float(context.getProperty(
            self.MAXIMUM_DISTANCE_VALUE).evaluateAttributeExpressions(flowfile).getValue())

        # 出力データの座標配列のDWH名
        output_coordinates_dwh_name = context.getProperty(
            self.OUTPUT_COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 出力データの距離のDWH名
        output_distance_dwh_name = context.getProperty(
            self.OUTPUT_DISTANCE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 出力データの角度のDWH名
        output_degree_dwh_name = context.getProperty(
            self.OUTPUT_DEGREE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return search_base_dwh_name, search_target_dwh_name, extraction_range_dwh_name, distance_calculation_option, maximum_distance_value, output_coordinates_dwh_name, output_distance_dwh_name, output_degree_dwh_name


    def get_flowfile(self, flowfile):
        """
        FlowFileのFieldSetFileをDataFrameに変換

        Parameters:
            flowfile: プロセッサに入ってくるデータ

        Returns:
            field_set_file_dataframe: FieldSetFileから作成したDataFrame
        """

        # flowfileから、CSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_dataframe = pd.read_csv(io.StringIO(input_field_set_file))

        return field_set_file_dataframe


    def get_coordinates_array_from_field_set_file_dataframe(self,
                                                            field_set_file_dataframe,
                                                            search_base_dwh_name,
                                                            search_target_dwh_name,
                                                            extraction_range_dwh_name):
        """
        FieldSetFileで作成したdataframeから、プロパティで指定したDWH名を持つ行のValue列をデコード、デシリアライズし取得

        Parameters:
            field_set_file_dataframe: FieldSetFileをDataFrameに加工したデータ
            target_dwh_name: 検索条件用マルチパッチ
            target_gcp_dwh_name: 検索対象マルチパッチ
            extraction_range_dwh_name: 抽出範囲指定用マルチパッチのDWH名

        Returns:
            search_base_coordinates_array: 検索条件用マルチパッチ
            search_target_coordinates_array: 検索対象のマルチパッチ
            extraction_range_coordinates_array: 抽出範囲指定用マルチパッチ
        """

        # 検索条件用マルチパッチを field_set_dataframeから取得
        search_base_coordinates_array = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, search_base_dwh_name)

        # 検索対象のマルチパッチを field_set_dataframeから取得
        search_target_coordinates_array = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, search_target_dwh_name)

        # 抽出範囲指定用マルチパッチを field_set_dataframeから取得
        extraction_range_coordinates_array = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, extraction_range_dwh_name)

        return search_base_coordinates_array, search_target_coordinates_array, extraction_range_coordinates_array


    def convert_murtipatch_array_to_xyz_array(self, multipatch_array):
        """
        8列構成のマルチパッチ配列(2次元配列)から4行ごとにグループ化して3次元配列に変換、
        x, y, z 列のみ抽出して、各グループの終点(4行目)を削除

        Parameters:
            multipatch_array: マルチパッチの2次元座標[fid, x, y, z, nx, ny, nz, surfaceid], shape (N/4, 8)

        Returns
            result_array: x, y, z だけのデータ, shape (N, 3, 3)
        """

        # reshape: (N, 8) → (N/4, 4, 8)
        grouped_array = multipatch_array.reshape(-1, 4, 8)

        # x, y, z列だけ抽出（列1〜3）
        xyz_array_with_endpoint = grouped_array[:, :, 1:4]

        # 4行目（インデックス3）を削除 → (N/4, 3, 3)
        xyz_array = np.delete(xyz_array_with_endpoint, 3, axis=1)

        return xyz_array


    def generate_coordinates_with_id_array(self, coordinates_array):
        """
        3次元の座標配列を2次元配列に変換し、ID列を適応する

        Parameters:
            coordinates_array: 3次元の座標配列, shape (N, M, 3)

        Returns:
            output_coordinates_array: ID列+座標列, shape (N*M, 4)
            id_column: 2次元のID列, shape (N*M, 1)
        """

        # 3次元座標配列を2次元に
        combined_array = coordinates_array.reshape(-1, 3)

        # 地物（グループ）の数 n を取得
        n = coordinates_array.shape[0]

        # 各地物に含まれる点の数 m を取得
        row_counts = coordinates_array.shape[1]

        # 0〜n-1 の ID を m 回ずつ繰り返して、(n*m, 1) の ID列を作成
        # 例: n=2, m=2 の場合 → [[0], [0], [1], [1]]
        id_column = np.repeat(np.arange(n), row_counts).reshape(-1, 1)

        # ID列と座標列を横方向に結合 → shape=(n*m, 4)
        # 最終的な形: [ID, X, Y, Z]
        output_coordinates_array = np.hstack((id_column, combined_array))

        return output_coordinates_array, id_column


    def process_face_only_mode(self, search_base_xyz_array, search_target_xyz_array, maximum_distance_value):
        """
        面対面の距離・角度・対応座標計算を行う

        Args:
            search_base_xyz_array: 検索対象の座標配列
            search_target_xyz_array: 検索対象の座標配列
            maximum_distance_value: 最大距離（垂線を下す距離範囲の上限）

        Returns:
            coordinates_array: 対応する座標配列
            distance_array: 各対応点間の距離配列
            degree_array: 各対応点間の角度配列b
        """

        distance_value_range = np.linspace(maximum_distance_value, 0.1, 5)

        # 結果格納用
        result_list1, result_list2, result_list3 = [], [], []

        # 全組み合わせで距離・角度・対応座標を取得
        for i in range(len(search_base_xyz_array)):

            for j in range(len(search_target_xyz_array)):

                for k in range(len(distance_value_range)):

                    t1, t2, t3 = get_coordinates_between_multipatches_face_only(search_base_xyz_array[i],
                                                                                search_target_xyz_array[j],
                                                                                distance_value_range[k])

                    result_list1.append(t1)
                    result_list2.append(t2)
                    result_list3.append(t3)

        coordinates_array = np.array([group[0] for group in result_list1 if group.size > 0])
        distance_array = np.array([group[0] for group in result_list2 if group.size > 0])
        degree_array = np.array([group[0] for group in result_list3 if group.size > 0])

        return coordinates_array, distance_array, degree_array


    def process_full_combinations_mode(self, search_base_xyz_array, search_target_xyz_array):
        """
        点・線・面など一般的な形状の全組み合わせで距離・角度・対応座標計算を行う

        Args:
            search_base_xyz_array: 検索対象の座標配列
            search_target_xyz_array: 検索対象の座標配列

        Returns:
            coordinates_array: 対応する座標配列
            distance_array: 各対応点間の距離配列
            degree_array: 各対応点間の角度配列
        """
        # 結果格納用
        result_list1, result_list2, result_list3 = [], [], []

        # 全組み合わせで距離・角度・対応座標を取得
        for i in range(len(search_base_xyz_array)):

            for j in range(len(search_target_xyz_array)):

                t1, t2, t3 = get_coordinates_between_multipatches(search_base_xyz_array[i],
                                                                  search_target_xyz_array[j])

                result_list1.append(t1)
                result_list2.append(t2)
                result_list3.append(t3)

        coordinates_array = np.array([result_list1])
        distance_array = np.array([result_list2])
        degree_array = np.array([result_list3])

        return coordinates_array, distance_array, degree_array

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # -----------------------------------------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # -----------------------------------------------------------------------------------------------------------
            search_base_dwh_name, \
                search_target_dwh_name, \
                extraction_range_dwh_name, \
                distance_calculation_option, \
                maximum_distance_value, \
                output_coordinates_dwh_name, \
                output_distance_dwh_name, \
                output_degree_dwh_name\
                = self.get_property(context, flowfile)

            process_mode = PROCESS_MODE_MAP[distance_calculation_option]

            # -----------------------------------------------------------------------------------------------------------
            #  flowfileをDataFrameに加工し、取得
            # -----------------------------------------------------------------------------------------------------------
            field_set_file_dataframe\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            #  FieldSetFileで作成したdataframeから、プロパティで指定したDWH名のValue列を取得
            # -----------------------------------------------------------------------------------------------------------
            search_base_coordinates_array, \
                search_target_coordinates_array, \
                extraction_range_coordinates_array\
                = WM.calc_func_time(self.logger)(self.get_coordinates_array_from_field_set_file_dataframe)(field_set_file_dataframe,
                                                                                                           search_base_dwh_name,
                                                                                                           search_target_dwh_name,
                                                                                                           extraction_range_dwh_name)

            # -----------------------------------------------------------------------------------------------------------
            # マルチパッチをXYZ座標配列に変換
            # -----------------------------------------------------------------------------------------------------------
            search_base_xyz_array\
                = WM.calc_func_time(self.logger)(self.convert_murtipatch_array_to_xyz_array)(search_base_coordinates_array)

            search_target_xyz_array\
                = WM.calc_func_time(self.logger)(self.convert_murtipatch_array_to_xyz_array)(search_target_coordinates_array)

            extraction_range_xyz_array\
                = WM.calc_func_time(self.logger)(self.convert_murtipatch_array_to_xyz_array)(extraction_range_coordinates_array)

            # -----------------------------------------------------------------------------------------------------------
            # 離隔計算対象抽出
            # -----------------------------------------------------------------------------------------------------------
            search_base_xyz_array = WM.calc_func_time(self.logger)(extract_array)(search_base_xyz_array, extraction_range_xyz_array, maximum_distance_value*2)
            search_target_xyz_array = WM.calc_func_time(self.logger)(extract_array)(search_target_xyz_array, extraction_range_xyz_array, maximum_distance_value*2)


            if process_mode == FACE_ONLY:

                coordinates_array, distance_array, degree_array=self.process_face_only_mode(search_base_xyz_array, search_target_xyz_array, maximum_distance_value)

            else:

                coordinates_array, distance_array, degree_array=self.process_full_combinations_mode(search_base_xyz_array, search_target_xyz_array)

            output_target_index = np.where(distance_array < maximum_distance_value)

            # # 対象インデックスのみ抽出
            filtered_coordinates_array = coordinates_array[output_target_index]
            filtered_distance_array = distance_array[output_target_index]
            filtered_degree_array = degree_array[output_target_index]

            output_coordinates_array, \
                id_column\
                = WM.calc_func_time(self.logger)(self.generate_coordinates_with_id_array)(filtered_coordinates_array)

            unique_values = np.unique(id_column)

            output_distance_list = list(zip(unique_values, filtered_distance_array))
            output_degree_list = list(zip(unique_values, filtered_degree_array))

            # -----------------------------------------------------------------------------------------------------------
            # 出力データの設定
            # -----------------------------------------------------------------------------------------------------------
            output_value_list = [output_coordinates_array, output_distance_list, output_degree_list]

            output_dwh_name_list = [output_coordinates_dwh_name, output_distance_dwh_name, output_degree_dwh_name]
            output_type_list = ["geometry"] + [type(value_list[0][1]).__name__ for value_list in output_value_list[1:]]

            # -----------------------------------------------------------------------------------------------------------
            # 出力用FieldSetFileを生成
            # -----------------------------------------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_name_list, output_type_list, output_value_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
