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

# Python標準ライブラリ
import traceback
import io
import pickle

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
ConvexHull = import_module("scipy.spatial").ConvexHull
Delaunay = import_module("scipy.spatial").Delaunay
pyproj = import_module("pyproj")
jit = import_module("numba").jit
f8 = import_module("numba").f8
i8 = import_module("numba").i8
b1 = import_module("numba").b1
prange = import_module("numba").prange

#----------------------------------------------------------------
#・構成点を内包する補正対象TINのインデックス取得処理
#  補正対象地物の構成点数がどのTINに内包されているか判定する
#----------------------------------------------------------------
#【引数】    
# ①:補正対象地物の構成点1点 (1次元NumPy配列([x,y]または[x,y,z]))
# ②:補正対象TINの3次元NumPy配列 (TIN>構成点の組み合わせ>座標)
# ※①と②の座標数は合わせること
#【戻り値】
# ①TINのインデックス
@jit(i8(f8[:],f8[:,:,:]),nopython=True,cache=True,nogil=True)
def get_tin_index(correction_target_feature,correction_target_tin_numpy_array):

    #補正対象TINごと
    #ti:TINのインデックス
    for ti in prange(len(correction_target_tin_numpy_array)):

        #TINを構成する3点取得
        temp_correction_target_tin_numpy_array = correction_target_tin_numpy_array[ti]
        
        #内外判定初期化
        judgment_boolean = False

        #外積計算
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

        #外積の正負がすべて一致していれば三角形内部に点が存在する。
        if (cross_product_ab_bp >= 0 and cross_product_bc_cp >= 0 and cross_product_ca_ap >= 0) or (cross_product_ab_bp <= 0 and cross_product_bc_cp <= 0 and cross_product_ca_ap <= 0):
            judgment_boolean = True
        else:
            judgment_boolean = False

        #構成点がTINに内包されているならTINのインデックスを返して終了
        if judgment_boolean == True:
            return ti
        
        #内包していない場合次のTINへ
        #内包しているTINが存在しない場合は初期値のままとなる
        else:
            continue
    
    #一度も内包判定されない場合-1を返して終了
    return -1

#tinが生み出す平面上に存在するxyが明確な点PのZ値を算出する
#参考：https://mathscience-teach.com/koukoumath-kuukanbekutoru3-5/
#引数1:TINの座標 [3点]>[xyz座標] np.float64の2次元配列
#引数2:Pの座標 [xy座標] np.float64の1次元配列
#戻り値:pのZ値 np.float64
@jit(f8(f8[:,:], f8[:]), nopython=True, cache=True, nogil=True)
def get_z_value_on_3points_plane(tin_array ,p_array):
    #abcが為す平面上に点pが存在するときのpの表し方
    #p = ra + sb + tc (r+s+t=1)
    #変数の数を2つへ
    #p = (1-s-t)a + sb + tc
    #(1,3,z) = (1-s-t)(3,2,0) + s(3,1,3) + t(1,4,-3)

    #-----------------------------------------------------
    #X座標係数
    #-----------------------------------------------------
    #(1-s-t)*3 + s * 3  + t * 1 = 1

    #xを表すs係数の和
    # -3s + 3s
    s1 = -tin_array[0,0] + tin_array[1,0]

    #xを表すt係数の和
    # -3t + t
    t1 = -tin_array[0,0] + tin_array[2,0]

    #xを表す定数の和
    #1 + (-3)
    u1 = p_array[0] - tin_array[0,0]
    #-----------------------------------------------------

    #-----------------------------------------------------
    #Y座標係数
    #-----------------------------------------------------
    #(1-s-t)*2 + s * 1  + t * 4 = 3

    #yを表すs係数の和
    #-2s + 1s 
    s2 = -tin_array[0,1] + tin_array[1,1]

    #yを表すt係数の和
    #-2t + 4t
    t2 = -tin_array[0,1] + tin_array[2,1]

    #yを表す定数の和 
    #3 + (-2)
    u2 = p_array[1] - tin_array[0,1]
    #-----------------------------------------------------

    #st係数2次元配列
    st_array = np.array([[s1, t1]
                        ,[s2, t2]])

    #定数1次元配列
    u_array = np.array([u1, u2])

    #xyのみでst算出
    result_array = np.linalg.solve(st_array, u_array)

    #stよりz値算出
    #z = (1-s-t)*0 + s * 3  + t * (-3)
    z_value = (1 - result_array[0] - result_array[1]) * tin_array[0,2]\
                                   + result_array[0]  * tin_array[1,2]\
                                   + result_array[1]  * tin_array[2,2]

    return z_value

#get_z_value_on_3points_planeを複数の地物で処理する用
#引数1:pixel中心座標配列 [pixel数]>[xy座標] 2次元配列 np.float64
#引数2:TINの配列 [TIN単位]>[TINの構成点]>[xyz] 3次元配列 np.float64
#引数3:TINのインデックス
#戻り値1:dem出力用array[縦のpixel]>[横のpixel]を平坦化させた配列 1次元配列 np.float64
@jit(f8[:](f8[:,:], f8[:,:,:],i8[:]), nopython=True, cache=True, nogil=True)
def get_z_array_on_3points_plane(center_corrdinate_array, tin_xyz_array, tin_index_array):
    
    #結果格納用array
    result_z_array = np.zeros(len(center_corrdinate_array), dtype = np.float64)
    
    for i in prange(len(center_corrdinate_array)):
        
        result_z_array[i] = get_z_value_on_3points_plane(tin_xyz_array[tin_index_array[i]], center_corrdinate_array[i])
        
    return result_z_array

class CreateObstacleMultipatch(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        底面ポリゴンの座標配列と、tinの座標配列から、影響判定ポリゴンを作成する
                        ①input: 底面ポリゴンの座標配列と、tinの座標配列を持った2行のFieldSetFile。
                        ②output: 影響判定ポリゴン持った1行のFieldSetFile。
                      """
        tags = ["TIN", "Polygon", "Python"]

    # 出力データのDWH名
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # 底面ポリゴン座標配列のDWH名
    INPUT_BOTTOM_POLYGON_COORDINATES_DWH_NAME = PropertyDescriptor(
        name="Input Bottom Polygon Coordinates DWH Name",
        description="底面ポリゴンの座標配列のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    # TINの座標配列のDWH名
    INPUT_TIN_COORDINATES_DWH_NAME = PropertyDescriptor(
        name="Input TIN Coordinates DWH Name",
        description="TINの座標配列のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        sensitive=False
    )

    property_descriptors = [OUTPUT_DWH_NAME,
                            INPUT_BOTTOM_POLYGON_COORDINATES_DWH_NAME,
                            INPUT_TIN_COORDINATES_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに渡されるデータ

        戻り値:
            input_csv_encoding_code: 入荷CSVの文字コード
            definition_csv: データ定義のCSV
            column_flag: 入荷CSVにカラムがあるのかのフラグ
        """
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        input_bottom_polygon_coordinates_dwh_name = context.getProperty(
            self.INPUT_BOTTOM_POLYGON_COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        input_tin_coordinates_dwh_name = context.getProperty(
            self.INPUT_TIN_COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return output_dwh_name, input_bottom_polygon_coordinates_dwh_name, input_tin_coordinates_dwh_name

    #影響判定ポリゴン作成用

    #基底部ポリゴンの辺から生成する方向ベクトルの算出
    #引数1:始点 1次元配列 xyz座標
    #引数2:終点 1次元配列 xyz座標
    #戻り値:45度線の方向ベクトル
    def get_d_vector(self, start_point, end_point):

        #方向ベクトルを求める 辺の始点側z値 xyは始点-始点の前点
        #dx = Cx - Bx
        #dy = Cy - By
        #Z値はCBベクトルのxy平面上の長さからCBのZ値の差分引く
        #dz = np.sqrt(np.sum(np.power(C[:,:2]-B[:,:2]))) - (Cz - Bz)
        
        #方向ベクトルのxy座標取得
        result_point = end_point - start_point

        # dx = end_point[0] - start_point[0]
        # dy = end_point[1] - start_point[1]
        # dz = end_point[2] - start_point[2]

        # horizontal_distance = np.hypot(dx, dy)  # √(dx² + dy²)
        # result_z = horizontal_distance - dz

        #基底部の傾きを考慮したZ値取得
        result_z = np.sqrt(np.sum(np.power(end_point[:2]-start_point[:2], 2))) - (end_point[2] - start_point[2])
        
        #Z値更新
        result_point[2] = result_z
        
        return result_point


    #参考：chrome-extension://efaidnbmnnnibpcajpcglclefindmkaj/https://gamescience.jp/text/PDF/3DMath.pdf
    #    ：https://risalc.info/src/line-triangle-intersection.html
    #基底部の1辺から45度線ベクトルを媒介変数表示させたときの始点と方向ベクトルを算出する。
    #引数:基底部の凸多角形 2次元配列 要素数3 [凸多角形構成点]>[xyz座標]
    #戻り値1:始点のlist 要素はnumpy.arrayの1次元配列 要素数は凸多角形の頂点数と同じ xyz座標
    #戻り値2:方向ベクトルのlist 要素はnumpy.arrayの1次元配列 要素数は凸多角形の頂点数の2倍 xyz座標
    def get_origin_and_d_vector(self, polygon_array):

        #45度線始点格納用list
        start_point_list = []

        #45度線方向ベクトル格納用list
        d_vector_list = []

        #ポリゴンの辺ごとに処理
        for i in range(len(polygon_array)-1):
            
            #前点と次点との距離を比較し短い方からのみベクトルを発生させる
            #before_distance = np.sqrt(np.sum(np.power(polygon_array[i]-polygon_array[i-1],2)))
            #after_distance = np.sqrt(np.sum(np.power(polygon_array[i]-polygon_array[i+1],2)))
            
            #if before_distance < after_distance:
            
            #始点格納
            start_point_list.append(polygon_array[i])

            #辺の始点終点を中心とした前点、次点の対称な点
            #C = 2B - A
            #ひとつ前の点側
            before_contrast_point = polygon_array[i]*2-polygon_array[i-1]

            #方向ベクトルを求める 辺の始点側z値 xyは始点-始点の前点
            #dx = Cx - Bx
            #dy = Cy - By
            #Z値はCBベクトルのxy平面上の長さからCBのZ値の差分引く
            #dz = np.sqrt(np.sum(np.power(C[:,:2]-B[:,:2]))) - (Cz - Bz)
            #方向ベクトルの計算方法は辺の始点側終点側どちらも同じ
            #方向ベクトルを求める 辺の終点側z値 xyは終点-終点の次点
            before_d = self.get_d_vector(polygon_array[i], before_contrast_point)
            
            #方向ベクトル格納 追加順は次点から生成した方向ベクトル→反時計回りになる
            d_vector_list.append(before_d)
            
            #else:
            #始点格納
            start_point_list.append(polygon_array[i])
            #辺の始点終点を中心とした前点、次点の対称な点
            #C = 2B - A
            #次の点側
            after_contrast_point = polygon_array[i]*2-polygon_array[i+1]

            #方向ベクトルを求める 辺の始点側z値 xyは始点-始点の前点
            #dx = Cx - Bx
            #dy = Cy - By
            #Z値はCBベクトルのxy平面上の長さからCBのZ値の差分引く
            #dz = np.sqrt(np.sum(np.power(C[:,:2]-B[:,:2]))) - (Cz - Bz)
            #方向ベクトルの計算方法は辺の始点側終点側どちらも同じ
            #方向ベクトルを求める 辺の終点側z値 xyは終点-終点の次点
            after_d = self.get_d_vector(polygon_array[i], after_contrast_point)
            
            #方向ベクトル格納 追加順は次点から生成した方向ベクトル→反時計回りになる
            d_vector_list.append(after_d)

        
        #最終ループのインデックス調整
        #最終ループも同様に短い方だけベクトル作成
        #before_distance = np.sqrt(np.sum(np.power(polygon_array[-1]-polygon_array[-2],2)))
        #after_distance = np.sqrt(np.sum(np.power(polygon_array[-1]-polygon_array[0],2)))

        #if before_distance < after_distance:
        start_point_list.append(polygon_array[-1])
        before_contrast_point = polygon_array[-1]*2-polygon_array[-2]
        before_d = self.get_d_vector(polygon_array[-1], before_contrast_point)
        d_vector_list.append(before_d)
        #else:
        start_point_list.append(polygon_array[-1])
        after_contrast_point = polygon_array[-1]*2-polygon_array[0]
        after_d = self.get_d_vector(polygon_array[-1], after_contrast_point)
        d_vector_list.append(after_d)

        return start_point_list, d_vector_list

    #頂点群から頂点群をすべて囲む凸多角形ポリゴン生成
    #構成点は頂点群から選択され反時計回りになっている
    #引数1:頂点群 2次元配列 [頂点]>[xyz座標]
    #戻り値1:凸多角形ポリゴンの座標 2次元配列 [頂点]>[xyz座標]
    def create_convex_polygon(self, points_array):
        #xy座標で凸多角形ポリゴンのインデックス取得
        hull = ConvexHull(points_array[:,:2])

        #元のxyz座標を凸多角形ポリゴンのインデックスで指定
        hull_points_array = points_array[hull.vertices]
            
        return hull_points_array

    #引数:始点 1次元配列 xyz座標
    #引数:方向ベクトル 1次元配列 xyz座標
    #引数:TINの座標 3次元配列 [TIN]>[TIN構成点3点]>[xyz座標]
    #戻り値45度線とTINとの交点 1次元配列 xyz座標
    def get_45_degree_line(self, origin, d_vector, tin_array):

        #①直線上の点を表すベクトルの媒介変数表示 媒介変数(t)
        # V = origin + t(d_vector)
        
        #以下TINごとに処理
        for ti in range(len(tin_array)):
            
            temp_tin_array = tin_array[ti]
            #②TINの3点が存在する平面式 媒介変数(u, v)
            # V = TIN[0] + u(TIN[1]-TIN[0]) + v(TIN[2]-TIN[0])
            #TIN[0] + u(TIN[1]-TIN[0]) + v(TIN[2]-TIN[0]) = origin + t(d_vector)
            #xyz座標展開
            #l = (x1,y1,z1)-(x0,y0,z0)
            #m = (x2,y2,z2)-(x0,y0,z0)
            #(x0,y0,z0) + u(lx, ly, lz) + v(mx, my, mz) = (Ox, Oy, Oz) + t((dx, dy, dz))
            #-t(dx, dy, dz) + u(lx, ly, lz) + v(mx, my, mz) = (Ox, Oy, Oz) - (x0,y0,z0)
            #x, y, zの係数と定数をそれぞれ抽出
            #x:(-dx)t + (lx)u + (mx)v = Ox - x0
            #y:(-dy)t + (ly)u + (my)v = Oy - y0
            #z:(-dz)t + (lz)u + (mz)v = Oz - z0
            l = temp_tin_array[1] - temp_tin_array[0]
            
            m = temp_tin_array[2] - temp_tin_array[0]
            
            left_array = np.array([[-d_vector[0], l[0], m[0]]
                                ,[-d_vector[1], l[1], m[1]]
                                ,[-d_vector[2], l[2], m[2]]], dtype = np.float64)

            right_array =  np.array([ origin[0]-temp_tin_array[0,0]
                                    , origin[1]-temp_tin_array[0,1]
                                    , origin[2]-temp_tin_array[0,2]], dtype = np.float64)

            #①と②が同値の時のstu係数と定数取得
            #numpy.linalgでtu係数取得
            tuv_array = np.linalg.solve(left_array, right_array)
            #stu係数からxyz取得
            result_array = origin + (d_vector)*tuv_array[0]
            #求めた点がTINの内側に存在するか判定

            #u>0,v>0,u+v<1
            if tuv_array[1] >= 0.0 and tuv_array[2] >= 0.0 and tuv_array[1] + tuv_array[2] <= 1.0:
                #存在すれば終了
                return result_array
            else:
                #存在しなければ次のTINへ
                pass

        #★最後まで存在しなければ？
        return result_array

    def get_top_surface(self, start_point_list, d_vector_list, tin_array):
        """
        上面ポリゴンと対応する基底TINを作成する関数。
        元のロジックをそのまま関数にまとめています。
        """

        #-------------------------------------------------------
        #上面
        #-------------------------------------------------------
        #45度線結果格納用
        result_list = []
        
        #45度線と始点紐づけ用dict
        result_dict = {}

        for i in range(len(start_point_list)):
            a = self.get_45_degree_line(start_point_list[i], d_vector_list[i], tin_array)
            result_list.append(a)

            result_dict[str(a[0]) + '@' + str(a[1]) + '@' + str(a[2])] = start_point_list[i]

        result_array = np.array(result_list)

        #45度線の外周取得
        result_array = self.create_convex_polygon(result_array)

        #45度線の座標から紐づく始点（基底部）
        result_start_array = np.array([
            result_dict[str(result_array[i][0]) + '@' + str(result_array[i][1]) + '@' + str(result_array[i][2])]
            for i in range(len(result_array))
        ])

        return result_array, result_start_array


    def generate_bottom_tin_from_result_start_array(self, result_start_array):
        """
        基底部の座標から TIN（三角形分割）を生成し、ループを閉じたポリゴン配列を返す。
        """
        # 基底部のユニークな点を取得
        bottom_array = np.unique(result_start_array, axis=0)

        # 2次元DelaunayでTINを作成（X, Y 座標のみ使用）
        tri = Delaunay(bottom_array[:, 0:2])
        bottom_tin_index = tri.simplices

        # ポリゴンとして閉じる（始点を終点として追加）
        bottom_tin_index = np.concatenate([bottom_tin_index, bottom_tin_index[:, 0:1]], axis=1).flatten()

        # インデックスに対応する座標を抽出
        bottom_tin_array = bottom_array[bottom_tin_index]

        return bottom_tin_array

    def generate_side_faces_from_result_arrays(self, result_array, result_start_array):
        """
        上面と基底面の対応点から側面ポリゴンの座標を構成。
        """
        result_side_list = []

        # 通常の辺ごとに三角形/四角形分割（前半）
        for i in range(len(result_start_array) - 1):
            result_side_list.append(result_array[i])
            result_side_list.append(result_array[i + 1])
            result_side_list.append(result_start_array[i])
            result_side_list.append(result_array[i])

            if not np.array_equal(result_start_array[i], result_start_array[i + 1]):
                result_side_list.append(result_array[i + 1])
                result_side_list.append(result_start_array[i + 1])
                result_side_list.append(result_start_array[i])
                result_side_list.append(result_array[i + 1])

        # 最後の辺（ループ閉じ）
        last = len(result_start_array) - 1
        result_side_list.append(result_array[last])
        result_side_list.append(result_array[0])
        result_side_list.append(result_start_array[last])
        result_side_list.append(result_array[last])

        if not np.array_equal(result_start_array[0], result_start_array[last]):
            result_side_list.append(result_array[0])
            result_side_list.append(result_start_array[0])
            result_side_list.append(result_start_array[last])
            result_side_list.append(result_array[0])

        return result_side_list

    def attach_ids_normals_to_result_array(self, bottom_tin_array, result_side_list):
        """
        result_array に対して、4点ごとに ID、初期法線、multipatch_id を付与する。
        """
        result_array = np.concatenate([bottom_tin_array, np.array(result_side_list)])

        num_vertices = len(result_array)

        # 4点ごとの ID（例：0,0,0,0, 1,1,1,1, ...）
        id_array = np.array([np.repeat(i, 4) for i in range(num_vertices // 4)]).reshape(num_vertices, 1)

        # 初期法線ベクトル (0,0,0)
        normal_array = np.zeros((num_vertices, 3), dtype=np.float64)

        # multipatch_id: 全部 0
        multipatch_id_array = np.zeros((num_vertices, 1), dtype=np.float64)

        # 結合：[id, x, y, z, nx, ny, nz, multi_id]
        result_array = np.concatenate([id_array, result_array, normal_array, multipatch_id_array], axis=1)

        return result_array

    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            output_dwh_name, \
                input_bottom_polygon_coordinates_dwh_name, \
                input_tin_coordinates_dwh_name \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # FieldSetFileをフローファイルから取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # FieldSetFileをDataFrameに変換
            field_set_file_dataframe = pd.read_csv(io.StringIO(input_field_set_file))

            bottom_array = NSP.get_value_from_field_set_file_dataframe(
                field_set_file_dataframe, input_bottom_polygon_coordinates_dwh_name)

            tin_array = NSP.get_value_from_field_set_file_dataframe(
                field_set_file_dataframe, input_tin_coordinates_dwh_name)

            bottom_array = self.create_convex_polygon(bottom_array[:, 1:])


            #始点と方向ベクトル取得
            #長編側のみ出力するように変更
            start_point_list, d_vector_list = self.get_origin_and_d_vector(bottom_array)

            # tin成型
            # ID列を除く
            tin_array = tin_array[:,1:]

            # 3次元配列にする
            tin_array = tin_array.reshape(int(len(tin_array)/3),3,3)

            #-------------------------------------------------------
            #内包判定
            #-------------------------------------------------------
            #TINのインデックス取得　内包判定
            #複数座標、複数TINに対して内包判定→numba化 全件まわさないように内包された時点で処理を次の座標へ
            tin_index_array = np.array([get_tin_index(bottom_array[i,:2], tin_array[:,:2,:]) for i in range(len(bottom_array))], dtype=np.int64)
            #-------------------------------------------------------

            #-------------------------------------------------------
            #基底垂線
            #-------------------------------------------------------
            z_array = get_z_array_on_3points_plane(bottom_array, tin_array, tin_index_array)
            bottom_up_array = bottom_array.copy()
            bottom_up_array[:,2] = z_array
            #-------------------------------------------------------

            # 45度線の外周と、座標から紐づく始点を取得
            result_array, result_start_array = \
                self.get_top_surface(start_point_list, d_vector_list, tin_array)

            # 基底部だけでTIN作成
            bottom_tin_array = \
                self.generate_bottom_tin_from_result_start_array(result_start_array)

            # 側面ポリゴンの座標を構成
            result_side_list = self.generate_side_faces_from_result_arrays(result_array, result_start_array)

            result_array = \
            self.attach_ids_normals_to_result_array(bottom_tin_array, result_side_list)

            # 影響判定ポリゴンをフィールドセットファイルに加工
            output_field_set_file = (PBP.set_field_set_file)(
                [output_dwh_name], ["geometry"], [result_array])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
