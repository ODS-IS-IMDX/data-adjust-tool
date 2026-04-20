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
# ジオメトリを特定するキーに対応する座標のZ値を算出する。
# ---------------------------------------------------------------------------------------------------------
# Python標準ライブラリ
import io
import pickle
import base64
import traceback
from collections import defaultdict

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")
np = import_module("numpy")
gpd = import_module("geopandas")
LineString = import_module("shapely.geometry").LineString
jit = import_module("numba").jit
f8 = import_module("numba").f8
i8 = import_module("numba").i8


# 構成点間の距離を計算する 連続する同一構成点は考慮していない
# 引数1：xy座標 or xyz座標（[構成点1, 構成点2, …, 構成点n]、構成点n = [x, y] あるいは[x, y, z]）
# 戻り値：構成点間の距離一次元配列（[距離1, 距離2, … 距離n]、距離n = 構成点n-1から構成点nまでの距離）
@jit(f8[:](f8[:, :]), nopython=True, cache=True, nogil=True)
def get_distance(target_array):

    #次の構成点の配列（最後の要素は始点）
    #次点表現用のroll数
    roll_number = -target_array.shape[1]
    target_array_next = np.roll(target_array, roll_number)
    
    #xy間の距離を算出
    #算出ロジック：xy座標の差→差の2乗→和→平方根
    target_distance_array = np.sqrt(np.sum(np.power(target_array_next - target_array,2),axis = 1))

    #最後の要素は始点と終点の距離になっているため不要 最後の要素以外をスライスで抽出
    target_distance_array = target_distance_array[0:target_distance_array.size - 1]
    
    return target_distance_array



#指定したインデックスから始点、終点までの距離の和を計算する
#例：点Aから点Dの配列で点Bを指定する 戻り値は以下のようになる
#    [-(点B→点Aまでの距離),0(点B→点B),点B→点Cまでの距離,点B→点C→点Dまでの距離]
#    [-1 ,0 ,3 ,7]
#引数1：xy座標 or xyz座標（[構成点1, 構成点2, …, 構成点n]、構成点n = [x, y] あるいは[x, y, z]）
#戻り値：構成点間の距離一次元配列（[距離1, 距離2, … 距離n]、距離n = 構成点n-1から構成点nまでの距離）
@jit(f8[:](f8[:, :], i8), nopython=True, cache=True, nogil=True)
def get_distance_to_start_end_point(target_array, index):

    #戻り値初期配列 構成点の数分距離の和が発生する
    result_distance_to_start_end_point_array = np.zeros(len(target_array))

    #構成点間の距離取得
    target_distance_array = get_distance(target_array)

    #始点から指定インデックスまでの距離の総和→★こちらはすべて負
    distance_from_start_to_index_array = -np.array([np.sum(target_distance_array[i:index]) for i in range(0,index)])

    #戻り値用配列に更新
    result_distance_to_start_end_point_array[0:index] = distance_from_start_to_index_array

    #指定インデックスから終点までの距離の総和
    distance_from_index_to_end_array =  np.array([np.sum(target_distance_array[index:i]) for i in range(index,len(target_array))])
    
    #戻り値用配列に更新
    result_distance_to_start_end_point_array[index:] = distance_from_index_to_end_array
    
    return result_distance_to_start_end_point_array

#推定標高付与処理→デフォルトのまま使用すると始点終点
#引数1：推定標高対象地物の2次元配列 xyz座標
#引数2：傾きと切片を計算する2点のインデックス配列（要素数は2）
#引数3：推定標高対象構成点のインデックス配列
#戻り値：推定標高対象地物の2次元配列
@jit(f8[:,:](f8[:,:], i8[:], i8[:]), nopython=True, cache=True, nogil=True)
def get_estimation_Z(target_ndarray, gradient_indices, estimate_target_indices):


    #推定標高用に指定した2点間の勾配を計算
    distance = np.sum(get_distance(target_ndarray[gradient_indices[0]:gradient_indices[1]+1,:2]))
    gradient = (target_ndarray[gradient_indices[1],2:] - target_ndarray[gradient_indices[0],2:]) / distance

    #切片を引数2の1点目のz座標とする この点を基準点とする
    #基準点のインデックス
    standard_point_index = gradient_indices[0]
    
    #基準点のz座標を切片として取得
    slice = target_ndarray[standard_point_index][2]
    
    #推定標高処理対象構成点が基準点とどれだけ距離があるか算出する
    distance_array = get_distance_to_start_end_point(target_ndarray[:,:2], standard_point_index)
    
    #傾きをかけ切片を加えてZ値とする
    if len(estimate_target_indices) == 0:

        #推定標高対象を指定していない場合、中間点（始点終点以外）を更新する
        estimate_z_value_array = distance_array[1:-1] * gradient + slice

        #更新用に配列を変換
        estimate_z_value_array = estimate_z_value_array.reshape(len(estimate_z_value_array),1)

        #Zを更新
        target_ndarray[1:-1,2:] = estimate_z_value_array
    else:

        #推定標高対象を指定している場合、引数で指定された構成点に対して推定標高を行う
        estimate_z_value_array = distance_array[estimate_target_indices] * gradient + slice
        
        #更新用に配列を変換
        estimate_z_value_array = estimate_z_value_array.reshape(len(estimate_z_value_array),1)

        #Zを更新
        target_ndarray[estimate_target_indices,2:] = estimate_z_value_array

    return target_ndarray


#引数の1列目にあるID列を元に始点と終点のインデックスを取得する。
#引数:id+xyz座標の2次元numpy配列
#戻り値:始点インデックス、終点インデックスの1次元numpy配列
@jit('Tuple((i8[:], i8[:]))(f8[:])', nopython=True, cache=True, nogil=True)
def get_start_end_index_by_1d_array(target_array):

    # 値が1種類しかない場合は始点と終点のインデックスを返す(0と配列の長さ-1)
    if len(np.unique(target_array)) == 1:
        return np.array([0]) ,np.array([len(target_array)-1])

    #始点インデックス取得用配列（例：②[2, 0, 0, 0, 1, 1, 2, 2]）
    geom_id_roll_array1 = np.roll(target_array,1)

    #終点インデックス取得用配列（例：③[0, 0, 1, 1, 2, 2, 2, 0]）
    geom_id_roll_array2 = np.roll(target_array,-1)

    #ID列を比較し異なるインデックスリストを取得→始点のインデックス
    #具体例：①[0, 0, 0, 1, 1, 2, 2, 2]
    #　　　　②[2, 0, 0, 0, 1, 1, 2, 2]
    #　　　　→[0, 3, 5]が始点のインデックス
    start_index_array = np.where(np.logical_not((target_array == geom_id_roll_array1)))[0]

    #ID列を比較し異なるインデックスリストを取得→終点のインデックス
    #具体例：①[0, 0, 0, 1, 1, 2, 2, 2]
    #　　　　③[0, 0, 1, 1, 2, 2, 2, 0]
    #　　　　→[2, 4, 7]が終点のインデックス
    end_index_array = np.where(np.logical_not((target_array == geom_id_roll_array2)))[0]

    return start_index_array,\
           end_index_array



class CreateGeoDataFrameFromDepthAttributeText(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        土被りテキストを用いてGeoDataFrameを作成する
                      """
        tags = ["coordinates", "Python"]

    XY_FEATURE_DWH_NAME = PropertyDescriptor(
        name="XY Feature GeoDataFrame DWH Name",
        description="XY地物GeoDataFrameのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    XY_FEATURE_CRS = PropertyDescriptor(
        name="XY Feature Crs",
        description="XY地物のCRS",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    XY_FEATURE_ID = PropertyDescriptor(
        name="XY Feature ID",
        description="XY地物のIDカラム名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    DEPTH_TEXT_DWH_NAME = PropertyDescriptor(
        name="Depth Text DWH Name",
        description="土被りテキストCSVのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    CONSTANT_DEPTH_Z = PropertyDescriptor(
        name="Constant Depth Z",
        description="土被りが設定できないときに用いる固定値",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    ATTRIBUTE_TEXT_DWH_NAME = PropertyDescriptor(
        name="Attribute Text DWH Name",
        description="属性テキストCSVのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    XY_THRESHOLD = PropertyDescriptor(
        name="XY Threshold",
        description="同一構成点として判定する、XY平面上の構成点間の距離",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    RESULT_GEODATAFRAME_DWH_NAME = PropertyDescriptor(
        name="Result GeoDataFrame DWH Name",
        description="土被りテキスト適用後地物のGeoDataFrame",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [XY_FEATURE_DWH_NAME,
                            XY_FEATURE_CRS,
                            XY_FEATURE_ID,
                            DEPTH_TEXT_DWH_NAME,
                            CONSTANT_DEPTH_Z,
                            ATTRIBUTE_TEXT_DWH_NAME,
                            XY_THRESHOLD,
                            RESULT_GEODATAFRAME_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors


    # 内分点取得
    def __get_internal_division_point(self,
                                      point1,
                                      point2,
                                      end_distance):

        '''
            Parameters
            ----------
            coordinates_array : np.array float64 
                座標配列(2次元) [構成点数]>[x,y,z]
                XY地物、Z値は固定値-9999を想定している

            Returns
            -------
            result_coordinates_array :  np.array float64 
                座標配列(2次元) [構成点数]>[fid,x,y,z]
        '''
        # 2点間のxy距離を算出
        target_distance = np.sqrt(np.sum(np.power(point1[:2] - point2[:2],2)))
        
        # 内分点間から終点側への距離
        start_distance=target_distance-end_distance
        
        internal_division_point = (point1 * end_distance + point2 * start_distance) / (start_distance + end_distance)

        return internal_division_point


    def __get_nearest_point_index(self,
                                  distance_array,
                                  start_distance):

        # 始点距離と線分ごとの距離の差の絶対値
        distance_difference = distance_array - start_distance
        abs_distance_difference = np.abs(distance_difference)
        
        # 差が最も小さいインデックス取得
        segment_index = int(np.argmin(abs_distance_difference))

        return distance_difference,\
            abs_distance_difference,\
            segment_index
        

    # 閾値によってxy地物が作成されるとき
    def __get_xy_coordinates_threshold(self,
                                       coordinates_array,
                                       distance_array,
                                       start_distance):
        
        # 返却用座標配列
        result_coordinates_array=coordinates_array.copy()
        
        # 差が最も小さいインデックス取得
        _,\
        _,\
        segment_index\
            =self.__get_nearest_point_index(distance_array,
                                            start_distance)
        
        # 最も近い構成点からxy地物とする
        result_coordinates_array\
            =result_coordinates_array[segment_index+1:,:]
                
        return result_coordinates_array

    # 閾値xy地物が作成されるとき
    def __get_xy_coordinates_not_threshold(self,
                                           coordinates_array,
                                           distance_array,
                                           start_distance,
                                           distance_difference):

        # 返却用座標配列
        result_coordinates_array=coordinates_array.copy()

        # どの線分の位置にいるのかのインデックス
        # 始点距離と線分間距離の差が0より大きい地点で最初のインデックスを取得
        segment_index = int(np.argmax(distance_difference >= 0))

        # 内分点算出
        internal_division_point\
            =self.__get_internal_division_point(result_coordinates_array[segment_index],
                                                result_coordinates_array[segment_index+1],
                                                distance_array[segment_index]-start_distance)
        
        # xy座標の始点を内分点で更新
        result_coordinates_array\
            =result_coordinates_array[segment_index:,:]
        result_coordinates_array[0]=internal_division_point

        return result_coordinates_array

    def __get_xy_coordinates(self,
                             xy_threshold,
                             start_distance,
                             distance_array,
                             coordinates_array):

        xy_feature_coordinates_array=coordinates_array.copy()

        # 始点位置が閾値未満の場合、xy地物そのまま使用
        if start_distance<xy_threshold:
            pass
        
        # 始点位置がxy地物の始点以外の場合
        else:
            
            # 始点距離と構成点間の距離の関係取得
            distance_difference,\
            abs_distance_difference,\
            segment_index=\
                self.__get_nearest_point_index(distance_array,
                                            start_distance)
            
            # 閾値未満なら上書き それ以外なら算出して上書き
            if abs_distance_difference[segment_index]<xy_threshold:
                
                # 最も近い構成点からxy地物とする

                xy_feature_coordinates_array\
                    =self.__get_xy_coordinates_threshold(xy_feature_coordinates_array,
                                                         distance_array,
                                                         start_distance)
            
            # 該当線分を取り出して内分点のxy座標を算出
            # xy地物の構成点付近（距離が閾値以内）に土被りテキストの構成点がない
            else:
                xy_feature_coordinates_array\
                    =self.__get_xy_coordinates_not_threshold(xy_feature_coordinates_array,
                                                             distance_array,
                                                             start_distance,
                                                             distance_difference)
        return xy_feature_coordinates_array

    # 土被りテキストの視点からの距離より構成点を追加
    # どの構成点が追加されているかのbool配列も返却
    def __add_depth_coordinates(self,
                                xy_depth_coordinates_array,
                                distance_from_start_array,
                                depth_array,
                                xy_threshold,
                                default_z_value):

            
        for i in range(len(distance_from_start_array)):

            # 構成点間の視点からの距離取得
            distance_array=get_distance_to_start_end_point(xy_depth_coordinates_array[:,:2], 0)
            
            # 始点距離と構成点間の距離の関係取得
            # 最も近い構成点のインデックスを返す
            distance_difference,\
            abs_distance_difference,\
            segment_index\
                =self.__get_nearest_point_index(distance_array,
                                                distance_from_start_array[i])

            # 閾値以内ならZ座標に土被りに設定
            if abs_distance_difference[segment_index]<xy_threshold:
                xy_depth_coordinates_array[segment_index,2]=depth_array[i]
            else:
                
                # どの線分の位置にいるのかのインデックス再計算
                # 始点距離と線分間距離の差が0より大きい地点で最初のインデックスを取得
                segment_index = int(np.argmax(distance_difference >= 0))

                # 追加する構成点のxy座標算出
                internal_division_point\
                    =self.__get_internal_division_point(xy_depth_coordinates_array[segment_index-1],
                                                        xy_depth_coordinates_array[segment_index],
                                                        distance_array[segment_index]-distance_from_start_array[i])            
                
                # 土被りを設定
                internal_division_point[2]=depth_array[i]

                # 元のxy地物の座標配列に挿入
                xy_depth_coordinates_array=np.insert(xy_depth_coordinates_array,
                                                     segment_index,
                                                     internal_division_point,
                                                     axis=0)
        
        # Z値のデフォルト値以外が出てくるところまでをスライスして返す
        slice_index=np.max(np.where(np.logical_not(xy_depth_coordinates_array[:,2]==default_z_value))[0])
        
        return xy_depth_coordinates_array[:slice_index+1,:]

    # 指定した値の、開始と終了のインデックス取得
    # 注意：両端が指定の値でないことが前提
    def __get_serial_start_end_index(self,
                                     target_array,
                                     target_value):
        
        # 指定の値のbool配列
        target_bool=target_array==target_value
        
        # 比較用bool配列 1つずらして元の配列と比較→Trueが現れたインデックスを求めに行く
        start_bool_array=np.roll(target_bool,-1)
        end_bool_array=np.roll(target_bool,1)
        
        # 連続する部分をFalseにする
        # 2回以上連続する部分を抽出
        serial_bool1=target_bool&end_bool_array
        serial_bool2=start_bool_array&target_bool

        serial_bool=serial_bool1|serial_bool2

        start_bool_array[serial_bool]=False
        end_bool_array[serial_bool]=False

        # 指定の値が始まる手前のインデックスとどの後のインデックス取得
        # 推定標高の指定インデックスとして扱う
        start_index_array=np.where(start_bool_array)[0]
        end_index_array=np.where(end_bool_array)[0]
        
        return start_index_array,\
               end_index_array
        
    def get_depth_coordinates_array(self,
                                    coordinates_array,
                                    depth_text_array,
                                    xy_threshold,
                                    default_z_value):
        '''
            Parameters
            ----------
            coordinates_array : np.array float64 
                座標配列(2次元) [構成点数]>[x,y,z]
                XY地物、Z値は固定値-9999を想定している
            depth_text_array : np.array float64
                土被りテキスト(2次元)  各列の概要は以下の通り
                ・地物ID:新しい座標配列作成後fidとする値
                ・始点距離:XY地物IDのどの地点から地物がスタートするか 
                ・始点位置からの距離:始点距離で定めた地点から土被りを設定するまでの距離
                ・土被り:土被りの値 Z値に設定する
            Returns
            -------
            result_coordinates_array :  np.array float64 
                座標配列(2次元) [構成点数]>[fid,x,y,z]
        '''
        
        # -------------------------------------------------------------------
        # 使用するxy地物の取得
        # -------------------------------------------------------------------
        # XY地物の構成点間の距離取得
        distance_array=get_distance_to_start_end_point(coordinates_array, 0)[1:]
        
        # 始点距離から始点位置取得 地物IDごとの1行目の1つ目
        start_distance=depth_text_array[0,1]
        # 土被り追加対象xy地物
        xy_coordinates_array\
            =self.__get_xy_coordinates(xy_threshold,
                                start_distance,
                                distance_array,
                                coordinates_array.copy())
        # -------------------------------------------------------------------

        # -------------------------------------------------------------------
        # 土被りの値をZ値に設定
        # -------------------------------------------------------------------
        
        # 土被りの値を土被りテキストから取得
        distance_from_start_array=depth_text_array[:,2]
        depth_array=depth_text_array[:,3]
        
        # 土被り設定
        xy_coordinates_array\
            =self.__add_depth_coordinates(xy_coordinates_array,
                                          distance_from_start_array,
                                          depth_array,
                                          xy_threshold,
                                          default_z_value)
        # -------------------------------------------------------------------

        depth_value_bool=xy_coordinates_array[:,2]==default_z_value
        not_depth_value_bool=np.logical_not(depth_value_bool)
        
        not_depth_value_index=np.where(not_depth_value_bool)[0]
        
        xy_coordinates_array[ 0,2]=xy_coordinates_array[np.min(not_depth_value_index),2]
        xy_coordinates_array[-1,2]=xy_coordinates_array[np.max(not_depth_value_index),2]    
        
        start_index_array,\
        end_index_array\
            =self.__get_serial_start_end_index(xy_coordinates_array[:,2],
                                               default_z_value)

        for si, ei in zip(start_index_array, end_index_array):

            #始点と終点のインデックス 傾き算出用
            gradient_target_indices=np.array([si, ei], dtype=np.int64)
            
            #中間点のインデックス 推定標高対象用
            estimate_target_indices=np.arange(si+1,ei, dtype=np.int64)

            #始点から終点までの傾きで中間点のZ値を推定標高で均す
            xy_coordinates_array\
                = get_estimation_Z(xy_coordinates_array,
                                   gradient_target_indices,
                                   estimate_target_indices)
        
        return xy_coordinates_array

    # shapelyのオブジェクトを要素とするList→座標配列
    def get_geometries_array(self,shapely_list, geometry_type):
        
        max_count_range = range(len(shapely_list))

        result_shape_list=[]
        result_shape_counts_list=[]

        #★ジオメトリタイプがポリゴンの場合
        if 'Polygon' in geometry_type:

            #★ジオメトリごとにまとめたリストを取得する
            #★ただし空のジオメトリは対象としない
            for i in max_count_range:
                
                temp_coords=shapely_list[i].exterior.coords
                result_shape_counts_list.append(len(temp_coords))
                result_shape_list.append(temp_coords)
                shapely_list[i]=None

        #★ジオメトリタイプがポリゴン以外（ポイントorライン）の場合
        else:
            for i in max_count_range:
                temp_coords=shapely_list[i].coords
                result_shape_counts_list.append(len(temp_coords))
                result_shape_list.append(temp_coords)
                shapely_list[i]=None

        #★ジオメトリごとにidを列挿入する idは0からの連番
        result_shape_array=np.concatenate(result_shape_list)
        result_shape_list=None
        
        result_id_array=np.repeat(np.arange(len(shapely_list)), np.array(result_shape_counts_list)).reshape(-1,1)

        result_shape_array=np.concatenate([result_id_array,result_shape_array], axis=1)
        result_id_array=None

        return result_shape_array


    def get_geometries_points_numpy(self,
                                    target_dataframe,
                                    crs,
                                    encoding):


        
        target_geometries_list=target_dataframe['geometry'].copy()
        target_geometry_type=target_geometries_list[0].geom_type

        result_array=self.get_geometries_array(target_geometries_list,target_geometry_type)

        #戻り値を設定
        return target_dataframe,result_array


    def convert_dict_to_linestring_shapely(self,
                                           geometries_dictionary):
        geometries_list = []
        geometries_list = [LineString(geometries_dictionary[keys]) for keys in geometries_dictionary]
        return geometries_list

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # --------------------------------------------------------------------------
            xy_feature_dwh_name = context.getProperty(
                self.XY_FEATURE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            xy_feature_crs = context.getProperty(
                self.XY_FEATURE_CRS).evaluateAttributeExpressions(flowfile).getValue()

            xy_feature_id = context.getProperty(
                self.XY_FEATURE_ID).evaluateAttributeExpressions(flowfile).getValue()

            depth_text_dwh_name = context.getProperty(
                self.DEPTH_TEXT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            constant_depth_z = float(context.getProperty(
                self.CONSTANT_DEPTH_Z).evaluateAttributeExpressions(flowfile).getValue())

            attribute_text_dwh_name = context.getProperty(
                self.ATTRIBUTE_TEXT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            xy_threshold = float(context.getProperty(
                self.XY_THRESHOLD).evaluateAttributeExpressions(flowfile).getValue())

            result_geodataframe_dwh_name = context.getProperty(
                self.RESULT_GEODATAFRAME_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # flowfileから、CSV形式のFieldSetFileを取得
            # --------------------------------------------------------------------------
            input_field_set_file=flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            fieldset_dataframe=pd.read_csv(
                io.StringIO(input_field_set_file))

            xy_dataframe=pickle.loads(base64.b64decode(
                fieldset_dataframe.loc[fieldset_dataframe["Dwh"] == xy_feature_dwh_name, "Value"].values[0]))

            depth_text=pickle.loads(base64.b64decode(
                fieldset_dataframe.loc[fieldset_dataframe["Dwh"] == depth_text_dwh_name, "Value"].values[0]))

            attribute_text=pickle.loads(base64.b64decode(
                fieldset_dataframe.loc[fieldset_dataframe["Dwh"] == attribute_text_dwh_name, "Value"].values[0]))

            depth_text_dataframe=pd.read_csv(
                io.StringIO(depth_text))

            attribute_text_dataframe=pd.read_csv(
                io.StringIO(attribute_text))
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
             # GeoDataFrameから座標配列
            # --------------------------------------------------------------------------
            target_geometries_list=xy_dataframe['geometry'].copy()
            target_geometry_type=target_geometries_list[0].geom_type
            xy_feature_array=self.get_geometries_array(target_geometries_list,target_geometry_type)
            default_z_value=-9999.0
            
            # xy座標のみしか持たない場合固定値を設定
            if xy_feature_array.shape[1]==3:
                default_z_value_array=np.full((len(xy_feature_array), 1), default_z_value)
                xy_feature_array=np.concatenate([xy_feature_array,default_z_value_array], axis=1)
            # Z座標を持つ場合は固定値を設定
            else:
                xy_feature_array[:,3]=default_z_value

            # XY地物IDのカラム取得
            xy_id_column=xy_dataframe[xy_feature_id].to_numpy()
            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # 土被りテキストから値取得
            # --------------------------------------------------------------------------
            depth_text_array=depth_text_dataframe.to_numpy()
            depth_text_array[np.isnan(depth_text_array)]=constant_depth_z

            # --------------------------------------------------------------------------
            # 属性テキスト読み込み
            # --------------------------------------------------------------------------
            # 属性テキストの地物ID列名取得 1列目を地物IDとする
            attribute_text_id=attribute_text_dataframe.columns.to_list()[0]
            # --------------------------------------------------------------------------

            xy_feature_si,\
            xy_feature_ei\
                =get_start_end_index_by_1d_array(xy_feature_array[:,0])

            # XY地物スライスインデックス
            # idをkeyにして設定
            xy_feature_slice_index_dict={fid:np.array([si, ei]) for fid,si, ei in zip(xy_id_column,xy_feature_si, xy_feature_ei)}
            
            depth_xy_si,\
            depth_xy_ei\
                =get_start_end_index_by_1d_array(depth_text_array[:,0])

            # 土被りテキストスライスインデックス
            # idをkeyにして設定
            depth_slice_index_dict={fid:np.array([si, ei]) for fid,si, ei in zip(xy_id_column,depth_xy_si, depth_xy_ei)}
            
            # XY地物IDごとに座標と土被りテキストを取得し
            result_dict={}
            
            for xy_id in xy_id_column:
                
                # ----------------------------------------------------
                # XY地物スライス
                # ----------------------------------------------------
                xy_se_array=xy_feature_slice_index_dict.get(xy_id)
                
                # xyzのみ取り出す
                temp_xy_feature_array\
                    =xy_feature_array[xy_se_array[0]:xy_se_array[1]+1,1:4]
                
                # ----------------------------------------------------
                
                # ----------------------------------------------------
                # 土被りスライス
                # ----------------------------------------------------
                depth_se_array=depth_slice_index_dict.get(xy_id)
            
                
                # 使用する行だけ抽出
                # XY地物ID以外を抽出（２列目以降）
                temp_depth_text_array\
                    =depth_text_array[depth_se_array[0]:depth_se_array[1]+1,1:]
                # ----------------------------------------------------
            
                
                # ----------------------------------------------------
                # 土被り設定後出力
                # ----------------------------------------------------
                # 土被りテキストに設定されたfidごとに座標配列を作成
                # fidをkeyに辞書型座標配列作成
                depth_fid_si,\
                depth_fid_ei\
                =get_start_end_index_by_1d_array(temp_depth_text_array[:,0])
                
                
                for di in range(len(depth_fid_si)):
                    
                    xy_depth_coordinates_array\
                        =self.get_depth_coordinates_array(temp_xy_feature_array,
                                                    temp_depth_text_array[depth_fid_si[di]:depth_fid_ei[di]+1,:],
                                                    xy_threshold,
                                                    default_z_value)
                    result_dict[temp_depth_text_array[depth_fid_si[di],0]]=xy_depth_coordinates_array

            geometry_list=self.convert_dict_to_linestring_shapely(result_dict)
                
            # geodataframeにする 土被りテキストの地物IDを属性に持たせる
            id_dataframe=pd.DataFrame(data=list(result_dict.keys()),columns=[xy_feature_id])
            gdf = gpd.GeoDataFrame(id_dataframe,
                                   geometry=geometry_list,
                                   crs = xy_feature_crs)
            
            # 属性テキスト結合
            # 土被りテキストの地物IDをKeyに外部結合
            gdf\
                =pd.merge(gdf,
                        attribute_text_dataframe,
                        left_on=xy_feature_id, 
                        right_on=attribute_text_id,
                        how='left')

            output_field_set_file\
                = WM.calc_func_time(self.logger)\
                                   (PBP.set_field_set_file)\
                                   (result_geodataframe_dwh_name,
                                    DDC.CONTENTS_CODE_GEODATAFRAME,
                                    [gdf])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
