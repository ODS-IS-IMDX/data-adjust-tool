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

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

pd = import_module("pandas")
np = import_module("numpy")

class ConvertCoordinatesZByExternalFields(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ジオメトリを特定するキーに対応する座標のZ値を算出する。
                        ①input: 管路の座標配列、マンホールの座標配列、マンホールのキー、始点キー、終点キー、土被り値、座標間の距離を持った7行のFieldSetFile。
                        ②output: Z値が更新された座標配列を持った1行のFieldSetFile。
                      """
        tags = ["coordinates", "Python"]

    LINE_COORDINATES_DWH_NAME = PropertyDescriptor(
        name="Line Coordinates DWH Name",
        description="管路の座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    MH_COORDINATES_DWH_NAME = PropertyDescriptor(
        name="MH Coordinates DWH Name",
        description="マンホールの座標配列のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    MH_KEY_DWH_NAME = PropertyDescriptor(
        name="MH Key DWH Name",
        description="マンホールのKEYのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    EARTH_COVER_START_KEY_DWH_NAME = PropertyDescriptor(
        name="Start Key DWH Name",
        description="始点KEYのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    EARTH_COVER_END_KEY_DWH_NAME = PropertyDescriptor(
        name="End Key DWH Name",
        description="終点KEYのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    DEPTH_VALUE_DWH_NAME = PropertyDescriptor(
        name="Depth Value DWH Name",
        description="深さとして扱えるフィールドのDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    EARTH_COVER_POINT_DISTANCE_DWH_NAME = PropertyDescriptor(
        name="Depth Point Distance DWH Name",
        description="点距離のDWH名",
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False
    )

    property_descriptors = [LINE_COORDINATES_DWH_NAME,
                            MH_COORDINATES_DWH_NAME,
                            MH_KEY_DWH_NAME,
                            EARTH_COVER_START_KEY_DWH_NAME,
                            EARTH_COVER_END_KEY_DWH_NAME,
                            DEPTH_VALUE_DWH_NAME,
                            EARTH_COVER_POINT_DISTANCE_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    #ジオメトリの座標Dictionary（key：地物ID、value：ジオメトリの座標を格納したnumpy.ndarray）を取得
    def split_geometry_points_dict(self, target_geometries_points, all_key_list=None):
        result_dict = {}

        #地物ID、地物IDが最初に表れるインデックス番号、同一地物IDの個数（座標数）を取得
        feature_key_list, first_index_list, key_count_list = np.unique(target_geometries_points[:,0], return_index=True, return_counts=True)

        #地物IDの数と点数が一致する（＝ジオメトリがポイントである）場合、座標値を1次元配列とする
        if len(feature_key_list) == np.shape(target_geometries_points)[0]:
            result_dict = dict(zip(target_geometries_points[:,0], target_geometries_points[:,1:]))

        #上記以外（＝ジオメトリがポイントでない）場合、座標値を2次元配列とする
        else:
            #対象ジオメトリの座標Dictionaryの作成（内包表記用）
            max_loop_count = len(feature_key_list)
            result_dict = {feature_key_list[i] : target_geometries_points[first_index_list[i] : first_index_list[i] + key_count_list[i], 1: ] for i in range(max_loop_count) }

        if all_key_list != None:
            #キーリストには存在するが対象ジオメトリに存在しない場合、座標DictionaryにNoneを追加（内包表記）
            result_dict = {key : None for key in all_key_list if key not in result_dict}

        return result_dict

    def reverse_parse_data(self, coordinates_dict):
        """
        coordinates_dictをcoordinates_arrayに変換する関数

        引数:
            coordinates_dict: IDをキーとして、構成点のlineを値とする辞書

        戻り値:
            coordinates_array:（ID, x, y, z）の配列
        """
        coordinates_array = []
        for line_id, component_points_of_line in coordinates_dict.items():
            for point in component_points_of_line:
                x, y, z = point
                # IDと座標をデータlistに追加
                coordinates_array.append([line_id, x, y, z])
        return np.array(coordinates_array)

    def transform(self, context, flowfile):

        try:

            # --------------------------------------------------------------------------
            # 画面の入力値取得
            # --------------------------------------------------------------------------
            line_coordinates_dwh_name = context.getProperty(
                self.LINE_COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            mh_coordinates_dwh_name = context.getProperty(
                self.MH_COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            mh_key_dwh_name = context.getProperty(
                self.MH_KEY_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            earth_cover_start_key_dwh_name = context.getProperty(
                self.EARTH_COVER_START_KEY_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            earth_cover_end_key_dwh_name = context.getProperty(
                self.EARTH_COVER_END_KEY_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            earth_cover_value_dwh_name = context.getProperty(
                self.DEPTH_VALUE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            earth_cover_point_distance_dwh_name = context.getProperty(
                self.EARTH_COVER_POINT_DISTANCE_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # --------------------------------------------------------------------------

            # flowfileから、CSV形式のFieldSetFileを取得。
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            fieldset_data_frame = pd.read_csv(
                io.StringIO(input_field_set_file))

            line_coordinates_array = pickle.loads(base64.b64decode(
                fieldset_data_frame.loc[fieldset_data_frame["Dwh"] == line_coordinates_dwh_name, "Value"].values[0]))

            mh_coordinates_array = pickle.loads(base64.b64decode(
                fieldset_data_frame.loc[fieldset_data_frame["Dwh"] == mh_coordinates_dwh_name, "Value"].values[0]))

            mh_equipment_key_list = pickle.loads(base64.b64decode(
                fieldset_data_frame.loc[fieldset_data_frame["Dwh"] == mh_key_dwh_name, "Value"].values[0]))

            earth_cover_up_equipment_key_list = pickle.loads(base64.b64decode(
                fieldset_data_frame.loc[fieldset_data_frame["Dwh"] == earth_cover_start_key_dwh_name, "Value"].values[0]))

            earth_cover_down_equipment_key_list = pickle.loads(base64.b64decode(
                fieldset_data_frame.loc[fieldset_data_frame["Dwh"] == earth_cover_end_key_dwh_name, "Value"].values[0]))

            earth_cover_value_list = pickle.loads(base64.b64decode(
                fieldset_data_frame.loc[fieldset_data_frame["Dwh"] == earth_cover_value_dwh_name, "Value"].values[0]))

            earth_cover_point_distance_list = pickle.loads(base64.b64decode(
                fieldset_data_frame.loc[fieldset_data_frame["Dwh"] == earth_cover_point_distance_dwh_name, "Value"].values[0]))

            # 各リストをデータフレームに変換し、'value'列名をリスト名に変更
            df1 = pd.DataFrame(earth_cover_up_equipment_key_list, columns=["id", "上部設備KEY"])
            df2 = pd.DataFrame(earth_cover_down_equipment_key_list, columns=["id", "下部設備KEY"])
            df3 = pd.DataFrame(earth_cover_value_list, columns=["id", "土被り値"])
            df4 = pd.DataFrame(earth_cover_point_distance_list, columns=["id", "点距離"])

            # 'id'列を基準に結合して、各リストを新しいカラムにする
            earth_cover_data_frame = (
                df1.merge(df2, on="id", how="outer")
                .merge(df3, on="id", how="outer")
                .merge(df4, on="id", how="outer")
            )

            #点距離をnumpy配列で取得
            point_distance_array = earth_cover_data_frame["土被り値"].to_numpy()

            #土被りをnumpy配列で取得
            earth_convering_array = earth_cover_data_frame["点距離"].to_numpy()

            # マンホールから取得した設備KEYをデータフレームに変換
            mh_dataframe = pd.DataFrame(mh_equipment_key_list, columns=["id", "設備KEY"])

            # 設備キー名(引数として渡す)
            mh_key_name = '設備KEY'

            test_mh_array = mh_coordinates_array

            line_test_array = line_coordinates_array

            result_dict = self.split_geometry_points_dict(line_test_array)

            key2_1 = '上部設備KEY'

            key2_2 = '下部設備KEY'

            # MHの始点終点設備キーの組み合わせをユニークで取得→地物分割処理回数
            mh_unique_key_dataframe = earth_cover_data_frame[[key2_1,key2_2]].drop_duplicates()

            # 設備キーからMHを取得
            # KEYのインデックス
            mh_unique_key_numpy = mh_unique_key_dataframe.to_numpy()

            # 設備キー検索回数
            mh_unique_key_range = range(len(mh_unique_key_numpy))

            # 管路の始点終点インデックスを取得する ID列でuniqueのreturn_index→始点 これを-1してroll→終点
            # 管路のID列取得
            line_id_test_array = line_test_array[:,0:1]

            # 始点インデックス取得用配列
            line_id_roll_array1 = np.roll(line_id_test_array,1)

            # 終点インデックス取得用配列
            line_id_roll_array2 = np.roll(line_id_test_array,-1)

            # ID列を比較し異なるインデックスリストを取得→始点のインデックス
            start_index_list = list(np.where((line_id_test_array == line_id_roll_array1) == False)[0])

            # ID列を比較し異なるインデックスリストを取得→終点のインデックス
            end_index_list = list(np.where((line_id_test_array == line_id_roll_array2) == False)[0])

            # 始点の情報取得
            line_start_point_test_array = line_test_array[start_index_list]

            # 終点の情報取得
            line_end_point_test_array = line_test_array[end_index_list]

            # 始点のxy座標取得
            line_start_point_test_xy_array = line_start_point_test_array[:,1:3]

            # 終点の座標情報取得
            line_end_point_test_xy_array = line_end_point_test_array[:,1:3]

            # 始点のID取得
            line_start_point_test_id_array = line_start_point_test_array[:,0:1]

            # 終点のID取得
            line_end_point_test_id_array = line_end_point_test_array[:,0:1]

            # DataFrameの設備キー列から始点と終点のインデックスを特定
            for i in mh_unique_key_range:
                if i % 1000 == 0:
                    print(f'{i + 1}ジオメトリ処理中')
                # 検索用文字列作成 始点のキーあるいは終点のキーをもつdataframeを検索
                mh_key_query = "{key_name} == '{key1}' | {key_name} == '{key2}'".format(key_name = mh_key_name,key1 = mh_unique_key_numpy[i][0] ,key2 = mh_unique_key_numpy[i][1])

                # 管路検索用のmhインデックスをmhのdataframeから取得
                mh_index_list = mh_dataframe.query(mh_key_query).index.tolist()

                # 検索用文字列作成 始点のキーあるいは終点のキーをもつdataframeを検索
                mh_key_query2 = "{key_name2_1} == '{key2_1}' & {key_name2_2} == '{key2_2}'".format(key_name2_1 = key2_1,key_name2_2 = key2_2,key2_1 = mh_unique_key_numpy[i][0] ,key2_2 = mh_unique_key_numpy[i][1])

                # 点距離、土被り値取得用indexをExcelのdataframeから取得
                mh_index_list2 = earth_cover_data_frame.query(mh_key_query2).index.tolist()

                # 対象のジオメトリの点距離取得
                temp_point_distance_array = point_distance_array[mh_index_list2]


                # 対象のジオメトリの土被り取得
                temp_earth_convering_array = earth_convering_array[mh_index_list2]

                # 点距離にNone又は、不明があった場合、次の設備keyに
                if temp_point_distance_array is None or "不明" in temp_point_distance_array or any(np.isnan(float(x)) for x in temp_point_distance_array if x != ''):

                    continue

                else:
                    # 点距離に値はあるが、土被りが不明、None→土被りを設定しない
                    if temp_earth_convering_array is None or "不明" in temp_earth_convering_array or any(np.isnan(float(x)) for x in temp_earth_convering_array if x != ''):

                        continue

                # 計算を行うため、float64に変換
                temp_point_distance_array = temp_point_distance_array.astype(np.float64)
                temp_earth_convering_array = temp_earth_convering_array.astype(np.float64)

                # ちょうど2件ヒットしなければ次の設備キーへ
                if len(mh_index_list) == 2:
                    pass
                else:
                    continue

                # MHのxy座標取得
                test_mh_xy_array = test_mh_array[mh_index_list][:,1:3]

                # 始点と終点で同じIDが存在する場合それが該当の管路
                # 両端点2パターンでチェック
                # 「1つ目が始点と等しいかつ2つ目が終点と等しい」または「1つ目が終点と等しいかつ2つ目が始点と等しい」
                # 後者の場合はLineの構成点順序を逆にする
                s_s_index = np.where(np.all(line_start_point_test_xy_array == test_mh_xy_array[0],axis = 1))[0]
                e_e_index = np.where(np.all(line_end_point_test_xy_array == test_mh_xy_array[1],axis = 1))[0]

                # 同じIDのインデックス
                same_id_index = np.intersect1d(s_s_index,e_e_index)

                # 等しいID取得できるか
                if len(same_id_index) == 1:

                    temp_line_test_array = line_test_array[np.where((line_test_array[:,0:1]==same_id_index) == True)[0]]

                # 並びを反転させ始点を合わせる 始点側のMHが最初になるように並びを反転
                else:
                    s_e_index = np.where(np.all(line_start_point_test_xy_array == test_mh_xy_array[1],axis = 1))[0]
                    e_s_index = np.where(np.all(line_end_point_test_xy_array == test_mh_xy_array[0],axis = 1))[0]

                    same_id_index = np.intersect1d(s_e_index,e_s_index)

                    if len(same_id_index) == 1:
                        temp_line_test_array = line_test_array[np.where((line_test_array[:,0:1]==same_id_index) == True)[0]]

                        #逆順にする
                        temp_line_test_array = temp_line_test_array[::-1]
                    else:

                        continue

                # 管路の構成点間の距離
                sum_a = np.array([np.sqrt(np.sum(np.power(temp_line_test_array[i+1] - temp_line_test_array[i],2))) for i in range(len(temp_line_test_array)-1)])
                total_distance_a = np.sum(sum_a)

                # 管路の始点から各点までの総距離を計算
                sum_a1 = np.array([np.sum(sum_a[:i]) for i in range(len(sum_a)+1)])

                # 点距離の始点から各点までの総距離を計算
                # 点距離の全量取得(属性をそれぞれ足すだけ ただし点距離の最初の行は必ず0が入るのでインデックス1から計上すること)
                sum_b = np.array([np.sum(temp_point_distance_array[:i+1]) for i in range(len(temp_point_distance_array))])
                total_distance_b = np.sum(temp_point_distance_array)

                # 管路と点距離間の縮尺を決める 管路の始点から終点までの総距離 / 点間距離の始点から終点までの総距離
                magnification = total_distance_a/total_distance_b

                # 点距離をそれぞれ更新
                sum_b = sum_b * magnification
                total_distance_b = total_distance_b * magnification

                # この倍率でそれぞれの距離を計算
                # 点距離がどのジオメトリ間に存在するか判定
                result_index_list = []

                for i in range(1,len(sum_b)-1):
                    
                    for j in range(len(sum_a1)-1):
                        if (sum_a1[j] <= sum_b[i]) & (sum_a1[j+1] >= sum_b[i]):
                            result_index_list.append(j)
                            break
                        else:
                            continue
                result_index_array = np.array(result_index_list)

                # 点距離の座標list
                point_distance_dict = defaultdict(list)

                # 内分点を利用して座標値を計算
                for i in range(len(result_index_array)):

                    # 両端点の座標
                    line_1 = temp_line_test_array[result_index_array[i]]
                    line_2 = temp_line_test_array[result_index_array[i] + 1]

                    # 内分点となる点距離の座標
                    temp_point_distance_array = point_distance_array[i]

                    # 管路と点距離の比
                    m = sum_b[i+1] - sum_a1[result_index_array[i]]
                    n = sum_a1[result_index_array[i]+1] - sum_b[i+1]

                    # 座標算出
                    x = (n * line_1[1] + m * line_2[1]) / (m + n)
                    y = (n * line_1[2] + m * line_2[2]) / (m + n)

                    # 構成点の土被り取得
                    z = temp_earth_convering_array[i+1]
                    result_array = np.array([line_1[0],x,y,z])

                    point_distance_dict[result_index_array[i]].append(result_array)

                point_distance_dict = dict(point_distance_dict)

                result_line_geometry_list = []

                # 管路と点距離の座標
                # 点距離座標判定配列
                point_distance_full_list = []

                for i in range(len(temp_line_test_array)):

                    result_line_geometry_list.append(temp_line_test_array[i])
                    point_distance_full_list.append(0)

                    if i in point_distance_dict:
                        [result_line_geometry_list.append(j) for j in point_distance_dict[i]]
                        [point_distance_full_list.append(1) for j in range(len(point_distance_dict[i]))]

                    else:
                        pass
                result_line_geometry_array = np.array(result_line_geometry_list)

                # 最初と最後の構成点のZ値を更新
                result_line_geometry_array[0,3:] = temp_earth_convering_array[0]
                result_line_geometry_array[len(result_line_geometry_array)-1,3:] = temp_earth_convering_array[len(temp_earth_convering_array)-1]

                if np.any(np.isnan(result_line_geometry_array)):
                    self.logger.error(result_line_geometry_array)
                    return FlowFileTransformResult(relationship="failure")

                # IDを除いた、xyzの配列にする
                result_line_geometry_xy_array = result_line_geometry_array[:,1:]

                #uniqueカウントでどの区間に何回点間距離が存在するか取得
                result_index_unique_array = np.unique(result_index_array,return_counts = True)

                # 構成点と点間距離を合わせたときに点間距離がどこに存在するか取得
                # 点距離存在インデックスから分割インデックス取得
                before_point_counts = 0
                split_geometry_index_list = []

                for i in range(len(point_distance_full_list)):
                    if point_distance_full_list[i] == 1:
                        temp_arange = np.arange(before_point_counts,i+1)
                        split_geometry_index_list.append(temp_arange)
                        before_point_counts = i
                    else:
                        pass

                # 最後の分割インデックス
                if point_distance_full_list[i] == 0:
                    temp_arange = np.arange(before_point_counts,i+1)
                    split_geometry_index_list.append(temp_arange)

                # 分割インデックスごとに構成点を分けてそれぞれ出力する
                split_geometry_array = [result_line_geometry_xy_array[si] for si in split_geometry_index_list]
                dataframe_index = result_line_geometry_array[0][0]

                # 座標配列で出荷するのであればline_dictを更新
                result_dict[dataframe_index] = result_line_geometry_xy_array

            output_coordinates_array = self.reverse_parse_data(result_dict)

            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(line_coordinates_dwh_name,
                                                                         "geometry",
                                                                         [output_coordinates_array])

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
