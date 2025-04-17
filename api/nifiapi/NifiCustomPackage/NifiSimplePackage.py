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
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import time
import xml.etree.ElementTree as ET
import pickle
import base64
import io
import json
import unicodedata
import pathlib

import importlib

# 外部ライブラリの動的インポート
from importlib import import_module

np = import_module("numpy")
pd = import_module("pandas")
gpd = import_module("geopandas")
pgl = import_module("pygltflib")
Point = import_module("shapely").geometry.Point
LineString = import_module("shapely").geometry.LineString
Polygon = import_module("shapely").geometry.Polygon


def get_geometries_points_numpy(target_geometries_list,
                                remove_duplicated_point=True):
    """
    概要:
        ジオメトリのリストを取得し、必要に応じて重複した座標を削除した上で、ジオメトリごとの構成点リストをNumPy配列として返す
        ジオメトリタイプがポリゴン、マルチポリゴン、ポイント、またはラインに対応

    引数:
        target_geometries_list: 対象となるジオメトリのリスト
        remove_duplicated_point: 重複する座標を削除するかどうかのフラグ（デフォルトはTrue）

    戻り値:
        NumPy配列: ジオメトリごとの座標を格納したNumPy配列（id付き）
    """

    # 初期化
    result_shape_list = []
    result_geohashes_list = []
    max_count = len(target_geometries_list)

    # ジオメトリタイプを一つ目のジオメトリから判定
    # すべての地物のジオメトリタイプはすべて同じとする。
    target_geometry_type = target_geometries_list[0].geom_type

    # list変換回数
    max_count_range = range(max_count)

    # ジオメトリタイプによる引数の分岐
    # ジオメトリタイプがポリゴンの場合
    if 'Polygon' in target_geometry_type:

        # Multiジオメトリの場合
        if 'Multi' in target_geometry_type:

            # 最初の要素から外観の座標値を取得する
            # ジオメトリごとにまとめたリストを取得する
            # ただし空のジオメトリは対象としない
            result_shape_list = [get_geometry_points_list(
                target_geometries_list[i].exterior.coords[0]) for i in max_count_range if target_geometries_list[i].is_empty == False]

        # Multiでない場合
        else:

            # ジオメトリごとにまとめたリストを取得する
            # ただし空のジオメトリは対象としない
            result_shape_list = [get_geometry_points_list(
                target_geometries_list[i].exterior.coords) for i in max_count_range if target_geometries_list[i].is_empty == False]

    # ジオメトリタイプがポリゴン以外（ポイントorライン）の場合
    else:

        # Multiジオメトリの場合
        if 'Multi' in target_geometry_type:

            # 最初の要素から座標値を取得する
            # ジオメトリごとにまとめたリストを取得する
            # ただし空のジオメトリは対象としない
            result_shape_list = [get_geometry_points_list(
                target_geometries_list[i].geoms[0].coords) for i in max_count_range if target_geometries_list[i].is_empty == False]

        else:
            # ジオメトリごとにまとめたリストを取得する
            # ただし空のジオメトリは対象としない
            result_shape_list = [get_geometry_points_list(
                target_geometries_list[i].coords) for i in max_count_range if target_geometries_list[i].is_empty == False]

    # ジオメトリごとにidを列挿入する idは0からの連番
    result_id_shapes_list = [np.insert(np.array(
        result_shape_list[i], dtype=np.float64), 0, i, axis=1).copy().tolist() for i in max_count_range]

    # id付ジオメトリnumpy配列格納list
    result_list = []

    # ジオメトリの構成点を一つずつ配列に格納する
    for result_id_shape_list in result_id_shapes_list:

        # ジオメトリごとの座標リスト
        list = [result_id_shape_list[l]
                for l in range(len(result_id_shape_list))]

        # リストに追加
        result_list.extend(list)

    et = time.perf_counter()
    # 戻り値を設定
    return np.array(result_list, dtype=np.float64).copy()


def get_geometry_points_list(target_shape,
                             remove_duplicated_next_point=True):
    """
    概要:
        ジオメトリから取得した構成点座標リスト内で必要に応じて連続した重複点を削除する

    引数:
        target_shape: ジオメトリから取得した構成点座標リスト
        remove_duplicated_next_point: 次点が重複する座標を削除するかどうかのフラグ（デフォルトはTrue）

    戻り値:
        result_points_list: 重複点が削除された座標リスト（ジオメトリがNoneの場合はNone）
    """

    st = time.perf_counter()
    # 戻り値の初期化
    result_points_list = []
    result_geohashes_list = []

    if target_shape != None:

        result_points_list = list(target_shape)
        if result_points_list == None:
            result_geohashes_list = None

        else:
            if remove_duplicated_next_point and len(result_points_list) > 1:
                # 次点重複除去がON、かつ、構成点数が１より大きい場合、以下の処理を実行する。

                # 連続構成点情報を取得
                temp_points_numpy = get_next_points_numpy(
                    np.array(result_points_list, dtype=np.float64))

                temp_index = None
                # 連続重複構成点となる現在点（次点で重複）のインデックス番号を取得
                if len(result_points_list[0]) == 3:
                    # Z値がある場合
                    temp_index = np.where((temp_points_numpy[:, 0] == temp_points_numpy[:, 3]) & (
                        temp_points_numpy[:, 1] == temp_points_numpy[:, 4]) & (temp_points_numpy[:, 2] == temp_points_numpy[:, 5]))

                else:
                    # Z値がない場合
                    temp_index = np.where((temp_points_numpy[:, 0] == temp_points_numpy[:, 2]) & (
                        temp_points_numpy[:, 1] == temp_points_numpy[:, 3]))

                if temp_index != None:
                    # 連続重複構成点がある場合、以下の処理を実行

                    # 連続重複構成点の検知点（現在点）から１点ずらす（numpy.ndarrayを経由することで全要素に加算）
                    remove_duplicated_index_numpy = np.array(temp_index) + 1

                    # 連続構成点を削除する（削除対象のインデックスを除いてスライスした結果を取得）
                    result_points_list = np.delete(np.array(
                        result_points_list), remove_duplicated_index_numpy.tolist(), 0)

                else:
                    # 連続重複構成点がない場合、何もしない
                    pass

    else:
        # ジオメトリがNoneの場合、戻り値にNoneを設定
        result_points_list = None

    et = time.perf_counter()
    return result_points_list


def get_next_points_numpy(target_points_ndarray,
                          continued_points_count=2,
                          reverse_direction=False):
    """
    概要:
        座標配列に対して、次点または前点の連続構成点情報を取得する
        現状、連続した重複点を削除に使用 get_geometry_points_list

    引数:
        target_points_ndarray: 座標配列
        continued_points_count: 取得する連続する点の数（デフォルトは2）
        reverse_direction: 連続構成点の方向（Trueなら逆順、Falseなら順方向）

    戻り値:
        result_ndarray: 連続する構成点の情報をまとめたarray
    """

    # 配列を初期化
    loop_range = []

    # 戻り値の配列を初期化
    result_list = []
    loop_step = 0

    # Inputの構成点数を取得する。
    target_points_count = target_points_ndarray.shape[0]

    # 連続構成点の方向設定
    if reverse_direction:
        # 連続する前点（Ｎ→０）
        loop_step = -1
        loop_range = range(continued_points_count - 1, -1, loop_step)
    else:
        # 連続する次点（０→Ｎ）
        loop_step = 1
        loop_range = range(0, continued_points_count, 1)

    # 連続構成点の要素を積み上げた配列を作成
    # 　・スライス範囲の設定
    # 　・指定範囲をスライス後、リスト型に追加。
    result_list = [target_points_ndarray[sliced_start_pos: target_points_count -
                                         (continued_points_count - sliced_start_pos - 1)] for sliced_start_pos in loop_range]

    # リスト→タプル→ndarray（np.hstackによりリストの各要素を列末尾へ追加）したものを戻り値に設定
    result_ndarray = np.hstack(tuple(result_list))
    return result_ndarray


def get_data_definition_index(data_definition_path,
                              data_definition_delimiter=DDC.DELIMITER_COMMA,
                              data_definition_encoding='shift-jis'):
    """
    概要:
        製品データ定義ファイルをCSVまたはTSV形式で読み込み、ジオメトリ（地理情報）と属性に関連するデータを抽出

    引数:
        data_definition_path: 製品データ定義ファイルパス(.csv or .tsv) 文字列
        data_definition_delimiter: 製品データ定義ファイルパスの区切り文字種別 1:タブ[\t](.tsv) それ以外:[,]カンマ(.csv) デフォルト値を0設定して何も指定がなければCSVとして読み込むものとする
        data_definition_encoding: 製品データ定義ファイルのencoding 日本語を含むので基本的に'shift-jis' デフォルト値に設定しておく 文字列

    戻り値:
        geometry_type_list: ジオメトリのファイルタイプリスト
        geometry_file_name_list: ジオメトリのDWHファイル名リスト
        geometry_distribution_name_list: ジオメトリの流通項目名リスト
        dwh_file_name_list: 属性項目のDWHファイル名リスト
        attribute_name_list: 属性項目の流通項目名リスト
        attribute_const_value_list: 属性項目の属性値リスト
        attribute_file_type_list: 属性項目のファイルタイプリスト
        all_attribute_name_list: すべての属性の流通項目名リスト
        all_dwh_file_name_list: すべてのDWHファイル名リスト
    """

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

    # 出力時ジオメトリDWHファイル名List
    geometry_file_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME][geometry_name_index])

    # 出力時ジオメトリ流通項目名List
    geometry_distribution_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME][geometry_name_index])

    # 出力時属性項目ファイル名LIst
    dwh_file_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME][attribute_name_index])

    # 入力ファイル元の属性名(流通項目名)List取得
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


def write_field_file(geodataframe,
                     field_name_list,
                     feature_id_column_name='FID'):
    """
    概要:
        ジオデータフレーム内の指定された列名の値を取得し、シリアライズエンコードされた形式で返す

    引数:
        geodataframe: 処理対象のジオデータフレーム
        field_name_list: フィールド名のリスト
        feature_id_column_name: FIDの列名（デフォルトは'FID'）

    戻り値:
        value_list: 各列の値がシリアライズエンコードされたデータのリスト
        type_list: 各列のデータ型のリスト
    """

    value_list = []
    type_list = []

    for i in range(len(field_name_list)):

        # ジオメトリの場合
        if field_name_list[i] == 'geometry':
            field_list = list(geodataframe[field_name_list[i]])

        # FIDの場合
        elif feature_id_column_name in field_name_list[i]:
            field_list = list(
                zip(geodataframe[field_name_list[i]], geodataframe['Findex']))

        # 属性値の場合
        else:
            field_list = list(
                zip(geodataframe['Findex'], geodataframe[field_name_list[i]]))

        serialize_field_list = pickle.dumps(field_list)

        base64_serialize_field_list = base64.b64encode(
            serialize_field_list).decode('utf-8')

        value_list.append(base64_serialize_field_list)

        type_list.append(geodataframe.dtypes[field_name_list[i]])

    return value_list, type_list


def write_feature_file(geodataframe,
                       feature_id_column_name='',
                       fid_property_name='features',
                       fid_key_name='id'):
    """
    概要:
        地物IDとFindexのペアリストをシリアライズエンコードしたものを返す
        feature_id_column_nameが指定されている場合は対応列とFindexのペア
        feature_id_column_nameが指定されていない場合は__geo_interface__から地物IDを取得

    引数:
        geodataframe: 処理対象のジオデータフレーム
        feature_id_column_name: 地物IDを格納しているカラム名。指定しない場合、GeoDataFrameの
                                    __geo_interface__ から取得。デフォルトは空文字列 ('')
        fid_property_name: `geodataframe.__geo_interface__` 内のプロパティ名。デフォルトは 'features'
        fid_key_name: 地物IDを指し示すキー名。デフォルトは 'id'

    戻り値:
        base64_geometry_items_list: `fid_list` と `Findex` のペアをシリアライズエンコードしたデータを格納したリスト
        int: 地物IDの数
    """

    # 地物IDがない場合はgeopandasの__geo_interface__から取得
    if feature_id_column_name == '':
        features = geodataframe.__geo_interface__[fid_property_name]

        fid_list = [features[i][fid_key_name] for i in range(len(features))]

    # 地物IDがある場合は対象カラムから取得
    else:
        fid_list = geodataframe[feature_id_column_name].astype(object)

    # [地物ID,Findex]のリストを取得
    geometry_items_list = list(zip(fid_list, geodataframe['Findex']))

    serialize_geometry_items_list = pickle.dumps(geometry_items_list)

    base64_geometry_items_list = base64.b64encode(
        serialize_geometry_items_list).decode('utf-8')

    return base64_geometry_items_list, len(fid_list)


def get_field_set_file(field_type_list,
                       field_value_list,
                       dwh_list):
    """
    概要:
        与えられたDWHリスト、フィールドタイプリスト、値リストを基にCSV形式のFieldSetFile（Dwh、Type、Value）を作成

    引数:
        field_type_list: フィールドタイプリスト
        field_value_list: フィールド値リスト
        dwh_list: DWH名リスト

    戻り値:
        field_set_file: 作成されたCSV形式のFieldSetFile
    """

    field_set_file_columns = ['Dwh', 'Type', 'Value']

    field_set_file_dataframe = pd.DataFrame(
        columns=field_set_file_columns, dtype=str)

    field_set_file_dataframe['Type'] = field_type_list
    field_set_file_dataframe['Dwh'] = dwh_list
    field_set_file_dataframe['Value'] = field_value_list

    field_set_file = field_set_file_dataframe.to_csv(
        index=False, encoding='utf-8')

    return field_set_file


def plus_coordinate_array(coordinate_array, offset_value, axis):
    """
    概要:
        指定された軸にオフセット値を座標配列に一律で加算

    引数:
        coordinate_array: 座標配列 2次元配列[ジオメトリの構成点]>[id + xyz座標]
        offset_value: 加算するオフセット値（float）
        axis: 加算対象軸 1:X軸, 2:Y軸, 3:Z軸（fint）

    戻り値:
        coordinate_array: オフセット値が加算された座標配列
    """

    if offset_value == 0:
        return coordinate_array

    else:
        coordinate_array[:, axis] = coordinate_array[:, axis] + offset_value

    return coordinate_array


def get_target_array(unique_id, attribute_array):
    """
    概要:
        ジオメトリIDと対応する属性の配列を与えられたIDに基づいて並び替え、必要な部分だけを抽出

    引数:
        unique_id: ジオメトリIDの1次元配列
        attribute_array: 属性の2次元配列（最初の列がジオメトリIDに対応）

    戻り値:
        unique_id: 抽出されたジオメトリIDの1次元配列
        attribute_array: 抽出された並び替え後の属性の2次元配列
    """

    # 属性のid列取得
    attribute_id = attribute_array[:, 0]

    # ジオメトリのidを正としたときに属性を並び替えるためのインデックス -1は属性がくっつかなかったデータ
    attribute_index_array = np.array([np.where(attribute_id == unique_id[i])[0][0] if np.sum(
        attribute_id == unique_id[i]) == 1 else -1 for i in range(len(unique_id))])

    # 出力対象マスク これを属性の配列に突っ込んで幅、深さ取得
    output_target_mask = attribute_index_array[attribute_index_array > -1]

    # 出力できるジオメトリのid取得
    unique_id = unique_id[attribute_index_array > -1]

    # エラー対象マスク→スキップ or エラーの対象
    error_target_mask = attribute_index_array[attribute_index_array == -1]

    # 抽出&並び替え
    attribute_array = attribute_array[attribute_index_array]

    return unique_id, attribute_array


def create_attribute_dataframe(field_set_data_frame,
                               dwh_file_name_list,
                               attribute_name_list,
                               attribute_const_value_list,
                               attribute_file_type_list,
                               geometry_number,
                               encoding='UTF-8',
                               input_file_type=0,
                               feature_id_column_name='地物ID'):
    """
    概要:
        データ定義ファイルから属性のDataFrameを作成する

    引数:
        field_set_data_frame: FieldSetFileから作成されたDataFranme
        dwh_file_name_list: 属性項目のDWHファイル名リスト field_set_data_frame から対応する属性を読み込む
        attribute_name_list: 属性項目の流通項目名リスト
        attribute_const_value_list: 属性項目の属性値リスト。固定値の属性がある場合に使用
        attribute_file_type_list: 属性項目のファイルタイプリスト
        geometry_number: ジオメトリの数
        encoding: エンコーディング。デフォルトは 'UTF-8'
        input_file_type: 属性の入力タイプを指定するフラグ。デフォルトは '0'
        feature_id_column_name: 地物IDを保持するカラム名。デフォルトは '地物ID'

    戻り値:
        all_attribute_dataframe: データ定義ファイルをもとに作成した属性のDataFrame
    """

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
        if i == 0:
            attribute_feature_id_array = temp_attribute_dataframe[feature_id_column_name].to_numpy(
            ).flatten()
        else:
            pass

        all_attribute_list.append(
            temp_attribute_dataframe[attribute_name_list[i]])

    # 属性のdataframe作成
    all_attribute_dataframe = pd.concat(
        all_attribute_list, axis=1).fillna('None')

    # 文字列からfloat64へ
    attribute_feature_id_array = attribute_feature_id_array.astype(np.float64)

    return all_attribute_dataframe


def create_attribute_dataframe_datatype(field_set_dataframe,
                                        attribute_dwh_file_name_list,
                                        attribute_distribution_name_list,
                                        attribute_const_value_list,
                                        attribute_file_type_list,
                                        attribute_data_type_list,
                                        geometry_number,
                                        encoding='UTF-8',
                                        input_file_type=1,
                                        feature_id_column_name='地物ID'):
    """
    概要:
        create_attribute_dataframeと同様にデータ定義ファイルから属性のDataFrameを作成するが、
        create_attribute_dataframe_datatypeは各属性に対して指定されたデータ型を適用する。

    引数:
        field_set_dataframe: FieldSetFileから作成されたDataFranme
        attribute_dwh_file_name_list: 属性のDWHファイル名リスト field_set_data_frame から対応する属性を読み込む
        attribute_distribution_name_list: 属性の名前リスト。最終的なDataFrameのカラム名として使用される。
        attribute_const_value_list: 属性の属性値リスト。固定値の属性がある場合に使用
        attribute_file_type_list: 属性のファイルタイプリスト
        attribute_data_type_list: 属性のデータ型リスト
        geometry_number: ジオメトリの数
        encoding: エンコーディング。デフォルトは 'UTF-8'
        input_file_type: 属性の入力タイプを指定するフラグ。デフォルトは '1'
        feature_id_column_name: 地物IDを保持するカラム名。デフォルトは '地物ID'

    戻り値:
        attribute_feature_id_array: 属性に対応する地物IDの配列
        all_attribute_dataframe: データ定義ファイルをもとに作成した属性のDataFrame
    """

    # dataframe格納用List
    all_attribute_list = []

    for i in range(len(attribute_dwh_file_name_list)):

        # ファイルタイプが固定値の場合
        if attribute_file_type_list[i] == '-2':

            # 列だけ定義して最後に値を固定で設定する
            # インデックスによって行数が変わる可能性があるため
            temp_dataframe = pd.DataFrame(
                columns=[attribute_distribution_name_list[i]])

        else:
            # listのdumpを取得する場合
            if input_file_type == 1:

                # field_set_fileから特定のDwh名のある行のValueの値を読み込み、デコード、デシリアライズをし、listを取得
                temp_attribute_list = pickle.loads(base64.b64decode(
                    field_set_dataframe.loc[field_set_dataframe['Dwh'] == attribute_dwh_file_name_list[i], 'Value'].values[0]))

                # 一度objectで読み込み
                temp_attribute_array = np.array(
                    temp_attribute_list, dtype=object)

                # 型を指定してdataframeに設定
                temp_dataframe = pd.DataFrame(temp_attribute_array[:, 1].astype(attribute_data_type_list[i]), columns=[attribute_distribution_name_list[i]], index=temp_attribute_array[:, 0].astype(np.int64)
                                              )

            # それ以外の場合（CSV）
            else:

                temp_dataframe = pd.read_csv(
                    field_set_dataframe + '\\' + attribute_dwh_file_name_list[i] + '.csv', encoding=encoding, dtype=str)

        # 作成したdataframeを格納
        # インデックスを指定したdataframeかカラム名のみ持ったdataframe
        all_attribute_list.append(temp_dataframe.copy())

    # 属性のdataframe作成
    all_attribute_dataframe = pd.concat(all_attribute_list, axis=1)

    # 属性の数
    attribute_number = geometry_number

    # 固定値追加
    for i in range(len(attribute_file_type_list)):

        # 固定値の場合値設定それ以外の場合は何もしない
        if attribute_file_type_list[i] == '-2':
            all_attribute_dataframe[attribute_distribution_name_list[i]] = np.full(attribute_number, attribute_const_value_list[i], dtype=attribute_data_type_list[i]
                                                                                   )

        else:
            pass

    # 文字列からfloat64へ
    attribute_feature_id_array = all_attribute_dataframe.index.to_numpy(
        np.float64)

    return attribute_feature_id_array, all_attribute_dataframe


def get_uri_value(array):
    """
    概要:
        numpy.arrayをuriに設定する文字列に変換

    引数:
        array: numpyの配列

    戻り値:
        result_string: Base64エンコードし、UTF-8形式でデコードした文字列
    """

    result_string = base64.b64encode(array).decode('utf8')

    return result_string


def create_ext_structural_metadata_schema_dict(column_name_list, datatype_list, metadataclass_name='Metadataclass'):
    """
    概要:
        指定されたカラム名とデータ型に基づいて、EXT_structural_metadata `properties` と `MetadataClass` を含むスキーマを作成

    引数:
        column_name_list: カラム名のリスト
        datatype_list: 各カラムのデータ型のリスト
        metadataclass_name: メタデータクラス名。デフォルトは 'Metadataclass'

    戻り値:
        classes_dict: EXT_structural_metadata スキーマを表す辞書型データ
    """

    # propertiesの作成
    properties_value_dict = {}

    # "properties"をキーとする辞書型配列
    properties_dict = {}

    # "MetadataClass"をキーとする辞書型配列
    metadata_class_dict = {}

    # classesの辞書型配列
    classes_dict = {}

    # 属性情報設定
    for i in range(len(column_name_list)):

        # →データ流通基盤の固定値として管理する
        if datatype_list[i] == object:

            # 文字列の場合はcomponenttypeなし
            properties_value_dict[column_name_list[i]] = {'type': 'STRING'}

        else:

            # 製品データ定義からデータ型を取得
            properties_value_dict[column_name_list[i]] = {
                'type': 'STRING', 'componentType': 'FLOAT32'}

    properties_dict["properties"] = properties_value_dict

    # Metadataclassの作成
    metadata_class_dict[metadataclass_name] = properties_dict

    # propertyTablesのdict作成
    classes_dict["classes"] = metadata_class_dict

    return classes_dict


def create_ext_structural_metadata_properties_list(column_name_list, datatype_list, geometry_number, metadataclass_name='Metadataclass'):
    """
    概要:
        指定されたカラム名リストとデータ型リストに基づいて、EXT_structural_metadata スキーマの一部として使用するプロパティテーブルを作成する

    引数:
        column_name_list: カラム名のリスト
        datatype_list: 各カラムのデータ型のリスト
        geometry_number: ジオメトリの数
        metadataclass_name: メタデータクラス名。デフォルトは 'Metadataclass'

    戻り値:
        properties_table_list: 作成されたプロパティテーブルのリスト
    """

    # propertyTablesのlist
    properties_table_list = []

    # propertyTablesのlist内のdict
    properties_table_dict = {}

    column_name_dict = {}

    # classとcountも追加

    properties_table_dict["class"] = metadataclass_name

    properties_table_dict["count"] = geometry_number

    # properties作成
    for i in range(len(column_name_list)):

        column_name_dict[column_name_list[i]] = {
            'values': i*2+4, 'stringOffsets': i*2+5}

    properties_table_dict["properties"] = column_name_dict

    properties_table_list.append(properties_table_dict)

    return properties_table_list


def get_dataframe_information(attribute_dataframe):
    """
    概要:
        与えられた属性のDataFrameから、カラム名、データ型、属性値をそれぞれリスト形式で返却する
        入荷ツールで似たようなことしている

    引数:
        attribute_dataframe: 属性dataframe

    戻り値:
        column_name_list: DataFrameのカラム名のリスト
        datatype_list: データ型のリスト。各要素はDataFrameのdtype
        data_list: 属性値リスト。各要素はカラムごとの属性値リスト
    """

    # カラム名取得
    column_name_list = attribute_dataframe.columns.values

    # カラム名の数だけループしてデータ型取得
    datatype_list = [attribute_dataframe[column_name_list[i]
                                         ].dtype for i in range(len(column_name_list))]

    # カラム名の数だけループして値取得
    data_list = [list(attribute_dataframe[column_name_list[i]])
                 for i in range(len(column_name_list))]

    return column_name_list, datatype_list, data_list


def get_target_gltf_meshes_list(geometry_number):
    """
    概要:
        与えられたジオメトリの数に基づいて、`EXT_mesh_features` 拡張を持つ `gltf` メッシュのリストを作成する
        mesh（座標値、頂点法線、地物IDの情報をつかさどる部分の設定）

    引数:
        geometry_number: ジオメトリの数

    戻り値:
        meshes_list: 作成されたメッシュ情報のリスト
    """

    meshes_list = []

    # "primitives"をキーとする辞書型配列
    primitives_dict = {}

    # "extensions"をキーとする辞書型配列
    extensions_dict = {}

    # "attributes"をキーとする辞書型配列
    attributes_dict = {}

    primitives_dict["primitives"] = [{"extensions": {"EXT_mesh_features": {"featureIds": [{"featureCount": geometry_number,
                                                                                           "attribute": 0, "propertyTable": 0}]}}, "attributes": {"POSITION": 1, "_FEATURE_ID_0": 2, "NORMAL": 3}, "indices": 0, "mode": 4}]

    meshes_list.append(primitives_dict)

    return meshes_list


def convert_array_3_points(array):
    """
    概要:
        閉じられたポリゴンのid + xyzの配列を、3点だけの配列に変換

    引数:
        array: 変換対象の配列

    戻り値:
        result_array: 変換後の配列
    """

    # id + xyz + 法線ベクトルxyz + multipatchid前提
    # multipatch数>4点>3座標の形へreshape
    result_array = array.reshape(int(len(array)/4), 4, 8)

    # 後ろ1点切り取り 3点へ
    result_array = result_array[:, :3, :]

    # 元の形式へ id + xyz + 法線ベクトルxyz
    result_array = result_array.reshape(int(len(result_array))*3, 8)

    return result_array


def get_binary_information(array):
    """
    概要:
        与えられた配列をバイナリ形式に変換し、そのバイナリデータの長さと、gltfのURIに設定するための文字列に変換した結果を返す

    引数:
        array: バイナリ形式に変換したい配列。xyz座標の場合は2次元配列 座標の組み合わせは1次元配列を想定している

    戻り値:
        array_blob: 配列をバイナリ形式に変換した後、gltfのURIに使用できる形式にエンコードされた文字列
        array_blob_length: バイナリ形式に変換した配列のバイナリデータの長さ
    """

    # arrayをbinaryに変換
    array_blob = array.tobytes()

    # binaryの長さ取得
    array_blob_length = len(array_blob)

    # gltfのuriに設定する文字列へ変換
    array_blob = get_uri_value(array_blob)

    return array_blob, array_blob_length


def get_geometry_information_list(temporary_xyz_array):
    """
    概要:
        指定された座標配列から、各地物ごとに構成点の座標とその組み合わせ（面の構成）を取得し、座標データを整理したリストを返す

    引数:
        temporary_xyz_array: 地物IDごとに格納されたXYZ座標データを含むNumPy配列。各行は地物ID、X、Y、Z座標を持つ。

    戻り値:
        coordinates_list: 各地物の座標は重複が削除され、各地物ごとに整理されたユニークな座標のリスト
        coordinates_combination_list: 各地物ごとの座標同士の結びつき（面の構成）の情報を格納したリスト
    """

    # 度分秒での座標精度維持のため、小数点の丸め誤差を一定方向にするため
    # 入力引数の配列から小数点以下の値を「第7位」まで残して切り捨てる。
    temporary_round_down_xyz_array = (
        np.trunc((temporary_xyz_array * 10000000)) / 10000000).copy()

    # IDごとの構成点座標取得（キー：地物ID、値：地物IDの構成点のxyz座標）
    id_array = np.unique(temporary_round_down_xyz_array[:, 0:1])

    id_coordinate_dict = {id_array[i]: temporary_round_down_xyz_array[list(np.where(
        temporary_round_down_xyz_array[:, 0] == id_array[i])[0]), 1:4] for i in range(len(id_array))}

    # 一意な構成点格納用List、マルチパッチの構成点組み合わせインデックス格納用List
    coordinates_list = []
    coordinates_combination_list = []
    feature_id_list = []

    # 構成点インデックスの最小値 全体の構成点から見たインデックス算出用
    start_index_value = 0

    for i in range(len(id_array)):

        # idごとの構成点取得
        temp_xyz_array = id_coordinate_dict[i]

        # 小数点「第7位」で切り捨てを行った「XYZ」の並び順の配列から
        # gltfの座標系に合わせるために「XZY」の順番に要素を入れ替えた「座標情報配列」を作成する。
        # 合わせて、立体の面を構成するため座標同士の結び付きの情報を作成する必要があるので
        # 後工程の準備として重複座標を削除した配列を作成する。
        # このuniqueは地物ごとに行いそれぞれ結合し同じ2次元配列を作成する
        temporary_unique_points_array = np.unique(
            temp_xyz_array, axis=0).copy()

        # 立体の面を構成する情報として座標同士の結びつきの情報を作成するための準備作業として
        # 重複座標を削除した座標ごとに連番を付番した辞書型配列を作成する。
        # 配列のキー：文字列化したX座標@文字列化したZ座標@文字列化したy座標　例：133.753951456@-1.5.963951852@85.753741069
        # 配列の値  ：連番　(0から始まる昇順)
        target_unique_point_range = range(len(temporary_unique_points_array))
        temporary_unique_points_dictionary = {str(temporary_unique_points_array[ui][DDC.TARGET_POINT_X]) + '@' + str(temporary_unique_points_array[ui][DDC.TARGET_POINT_Y]) + '@' + str(temporary_unique_points_array[ui][DDC.TARGET_POINT_Z]): ui
                                              for ui in target_unique_point_range}.copy()

        # 小数点「第7位」で切り捨てを行ったXYZの並び順の配列から、立体の面を構成するための座標同士の結びつきの配列である「構成面座標連番配列」を作成する。
        # 配列を3つの座標の連番を１つの配列とした2次元配列で作成する。
        # 例：[[0,1,2],[3,4,5]...]
        target_points_range = range(len(temp_xyz_array))
        temporary_triangles_point_list = [temporary_unique_points_dictionary[str(temp_xyz_array[ti][DDC.TARGET_POINT_X]) + '@' + str(temp_xyz_array[ti][DDC.TARGET_POINT_Y]) + '@' + str(temp_xyz_array[ti][DDC.TARGET_POINT_Z])]
                                          for ti in target_points_range].copy()

        # 「構成面座標連番配列」は後述の処理でデータをスカラー値で格納するため
        # 「構成面座標連番配列」を1次元配列化し、バイナリデータ化する
        # tobytes()のoptional order = 'C
        # バイトオーダーの概念
        target_triangles_array = np.array(temporary_triangles_point_list, dtype=DDC.TARGET_UINT32).reshape(
            DDC.TARGET_RESHAPE_FROM, DDC.TARGET_RESHAPE_TO)
        target_triangles_array = target_triangles_array + start_index_value

        # インデックス最小値更新
        start_index_value = start_index_value + \
            len(temporary_unique_points_array)

        # 結果格納
        coordinates_list.append(temporary_unique_points_array)
        coordinates_combination_list.append(target_triangles_array)
        feature_id_list.append(
            np.full(len(target_triangles_array), id_array[i]))

    return coordinates_list, coordinates_combination_list


def create_obj_strings(geometry_list, geometry_combination_list):
    """
    概要:
        ジオメトリと組み合わせのリストからOBJ形式の文字列を作成する

    引数:
        geometry_list: 各地物のジオメトリ配列のリスト
        geometry_combination_list: 各地物の組み合わせ配列のリスト

    戻り値:
        obj_string: すべての地物の結合されたOBJ形式の文字列。
    """

    # OBJ形式の文字列のリスト
    obj_strings = []

    # 各地物についてのループ
    for fi in range(len(geometry_list)):
        # 地物ごとの開始コメントを追加
        obj_strings.append('g ' + str(fi) + '\n')

        # ジオメトリと組み合わせの取得
        temp_geometry_array = geometry_list[fi]
        temp_geometry_combination_array = geometry_combination_list[fi] + 1

        # 頂点情報の生成
        v_string_list = [str(temp_geometry_array[ti, 0]) + " " + str(temp_geometry_array[ti, 1]) +
                         " " + str(temp_geometry_array[ti, 2]) + "\n" for ti in range(len(temp_geometry_array))]
        obj_strings.extend(["v " + v_string for v_string in v_string_list])

        # 面情報の生成
        f_string_list = [str(temp_geometry_combination_array[ti, 0]) + "// " + str(temp_geometry_combination_array[ti, 1]) +
                         "// " + str(temp_geometry_combination_array[ti, 2]) + "//" + "\n" for ti in range(len(temp_geometry_combination_array))]
        obj_strings.extend(["f " + f_string for f_string in f_string_list])

    # OBJ形式の文字列を結合
    obj_string = ''.join(obj_strings)
    return obj_string


def get_data_definition_index_datatype(data_definition_stream,
                                       data_definition_delimiter=DDC.DELIMITER_COMMA,
                                       data_definition_encoding='utf-8'):
    """
    概要:
        製品データ定義ファイルからジオメトリと属性の情報を取得し、対応するリストを返す
        この関数の目的は製品データ定義ファイルの情報をListにして出力することにある
        たとえジオメトリ型が複数でもファイル名は複数返す必要がある

    引数:
        data_definition_stream: 製品データ定義ファイルパス(.csv or .tsv) 文字列
        data_definition_delimiter: 製品データ定義ファイルパスの区切り文字種別 1:タブ[\t](.tsv) それ以外:[,]カンマ(.csv) デフォルト値を0設定して何も指定がなければCSVとして読み込むものとする
        data_definition_encoding: 製品データ定義ファイルのencoding 日本語を含むので基本的に'shift-jis' デフォルト値に設定しておく 文字列

    戻り値:
        geometry_type_list: ジオメトリのファイルタイプリスト
        geometry_dwh_file_name_list: ジオメトリのDWHファイル名リスト
        geometry_distribution_name_list: ジオメトリの流通項目名リスト
        attribute_file_type_list: 属性のファイルタイプリスト
        attribute_dwh_file_name_list: 属性のDWHファイル名リスト
        attribute_distribution_name_list: 属性の流通項目名リスト
        attribute_const_value_list: 属性の属性値リスト
        attribute_data_type_list: 属性のデータ型リスト
    """

    # 製品データ定義ファイルをすべて文字列として読み込み
    # 区切り文字が[\t](タブ)の時
    if data_definition_delimiter == DDC.DELIMITER_TAB:
        data_definition_dataframe = pd.read_csv(
            data_definition_stream, sep='\t', encoding=data_definition_encoding, dtype=str)

    # それ以外の場合、区切り文字をカンマとして読み込む
    else:
        data_definition_dataframe = pd.read_csv(
            data_definition_stream, encoding=data_definition_encoding, dtype=str)

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
    # ジオメトリ取得対象
    # -----------------------------------------------------------------------------------------------------------
    # ジオメトリファイルタイプList
    geometry_type_list = list(file_type_array[geometry_name_index])

    # ジオメトリDWHファイル名List
    geometry_dwh_file_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME][geometry_name_index])

    # ジオメトリ流通項目名List
    geometry_distribution_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME][geometry_name_index])

    # -----------------------------------------------------------------------------------------------------------
    # 属性取得対象
    # -----------------------------------------------------------------------------------------------------------
    # 属性ファイルタイプリスト
    attribute_file_type_list = list(file_type_array[attribute_name_index])

    # 属性DWHファイル名List
    attribute_dwh_file_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DWH_COLUMN_NAME][attribute_name_index])

    # 属性流通項目名List
    attribute_distribution_name_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DISTRIBUTION_COLUMN_NAME][attribute_name_index])

    # 属性値List
    attribute_const_value_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_CONST_ATTRIBUTE_COLUMN_NAME][attribute_name_index])

    # 属性データ型List
    attribute_data_type_list = list(
        data_definition_dataframe[DDC.DATA_DEFINITION_DATA_TYPE_COLUMN_NAME][attribute_name_index])
    # -----------------------------------------------------------------------------------------------------------

    return geometry_type_list, geometry_dwh_file_name_list, geometry_distribution_name_list, attribute_file_type_list, attribute_dwh_file_name_list, attribute_distribution_name_list, attribute_const_value_list, attribute_data_type_list


def get_multipatch_line_segment_array(multipatch_array):
    """
    概要:
        multipatch配列からmultipatchを構成する線分を取得
        1地物分を処理する

    引数:
        multipatch_array: マルチパッチ座標2次元配列 [4点ずつの組み合わせ構成点]>[xyz]

    戻り値:
        line_segment_array: 線分3次元配列 [地物]>[線分]>[2点のxyz座標]

    """

    # 三角形の数
    line_segment_range = range(int(len(multipatch_array)/4))

    # 結果格納用List
    result_list = []

    # 三角形ごとに3本線分取り出す
    [result_list.append([[multipatch_array[i*4], multipatch_array[i*4+1]], [multipatch_array[i*4+1],
                        multipatch_array[i*4+2]], [multipatch_array[i*4+2], multipatch_array[i*4+3]]]) for i in line_segment_range]

    line_segment_array = np.concatenate(result_list, axis=0)

    return line_segment_array


def get_map_information_level_from_unit_code(unit_code_string):
    """
    概要:
        桁数、各桁の文字列の有無によってレベルの判定を行う
        エラーのパターンはたくさんある
        各レベルごとの各桁数パターンは固定されているので固定値にしておく
        公共測量標準図式 付録7 84条参照
        地図情報レベル250････････09LD00AA～TT→今は使わない
        地図情報レベル500････････09LD0000～99→ファイルサイズ上限を超える場合使用する
        地図情報レベル1000･･･････09LD000A～4E→今は使わない
        地図情報レベル2500･･･････09LD001～4
        地図情報レベル5000･･･････09LD00～99

    引数:
        unit_code_string: 国土基本図図郭コード 文字列

    戻り値:
        map_information_level: 地図情報レベル int

    """

    # 桁数
    unit_code_string_length = len(unit_code_string)

    # 地図情報レベル
    map_information_level = 0

    # 桁数が4の場合レベル50000
    if unit_code_string_length == 4:
        map_information_level = 50000

    # 桁数が6の場合レベル5000
    elif unit_code_string_length == 6:
        map_information_level = 5000

    # 桁数が7の場合レベル2500
    elif unit_code_string_length == 7:
        map_information_level = 2500

    # 桁数が8の場合3パターンにわれる
    elif unit_code_string_length == 8:
        map_information_level = 500
    else:
        pass

    return map_information_level


def get_index_from_unit_code(unit_code_string, map_information_level):
    """
    概要:
        図郭コードを受け取りインデックスに変換する

    引数:
        unit_code_string: 国土基本図図郭コード 文字列
        map_information_level: 地図情報レベル int

    戻り値:
        - np.array(result_list): インデックス配列
        - np.array(DDC.LEVEL_MESH_DICT[map_information_level]): 各レベルのメッシュのサイズ（x方向、y方向）の情報を含む2次元配列 [レベルごとインデックス]>[x_index, y_index]

    """

    # 結果格納用
    result_list = []

    # レベルごとに図郭コードの切り取る範囲を決める
    # 例 '09LD'
    if map_information_level == 50000:

        # それぞれの文字列からインデックス取得
        result_list.append([DDC.LEVEL_50000_X_DICT[unit_code_string[3]],
                           DDC.LEVEL_50000_Y_DICT[unit_code_string[2]]])

    # 例 '09LD35'
    elif map_information_level == 5000:

        result_list.append([DDC.LEVEL_50000_X_DICT[unit_code_string[3]],
                           DDC.LEVEL_50000_Y_DICT[unit_code_string[2]]])
        result_list.append(
            [int(unit_code_string[5]), -int(unit_code_string[4])])

    # 例 '09LD351'
    elif map_information_level == 2500:
        result_list.append([DDC.LEVEL_50000_X_DICT[unit_code_string[3]],
                           DDC.LEVEL_50000_Y_DICT[unit_code_string[2]]])
        result_list.append(
            [int(unit_code_string[5]), -int(unit_code_string[4])])
        result_list.append(DDC.LEVEL_2500_DICT[unit_code_string[6]])

    # 例 '09LD3599'
    elif map_information_level == 500:
        result_list.append([DDC.LEVEL_50000_X_DICT[unit_code_string[3]],
                           DDC.LEVEL_50000_Y_DICT[unit_code_string[2]]])
        result_list.append(
            [int(unit_code_string[5]), -int(unit_code_string[4])])
        result_list.append(
            [int(unit_code_string[7]), -int(unit_code_string[6])])
    else:
        pass

    return np.array(result_list), np.array(DDC.LEVEL_MESH_DICT[map_information_level])


def get_unit_origin(unit_code_string):
    """
    概要:
        指定図郭の原点取得

    引数:
        unit_code_string:  国土基本図図郭コード 文字列

    戻り値:
        unit_origin_array: 指定された図郭コードの原点座標
        level_mesh_array: 各レベルのメッシュのサイズ（x方向、y方向）の情報を含む2次元配列

    """

    # 地図情報レベル取得
    map_information_level = get_map_information_level_from_unit_code(
        unit_code_string)

    # レベルごとのインデックスと1meshの単位
    index_array, level_mesh_array = get_index_from_unit_code(
        unit_code_string, map_information_level)

    # 原点取得
    unit_origin_array = np.sum(np.multiply(
        index_array, level_mesh_array), axis=0)

    return unit_origin_array, level_mesh_array


def get_unit_min_max(unit_origin_array, level_mesh_array):
    """
    概要:
        図郭の最小座標と最大座標を計算する

    引数:
        unit_origin_array: 図郭の原点座標（x, y）を格納した配列。
        level_mesh_array: 各レベルのメッシュのサイズ（x方向、y方向）の情報を含む2次元配列

    戻り値:
        unit_min_x: 図郭の最小X座標（原点からの最小の水平方向の座標）
        unit_max_x: 図郭の最大X座標（原点からの最大の水平方向の座標）
        unit_min_y: 図郭の最小Y座標（原点からの最小の垂直方向の座標）
        unit_max_y: 図郭の最大Y座標（原点からの最大の垂直方向の座標）
    """

    # 図郭のmesh単位
    x_unit = level_mesh_array[-1, 0]
    y_unit = level_mesh_array[-1, 1]

    # 図郭の座標
    unit_min_x = unit_origin_array[0].copy()
    unit_max_x = unit_min_x + x_unit
    unit_max_y = unit_origin_array[1].copy()
    unit_min_y = unit_max_y - y_unit

    return unit_min_x, unit_max_x, unit_min_y, unit_max_y


def get_bool_in_rectangle_area(min_x, max_x, min_y, max_y, x_coordinate, y_coordinate):
    """
    概要:
        矩形範囲内に存在する座標のbool配列取得

    引数:
        min_x: 矩形範囲の最小x座標
        max_x: 矩形範囲の最大x座標
        min_y: 矩形範囲の最小y座標
        max_y: 矩形範囲の最大y座標
        x_coordinate: 座標のx値を含む配列
        y_coordinate: 座標のy値を含む配列

    戻り値:
        result_bool: 指定した矩形範囲内に存在する座標に対応するbool値の配列(True: 範囲内, False: 範囲外)
    """

    result_bool = (min_x <= x_coordinate) & (x_coordinate <= max_x)\
        & (min_y <= y_coordinate) & (y_coordinate <= max_y)

    return result_bool


def get_origin_point_from_coordinates_array(coordinate_array, level_mesh_array):
    """
    概要:
        指定された座標配列から、与えられたメッシュ単位に基づいて、図郭左上の座標を取得する

    引数:
        coordinate_array: 座標配列
        level_mesh_array: 各レベルのメッシュのサイズ（x方向、y方向）の情報を含む2次元配列

    戻り値:
        edit_coordinate_array: 調整後の座標配列

    """

    # 編集用座標配列
    edit_coordinate_array = coordinate_array.copy()

    # 余り計算用配列
    remainder_array = coordinate_array.copy()

    # メッシュ単位ごとに処理
    for i in range(len(level_mesh_array)):

        # 余り配列
        remainder_array = np.remainder(remainder_array, level_mesh_array[i])

    # x座標調整用 割り切れた時だけ左の図郭に含む
    x_adjust_array = np.zeros((remainder_array.shape))
    x_remainer_zero_bool = remainder_array[:, 0] == 0

    x_adjust_array[x_remainer_zero_bool, 0] = -level_mesh_array[-1][0]

    # y座標調整用 常に左上
    y_adjust_array = np.zeros((remainder_array.shape))
    y_adjust_array[:, 1] = level_mesh_array[-1][1]

    # 左上の座標取得
    edit_coordinate_array = edit_coordinate_array - \
        remainder_array + y_adjust_array + x_adjust_array

    return edit_coordinate_array


def create_feature_rectangle(feature_array, level_mesh_array):
    """
    概要:
        その地物が含まれる国土基本図図郭の範囲を矩形で取得し、対象範囲内に含まれる図郭の線分を2次元配列で取得

    引数:
        feature_array: 地物の座標を含む2次元配列
        level_mesh_array: メッシュ単位を含む2次元配列（レベルごとのメッシュサイズ）

    戻り値:
        result_list: 地物の矩形範囲に含まれる図郭の線分を格納した2次元配列

    """

    # 地物を矩形で囲うときの左上の座標と右下の座標を取得する
    min_x = np.min(feature_array[:, 0])
    min_y = np.min(feature_array[:, 1])
    max_x = np.max(feature_array[:, 0])
    max_y = np.max(feature_array[:, 1])

    # 取得した矩形の座標が存在する図郭の原点取得
    area_array = get_origin_point_from_coordinates_array(
        np.array([[min_x, max_y], [max_x, min_y]]), level_mesh_array)
    # 図郭の最も左上の点
    temp_origin_array = area_array[0].copy()

    # 図郭の原点の差からx,y方向の図郭の枚数取得
    x_number = int(
        ((area_array[1, 0] - area_array[0, 0]) / level_mesh_array[-1, 0])) + 1
    y_number = int(
        ((area_array[0, 1] - area_array[1, 1]) / level_mesh_array[-1, 1])) + 1

    # 1mesh進むごとに変化する値
    x_mesh_array = np.array([level_mesh_array[-1, 0], 0])
    y_mesh_array = np.array([0, -level_mesh_array[-1, 1]])

    # 図郭の線分格納用List
    result_list = []

    # 枚数分だけ図郭の線分取得
    for xi in range(1, x_number+1):
        for yi in range(1, y_number+1):

            # 3点生成
            point2 = temp_origin_array + x_mesh_array * xi + y_mesh_array * yi
            point1 = point2.copy() - x_mesh_array
            point3 = point2.copy() - y_mesh_array

            # 縦線追加
            result_list.append(np.array([point1.copy(), point2.copy()]))

            # 横線追加
            result_list.append(np.array([point2.copy(), point3.copy()]))

    # 1ループ2本
    # 原点から右と下に進む線分生成
    [result_list.append(np.array([temp_origin_array + x_mesh_array * (i-1),
                        temp_origin_array + x_mesh_array * (i)])) for i in range(1, x_number+1)]
    [result_list.append(np.array([temp_origin_array + y_mesh_array * (i-1),
                        temp_origin_array + y_mesh_array * (i)])) for i in range(1, y_number+1)]

    return np.array(result_list)


def get_cross(line_string_array, unit_line_string_array):
    """
    概要:
        外積を利用して線分の交差判定用の計算を行う

    引数:
        line_string_array: 交差判定を行う1つ目の線分の2点を格納した配列
        unit_line_string_array: 交差判定を行う2つ目の線分の2点を格納した配列

    戻り値:
        cross_array1: 交差判定用計算結果
        cross_array2: 交差判定用計算結果
        cross_array3: 交差判定用計算結果
        cross_array4: 交差判定用計算結果

    """

    # 基準1
    ab_array = line_string_array[1] - line_string_array[0]

    # 外積1-1

    ac_array = unit_line_string_array[:, 0, :] - line_string_array[0]

    # 外積1-2
    ad_array = unit_line_string_array[:, 1, :] - line_string_array[1]

    # 両方0の場合は同一直線上にある→交差判定としない
    # 積が0だが片方のみ0の場合は同じ点を共有するので交差とする
    # 外積の計算
    cross_array1 = np.cross(ab_array, ac_array)
    cross_array2 = np.cross(ab_array, ad_array)

    # 基準2
    cd_array = unit_line_string_array[:, 1,
                                      :] - unit_line_string_array[:, 0, :]

    # 外積2-1
    cb_array = line_string_array[1] - unit_line_string_array[:, 0, :]

    # 外積2-2
    ca_array = line_string_array[0] - unit_line_string_array[:, 0, :]

    # 両方0の場合は同一直線上にある→交差判定としない
    # 積が0だが片方のみ0の場合は同じ点を共有するので交差とする
    # 外積の計算
    cross_array3 = np.cross(cd_array, cb_array)
    cross_array4 = np.cross(cd_array, ca_array)

    # 1,2は地物の線分側から見た外積
    # 3,4は図郭の線分側から見た外積
    return cross_array1, cross_array2, cross_array3, cross_array4


def is_intersect(cross1, cross2, cross3, cross4):
    """
    概要:
        線分が交差しているかを判定
        外積から交差判定関数1：「2つの外積の積が0以下」かつ「少なくとも一方は0でない」→端点が線上の場合は交差扱いとする
        その判定を線分2本の視点で行う
        図郭線で地物を分割するための関数 交差している場合その交差点で地物を分割する

    引数:
        cross_array1: 交差判定用計算結果
        cross_array2: 交差判定用計算結果
        cross_array3: 交差判定用計算結果
        cross_array4: 交差判定用計算結果

    戻り値:
        bool: 交差している場合はTrue、交差していない場合はFalse
    """

    # 外積1,2による交差判定結果 「2つの外積の積が0以下」かつ「少なくとも一方は0でない」
    cross_1_2_bool = (cross1 * cross2 <=
                      0) & np.logical_not((cross1 == 0) & (cross2 == 0))

    # 外積3,4による交差判定結果 「2つの外積の積が0以下」かつ「少なくとも一方は0でない」
    cross_3_4_bool = (
        (cross3 * cross4)) <= 0 & np.logical_not((cross3 == 0) & (cross4 == 0))

    return cross_1_2_bool & cross_3_4_bool


def get_intersect_point_array(line_array, unit_array):
    """
    概要:
        交差している場合交差する点を取得する
        is_intersectによる交差判定を抜けた線分同士のみ引数の対象とする
        線分は2次元配列、図郭線は3次元配列 1つの線分が複数図郭線をまたぐ可能性があるため
        線分 [構成点]>[xy座標] 図郭 [図郭数]>[構成点]>[xy座標]
        線分:図郭線=1:N
        交点は始点から近い順に並び替え

    引数:
        line_array: 地物の線分を示す配列
        unit_array: 図郭の線分を示す配列

    戻り値:
        result_point_array: 交差点を示す2次元配列
    """

    # 地物の線分cd
    # 図郭線abとする
    cd = unit_array[:, 0, :] - unit_array[:, 1, :]
    ca = line_array[0] - unit_array[:, 0, :]
    ab = line_array[1] - line_array[0]

    # 交差した図郭の線分ab
    # tが0あるいは1の時線分の端点になる
    # 0より大きく1より小さいものが地物を分割する点となる
    t = -np.cross(cd, ca) / np.cross(cd, ab)

    t = np.sort(t)
    # 複数の点を同時に計算するため2次元配列にする
    t = t.reshape((len(t), 1))

    result_point_array = line_array[0] + (line_array[1] - line_array[0]) * t

    return result_point_array


def judge_citygml(feature_array, unit_origin_array, level_mesh_array):
    # ---------------------------------------------------------------
    # 高級関数作成
    # 地物が抽出したい図郭にあるかないかの結果を返す
    # 1地物
    # 抽出範囲図郭原点
    # 50000レベルから抽出範囲図郭までの1meshの単位
    # ---------------------------------------------------------------
    unit_min_x, unit_max_x, unit_min_y, unit_max_y = get_unit_min_max(
        unit_origin_array, level_mesh_array)

    # 構成点が図郭内に存在するか
    isin_bool = get_bool_in_rectangle_area(
        unit_min_x, unit_max_x, unit_min_y, unit_max_y, feature_array[:, 0], feature_array[:, 1])

    # すべてx座標、y座標がminより小さい maxより大きいは抽出対象外　抽出したくない地物の大多数を占める
    bool5 = np.all(feature_array[:, 0] < unit_min_x)
    bool6 = np.all(unit_max_x < feature_array[:, 0])
    bool7 = np.all(feature_array[:, 1] < unit_min_y)
    bool8 = np.all(unit_max_y < feature_array[:, 1])

    if np.all(isin_bool):
        return True
    elif np.any([bool5, bool6, bool7, bool8]):
        return False
    else:

        # 原点と延長格納用List
        # [x,y,length]→この図郭（原点）にどれだけの延長が存在しているか
        origin_length_list = []

        # 線分ごとに交差図郭抽出
        for i in range(len(feature_array)-1):

            # 線分が存在する図郭の線分座標生成
            temp_unit_array = create_feature_rectangle(
                feature_array[i:i+2, :], level_mesh_array)

            # 線分と図郭の外積取得
            cross1, cross2, cross3, cross4 = get_cross(
                feature_array[i:i+2, :], temp_unit_array)

            # 交差判定
            intersect_bool = is_intersect(cross1, cross2, cross3, cross4)

            if np.any(intersect_bool):

                # 線分ごとに交差箇所特定→ここでの交差判定は外積の積が0以下かつ少なくとも片方は0でない場合
                cross_point = get_intersect_point_array(
                    feature_array[i:i+2, :], temp_unit_array[intersect_bool])

                # 始点の原点
                start_origin = get_origin_point_from_coordinates_array(
                    feature_array[i:i+1, :], level_mesh_array)

                # 始点から交差点までの長さ
                start_distance = np.sqrt(
                    np.sum(np.power(feature_array[i] - cross_point[0], 2)))
                origin_length_list.append(
                    [start_origin[0, 0].copy(), start_origin[0, 1].copy(), start_distance.copy()])

                if len(cross_point) > 1:
                    # 交差点間
                    # 分割単位で図郭の左上特定と長さの計算
                    #    交差図郭で以上以下を調べて左上とする
                    #    交差点の数よる1の時0の時2以上の時
                    #    延長の計算は始点→交差点 交差点→交差点・・・・交差点→終点
                    #    それぞれどの図郭に存在するかは交差点以外で判定
                    #    交差点同士で挟まれている場合は中点で判定
                    # 交差する点が1点しかなければ交差点同士の計算はしない
                    for ci in range(len(cross_point)-1):
                        temp_distance = np.sqrt(
                            np.sum(np.power(cross_point[ci] - cross_point[ci+1], 2)))
                        temp_middle_point = (
                            cross_point[ci:ci+1, :] + cross_point[ci+1:ci+2, :]) / 2
                        temp_origin = get_origin_point_from_coordinates_array(
                            temp_middle_point, level_mesh_array)
                        origin_length_list.append(
                            [temp_origin[0, 0].copy(), temp_origin[0, 1].copy(), temp_distance.copy()])

                else:
                    pass

                # 交差点から終点まで
                # 終点の存在する図郭取得
                end_origin = get_origin_point_from_coordinates_array(
                    feature_array[i+1:i+2, :], level_mesh_array)

                # 交差点から終点までの長さ
                end_distance = np.sqrt(
                    np.sum(np.power(feature_array[i+1] - cross_point[-1], 2)))
                origin_length_list.append(
                    [end_origin[0, 0].copy(), end_origin[0, 1].copy(), end_distance.copy()])

            else:
                # 交差がなければ原点追加、線分の距離
                # 終点の存在する図郭取得
                temp_origin = get_origin_point_from_coordinates_array(
                    feature_array[i:i + 1, :], level_mesh_array)

                # 交差点から終点までの長さ
                temp_distance = np.sqrt(
                    np.sum(np.power(feature_array[i] - feature_array[i + 1], 2)))
                origin_length_list.append(
                    [temp_origin[0, 0].copy(), temp_origin[0, 1].copy(), temp_distance.copy()])

        origin_length_array = np.array(origin_length_list).round(decimals=2)

        # 分割単位が同じ（含まれる原点が同じ）もので合計
        unique_origin = np.unique(
            origin_length_array[:, :2], axis=0).astype(np.int64)
        sum_distance = np.array([np.sum(origin_length_array[np.all(origin_length_array[:, :2].astype(
            np.int64) == unique_origin[ui], axis=1), 2]) for ui in range(len(unique_origin))])

        # 集計結果から延長最大の原点取得
        # 延長が最大の原点が引数の図郭と同じなら出力対象
        if np.all(np.isclose(unique_origin[np.argmax(sum_distance)], unit_origin_array)):
            return True

        # それ以外は出力対象としない
        else:
            return False


def judge_citygmls(linestring_array,
                   s_index,
                   e_index,
                   unit_origin_array,
                   mesh_array,
                   coordinates_id_array
                   ):
    # ---------------------------------------------------------------
    # judge_citygmlの複数地物版
    # ---------------------------------------------------------------

    feature_bool = [judge_citygml(linestring_array[s_index[i]:e_index[i]+1, :],
                                  unit_origin_array,
                                  mesh_array) for i in range(len(coordinates_id_array))]

    return np.array(feature_bool, dtype=np.bool_)


def create_element(element_list, element_dict):
    # ---------------------------------------------------------------
    # ネストされた要素を作成 最後の要素を返す←値を追加する用
    # 引数1：要素名List 要素はstr
    # ---------------------------------------------------------------
    root = ET.Element(element_list[0])
    b = ET.SubElement(root, element_list[1])

    element_dict[element_list[0]] = root
    element_dict[element_list[1]] = b
    for fi in range(len(element_list)-2):
        b = ET.SubElement(b, element_list[fi + 2])
        element_dict[element_list[fi+2]] = b

    return root, b, element_dict


def create_element2(element_list, element_dict):
    # ---------------------------------------------------------------
    # ネストされた要素を作成 最後の要素を返す←値を追加する用
    # 引数1：要素名List 要素はstr
    # ---------------------------------------------------------------

    # elemntlistが2次元配列になるので一つ目の要素だけ取り出す

    root = ET.Element(element_list[0][0])
    b = ET.SubElement(root, element_list[1][0])

    element_dict[element_list[0][0]] = root
    element_dict[element_list[1][0]] = b
    for fi in range(len(element_list)-2):
        b = ET.SubElement(b, element_list[fi + 2][0])
        element_dict[element_list[fi + 2][0]] = b

    return root, b, element_dict


def judge_multipatch_in_unit(feature_array, unit_origin_array, level_mesh_array):
    # ---------------------------------------------------------------
    # 1地物
    # 抽出範囲図郭原点 1次元配列 [x,y]
    # 50000レベルから抽出範囲図郭までの1meshの単位 2次元配列[[x_unit_1,y_unit_1]
    # ---------------------------------------------------------------

    unit_min_x, unit_max_x, unit_min_y, unit_max_y = get_unit_min_max(
        unit_origin_array, level_mesh_array)

    # 構成点が図郭内に存在するか
    isin_bool = get_bool_in_rectangle_area(
        unit_min_x, unit_max_x, unit_min_y, unit_max_y, feature_array[:, 0, :], feature_array[:, 1, :])

    # すべてx座標、y座標がminより小さい maxより大きいは抽出対象外　抽出したくない地物の大多数を占める
    bool5 = np.all(feature_array[:, 0, :] < unit_min_x)
    bool6 = np.all(unit_max_x < feature_array[:, 0, :])
    bool7 = np.all(feature_array[:, 1, :] < unit_min_y)
    bool8 = np.all(unit_max_y < feature_array[:, 1, :])

    if np.all(isin_bool):
        return True
    elif np.any([bool5, bool6, bool7, bool8]):
        return False
    else:

        # 原点と延長格納用List
        # [x,y,length]→この図郭（原点）にどれだけの延長が存在しているか
        origin_length_list = []

        # 線分ごとに交差図郭抽出

        for i in range(len(feature_array)):

            # 線分が存在する図郭の線分座標生成
            temp_unit_array = create_feature_rectangle(
                feature_array[i], level_mesh_array)

            # 線分と図郭の外積取得
            cross1, cross2, cross3, cross4 = get_cross(
                feature_array[i], temp_unit_array)

            # 交差判定
            intersect_bool = is_intersect(cross1, cross2, cross3, cross4)

            if np.any(intersect_bool):

                # 線分ごとに交差箇所特定→ここでの交差判定は外積の積が0以下かつ少なくとも片方は0でない場合
                cross_point = get_intersect_point_array(
                    feature_array[i], temp_unit_array[intersect_bool])

                # 始点の原点
                start_origin = get_origin_point_from_coordinates_array(
                    feature_array[i], level_mesh_array)

                # 始点から交差点までの長さ
                start_distance = np.sqrt(
                    np.sum(np.power(feature_array[i, 0] - cross_point[0], 2)))
                origin_length_list.append(
                    [start_origin[0, 0].copy(), start_origin[0, 1].copy(), start_distance.copy()])

                if len(cross_point) > 1:
                    # 交差点間
                    # 分割単位で図郭の左上特定と長さの計算
                    #    交差図郭で以上以下を調べて左上とする
                    #    交差点の数よる1の時0の時2以上の時
                    #    延長の計算は始点→交差点 交差点→交差点・・・・交差点→終点
                    #    それぞれどの図郭に存在するかは交差点以外で判定
                    #    交差点同士で挟まれている場合は中点で判定
                    # 交差する点が1点しかなければ交差点同士の計算はしない
                    for ci in range(len(cross_point)-1):
                        temp_distance = np.sqrt(
                            np.sum(np.power(cross_point[ci] - cross_point[ci+1], 2)))
                        temp_middle_point = (
                            cross_point[ci:ci+1, :] + cross_point[ci+1:ci+2, :]) / 2
                        temp_origin = get_origin_point_from_coordinates_array(
                            temp_middle_point, level_mesh_array)
                        origin_length_list.append(
                            [temp_origin[0, 0].copy(), temp_origin[0, 1].copy(), temp_distance.copy()])

                else:
                    pass

                # 交差点から終点まで
                # 終点の存在する図郭取得
                end_origin = get_origin_point_from_coordinates_array(
                    feature_array[i, 1:2], level_mesh_array)

                # 交差点から終点までの長さ
                end_distance = np.sqrt(
                    np.sum(np.power(feature_array[i, 1] - cross_point[-1], 2)))
                origin_length_list.append(
                    [end_origin[0, 0].copy(), end_origin[0, 1].copy(), end_distance.copy()])

            else:
                # 交差がなければ原点追加、線分の距離
                # 終点の存在する図郭取得
                temp_origin = get_origin_point_from_coordinates_array(
                    feature_array[i, 1:2], level_mesh_array)

                # 始点から終点までの長さ
                temp_distance = np.sqrt(
                    np.sum(np.power(feature_array[i, 0] - feature_array[i, 1], 2)))
                origin_length_list.append(
                    [temp_origin[0, 0].copy(), temp_origin[0, 1].copy(), temp_distance.copy()])

        origin_length_array = np.array(origin_length_list).round(decimals=2)

        # 分割単位が同じ（含まれる原点が同じ）もので合計
        unique_origin = np.unique(
            origin_length_array[:, :2], axis=0).astype(np.int64)
        sum_distance = np.array([np.sum(origin_length_array[np.all(origin_length_array[:, :2].astype(
            np.int64) == unique_origin[ui], axis=1), 2]) for ui in range(len(unique_origin))])

        # 集計結果から延長最大の原点取得
        # 延長が最大の原点が引数の図郭と同じなら出力対象
        if np.all(np.isclose(unique_origin[np.argmax(sum_distance)], unit_origin_array)):
            return True

        # それ以外は出力対象としない
        else:
            return False


def get_squared(target_array):
    # ---------------------------------------------------------------
    # それぞれの要素の2乗
    # ---------------------------------------------------------------
    return np.sum(np.power(target_array, 2))


def get_magnitude(target_array):
    # ---------------------------------------------------------------
    # 大きさ
    # ---------------------------------------------------------------
    return np.sqrt(get_squared(target_array))


def get_normalize(cartesian):
    # ---------------------------------------------------------------
    # 単位ベクトル
    # ---------------------------------------------------------------
    return cartesian / get_magnitude(cartesian)


def get_cartesian3_from_degree(lon, lat, height, ellipsoid):
    # ---------------------------------------------------------------
    # 経緯度からWGS84のデカルト座標取得
    # ---------------------------------------------------------------

    # 緯度、経度をラジアン単位へ変換
    lon_rad = np.deg2rad(lon)
    lan_rad = np.deg2rad(lat)

    # 楕円体座標それぞれ2乗
    radius_squared = np.power(ellipsoid, 2)

    cos_latitude = np.cos(lan_rad)

    scratch_x = cos_latitude * np.cos(lon_rad)
    scratch_y = cos_latitude * np.sin(lon_rad)
    scratch_z = np.sin(lan_rad)
    scratch = np.array([scratch_x, scratch_y, scratch_z])
    scratch_n = get_normalize(scratch)
    scratch_k = np.multiply(radius_squared, scratch_n)
    gamma = np.sqrt(np.sum(np.multiply(scratch_n, scratch_k)))

    scratch_k = np.divide(scratch_k, gamma)
    scratch_n = np.multiply(scratch_n, height)

    return scratch_k + scratch_n


def get_matrix_from_cartesian3(cartesian3, ellipsoid):
    # ---------------------------------------------------------------
    # デカルト座標から平面直角座標変換パラメータ取得
    # ---------------------------------------------------------------

    # 必要なのはeast, north, up
    east_array = np.array([-cartesian3[1], cartesian3[0], 0.0])

    one_over_radius_squared = 1 / np.power(ellipsoid, 2)

    up_array = get_normalize(np.multiply(cartesian3, one_over_radius_squared))

    east_array = get_normalize(east_array)

    north_array = np.cross(up_array, east_array)

    return np.array([east_array[0], north_array[0], up_array[0], cartesian3[0], east_array[1], north_array[1], up_array[1], cartesian3[1], east_array[2], north_array[2], up_array[2], cartesian3[2],          0.0,            0.0,         0.0,           1.0])


def get_matrix_gltf_from_cartesian3(cartesian3, ellipsoid):
    # ---------------------------------------------------------------
    # gltfに設定する用 matrixを取得する
    # ---------------------------------------------------------------

    # matrix取得
    matrix_array = get_matrix_from_cartesian3(cartesian3, ellipsoid)

    # gltf設定用に並び替え
    return np.array([matrix_array[1], matrix_array[9], -matrix_array[5], matrix_array[12], matrix_array[2], matrix_array[10], -matrix_array[6], matrix_array[13], matrix_array[0], matrix_array[8], -matrix_array[4], matrix_array[14], matrix_array[3], matrix_array[11], -matrix_array[7], matrix_array[15]])


def get_buffer_attribute(input_data_list):

    data_list = [str(input_data_list[i]) for i in range(len(input_data_list))]

    data_len_array = np.array([len(data_list[i].encode('utf-8'))
                              for i in range(len(data_list))], dtype=np.uint32)

    data_offset_array = np.array([0 if i == 0 else np.sum(
        data_len_array[0:i]) for i in range(len(data_len_array)+1)], dtype=np.uint32)

    data_string = "".join(data_list)

    data_string = data_string.encode('UTF-8')

    data_string_length = int(np.sum(data_len_array))

    attribute_uri = DDC.DATA_URI_HEADER + get_uri_value(data_string)

    return data_string_length, data_offset_array, attribute_uri


def create_gltf_object(temporary_xyz_array, all_attribute_dataframe, matrix_list=None):
    # -----------------------------------------------------------------------------------------------------------
    # 関数名      ：glTF出力データクラスオブジェクト作成処理
    # 第１引数    ：【XYZ】の順に座標情報が格納された2次元のndarray配列
    # 戻り値      ：glTFのデータクラスオブジェクト
    # 処理概要    ：XYZの配列から立体の面を構成するための座標同士の結びつきの情報と、
    #              XZYの並びに変換した座標情報をもとにglTFに出力するデータ配列を作成する。
    # 【特記事項】：本処理を実行すると64bit浮動小数点が32bit浮動小数点に変換されます。
    #              そのため小数点第7位未満が切り捨てられます。
    # -----------------------------------------------------------------------------------------------------------

    # 引数チェック
    # 入力引数が何もなければ何も処理を行わない。
    if len(temporary_xyz_array) > 0:

        ######################################################################################################
        # 【１．glTFの出力データ作成の準備処理】
        # ※入力引数をもとにglTFの仕様に合わせてデータ変換を行い、
        # 立体の面を構成するための座標の組み合わせの配列である「構成面座標連番配列」を作成し
        # glTFの仕様に合わせて座標の並びをXZYの順に変換した「座標情報配列」を作成する。
        ######################################################################################################

        # マルチパッチの頂点座標をgltfに設定する形式へ変換
        result_coordinates_array = convert_array_3_points(temporary_xyz_array)

        # 構成点のインデックスを0から順番に頂点分作成
        result_coordinates_combination_array = np.arange(
            len(result_coordinates_array), dtype=np.uint32)

        # 地物数
        feature_unique_id_array, feature_unique_id_counts_array = np.unique(
            result_coordinates_array[:, 0], return_counts=True)
        feature_counts = len(np.unique(feature_unique_id_array))

        result_feature_id_array = np.concatenate(
            [[i]*feature_unique_id_counts_array[i] for i in range(len(feature_unique_id_counts_array))])

        # マルチパッチの頂点法線座標をgltfに設定する形式へ変換
        # 「構成面座標連番配列」作成
        target_triangles_binary_blob, target_triangles_binary_length = get_binary_information(
            result_coordinates_combination_array)

        # glTFの仕様で浮動小数点の値は32bitのデータ型でしか持てないため、
        # 「座標情報配列」を32bit浮動小数に変換し、バイナリデータ化する。
        target_points_array = result_coordinates_array[:, [
            2, 3, 1]].astype(np.float32)
        target_points_binary_blob, target_points_binary_length = get_binary_information(
            target_points_array)

        # buffers,bufferViewsでデータ長の設定をするため、
        # 事前に「座標情報配列」と「構成面座標連番配列」のバイナリデータ長の取得する。
        # 座標情報配列は一意な点情報の塊　構成面座標連番配列とはその組み合わせ（インデックスで指定）
        # 「地物ID配列」作成（複数の三角形を一つの地物とみなすため）
        target_feature_id_binary_blob, target_feature_id_binary_length = get_binary_information(
            result_feature_id_array.flatten())

        # 「頂点法線座標連番配列」作成
        # 「頂点法線座標情報配列」を32bit浮動小数に変換し、バイナリデータ化する。

        target_vertex_normal_array = result_coordinates_array[:, [
            5, 6, 4]].astype(np.float32)
        target_vertex_normal_points_binary_blob, target_vertex_normal_points_binary_length = get_binary_information(
            target_vertex_normal_array)

        ######################################################################################################
        # 【２．glTFの出力データ配列作成】
        ######################################################################################################

        ######################################################################################################
        # ＜２－１．buffersの出力内容設定＞
        ######################################################################################################

        # バイナリデータのデータ長を「構成面座標連番配列」と「座標情報配列」の長さの合計とする。
        # 「地物ID配列」、「頂点法線座標連番配列」、「頂点法線座標情報配列」
        target_gltf_buffers_list = [pgl.Buffer(byteLength=target_triangles_binary_length
                                               + target_points_binary_length
                                               + target_feature_id_binary_length
                                               + target_vertex_normal_points_binary_length, uri=DDC.DATA_URI_HEADER
                                               + target_triangles_binary_blob
                                               + target_points_binary_blob
                                               + target_feature_id_binary_blob
                                               + target_vertex_normal_points_binary_blob
                                               )]

        ######################################################################################################
        # ＜２－２．bufferViewsの出力内容設定＞
        ######################################################################################################

        # 「構成面座標連番配列」のbufferViewsを設定する。
        # 引数に設定した値は以下の通り。
        # buffer    ：「構成面座標連番配列」のbufferViewsが参照するbufferのインデックスを指定する。
        # byteLength： buffersのデータ領域のうち、「構成面座標連番配列」のデータ領域を「構成面座標連番配列」のバイナリデータの長さとする。
        # target    ：「頂点要素配列バッファ」を設定する。（頂点要素配列バッファ：glTFで「構成面座標連番配列」をバイナリデータに変換した値を格納する領域）
        target_gltf_bufferViews_list = [pgl.BufferView(
            buffer=0, byteLength=target_triangles_binary_length, target=pgl.ELEMENT_ARRAY_BUFFER)]

        # 「座標情報配列」のbufferViewsを設定する。
        # 引数に設定した値は以下の通り。
        # buffer    ：「座標情報配列」のbufferViewsが参照するbufferのインデックスを指定する。
        # byteOffset：「座標情報配列」のバイナリデータ読み込み開始位置を「構成面座標連番配列」のデータ領域の後にセットする。
        # byteLength： buffersのデータ領域のうち、「座標情報配列」のデータ領域を「座標情報配列」のバイナリデータの長さとする。
        # target    ：「配列バッファ」を設定する。（配列バッファ：glTFで「座標情報配列」をバイナリデータに変換した値を格納する領域）
        target_gltf_bufferViews_list.append(pgl.BufferView(
            buffer=0, byteOffset=target_triangles_binary_length, byteLength=target_points_binary_length, target=pgl.ARRAY_BUFFER))

        # 地物IDセット
        target_gltf_bufferViews_list.append(pgl.BufferView(buffer=0, byteOffset=target_points_binary_length + target_triangles_binary_length, byteLength=target_feature_id_binary_length, target=pgl.ELEMENT_ARRAY_BUFFER
                                                           ))

        # ここに法線ベクトル
        target_gltf_bufferViews_list.append(pgl.BufferView(buffer=0, byteOffset=target_points_binary_length + target_triangles_binary_length + target_feature_id_binary_length, byteLength=target_vertex_normal_points_binary_length, target=pgl.ARRAY_BUFFER
                                                           ))

        ######################################################################################################
        # ＜２－３．accessorsの出力内容設定＞
        ######################################################################################################

        # 「構成面座標連番配列」のaccessorsの出力内容設定
        # 引数に設定した値は以下の通り。
        # bufferView   ：「構成面座標連番配列」のaccessorsが参照するbufferViewsのインデックスを指定する。「構成面座標連番配列のaccessors」ではbufferViewの0個目を参照する。
        # componentType： エレメントを「符号なし16bit整数」とセットする。
        # count        ： データの個数を「構成面座標連番配列」の長さにセットする。
        # type         ： データ型を「スカラー値」にセットする。
        # max          ：「構成面座標連番配列」のaccessor内の値の範囲の最大値を「構成面座標連番配列」から取得してセットする。
        # min          ：「構成面座標連番配列」のaccessor内の値の範囲の最小値を「構成面座標連番配列」から取得してセットする。
        target_gltf_accessors_list = [pgl.Accessor(bufferView=0, componentType=pgl.UNSIGNED_INT, count=result_coordinates_combination_array.size, type=pgl.SCALAR, max=[
                                                   int(result_coordinates_combination_array.max())], min=[int(result_coordinates_combination_array.min())])]

        # 「座標情報配列」のaccessorsの出力内容設定
        # 引数に設定した値は以下の通り。
        # bufferView   ：「座標情報配列」のaccessorsが参照するbufferViewsのインデックスを指定する。「座標情報配列のaccessors」ではbufferViewの1個目を参照する。
        # componentType： componentTypeでエレメントを「32bit浮動小数点」とセットする。
        # count        ： countでデータの個数を「座標情報配列」の長さにセットする。
        # type         ： データ型をXYZの「3次元ベクトルデータ」にセットする。
        # max          ：「座標情報配列」のaccessor内の値の範囲の最大値を「座標情報配列」から取得してセットする。
        # min          ：「座標情報配列」のaccessor内の値の範囲の最小値を「座標情報配列」から取得してセットする。
        target_gltf_accessors_list.append(pgl.Accessor(bufferView=1, componentType=pgl.FLOAT, count=len(
            target_points_array), type=pgl.VEC3, max=target_points_array.max(axis=0).tolist(), min=target_points_array.min(axis=0).tolist()))

        target_gltf_accessors_list.append(pgl.Accessor(bufferView=2, componentType=pgl.UNSIGNED_INT, count=len(
            result_feature_id_array), type=pgl.SCALAR, max=[int(result_feature_id_array.max())], min=[int(result_feature_id_array.min())]))

        # bufferView3,4追加
        # 頂点法線のxyz座標
        target_gltf_accessors_list.append(pgl.Accessor(bufferView=3, componentType=pgl.FLOAT, count=len(
            target_vertex_normal_array), type=pgl.VEC3, max=target_vertex_normal_array.max(axis=0).tolist(), min=target_vertex_normal_array.min(axis=0).tolist()))

        ######################################################################################################
        # ＜２－４．meshesの出力内容設定＞
        ######################################################################################################

        # 引数に設定した値は以下の通り。
        # attributes：「座標情報配列」の参照先をaccessors配列の1番目を参照するようにセット
        # indices   ：「構成面座標連番配列」の参照先をaccessors配列の0番目にセット
        target_gltf_meshes_list = get_target_gltf_meshes_list(feature_counts)

        ######################################################################################################
        # ＜２－５．nodesの出力内容設定＞
        ######################################################################################################

        # nodes配列の参照先をmeshes配列の0番目にセットする。
        target_gltf_nodes_list = [pgl.Node(mesh=0, matrix=matrix_list)]

        ######################################################################################################
        # ＜２－６．scenesの出力内容設定＞
        ######################################################################################################

        # scenes配列の参照先をnodes配列の0番目のにセットする。
        target_gltf_scenes_list = [pgl.Scene(nodes=[0])]

        ######################################################################################################
        # ＜２－７．sceneの出力内容設定＞
        ######################################################################################################
        # sceneの出力内容設定
        target_gltf_scene = 0

        ######################################################################################################
        # 属性値
        ######################################################################################################
        # 属性値取得
        column_name_list, datatype_list, data_list = get_dataframe_information(
            all_attribute_dataframe)

        # 属性を設定 buffer = 1から順番 長さに関する情報はあらかじめ生成しておくここは設定するだけ
        for di in range(len(data_list)):

            attribute_binary_length, offset_array, attribute_uri = get_buffer_attribute(
                data_list[di])

            # バッファ分け
            target_gltf_buffers_list.append(pgl.Buffer(
                byteLength=attribute_binary_length, uri=attribute_uri))

            # 属性のbufferView設定
            target_gltf_bufferViews_list.append(pgl.BufferView(buffer=di*2+1, byteLength=attribute_binary_length
                                                               ))

            offset_uri, offset_binary_length = get_binary_information(
                offset_array)

            # バッファ分け
            target_gltf_buffers_list.append(pgl.Buffer(byteLength=offset_binary_length, uri=DDC.DATA_URI_HEADER + offset_uri
                                                       ))

            # オフセットのbufferView設定
            target_gltf_bufferViews_list.append(pgl.BufferView(buffer=di*2+2, byteLength=offset_binary_length, target=pgl.ELEMENT_ARRAY_BUFFER
                                                               ))

        # -----------------------------------------------------------------------------------------------------------
        # EXT_structural_metadataのschema部分作成
        # -----------------------------------------------------------------------------------------------------------
        classes_dict = create_ext_structural_metadata_schema_dict(
            column_name_list, datatype_list)

        # -----------------------------------------------------------------------------------------------------------
        # EXT_structural_metadataのpropertyTables部分作成
        # -----------------------------------------------------------------------------------------------------------

        # classとcountも追加

        properties_table_list = create_ext_structural_metadata_properties_list(
            column_name_list, datatype_list, feature_counts)

        # -----------------------------------------------------------------------------------------------------------
        # 【３．glTFデータクラスオブジェクト作成】
        # -----------------------------------------------------------------------------------------------------------

        # 各要素の配列からgltfデータクラスオブジェクトを作成する。
        target_gltf_object = pgl.GLTF2(scene=target_gltf_scene,
                                       scenes=target_gltf_scenes_list,
                                       nodes=target_gltf_nodes_list,
                                       meshes=target_gltf_meshes_list,
                                       accessors=target_gltf_accessors_list,
                                       bufferViews=target_gltf_bufferViews_list,
                                       buffers=target_gltf_buffers_list,
                                       extensionsUsed=[
                                           "EXT_mesh_features", "EXT_structural_metadata"],
                                       extensions={"EXT_structural_metadata": {
                                           "schema": classes_dict, "propertyTables": properties_table_list}}
                                       )

        return target_gltf_object

    else:
        return None


def split_geometry_points_dict(target_geometries_points, all_key_list=None):
    # ---------------------------------------------------------------
    # ジオメトリの座標Dictionary（key：地物ID、value：ジオメトリの座標を格納したnumpy.ndarray）を取得
    # ---------------------------------------------------------------

    result_dict = {}

    # 地物ID、地物IDが最初に表れるインデックス番号、同一地物IDの個数（座標数）を取得
    feature_key_list, first_index_list, key_count_list = np.unique(
        target_geometries_points[:, 0], return_index=True, return_counts=True)

    # 地物IDの数と点数が一致する（＝ジオメトリがポイントである）場合、座標値を1次元配列とする
    if len(feature_key_list) == np.shape(target_geometries_points)[0]:
        result_dict = dict(
            zip(target_geometries_points[:, 0], target_geometries_points[:, 1:]))

    # 上記以外（＝ジオメトリがポイントでない）場合、座標値を2次元配列とする
    else:
        # 対象ジオメトリの座標Dictionaryの作成（内包表記用）
        max_loop_count = len(feature_key_list)

        result_dict = {feature_key_list[i]: target_geometries_points[first_index_list[i]
            : first_index_list[i] + key_count_list[i], 1:] for i in range(max_loop_count)}

    if all_key_list != None:
        # キーリストには存在するが対象ジオメトリに存在しない場合、座標DictionaryにNoneを追加（内包表記）
        result_dict = {
            key: None for key in all_key_list if key not in result_dict}

    return result_dict


def convert_dict_to_point_geometries(geometries_dictionary):
    # ---------------------------------------------------------------
    # dictionaryからポイントジオメトリへ変換
    # 概要　：辞書型配列に格納されているndarryの座標情報から
    # 　　　　shaplyのジオメトリオブジェクトを作成しリストで返す。
    # 引数　：座標情報を格納した辞書型配列
    # 戻り値：辞書型配列にキーとラインジオメトリを１組としたリスト
    # ---------------------------------------------------------------

    geometries_list = []

    geometries_list = [[keys, Point(geometries_dictionary[keys])]
                       for keys in geometries_dictionary]

    return geometries_list


def convert_dict_to_linestring_geometries(geometries_dictionary):
    # ---------------------------------------------------------------
    # dictionaryからラインジオメトリへ変換
    # 概要　：辞書型配列に格納されているndarryの座標情報から
    # 　　　　shaplyのジオメトリオブジェクトを作成しリストで返す。
    # 引数　：座標情報を格納した辞書型配列
    # 戻り値：辞書型配列にキーとラインジオメトリを１組としたリスト
    # ---------------------------------------------------------------

    geometries_list = []

    geometries_list = [[keys, LineString(
        geometries_dictionary[keys])] for keys in geometries_dictionary]

    return geometries_list


def convert_dict_to_polygon_geometries(geometries_dictionary):
    # ---------------------------------------------------------------
    # dictionaryからポリゴンジオメトリへ変換
    # 概要　：辞書型配列に格納されているndarryの座標情報から
    # 　　　　shaplyのジオメトリオブジェクトを作成しリストで返す。
    # 引数　：座標情報を格納した辞書型配列
    # 戻り値：辞書型配列にキーとラインジオメトリを１組としたリスト
    # ---------------------------------------------------------------

    geometries_list = []

    geometries_list = [[keys, Polygon(geometries_dictionary[keys])]
                       for keys in geometries_dictionary]

    return geometries_list


def get_attribute_coordinates_by_geometry_items(coordinates_array, geometry_type):
    # ---------------------------------------------------------------
    # ジオメトリ項目ファイルによる属性と構成点データの並び替え
    # ---------------------------------------------------------------

    coordinates_dict = split_geometry_points_dict(coordinates_array[:, :4])

    # 各ジオメトリタイプごとにshapelyのListを作成
    if geometry_type == "+1":
        coordinates_shapely_array = np.array(
            convert_dict_to_point_geometries(coordinates_dict))[:, 1]

    elif geometry_type == "+2":
        coordinates_shapely_array = np.array(
            convert_dict_to_linestring_geometries(coordinates_dict))[:, 1]

    elif geometry_type == "+3":
        coordinates_shapely_array = np.array(
            convert_dict_to_polygon_geometries(coordinates_dict))[:, 1]

    else:
        pass

    return coordinates_shapely_array


def get_gradient(target_array):
    # ---------------------------------------------------------------
    # 構成点間の勾配（傾き）を計算する 単位は%で返す
    # 連続する同一構成点は考慮していないためxy平面上のでの距離が0の場合numpyでwarningが発生してしまう
    # 引数1：xyz座標（[構成点1, 構成点2, …, 構成点n]、構成点n = [xn, yn, zn]）
    # 戻り値：勾配の一次元配列（[勾配1, 勾配2, … 勾配n]、勾配n = 構成点n-1と構成点nがなす勾配）
    # ---------------------------------------------------------------

    # xy座標のみスライス
    target_xy_array = target_array[:, 0:2]

    # z値のみスライス
    target_z_array = target_array[:, 2:]

    # 次の構成点のz値配列
    target_z_array_next = np.roll(target_z_array, -1, axis=0)

    # xy平面上の構成点間距離
    target_xy_distance_array = get_distance(target_xy_array)

    # 次点とのz値の差
    target_z_difference_array = target_z_array_next - target_z_array

    # 勾配を計算するために1次元配列に変換
    target_z_difference_array = target_z_difference_array.flatten()

    # 最後の要素は始点と終点の勾配になっているため不要 最後の要素以外をスライスで抽出
    target_z_difference_array = target_z_difference_array[0:
                                                          target_z_difference_array.size - 1]

    # 分母が極端に小さい場合（0に近い）にゼロ割り算を回避するための処理。
    # target_xy_distance_arrayの各要素が0なら1e-10を代入し、安全な計算を保証する。
    target_xy_distance_array = np.where(
        (target_xy_distance_array == 0), 1e-10, target_xy_distance_array)

    # xy平面上の距離でz値の差を除算して勾配を算出
    return target_z_difference_array / target_xy_distance_array


def get_distance(target_array):
    # ---------------------------------------------------------------
    # 構成点間の距離を計算する 連続する同一構成点は考慮していない
    # 引数1：xy座標 or xyz座標（[構成点1, 構成点2, …, 構成点n]、構成点n = [x, y] あるいは[x, y, z]）
    # 戻り値：構成点間の距離一次元配列（[距離1, 距離2, … 距離n]、距離n = 構成点n-1から構成点nまでの距離）
    # ---------------------------------------------------------------

    # 次の構成点の配列（最後の要素は始点）
    target_array_next = np.roll(target_array, -1, axis=0)

    # xy間の距離を算出
    # 算出ロジック：xy座標の差→差の2乗→和→平方根
    target_distance_array = np.sqrt(
        np.sum(np.power(target_array_next - target_array, 2), axis=1))

    # 最後の要素は始点と終点の距離になっているため不要 最後の要素以外をスライスで抽出
    target_distance_array = target_distance_array[0:target_distance_array.size - 1]

    return target_distance_array


def get_distance_to_start_end_point(target_array, index):
    # ---------------------------------------------------------------
    # 指定したインデックスから始点、終点までの距離の和を計算する
    # 例：点Aから点Dの配列で点Bを指定する 戻り値は以下のようになる
    #    [-(点B→点Aまでの距離),0(点B→点B),点B→点Cまでの距離,点B→点C→点Dまでの距離]
    #    [-1 ,0 ,3 ,7]
    # 引数1：xy座標 or xyz座標（[構成点1, 構成点2, …, 構成点n]、構成点n = [x, y] あるいは[x, y, z]）
    # 戻り値：構成点間の距離一次元配列（[距離1, 距離2, … 距離n]、距離n = 構成点n-1から構成点nまでの距離）
    # ---------------------------------------------------------------

    # 戻り値初期配列 構成点の数分距離の和が発生する
    result_distance_to_start_end_point_array = np.zeros(len(target_array))

    # 構成点間の距離取得
    target_distance_array = get_distance(target_array)

    # 始点から指定インデックスまでの距離の総和→こちらはすべて負
    distance_from_start_to_index_array = - \
        np.array([np.sum(target_distance_array[i:index])
                 for i in range(0, index)])

    # 戻り値用配列に更新
    result_distance_to_start_end_point_array[0:
                                             index] = distance_from_start_to_index_array

    # 指定インデックスから終点までの距離の総和
    distance_from_index_to_end_array = np.array(
        [np.sum(target_distance_array[index:i]) for i in range(index, len(target_array))])

    # 戻り値用配列に更新
    result_distance_to_start_end_point_array[index:
                                             ] = distance_from_index_to_end_array

    return result_distance_to_start_end_point_array


def get_estimation_Z(target_ndarray, gradient_indices=[0, -1], estimate_target_indices=[]):
    # ---------------------------------------------------------------
    # 推定標高付与処理→デフォルトのまま使用すると始点終点
    # 引数1：推定標高対象地物の2次元配列 xyz座標
    # 引数2：傾きと切片を計算する2点のインデックス配列（要素数は2）
    # 引数3：推定標高対象構成点のインデックス配列
    # 戻り値：推定標高対象地物の2次元配列
    # ---------------------------------------------------------------

    # 2点の地物の場合推定標高せず（できない）そのまま返す
    if len(target_ndarray) == 2:

        return target_ndarray

    else:

        # xyの長さは構成点間の距離の合計
        distance = np.sum(get_distance(target_ndarray[:, :2]))
        # 始点と終点のZ値の差を割って傾きを算出
        gradient = (target_ndarray[gradient_indices[1], 2] -
                    target_ndarray[gradient_indices[0], 2]) / distance

        # 切片を引数2の1点目のz座標とする この点を基準点とする
        # 基準点のインデックス
        standard_point_index = gradient_indices[0]

        # 基準点のz座標
        slice = target_ndarray[standard_point_index][2]

        # 推定標高処理対象構成点が基準点とどれだけ距離があるか算出する
        distance_array = get_distance_to_start_end_point(
            target_ndarray[:, :2], standard_point_index)

        # 傾きをかけ切片を加えてZ値とする
        if len(estimate_target_indices) == 0:

            # 推定標高対象を指定していない場合、中間点（始点終点以外）を更新する
            estimate_z_value_array = distance_array[1:-1] * gradient + slice

            # 更新用に配列を変換
            estimate_z_value_array = estimate_z_value_array.reshape(
                len(estimate_z_value_array), 1)

            # Zを更新
            target_ndarray[1:-1, 2:] = estimate_z_value_array
        else:

            # 推定標高対象を指定している場合、引数で指定された構成点に対して推定標高を行う
            estimate_z_value_array = distance_array[estimate_target_indices] * \
                gradient + slice

            # 更新用に配列を変換
            estimate_z_value_array = estimate_z_value_array.reshape(
                len(estimate_z_value_array), 1)

            # Zを更新
            target_ndarray[estimate_target_indices,
                           2:] = estimate_z_value_array

    return target_ndarray


def calculate_and_convert_unique_origin_indices(coordinates_array,
                                                mesh_array):
    # -----------------------------------------------------------------------------------------------------------
    # 概要   : ジオメトリの存在範囲からユニークな原点インデックスを計算し、メッシュ単位に変換
    # 引数   : coordinates_array - ジオメトリの座標配列
    # 　　   : mesh_array        - meshのサイズを表す配列
    # 戻り値 : unit_origin_array - ジオメトリの原点座標
    # 　　　 : unit_index_array  - メッシュ単位のユニークな原点インデックス配列
    # -----------------------------------------------------------------------------------------------------------

    # 座標から指定レベルの図郭左上の座標を取得する
    unit_origin_array = get_origin_point_from_coordinates_array(
        coordinates_array[:, 1:3], mesh_array)

    # 重複する座標を削除
    unit_origin_array = np.unique(unit_origin_array, axis=0)

    # 存在する図郭の原点インデックス配列
    unit_index_array = unit_origin_array / mesh_array

    return unit_origin_array, unit_index_array


def extract_output_target(all_attribute_dataframe, feature_bool,
                          coordinates_id_array, coordinates_dict):
    # -----------------------------------------------------------------------------------------------------------
    # 概要  : 出力対象抽出
    # 引数  : all_attribute_dataframe　- 全属性項目をまとめたデータフレーム
    # 　　  : feature_bool             - 出力対象があるかないかのbool型
    # 　　  : coordinates_id_array     - 座標配列のID配列
    # 　　  : coordinates_dict         - IDに対しての座標配列
    # 戻り値: target_attribute_dataframe - all_attribute_dataframeから、出力対象だけまとめたデータフレーム
    # 　　　: target_coordinates_array   - 出力対象のcoordinates_array
    # -----------------------------------------------------------------------------------------------------------

    # feature_bool に基づいて、出力対象の行のみを選択
    # インデックスをリセットして、新しいデータフレームを作成
    target_attribute_dataframe = all_attribute_dataframe[feature_bool].reset_index(
        drop=True)

    # 出力対象のIDに基づいて座標配列を抽出
    # feature_bool に基づいて、出力対象のID配列を選択
    # ID配列に対応する座標配列をcoordinates_dictから取得し、全てを一つの配列に結合
    target_id_array = coordinates_id_array[feature_bool].copy()
    target_coordinates_array = np.concatenate(
        [coordinates_dict[target_id_array[i]] for i in range(len(target_id_array))])

    # 出力対象の座標配列のIDを再整列する
    # 座標配列の最初の列（ID列）のユニークな値とその出現回数を取得
    # 各IDが出現する回数に応じて、再度ID配列を生成し、ID列を更新
    target_unique_id_array, target_unique_id_counts_array = np.unique(
        target_coordinates_array[:, 0], return_counts=True)
    target_unique_id_array = np.concatenate(
        [[i]*target_unique_id_counts_array[i] for i in range(len(target_unique_id_counts_array))])
    target_coordinates_array[:, 0] = target_unique_id_array

    return target_attribute_dataframe, target_coordinates_array


def append_gltf_uri_string_to_uri_list(geometry_dwh_file_name_list, unit_index_array,
                                       unit_origin_array_index, gltf_directory_path, uri_list):
    # ---------------------------------------------------------------------------------------------
    # 概要   : 出力することが決まればgltf、tileset.jsonに設定する値を生成し、uri設定用パス格納する。
    # 引数   : geometry_dwh_file_name_list   -データ定義ファイルで定義された、geometryのファイル名が格納されたlist
    # 　　   : unit_index_array              -存在する図郭の原点インデックス配列
    # 　　   : unit_origin_array_index       -存在する図郭の原点配列（左上）
    # 　　   : uri_list                      -tileset.json用gltf相対パス格納用list
    # 戻り値 : gltf_file_name                -拡張子のついていない、ファイル名
    # 　　　 : uri_list                      -tileset.json用gltf相対パス格納用list
    # ---------------------------------------------------------------------------------------------

    # geometryのファイル名に該当の数字をつける。
    gltf_file_name = geometry_dwh_file_name_list[0].split("/")[0] + "_" + str(int(
        unit_index_array[unit_origin_array_index, 0])) + "_" + str(int(unit_index_array[unit_origin_array_index, 1]))

    # ディレクトリに何が保存されるのかの相対パス
    gltf_uri_string = gltf_directory_path + "/" + gltf_file_name + ".gltf"

    # uri_listに格納
    uri_list.append(gltf_uri_string)

    return gltf_file_name, uri_list


def get_xy_center_point(unit_origin_array, unit_origin_array_index,
                        mesh_array, target_coordinates_array):
    # ---------------------------------------------------------------------------------------------
    # 概要   :図郭のxy中心点取得（原点とする点のこと）
    # 引数   :unit_origin_array        -存在する図郭の原点配列（左上）
    # 　　   :unit_origin_array_index  -unit_origin_arrayの該当のインデックス
    # 　　   :mesh_array               -mesh配列
    # 　　   :target_coordinates_array -出力対象の座標配列
    # 戻り値 :unit_min_x               -x軸の最小の点
    # 　　　 :unit_max_x               -x軸の最大の点
    # 　　　 :unit_min_y               -y軸の最小の点
    # 　　　 :unit_max_y               -y軸の最大の点
    # 　　　 :target_min_z             -target_coordinates_arrayのZ軸の最小の値
    # 　　　 :target_max_z             -target_coordinates_arrayのZ軸の最大の値
    # 　　　 :center_point             -図郭のxy中心点
    # ---------------------------------------------------------------------------------------------

    # 図郭の座標を抽出
    unit_min_x, unit_max_x, unit_min_y, unit_max_y\
        = get_unit_min_max(unit_origin_array[unit_origin_array_index],
                           mesh_array)

    # 出力対象の配列からZ軸の最小値を取得
    target_min_z = np.min(target_coordinates_array[:, 3])

    # 出力対象の配列からZ軸の最大値を取得
    target_max_z = np.max(target_coordinates_array[:, 3])

    # 図郭の中心点を作成
    center_point = np.array([(unit_min_x + unit_max_x) / 2,
                             (unit_min_y + unit_max_y) / 2,
                             (target_min_z + target_max_z) / 2])

    return unit_min_x, unit_max_x, unit_min_y, unit_max_y, \
        target_min_z, target_max_z, center_point


def generate_bounding_box_with_range_of_radians(transformer_object, unit_min_x, unit_min_y, unit_max_x, unit_max_y,
                                                bounding_volume_list, target_min_z, target_max_z):
    # ---------------------------------------------------------------------------------------------
    # 概要   : boundingbox範囲ラジアンで生成する関数
    # 引数   : transformer_object   -pyprojオブジェクト
    # 　　   : unit_min_x           -x軸の最小の点
    # 　　   : unit_min_y           -y軸の最小の点
    # 　　   : unit_max_x           -x軸の最大の点
    # 　　   : unit_max_y           -y軸の最大の点
    # 　　   : bounding_volume_list -tileset.json用boundingVolume格納用list
    # 　　   : target_min_z         -target_coordinates_arrayのZ軸の最小の値
    # 　　   : target_max_z         -target_coordinates_arrayのZ軸の最大の値
    # 戻り値 : bounding_volume_list -tileset.json用boundingVolume格納用list
    # ---------------------------------------------------------------------------------------------

    # 入力された最小・最大の座標（x軸、y軸）を用いて緯度経度に変換する
    unit_min_lon, unit_min_lat = transformer_object.transform(
        unit_min_x, unit_min_y)
    unit_max_lon, unit_max_lat = transformer_object.transform(
        unit_max_x, unit_max_y)

    # 緯度経度の値を度からラジアンに変換する
    unit_min_lon_rad = np.deg2rad(unit_min_lon)
    unit_min_lat_rad = np.deg2rad(unit_min_lat)
    unit_max_lon_rad = np.deg2rad(unit_max_lon)
    unit_max_lat_rad = np.deg2rad(unit_max_lat)

    # ラジアン単位でのbounding boxとZ軸の範囲をbounding_volume_listに追加する
    bounding_volume_list.append([unit_min_lon_rad, unit_min_lat_rad, unit_max_lon_rad, unit_max_lat_rad, target_min_z, target_max_z
                                 ])

    return bounding_volume_list


def parallel_shift_of_coordinates(target_coordinates_array, center_point):
    # ---------------------------------------------------------------------------------------------
    # 概要   : 座標平行移動する関数
    # 引数   : target_coordinates_array -出力対象の座標配列
    # 　　   : center_point             -図郭のxy中心点
    # 戻り値 : target_coordinates_array -平行移動された座標配列
    # ---------------------------------------------------------------------------------------------

    # target_coordinates_arrayの座標のx, y, z成分を、center_pointを基準に平行移動する
    # center_point から各座標を引くことで、中心点が原点（0,0,0）に移動した座標を得る
    target_coordinates_array[:,
                             1:4] = target_coordinates_array[:, 1:4] - center_point

    # nanを0に置き換える
    target_coordinates_array = np.nan_to_num(target_coordinates_array)

    return target_coordinates_array


def convert_gltf_to_json_and_format_with_windows_newline(gltf_data):
    # ---------------------------------------------------------------------------------------------
    # 概要   : gltfをflowfileに送るために、json形式の文字列型にし、改行文字を{\n}から{\r\n}に変える関数。
    # 引数   : gltf_data                               -マルチパッチをgltfにしたデータ
    # 戻り値 : target_gltf_object_json_string_windows  -json形式で改行文字もwindows用に変更したデータ
    # ---------------------------------------------------------------------------------------------

    # flowfileにデータを送るために、json形式にする。
    target_gltf_object_json = gltf_data.to_json()

    # インデントと、区切り文字の整形を行う。これによってデータが１行で表さないようになる。
    target_gltf_object_json_formatted = json.dumps(json.loads(
        target_gltf_object_json), indent=2, separators=(",", ": "))

    # このままでは、改行文字がwindows用ではないので変更。
    target_gltf_object_json_string_windows = target_gltf_object_json_formatted.replace(
        "\n", "\r\n")

    return target_gltf_object_json_string_windows


def create_target_dict_with_uri_and_bounding_volume(uri_list,
                                                    bounding_volume_list_index,
                                                    bounding_volume_list):
    # ---------------------------------------------------------------------------------------------
    # 概要   :target_dictを作る関数
    # 引数   :uri_list                    -uri設定用パスが格納されたlist
    # 　　   :bounding_volume_list_index  -該当のbounding_volume_listのインデックス
    # 　　   :bounding_volume_list        -tileset.json用boundingVolume格納用List
    # 戻り値 :target_dict                 -#各URIとそのboundingVolume設定の範囲を追加されたdict
    # ---------------------------------------------------------------------------------------------

    # uriと範囲を持つ辞書型配列作成
    target_dict = {"uri": uri_list[bounding_volume_list_index], "boundingVolume": {"region": bounding_volume_list[bounding_volume_list_index]}
                   }

    return target_dict


def create_children_dict_with_bounding_volume_and_contents(region_list,
                                                           contents_list):
    # ---------------------------------------------------------------------------------------------
    # 概要   :children_dictを作る関数
    # 引数   :region_list   - 全体の範囲が格納されたlist
    # 　　   :contents_list - target_dictが格納されたlist
    # 戻り値 :children_dict - tileset_dictで使われるdict
    # ---------------------------------------------------------------------------------------------

    children_dict = {
        "boundingVolume": {
            "region": region_list
        },
        "geometricError": 512.0,
        "contents": contents_list
    }
    return children_dict


def create_tileset_dict(region_list,
                        children_list):
    # ---------------------------------------------------------------------------------------------
    # 概要  :tileset_dictを作る関数
    # 引数  :region_list    - 全体の範囲が格納されたlist
    # 　　  :children_list  - 範囲追加
    # 戻り値:tileset_dict   - tileset_jsonにするためのデータ
    # ---------------------------------------------------------------------------------------------
    tileset_dict = {"asset": {"version": "1.1"},
                    "root": {"boundingVolume": {"region": region_list},
                             "geometricError": 512.0,
                             "refine": "REPLACE",
                             "children": children_list}}
    return tileset_dict


def calculate_bounding_region_from_volume_list(bounding_volume_list):
    # ---------------------------------------------------------------------------------------------
    # 概要   :全体の範囲求める関数
    # 引数   :bounding_volume_list -tileset.json用boundingVolume格納用list
    # 戻り値 :region_list          -全体の範囲が格納されたlist
    # ---------------------------------------------------------------------------------------------

    # bounding_volume_list を NumPy の配列に変換
    bounding_volume_array = np.array(bounding_volume_list)

    # 各軸の最大値、最小値を求め、listにまとめる
    region_list = [np.min(bounding_volume_array[:, 0]),
                   np.min(bounding_volume_array[:, 1]),
                   np.max(bounding_volume_array[:, 2]),
                   np.max(bounding_volume_array[:, 3]),
                   np.min(bounding_volume_array[:, 4]),
                   np.max(bounding_volume_array[:, 5])]

    return region_list


def get_data_definition_list(flowfile):
    # ------------------------------------------------------------------------------------------------------------
    # 概要   : flowfileからCSV形式のfieldsetfileを受け取り、データフレームに加工したデータを取得し、attributeから、データ定義ファイルを取得する関数。
    # 引数   : flowfile　           - プロセッサなのかに入ってくるデータ
    # 戻り値 : field_set_data_frame - field_set_fileをデータフレームに加工した物
    # 　　　 : data_definition_list - データ定義ファイルがシリアライズ、Base64でエンコード、されたものが格納されたlist
    # ------------------------------------------------------------------------------------------------------------

    # flowfileからCSV形式のfield_set_fileを取得し、データフレームに加工
    field_set_data_frame = pd.read_csv(io.StringIO(
        flowfile.getContentsAsBytes().decode("utf-8")))

    attribute_dict = flowfile.getAttributes()

    # 値を追加するためのリスト
    data_definition_list = []

    # 辞書のキーに指定の文字が含まれているか確認
    for key, value in attribute_dict.items():
        if "DataDefinition" in key:
            if value:
                data_definition_list.append(value)

            else:
                pass

    return field_set_data_frame, data_definition_list


def create_coordinates_id_array_and_dict_from_coordinates_array(field_set_file_data_frame,
                                                                geometry_dwh_file_name_list):

    # -----------------------------------------------------------------------------------------------------------
    # 概要   : ジオメトリnpy取得
    # 引数   : field_set_file_data_frame        - field_set_fileをデータフレームに加工した物
    # 　　   : geometry_dwh_file_name_list      - データ定義ファイルで定義されたgeometryのdwhファイル名が格納されたlist
    # 戻り値 : geometry_value_coordinates_array - field_set_fileの中に格納されていたgeometryの配列
    # 　　　 : coordinates_id_array             - 座標配列のID配列
    # 　　　 : coordinates_dict                 - IDに対しての座標配列
    # -----------------------------------------------------------------------------------------------------------

    # field_set_data_frameから、マルチパッチ後のcoordinates_arrayを抜き出す
    geometry_value_coordinates_array\
        = pickle.loads(base64.b64decode(field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == geometry_dwh_file_name_list[0], "Value"].values[0]))

    # ユニークIDの配列を作成
    coordinates_id_array\
        = np.unique(geometry_value_coordinates_array[:, 0])

    # IDに対しての二次元配列を作成
    coordinates_dict\
        = {coordinates_id_array[i]: geometry_value_coordinates_array[list(np.where(geometry_value_coordinates_array[:, 0] == coordinates_id_array[i])[0])] for i in range(len(coordinates_id_array))}

    return geometry_value_coordinates_array, \
        coordinates_id_array, \
        coordinates_dict


def get_target_point_gltf_meshes_list(geometry_number):

    # --------------------------------------------------------------
    # mesh（座標値、頂点法線、地物IDの情報をつかさどる部分の設定）
    # --------------------------------------------------------------
    meshes_list = []

    # "primitives"をキーとする辞書型配列
    primitives_dict = {}

    primitives_dict["primitives"] = [
        {"attributes": {"POSITION": 0}, "mode": 0}]

    meshes_list.append(primitives_dict)

    return meshes_list


def create_point_gltf_object(temporary_xyz_array,
                             attribute_dataframe,
                             matrix_list=None):

    # -----------------------------------------------------------------------------------------------------------
    # 関数名      ：glTF出力データクラスオブジェクト作成処理
    # 第１引数    ：【XYZ】の順に座標情報が格納された2次元のndarray配列
    # 戻り値      ：glTFのデータクラスオブジェクト
    # 処理概要    ：XYZの配列から立体の面を構成するための座標同士の結びつきの情報と、
    #              XZYの並びに変換した座標情報をもとにglTFに出力するデータ配列を作成する。
    # 【特記事項】：本処理を実行すると64bit浮動小数点が32bit浮動小数点に変換されます。
    #              そのため小数点第7位未満が切り捨てられます。
    # -----------------------------------------------------------------------------------------------------------
    # 引数チェック
    # 入力引数が何もなければ何も処理を行わない。
    if len(temporary_xyz_array) > 0:
        result_coordinates_array = temporary_xyz_array.copy()
        feature_counts = len(result_coordinates_array)

        # -----------------------------------------------------------------------------------------------------------
        # 【１．glTFの出力データ作成の準備処理】
        # ※入力引数をもとにglTFの仕様に合わせてデータ変換を行い、
        # 立体の面を構成するための座標の組み合わせの配列である「構成面座標連番配列」を作成し
        # glTFの仕様に合わせて座標の並びをXZYの順に変換した「座標情報配列」を作成する。
        # -----------------------------------------------------------------------------------------------------------

        # glTFの仕様で浮動小数点の値は32bitのデータ型でしか持てないため、
        # 「座標情報配列」を32bit浮動小数に変換し、バイナリデータ化する。
        target_points_array = result_coordinates_array[:, [
            2, 3, 1]].astype(np.float32)
        target_points_binary_blob, \
            target_points_binary_length\
            = get_binary_information(target_points_array)

        # -----------------------------------------------------------------------------------------------------------
        # 【２．glTFの出力データ配列作成】
        # -----------------------------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------------------------------------
        # ＜２－１．buffersの出力内容設定＞
        # -----------------------------------------------------------------------------------------------------------

        # バイナリデータのデータ長を「構成面座標連番配列」と「座標情報配列」の長さの合計とする。
        # 「地物ID配列」、「頂点法線座標連番配列」、「頂点法線座標情報配列」
        target_gltf_buffers_list = [pgl.Buffer(byteLength=target_points_binary_length,
                                               uri=DDC.DATA_URI_HEADER+target_points_binary_blob
                                               )]

        # -----------------------------------------------------------------------------------------------------------
        # ＜２－２．bufferViewsの出力内容設定＞
        # -----------------------------------------------------------------------------------------------------------
        # 「座標情報配列」のbufferViewsを設定する。
        # 引数に設定した値は以下の通り。
        # buffer    ：「座標情報配列」のbufferViewsが参照するbufferのインデックスを指定する。
        # byteOffset：「座標情報配列」のバイナリデータ読み込み開始位置を「構成面座標連番配列」のデータ領域の後にセットする。
        # byteLength：buffersのデータ領域のうち、「座標情報配列」のデータ領域を「座標情報配列」のバイナリデータの長さとする。
        # target    ：「配列バッファ」を設定する。（配列バッファ：glTFで「座標情報配列」をバイナリデータに変換した値を格納する領域）
        target_gltf_bufferViews_list = [pgl.BufferView(buffer=0,
                                                       byteLength=target_points_binary_length,
                                                       target=pgl.ARRAY_BUFFER)]

        # -----------------------------------------------------------------------------------------------------------
        # ＜２－３．accessorsの出力内容設定＞
        # -----------------------------------------------------------------------------------------------------------

        # 「座標情報配列」のaccessorsの出力内容設定
        # 引数に設定した値は以下の通り。
        # bufferView   ：「座標情報配列」のaccessorsが参照するbufferViewsのインデックスを指定する。「座標情報配列のaccessors」ではbufferViewの1個目を参照する。
        # componentType：componentTypeでエレメントを「32bit浮動小数点」とセットする。
        # count        ：countでデータの個数を「座標情報配列」の長さにセットする。
        # type         ：データ型をXYZの「3次元ベクトルデータ」にセットする。
        # max          ：「座標情報配列」のaccessor内の値の範囲の最大値を「座標情報配列」から取得してセットする。
        # min          ：「座標情報配列」のaccessor内の値の範囲の最小値を「座標情報配列」から取得してセットする。
        target_gltf_accessors_list = [pgl.Accessor(bufferView=0,
                                                   componentType=pgl.FLOAT,
                                                   count=len(
                                                       target_points_array),
                                                   type=pgl.VEC3,
                                                   max=target_points_array.max(
                                                       axis=0).tolist(),
                                                   min=target_points_array.min(axis=0).tolist())]

        # -----------------------------------------------------------------------------------------------------------
        # ＜２－４．meshesの出力内容設定＞
        # -----------------------------------------------------------------------------------------------------------

        # 引数に設定した値は以下の通り。
        # attributes：「座標情報配列」の参照先をaccessors配列の1番目を参照するようにセット
        target_gltf_meshes_list = get_target_point_gltf_meshes_list(
            feature_counts)

        # -----------------------------------------------------------------------------------------------------------
        # ＜２－５．nodesの出力内容設定＞
        # -----------------------------------------------------------------------------------------------------------

        # nodes配列の参照先をmeshes配列の0番目にセットする。
        target_gltf_nodes_list = [pgl.Node(mesh=0, matrix=matrix_list)]

        # -----------------------------------------------------------------------------------------------------------
        # ＜２－６．scenesの出力内容設定＞
        # -----------------------------------------------------------------------------------------------------------

        # scenes配列の参照先をnodes配列の0番目のにセットする。
        target_gltf_scenes_list = [pgl.Scene(nodes=[0])]

        # -----------------------------------------------------------------------------------------------------------
        # ＜２－７．sceneの出力内容設定＞
        # -----------------------------------------------------------------------------------------------------------

        # sceneの出力内容設定
        target_gltf_scene = 0

        # -----------------------------------------------------------------------------------------------------------
        # 【３．glTFデータクラスオブジェクト作成】
        # -----------------------------------------------------------------------------------------------------------

        # 各要素の配列からgltfデータクラスオブジェクトを作成する。
        target_gltf_object = pgl.GLTF2(scene=target_gltf_scene,
                                       scenes=target_gltf_scenes_list,
                                       nodes=target_gltf_nodes_list,
                                       meshes=target_gltf_meshes_list,
                                       accessors=target_gltf_accessors_list,
                                       bufferViews=target_gltf_bufferViews_list,
                                       buffers=target_gltf_buffers_list
                                       )

        return target_gltf_object

    else:
        return None


def convert_flowfile_to_field_set_file_dataframe(flowfile):
    # ---------------------------------------------------------------
    # flowfileをフィールド集合ファイルのdataframeへ変換
    # 引数:NifiFlowファイルオブジェクト
    # ---------------------------------------------------------------

    # FlowFileからデータを取得プロパティから取得
    input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

    # FlowFileのデータをfile_objectへ変換
    field_set_file_stringio = io.StringIO(input_field_set_file)

    # Dataframeに変換。
    field_set_file_dataframe = pd.read_csv(field_set_file_stringio)

    return field_set_file_dataframe


def get_value_dwh_list_from_field_set_file_dataframe(field_set_file_dataframe,
                                                     dwh_column_name='Dwh',
                                                     type_column_name='Type',
                                                     value_column_name='Value'):

    # フィールド集合ファイルのdataframeからValue列、Dwh列のList取得
    value_list = field_set_file_dataframe[value_column_name].tolist()
    type_list = field_set_file_dataframe[type_column_name].tolist()
    dwh_list = field_set_file_dataframe[dwh_column_name].tolist()

    return value_list, type_list, dwh_list


def get_value_field_from_value_dwh_list(value_list,
                                        dwh_list,
                                        target_dwh_name):
    # ---------------------------------------------------------------
    # 指定Dwh名のValueをValue列、Dwh列のListから取得
    # クラス内のプロパティにアクセスする形のほうがよさそう
    # ---------------------------------------------------------------

    # 指定したDwh名の行数インデックス取得
    target_dwh_index = dwh_list.index(target_dwh_name)

    # value_listの各要素をデコード、デシリアライズし、list or arrayにする。
    target_value = pickle.loads(base64.b64decode(value_list[target_dwh_index]))

    return target_value


def convert_list_to_value(target_list):
    # ---------------------------------------------------------------
    # ValueのListの要素となる文字列をlistから作成する
    # ---------------------------------------------------------------
    serialized_and_encoded_value = base64.b64encode(
        pickle.dumps(target_list)).decode("utf-8")

    return serialized_and_encoded_value


def add_record_to_field_set_file_list(dwh_list,
                                      type_list,
                                      value_list,
                                      add_target_dwh_name_string,
                                      add_target_type_string,
                                      add_target_value_list):
    # ---------------------------------------------------------------
    # Value列、Type列、Dwh列のListに要素を追加
    # クラスに実装するほうがよさそう
    # 元のListはクラスのプロパティにする
    # ---------------------------------------------------------------

    serialized_and_encoded_value = convert_list_to_value(add_target_value_list)

    dwh_list.append(add_target_dwh_name_string)
    type_list.append(add_target_type_string)
    value_list.append(serialized_and_encoded_value)

    return dwh_list, type_list, value_list


def split_string_list(string_list,
                      delimiter):
    # ---------------------------------------------------------------
    # Listに格納された文字列を区切り文字指定でsplitし、numpy配列にする
    # ---------------------------------------------------------------
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

    return target_dwh_list, target_type_list, target_value_list


def set_list_to_field_set_file(dwh_list,
                               type_list,
                               value_list,
                               dwh_column_name='Dwh',
                               type_column_name='Type',
                               value_column_name='Value'):
    # ---------------------------------------------------------------
    # フィールド集合ファイル作成
    # ---------------------------------------------------------------

    # list or array を、シリアライズ、base64でエンコード、バイト列を文字列にしたいのでutf-8でデコード。

    # field_set_file用のdata_dictを作成。
    data_dict = {dwh_column_name: dwh_list,
                 type_column_name: type_list,
                 value_column_name: value_list}

    # data_dictをfield_set_file(CSV形式)に加工。
    result_df = pd.DataFrame(data_dict)
    field_set_file = result_df.to_csv(index=False)

    return field_set_file


def judge_point_in_unit(point_array,
                        unit_origin_array,
                        level_mesh_array):
    # ---------------------------------------------------------------
    # 抽出範囲図郭原点 1次元配列 [x,y]
    # 50000レベルから抽出範囲図郭までの1meshの単位 2次元配列[[x_unit_1,y_unit_1]
    #                                                      ,[x_unit_2,y_unit_2]]
    # ---------------------------------------------------------------
    unit_min_x, \
        unit_max_x, \
        unit_min_y, \
        unit_max_y\
        = get_unit_min_max(unit_origin_array,
                           level_mesh_array)

    # 構成点が図郭内に存在するか
    isin_bool = get_bool_in_rectangle_area(
        unit_min_x, unit_max_x, unit_min_y, unit_max_y, point_array[:, 0], point_array[:, 1])

    return isin_bool


def get_geodataframe_csv(csv_path,
                         encoding,
                         wkt_column_name):

    # ---------------------------------------------------------------
    # csvファイルからgeodataframe取得
    # CSVの場合はWKT列を指定する必要がある→geopandasのgeometry列に設定できないため
    # まずpandasで読み込みwktの列名を指定
    # ---------------------------------------------------------------

    # pandasで読み込み
    csv_dataframe = pd.read_csv(csv_path, encoding=encoding)

    # 属性のカラム取得
    csv_column_name_list = csv_dataframe.columns.to_list()
    attribute_list = []
    [attribute_list.append(
        temp_name) for temp_name in csv_column_name_list if temp_name != wkt_column_name]

    # wktのカラム取得
    wkt_dataframe = csv_dataframe[wkt_column_name]
    geometry_geoseries = gpd.GeoSeries.from_wkt(wkt_dataframe)

    # 属性部分取得（wkt以外）
    attribute_dataframe = csv_dataframe[attribute_list].copy()

    # geopandasにセット
    result_geodataframe\
        = gpd.GeoDataFrame(attribute_dataframe,
                           geometry=geometry_geoseries)

    return result_geodataframe


def get_geodataframe_gpkg(gpkg_path,
                          encoding,
                          layer_name):
    # ---------------------------------------------------------------
    # gpkgファイルからgeodataframe取得
    # ---------------------------------------------------------------
    result_geodataframe\
        = gdf_dataframe = gpd.read_file(gpkg_path,
                                        encoding=encoding,
                                        layer=layer_name)

    return result_geodataframe


def get_geodataframe_shp(shp_path,
                         encoding):
    # ---------------------------------------------------------------
    # shpファイルからgeodataframe取得
    # ---------------------------------------------------------------
    result_geodataframe = gpd.read_file(shp_path,
                                        encoding=encoding)
    return result_geodataframe


def get_geodataframe_geojson(geojson_path,
                             encoding):
    # ---------------------------------------------------------------
    # geojsonからgeodataframe取得
    # ---------------------------------------------------------------
    result_geodataframe = gpd.read_file(geojson_path,
                                        encoding=encoding)
    return result_geodataframe


def get_geodataframe_from_datasource(datasource_path,
                                     encoding,
                                     extension='',
                                     read_args=[]):

    # ---------------------------------------------------------------
    # ①ファイルを参照してgeodataframeに変換する
    # なおデータの種類については拡張子にて判定する
    # 拡張子を外から設定することも可能とする
    # CSVの場合はWKT列を指定する必要がある→geopandasのgeometry列に設定できないため
    # GPKGの場合はlayer名を指定する必要がある→指定しないと一つしか読み込まれないため
    # GPKG出力時もレイヤ名は明記すること

    # 引数1:データファイルパス
    # 引数2:拡張子（「.」付き）
    #      （例：「.csv」 pathlib.Path().suffixで取得することを想定している
    # 引数3:データを読み込む際に必要な引数を格納したList(データファイルパス以外)
    #      CSVの場合だとWKTのカラム名を明記する必要があるのでListにカラム名を格納して渡す
    #      Listはunpackしてメソッドに渡すこと
    # ---------------------------------------------------------------

    # 拡張子設定なしならファイルの拡張子を取得
    if extension == '':
        extension = pathlib.Path(datasource_path).suffix
    else:
        pass

    # 入力対象リストにないファイルは読み込まない
    if extension in DDC.INPUT_FILE_EXTENSION_LIST:
        pass
    else:
        raise Exception('入荷できないファイルです')

    # 拡張子ごとにメソッド切り替え
    if extension == DDC.CSV_EXTENSION:
        # CSVの場合
        target_geodataframe = get_geodataframe_csv(
            datasource_path, encoding, *read_args)

    elif extension == DDC.SHP_EXTENSION:
        # shpファイルの場合
        target_geodataframe = get_geodataframe_shp(datasource_path, encoding)

    elif extension == DDC.GEOJSON_EXTENSION:

        # geojsonの場合
        target_geodataframe = get_geodataframe_geojson(
            datasource_path, encoding)

    elif extension == DDC.GPKG_EXTENSION:

        # GPKGの場合
        target_geodataframe = get_geodataframe_gpkg(
            datasource_path, encoding, *read_args)

    else:
        raise

    return target_geodataframe


def write_datasource_from_geodataframe(geodataframe,
                                       datasource_path,
                                       extension,
                                       encoding,
                                       write_args=[]):

    # ---------------------------------------------------------------
    # ②geodataframeを参照してファイルを出力する
    # 拡張子は指定
    # 引数1:geodataframe
    # 引数2:出力ファイルパス
    # 引数3:拡張子（「.」付き）
    #      （例：「.csv」 pathlib.Path().suffixで取得することを想定している
    # ---------------------------------------------------------------

    # 入力対象リストにないファイルは読み込まない
    if extension in DDC.OUTPUT_FILE_EXTENSION_LIST:
        pass
    else:
        raise Exception('出荷できないファイルです')

    # 拡張子ごとにメソッド切り替え
    if extension == DDC.CSV_EXTENSION:

        # CSVの場合
        geodataframe.to_csv(datasource_path,
                            encoding=encoding,
                            index=False)

    elif extension == DDC.SHP_EXTENSION:

        # shpファイルの場合
        geodataframe.to_file(datasource_path,
                             encoding=encoding)

    elif extension == DDC.GEOJSON_EXTENSION:

        # geojsonの場合
        geodataframe.to_file(datasource_path,
                             encoding=encoding,
                             driver='GeoJSON')

    elif extension == DDC.GPKG_EXTENSION:

        # GPKGの場合
        geodataframe.to_file(datasource_path,
                             encoding=encoding,
                             driver='GPKG',
                             layer=write_args[0])

    else:
        raise


def get_geometry_array(shapely_object, index):

    # ---------------------------------------------------------------
    # shapelyをCoordinates配列に変換
    # ---------------------------------------------------------------
    # geometrytype取得
    geom_type = shapely_object.geom_type

    # Polygonの場合
    if geom_type == 'Polygon':

        geometry_array = np.array(shapely_object.exterior.coords)
        result_array = np.zeros(
            (geometry_array.shape[0], geometry_array.shape[1]+1))
        result_array[:, 1:] = geometry_array
        result_array[:, 0] = index

    # Point,Linestringの場合
    else:
        geometry_array = np.array(shapely_object.coords)
        result_array = np.zeros(
            (geometry_array.shape[0], geometry_array.shape[1]+1))
        result_array[:, 1:] = geometry_array
        result_array[:, 0] = index

    return result_array


def get_geometry_type(shapely_object):

    # ---------------------------------------------------------------
    # shapelyからgeometrytype取得
    # ---------------------------------------------------------------
    geom_type = shapely_object.geom_type

    return geom_type


def get_coordinates_array_from_geodataframe(geodataframe):
    # ---------------------------------------------------------------
    # explodeによるシングル化を行っている想定
    # dataframeに存在するのはshapelyの「Point」,「Linestring」,「Polygon」のみとする
    # multiが存在するdataframeはチェック処理でエラー
    # Collection内許容ジオメトリタイプ
    # →point,linestring,polygon
    # →multi
    # multipatchはgeodataframeにできないので容認しない→multipolygonにpolygon複数格納
    # Collection内にMulti,Collectionは容認しない→再起処理になってしまう
    # Citygml→をそれぞれのジオメトリタイプに分けて格納
    # マルチパッチはgeopandasでは扱えないがどうするか？→Multipolygonに4点のポリゴン詰める
    # ---------------------------------------------------------------
    # ---------------------------------------------------------------
    # geopandasからCoordinates配列を取得
    # ジオメトリのタイプはシングルであり１種しかないものとする
    # ---------------------------------------------------------------
    # ジオメトリのSeries取得→dict
    geometry_column_series = geodataframe[geodataframe.geometry.name]
    geometry_column_dict = geometry_column_series.to_dict()
    index_array = geodataframe.index.to_numpy()

    # ジオメトリの座標配列取得
    geometry_list\
        = list(map(get_geometry_array, geometry_column_dict.values(), index_array))
    result_array = np.concatenate(geometry_list)

    # ジオメトリタイプ取得
    geometry_type_list\
        = list(map(get_geometry_type, geometry_column_dict.values()))

    return result_array, \
        geometry_type_list, \
        index_array


def get_single_geodateframe(geodataframe):
    # ---------------------------------------------------------------
    # マルチジオメトリをシングルジオメトリに変換
    # geodataframe全体のindex(左端の連番)は再附番、もとのindexは属性に保持しておく
    # ---------------------------------------------------------------
    return geodataframe.explode(index_parts=True).reset_index()


def get_start_index_and_end_index(feature_array):

    # ---------------------------------------------------------------
    # Coordinates配列から始点と終点のインデックスをそれぞれ取得
    # ---------------------------------------------------------------
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


def get_shapely_dict_from_coordinates_array(coordinates_array,
                                            geometry_type_list):

    # ---------------------------------------------------------------
    # Coordinates配列とジオメトリタイプ配列からshaplyの辞書型配列を取得
    # この辞書型からvaluesだけを取り出しgeopandasのgeometryに直接設定すると座標を更新可能
    # ---------------------------------------------------------------

    # 座標の始点終点のインデックスを取得
    si, \
        ei = get_start_index_and_end_index(coordinates_array)

    shapely_dict = {}
    # 1地物ごとに座標を取り出してそれぞれのジオメトリタイプによるshapelyのオブジェクトを格納する
    for i in range(len(geometry_type_list)):
        if geometry_type_list[i] == 'Point':
            shapely_dict[coordinates_array[si[i], 0]] = Point(
                coordinates_array[si[i], 1:])
        elif geometry_type_list[i] == 'LineString':
            shapely_dict[coordinates_array[si[i], 0]] = LineString(
                coordinates_array[si[i]:ei[i]+1, 1:])
        else:
            shapely_dict[coordinates_array[si[i], 0]] = Polygon(
                coordinates_array[si[i]:ei[i]+1, 1:])

    return shapely_dict


def get_geodataframe_from_contents_or_field_set_file(flowfile):
    # ---------------------------------------------------------------
    # contentsかフィールド集合ファイルからgeodataframeを取得する
    # ---------------------------------------------------------------

    # インプットの形式がfieldsetfileのとき
    try:
        # Flowfileから、csv形式のfield_set_fileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        # 1行目のデータを変数に格納
        geodataframe = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[0, "Value"]))

    # インプットの形式がシリアライズされたGeoDataFrameのとき
    except Exception as e:

        # FlowFileからシリアライズされたGeoDataFrameのバイトデータを取得し、バイトデータからGeoDataFrameを復元
        geodataframe = pickle.loads(flowfile.getContentsAsBytes())

    return geodataframe


def count_dataframe_values(df, column_names):
    """
    データフレームの列を指定し、その型判定と内容解析結果をテキストにする

    :param df: 解析するデータフレーム
    :type df: pandas.dataframe
    :param column_names: カンマで区切られた列名のリスト
    :type column_names: str
    :return: 解析結果のテキスト
    :rtype: str
    """
    try:
        # タイトルを設定する
        content_txt = "データフレームの指定列の型判定と内容解析結果"

        if column_names:
            # カラム名が指定されている場合
            # カンマで区切りのプロパティの値を分割してリストにする。
            column_list = column_names.split(",")

        else:
            # カラム名が空文字やNoneの場合はgeometry以外の全ての列を処理する。
            column_list = df.columns.tolist()
            if "geometry" in column_list:
                # geometry列を除外
                column_list.remove("geometry")

        # 列ごとにループ
        for column in column_list:
            # 前後の空白を削除
            col = column.strip()

            # その列が存在すれば解析処理
            if column in df.columns:
                # カラム名とその列のデータ型をテキストに追加
                content_txt += f"\n\n# {col}({df[col].dtypes})"

                # nan等も含めて値の数をカウントし、値の名前順にソート
                cnt = df[column].value_counts(dropna=False).sort_index()

                # 値の名前とその数を文字列にしテキストに追加していく
                for index, value in zip(cnt.index, cnt.values):
                    content_txt += f"\n   {index} - Count:{value}"

            else:
                # その列が存在しない場合
                content_txt += f"\n\n# 列名'{col}'は存在しません。"

        return content_txt
    except Exception as e:
        raise Exception(f"[データフレームの指定列の型判定と内容解析結果]: {str(e)}")


def count_dataframe_duplicate_keys(df, key_names):
    """
    データフレームの列を指定し、キーの重複結果をテキストにする

    :param df: 解析するデータフレーム
    :type df: pandas.dataframe or geopandas.GeoDataFrame
    :param key_names: カンマで区切られた列名のリスト
    :type key_names: str
    :return: 解析結果のテキスト
    :rtype: str
    """
    try:
        # 後続の処理に影響を与えないようコピーを作成
        df_copy = df.copy()
        content_txt = "指定列のキー重複チェック結果\n\n"

        # 着目するカラムを取得（重複を削除）
        if key_names:
            key_set = set([key.strip()
                          for key in key_names.split(",") if key.strip() != ""])

        else:
            content_txt += "# チェックなし"
            return content_txt

        if len(key_set) == 0:
            # 指定なしの場合
            content_txt += "# チェックなし"
            return content_txt

        # 存在しないカラムを取得
        not_existing_colums = set(key_set) - set(df_copy.columns)
        if len(not_existing_colums) != 0:
            content_txt += "\n".join(
                [f"# 列名'{key}'は存在しません。" for key in not_existing_colums]) + "\n\n"
            if len(key_set) == len(not_existing_colums):
                content_txt += "# チェックなし"
                return content_txt

        # 存在するカラムを元のカラムの順番に揃える
        key_list = sorted(key_set - not_existing_colums,
                          key=df_copy.columns.get_loc)

        # geometryカラムが対象の場合、ジオメトリデータをWTK変換
        if "geometry" in df_copy.columns:
            df_copy["geometry"] = df_copy["geometry"].apply(
                lambda geom: geom.wkt)

        # 指定カラムに対しグルーピングを行い、グループ内の最小インデックス情報のカラムを追加
        df_copy["min_index"] = df_copy.groupby(
            key_list)[key_list[0]].transform(lambda x: x.index.min())
        # 指定カラムでグルーピングした後、最小インデックス順でソート＆1グループあたりのデータカウントを追加
        count_df = df_copy.groupby(key_list + ["min_index"]).size().reset_index(name="count").sort_values(
            by="min_index")

        # ソート用のカラムを削除
        del df_copy["min_index"]

        # 重複データのみを抽出
        duplicated_df = count_df[count_df['count'] > 1]
        content_txt += f"# Duplicate Count：{len(duplicated_df)}"
        if len(duplicated_df) == 0:
            return content_txt

        content_txt += "\n"

        # 重複データごとに対象となるレコードを取得
        for i, row in duplicated_df.iterrows():
            content_txt += "\n"

            # 対象のカラムの値を辞書型で持つ
            duplicated_data = row[key_list].to_dict()

            # タイトルを作成
            column_data = ','.join([f"{key}={row[key]}" for key in key_list])
            content_txt += f"# {column_data}\n"

            # ヘッダを作成
            header_info = '|'.join(
                [f" Index "] + [f" {col} " for col in df_copy.columns])
            header_len = sum(2 if unicodedata.east_asian_width(
                c) in "FW" else 1 for c in header_info)
            content_txt += "-" * header_len + "\n"
            content_txt += header_info + "\n"
            content_txt += "-" * header_len + "\n"

            # レコードの内容を作成
            # 対象のレコードをクエリで抽出
            query_string = ' and '.join(
                [f'{col} == @{col}' for col, value in duplicated_data.items()])
            extracted_df = df_copy.query(
                query_string, local_dict=duplicated_data)
            # 抽出されたレコードから表示内容を作成
            for n, ex_row in extracted_df.iterrows():
                row_data = '|'.join(
                    [f" {n} "] + [f" {ex_row[col]} " for col in extracted_df.columns])
                content_txt += row_data
                if n != extracted_df.index[-1]:
                    content_txt += "\n"

            if i != duplicated_df.index[-1]:
                content_txt += "\n"

        return content_txt

    except Exception as e:
        raise Exception(f"[指定列のキー重複チェック結果]: {str(e)}")


def generate_attributes(input_data, data_name):
    """
    DataFrameまたはGeoDataFrameから属性情報を生成（空値のみの列は除外）

    :param input_data: DataFrame または GeoDataFrame
    :type input_data: pandas.DataFrame または geopandas.GeoDataFrame

    :param data_name: DataDefinitionのDWHファイル名
    :type data_name: string

    :return: geometry以外の属性に対するDataDefinition
    :rtype: string

    :raises Exception: 処理中にエラーが発生した場合に例外をスローする
    """
    try:
        attribute_lines = []
        for column in input_data.columns:
            if column not in ["geometry"]:  # geometry列はスキップ
                if not input_data[column].isna().all():  # 空値のみの列はスキップ
                    attribute_lines.append(
                        f"-1,{data_name}{column},{column},1,")
        return attribute_lines
    except Exception as e:
        raise Exception(f"[データ定義出力結果\n]: {str(e)}")


def generate_data_definition(input_data, file_name):
    """
    GeoDataFrameまたはDataFrameからデータ定義（DataDefinition）の出力を行う

    :param input_data: DataFrame または GeoDataFrame
    :type input_data: pandas.DataFrame または geopandas.GeoDataFrame

    :param file_name: DWHファイル名を構成するためのファイル名
    :param file_name: string

    :return: DataDefinition
    :rtype: string

    :raises Exception: 処理中にエラーが発生した場合に例外をスローする
    """
    try:
        if isinstance(input_data, gpd.GeoDataFrame):
            gdf = input_data
            geom_type_map = {"Point": "+1",
                             "LineString": "+2", "Polygon": "+3"}
            result = []

            has_layer = "layer" in gdf.columns
            layers = gdf["layer"].unique() if has_layer else [None]

            for layer_name in layers:
                result.append("ファイルタイプ,DWHファイル名,流通項目名,流通項目階層,属性値")
                layer_gdf = gdf[gdf["layer"] ==
                                layer_name] if has_layer else gdf
                unique_geom_types = layer_gdf.geometry.geom_type.unique()

                for geom_type in unique_geom_types:
                    if geom_type not in geom_type_map:
                        raise ValueError("不正なジオメトリタイプが含まれています。")

                    geom_type_id = geom_type_map[geom_type]
                    data_name = f"{file_name}_{layer_name}/" if has_layer else f"{file_name}/"

                    # ジオメトリ情報を追加（ジオメトリタイプごとに1回のみ）
                    result.append(
                        f"{geom_type_id},{data_name}geometry,geometry,1,")

                    # 属性情報を追加
                    attribute_lines = generate_attributes(layer_gdf, data_name)
                    if attribute_lines:
                        result.extend(attribute_lines)

                    result.append("")

            return "\n".join(["データ定義出力結果\n"] + result).strip("\n")

        elif isinstance(input_data, pd.DataFrame):
            df = input_data
            result = []
            data_name = f"{file_name}/"
            result.append("ファイルタイプ,DWHファイル名,流通項目名,流通項目階層,属性値")

            # 属性情報を追加
            attribute_lines = generate_attributes(df, data_name)
            if attribute_lines:
                result.extend(attribute_lines)

            return "\n".join(["データ定義出力結果\n"] + result)

    except Exception as e:
        raise (f"[データ定義出力結果\n]: {str(e)}")


def get_contents_from_geodataframe(geodataframe,
                                   extension,
                                   encoding,
                                   write_args=[]):

    # ---------------------------------------------------------------
    # geodataframeを参照してファイルを出力する
    # 拡張子は指定
    # 引数1:geodataframe
    # 引数2:出力ファイルパス
    # 引数3:拡張子（「.」付き）
    #      （例：「.csv」 pathlib.Path().suffixで取得することを想定している
    # ---------------------------------------------------------------

    # 入力対象リストにないファイルは読み込まない
    if extension in DDC.OUTPUT_FILE_EXTENSION_LIST:
        pass
    else:
        raise Exception('出荷できないファイルです')

    # 拡張子ごとにメソッド切り替え
    if extension == DDC.CSV_EXTENSION:

        # CSVの場合
        result_object = geodataframe.to_csv(index=False)

    elif extension == DDC.GEOJSON_EXTENSION:

        # geojsonの場合
        result_object = geodataframe.to_json(drop_id=True).encode(encoding)

    elif extension == DDC.GPKG_EXTENSION:

        temp_object = io.BytesIO()

        # GPKGの場合
        geodataframe.to_file(temp_object,
                             encoding=encoding,
                             driver='GPKG',
                             layer=write_args[0])

        result_object = temp_object.getvalue()

    else:
        raise

    return result_object


def extract_by_location_bool_array(input_geodataframe,
                                   target_to_compare_geodataframe,
                                   extract_condition):
    # ---------------------------------------------------------------
    # 場所による抽出
    # 引数1：抽出対象の地物（geodataframe）
    # 引数2：比較対象の地物（geodataframe）
    # 引数3：抽出条件（文字列）
    # ---------------------------------------------------------------

    # 比較対象ジオメトリの列を取得
    target_to_compare_shapely_series\
        = target_to_compare_geodataframe[target_to_compare_geodataframe.geometry.name].copy()

    # 抽出条件がwithin(完全内包)
    if extract_condition == DDC.EXTRACT_CONDITION_WITHIN:
        bool_numpy_list = [input_geodataframe.within(target_to_compare_shapely_series[i]).to_numpy()
                           for i in range(len(target_to_compare_shapely_series))]

    # 抽出条件がintersects(少なくとも1つの構成点がポリゴン内にある、あるいは線分がポリゴンに交差している)
    elif extract_condition == DDC.EXTRACT_CONDITION_INTERSECTS:
        bool_numpy_list = [input_geodataframe.intersects(target_to_compare_shapely_series[i]).to_numpy()
                           for i in range(len(target_to_compare_shapely_series))]

    # 抽出条件が判定できない場合は抽出対象のgeodataframeをそのまま返す
    else:
        bool_numpy_list = [np.ones(len(input_geodataframe), dtype=np.bool_)]

    # 複数のbool配列の論理和をmaskとして取得
    bool_numpy = np.logical_or.reduce(bool_numpy_list)

    return bool_numpy


def add_coordinates_record_to_field_set_file_list(dwh_list,
                                                  type_list,
                                                  value_list,
                                                  add_target_dwh_name_string,
                                                  add_target_type_string,
                                                  add_target_value_list,
                                                  add_target_attribute_id_name='',
                                                  add_target_fid_name=''):
    # ---------------------------------------------------------------
    # 座標用Value列、Type列、Dwh列のListに要素を追加
    # 追加する要素としては３種類
    # 座標、属性に登録する連番のID、地物ID（FID）
    # Dwh名に関しては以下の通り
    # ・属性に登録する連番のID：[ファイル名]_ATTRIBUTE_ID
    # ・地物ID：[ファイル名]_FID
    # ---------------------------------------------------------------

    # 属性に設定するid名が空の場合、座標配列のdwh名に'_ATTRIBUTE_ID'を付与して設定
    if add_target_attribute_id_name == '':
        add_target_attribute_id_name = add_target_dwh_name_string+'_ATTRIBUTE_ID'
    else:
        pass

    # fid名が空の場合、座標配列のdwh名に'_FID'を付与して設定
    if add_target_fid_name == '':
        add_target_fid_name = add_target_dwh_name_string+'_FID'
    else:
        pass

    # 座標配列追加
    dwh_list, \
        type_list, \
        value_list\
        = add_record_to_field_set_file_list(dwh_list,
                                            type_list,
                                            value_list,
                                            add_target_dwh_name_string,
                                            add_target_type_string,
                                            add_target_value_list)

    coordinates_array = np.array(add_target_value_list)

    # 一意な地物のidを取得
    unique_id_array = np.unique(coordinates_array[:, 0:1]).astype(np.int64)

    # データ流通基盤のidの配列生成
    feature_id_list = [(str(i), i) for i in range(len(unique_id_array))]

    # データ流通基盤の属性へidの配列生成
    feature_attribute_id_list = [(i, i) for i in range(len(unique_id_array))]

    # 連番属性配列追加
    dwh_list, \
        type_list, \
        value_list\
        = add_record_to_field_set_file_list(dwh_list,
                                            type_list,
                                            value_list,
                                            add_target_attribute_id_name,
                                            'int64',
                                            feature_attribute_id_list)

    # 地物ID(FID)配列追加
    dwh_list, \
        type_list, \
        value_list\
        = add_record_to_field_set_file_list(dwh_list,
                                            type_list,
                                            value_list,
                                            add_target_fid_name,
                                            'string',
                                            feature_id_list)

    return dwh_list, \
        type_list, \
        value_list
