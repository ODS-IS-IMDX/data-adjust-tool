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
import base64
import io
import pickle
import zipfile
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
pyproj = import_module("pyproj")
b1 = getattr(import_module("numba"), "b1")
f8 = getattr(import_module("numba"), "f8")
i8 = getattr(import_module("numba"), "i8")
jit = getattr(import_module("numba"), "jit")
prange = getattr(import_module("numba"), "prange")
types = getattr(import_module("numba.core"), "types")
Dict = getattr(import_module("numba.typed"), "Dict")
Delaunay = getattr(import_module("scipy.spatial"), "Delaunay")

import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP

from nifiapi.properties import (
    PropertyDescriptor,
    ExpressionLanguageScope,
    StandardValidators,
)
from nifiapi.flowfiletransform import (FlowFileTransform,
                                       FlowFileTransformResult)

# 小数点以下の桁数や指数表示の有無を設定
np.set_printoptions(suppress=True, precision=13)


@jit(nopython=True, cache=True, nogil=True)
def calculate_norm(vec):
    """
    ベクトルのノルム（長さ）を計算する。

    :return: ベクトルのノルム（長さ）
    :rtype: numpy.ndarray
    """
    return np.sqrt(np.sum(vec ** 2))


@jit("f8[:,:](f8[:,:])", nopython=True, cache=True, nogil=True, parallel=True)
def adjust_line_coords(center_line):
    """
    連続する同一の座標を省いた配列を返す（効率化：事前チェック版）。

    :param center_line: 元の中心線を構成する3D座標の2次元配列。
    :type center_line: numpy.ndarray
    :return: 連続する同じ座標を省いた中心線。
    :rtype: numpy.ndarray
    """
    n = center_line.shape[0]
    if n == 0:
        return center_line  # 空配列をそのまま返す

    # 重複を判定するためのブール配列
    is_unique = np.zeros(n, dtype=np.bool_)
    is_unique[0] = True  # 最初の点は必ずユニーク

    # 重複チェック
    for i in prange(n):
        if i == 0:
            is_unique[0] = True  # 最初の点は必ずユニーク
            continue
        # 次の点以降
        elif not np.array_equal(center_line[i], center_line[i - 1]):
            is_unique[i] = True

    # ユニークな行だけを返す
    return center_line[is_unique]


@jit(nopython=True, cache=True, nogil=True, parallel=True)
def roll(arr, shift):
    n = len(arr)
    shift %= n
    result = np.empty_like(arr)
    for i in prange(n):
        result[i] = arr[(i - shift) % n]
    return result


@jit("b1(f8[::1], f8[:, ::1])", nopython=True, cache=True, nogil=True)
def is_point_in_polygon(point, polygon):
    """
    重心がポリゴン内にあるか判定する関数

    :pram point: 点の座標
    :type point: numpy.ndarray
    :param polygon: ポリゴンの頂点座標
    :type polygon: numpy.ndarray
    """
    x, y = point
    n = len(polygon)
    crossings = 0

    for i in range(n):
        x1, y1, z1 = polygon[i]
        x2, y2, z2 = polygon[(i + 1) % n]  # 次の点を取得

        # y座標が点のyをまたぐかを判定
        if (y1 > y) != (y2 > y):
            # 線分のx座標が点のxより小さいかを判定
            if x < (x2 - x1) * (y - y1) / (y2 - y1) + x1:
                crossings += 1

    # 交差回数が奇数なら内側、偶数なら外側
    return crossings % 2 == 1


@jit("f8[:, :, ::1](f8[:, ::1], f8[:, :, ::1])", nopython=True, parallel=True, cache=True)
def extract_inner_triangles(polygon, triangles):
    """
    ポリゴン内に含まれる三角形を抽出する（並列処理で結果を事前保存）。

    :param polygon: ポリゴンの頂点座標 [(x1, y1), (x2, y2), ...] の配列。
    :type polygon: numpy.ndarray
    :param triangles: 三角形の頂点座標 [(p1, p2, p3), ...] の配列。
    :type triangles: numpy.ndarray
    :return: ポリゴン内に含まれる三角形の配列。
    :rtype: numpy.ndarray
    """
    num_triangles = len(triangles)

    # 各三角形の内外判定結果を保存する配列
    is_inside = np.zeros(num_triangles, dtype=np.bool_)

    # 並列で重心の内外判定を計算
    for i in prange(num_triangles):
        triangle = triangles[i]
        p1, p2, p3 = triangle

        # 重心を計算
        centroid = (p1[:2] + p2[:2] + p3[:2]) / 3.0

        # 重心がポリゴン内にあるか判定
        is_inside[i] = is_point_in_polygon(centroid, polygon)

    # 有効な三角形だけを収集
    count = np.sum(is_inside)
    final_results = np.empty((count, 3, 3), dtype=np.float64)

    # 内側と判定された三角形を収集
    index = 0
    for i in range(num_triangles):
        if is_inside[i]:
            final_results[index] = triangles[i]
            index += 1

    return final_results


@jit(nopython=True, cache=True, nogil=True)
def expand_points_along_vector(data, vector_dict, interval, findex_column):
    """
    点群をp1からp2へのベクトル方向に拡張する (Numba版)

    :param data: 入力データ（各行に fIndex, x, y, z を含む ndarray）。
    :type data: numpy.ndarray

    :param vector_dict: fIndex をキーとし、ベクトルを値とする辞書。
    :type vector_dict: dict[float, numpy.ndarray]

    :param interval: 点を拡張する間隔。
    :type interval: float

    :param findex_column: fIndex が格納されている列のインデックス。
    :type findex_column: int

    :return: 拡張された点群を含むリスト。各要素は拡張された点群の ndarray。
    :rtype: list[numpy.ndarray]
    """
    # 1. fIndex列を抽出
    #    データの各行に格納されている fIndex を取得して配列として保持
    findex = data[:, findex_column]

    # 2. X, Y, Z の座標を抽出
    #    各行の [x, y, z] 座標を一括して取得し、開始点として使用
    start_points = data[:, 1:4]

    # 3. 入力データの行数を取得
    #    データ全体のサイズを特定する
    num_rows = len(data)

    # 4. 結果を格納するリストを初期化
    #    拡張された点群を格納するリスト（各行ごとに追加）
    results = []

    # 5. 各行について処理を開始
    for i in range(num_rows):
        # 6. 現在の fIndex に対応するベクトルを取得
        #    vector_dict から fIndex をキーとしてベクトルを取得
        vector = vector_dict[findex[i]]

        # 7. ベクトルの長さ（ノルム）を計算
        #    ベクトルの大きさを求める
        norm_vector = np.linalg.norm(vector)

        # 8. ベクトルを正規化
        #    ベクトルを単位ベクトル（長さ1）に変換
        unit_vector = vector / norm_vector

        # 9. ベクトルの長さと間隔に基づいて生成する点の数を計算
        #    分割間隔 `interval` で分割し、始点と終点を含めるため +2
        num_points = int(norm_vector // interval) + 2

        # 10. 点群を格納する配列を初期化
        #     num_points × 3 の空配列を作成
        points = np.empty((num_points, 3), dtype=np.float64)

        # 11. 各点間の距離を計算
        #     ベクトルを均等に分割する間隔を計算
        step_val = norm_vector / (num_points - 1)

        # 12. 各点を生成
        for j in range(num_points):
            # 現在の点 = 始点 + 単位ベクトル * (j * step_val)
            # 始点から step_val ずつ間隔をあけて点を生成
            points[j] = start_points[i] + unit_vector * (j * step_val)

        # 13. 結果リストに拡張された点群を追加
        results.append(points)

    # 14. 拡張された点群を返却
    return results


@jit("f8[:, :](f8[:, :], f8)", nopython=True, cache=True, nogil=True)
def prepend_column_with_numba(array, value):
    """
    Numbaで配列の先頭に新しい列を追加する。

    :param array: 元の2次元配列 (形状: [N, M])。
    :type array: numpy.ndarray

    :param value: 新しい列に設定する値。
    :type value: float

    :return: 新しい列が追加された配列 (形状: [N, M+1])。
    :rtype: numpy.ndarray
    """
    rows, cols = array.shape
    result = np.empty((rows, cols + 1), dtype=array.dtype)  # 新しい配列を生成
    for i in range(rows):
        result[i, 0] = value  # 最初の列に値を挿入
        for j in range(cols):
            result[i, j + 1] = array[i, j]  # 元のデータをコピー
    return result


@jit(types.Tuple((i8[:], i8[:], i8[:]))(f8[:], f8[:], f8[:], i8, f8, f8), nopython=True, cache=True, nogil=True)
def calculate_indices(x_list, lat_rad_list, h_list, n, n_v_ratio, pi_reciprocal):
    """
    numbaを用いてx, y, zインデックスを高速計算

    :param x_list: 経度リスト。
    :type x_list: numpy.ndarray

    :param lat_rad_list: 緯度（ラジアン）リスト。
    :type lat_rad_list: numpy.ndarray

    :param h_list: 高さリスト。
    :type h_list: numpy.ndarray

    :param n: 分割数（2次元方向）。
    :type n: int

    :param n_v_ratio: 高さ方向の分割スケール。
    :type n_v_ratio: float

    :param pi_reciprocal: πの逆数（1/π）。
    :type pi_reciprocal: float

    :return: X, Y, Z方向のインデックス。
    :rtype: tuple(numpy.ndarray, numpy.ndarray, numpy.ndarray)
    """
    xidx_list = np.floor(n * ((x_list + 180) / 360)).astype(np.int64)
    yidx_list = np.floor(
        n * (1 - np.log(np.tan(lat_rad_list) +
                        (1 / np.cos(lat_rad_list))) * pi_reciprocal) / 2
    ).astype(np.int64)
    zidx_list = np.floor(n_v_ratio * h_list).astype(np.int64)
    return xidx_list, yidx_list, zidx_list


@jit("f8[:](f8, f8, f8)", nopython=True, cache=True, nogil=True)
def generate_z_values(initial_z, max_height, interval):
    """
    Z方向に値を生成する補助関数

    :param initial_z: 初期のZ値。
    :type initial_z: float

    :param max_height: 最大のZ値。
    :type max_height: float

    :param interval: 値を生成する間隔。
    :type interval: float

    :return: 生成されたZ方向の値リスト。
    :rtype: numpy.ndarray
    """
    max_points = int((max_height - initial_z) // interval) + 2  # 最大要素数を計算
    z_values = np.empty(max_points, dtype=np.float64)
    count = 0
    current_z = initial_z
    while current_z <= max_height:
        z_values[count] = current_z
        count += 1
        current_z += interval
    if z_values[count - 1] < max_height:  # 最後にmax_heightを追加
        z_values[count] = max_height
        count += 1
    return z_values[:count]  # 使用した部分のみ返す


@jit("f8[:, ::1](f8[:, ::1], types.DictType(f8, f8), f8, i8, i8)", nopython=True, cache=True, nogil=True)
def expand_points(data, height_dict, interval, z_column, findex_column):
    """
    点群をZ方向に拡張する

    :param data: 入力データ（各行に fIndex, x, y, z を含む ndarray）。
    :type data: numpy.ndarray

    :param height_dict: fIndex をキーとし、最大高さを値とする辞書。
    :type height_dict: dict[float, float]

    :param interval: Z方向に値を生成する間隔。
    :type interval: float

    :param z_column: Z値が格納されている列のインデックス。
    :type z_column: int

    :param findex_column: fIndex が格納されている列のインデックス。
    :type findex_column: int

    :return: 拡張された点群データ。
    :rtype: numpy.ndarray
    """
    total_rows = 0
    # 1. 結果の総行数を事前計算
    for row in data:
        fid = row[findex_column]
        z = row[z_column]
        max_height = height_dict[fid] + z
        total_rows += len(generate_z_values(z, max_height, interval))

    # 2. 固定長配列を作成
    results = np.empty((total_rows, data.shape[1]), dtype=data.dtype)

    # 3. データを展開して格納
    current_index = 0
    for row in data:
        fid = row[findex_column]
        z = row[z_column]
        max_height = height_dict[fid] + z
        z_values = generate_z_values(z, max_height, interval)

        for z_value in z_values:
            new_row = row.copy()
            new_row[z_column] = z_value
            results[current_index] = new_row
            current_index += 1

    return results


@jit("f8[:, :](f8[:], f8[:], i8)", nopython=True, cache=True, nogil=True, parallel=True)
def generate_edge_points(p1, p2, divisions):
    """
    2つの点p1, p2間の補完点を生成する。

    :param p1: 始点の座標 (ベクトル)。
    :type p1: numpy.ndarray

    :param p2: 終点の座標 (ベクトル)。
    :type p2: numpy.ndarray

    :param divisions: 補間の分割数。
    :type divisions: int

    :return: 補間点の座標を格納した配列。
    :rtype: numpy.ndarray
    """

    # 最低でも1にする
    if divisions < 1:
        divisions = 1

    t_vals = np.linspace(0, 1, divisions + 1)
    result = np.zeros((len(t_vals), len(p1)))  # 結果を格納する配列
    for i in prange(len(t_vals)):
        result[i] = p1 * (1 - t_vals[i]) + p2 * t_vals[i]
    return result


@jit("f8[:, :](f8[:], f8[:], f8[:], i8)", nopython=True, cache=True, nogil=True, parallel=True)
def generate_barycentric_points(p1, p2, p3, divisions):
    """
    三角形内に等間隔で点を生成する。

    :param p1: 三角形の頂点座標 (3次元対応)
    :type p1: numpy.ndarray
    :param p2: 三角形の頂点座標 (3次元対応)
    :type p2: numpy.ndarray
    :param p3: 三角形の頂点座標 (3次元対応)
    :type p3: numpy.ndarray
    :param divisions: 分割数
    :type divisions: int

    :return: 三角形内の点群
    :rtype: numpy.ndarray
    """
    # 点群の総数を計算
    num_points = (divisions + 1) * (divisions + 2) // 2

    # 結果を格納する配列を初期化
    points = np.zeros((num_points, len(p1)))

    # 各点を並列計算
    for i in prange(divisions + 1):
        for j in range(divisions + 1 - i):
            # index を計算で求める
            index = (i * (2 * divisions + 3 - i)) // 2 + j
            u = i / divisions
            v = j / divisions
            w = 1 - u - v
            points[index] = u * p1 + v * p2 + w * p3

    return points


@jit("f8[:,:](f8[:], f8)", nopython=True, cache=True, nogil=True)
def calculate_rotation_matrix_by_axis(axis, angle):
    """
    回転軸と角度に基づいて回転行列を計算する。

    :param axis: 回転軸を表すベクトル。長さ3のNumPy配列。
    :type axis: numpy.ndarray

    :param angle: 回転角度（ラジアン単位）。
    :type angle: float

    :return: 3x3の回転行列。
    :rtype: numpy.ndarray
    """
    # 回転軸を正規化
    axis = axis / np.linalg.norm(axis)

    # 三角関数を計算
    cos_theta = np.cos(angle)
    sin_theta = np.sin(angle)
    one_minus_cos = 1 - cos_theta

    # 回転軸の成分
    x, y, z = axis

    # 回転行列を計算

    result = np.array([
        [cos_theta + x * x * one_minus_cos, x * y * one_minus_cos - z * sin_theta,
         x * z * one_minus_cos + y * sin_theta],
        [y * x * one_minus_cos + z * sin_theta, cos_theta + y * y * one_minus_cos,
         y * z * one_minus_cos - x * sin_theta],
        [z * x * one_minus_cos - y * sin_theta, z * y * one_minus_cos + x * sin_theta,
         cos_theta + z * z * one_minus_cos]
    ])

    return result


@jit("f8[:, :](f8[:], f8[:])", nopython=True, cache=True, nogil=True)
def calculate_rotation_matrix(v1, v2):
    """
    ベクトル v1 を v2 に回転させるための回転行列を計算する。

    :param v1: 現在の方向ベクトル（長さ3の1次元配列）。
    :type v1: numpy.ndarray
    :param v2: 次の方向ベクトル（長さ3の1次元配列）。
    :type v2: numpy.ndarray
    :return: 回転行列（3x3の2次元配列）。
    :rtype: numpy.ndarray

    """
    # ベクトルを正規化
    v1 = v1 / np.linalg.norm(v1)
    v2 = v2 / np.linalg.norm(v2)

    # 回転軸を計算
    axis = np.cross(v1, v2)
    axis_norm = np.linalg.norm(axis)

    # ベクトルが平行または反平行の場合
    if axis_norm < 1e-6:
        return np.eye(3)  # 単位行列を返す

    # 回転軸を正規化
    axis = axis / axis_norm

    # 内積を計算
    dot_product = np.dot(v1, v2)
    # 値を-1.0と1.0の範囲に制限
    if dot_product < -1.0:
        dot_product = -1.0
    elif dot_product > 1.0:
        dot_product = 1.0

    # 回転角度を計算
    angle = np.arccos(dot_product)

    # 回転行列を構築するための補助行列
    K = np.array([
        [0, -axis[2], axis[1]],
        [axis[2], 0, -axis[0]],
        [-axis[1], axis[0], 0]
    ])

    # 回転行列を計算(ロドリゲスの回転公式)
    return np.eye(3) + np.sin(angle) * K + (1 - np.cos(angle)) * np.dot(K, K)


@jit("i8(f8, f8)", nopython=True, cache=True)
def calculate_num_divisions(edge_length, target_distance):
    """
    avg_edge_length / target_distance の商を切り上げ、分割数を求める。
    ただし、1cm未満の誤差は切り捨てる。

    :param edge_length: 線分の全長 (単位: m)
    :type edge_length: float

    :param target_distance: 各区間の目標距離 (単位: m)
    :type target_distance: float

    :return: 線分の分割数
    :rtype: int
    """
    # 1cm未満の誤差を無視するための丸め込み
    rounded_edge_length = np.floor(edge_length * 100) / 100  # 小数第2位に切り捨て
    rounded_target_distance = np.floor(
        target_distance * 100) / 100  # 小数第2位に切り捨て

    # 分割数を計算し、切り上げ
    return int(np.ceil(rounded_edge_length / rounded_target_distance))


class GenerateSpatialID(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """空間IDの情報をテキストで出力する。"""
        tags = ["fieldsetfile"]

    # プロパティの定義
    #:
    ZOOM_LEVEL = PropertyDescriptor(
        name="Zoom Level",
        description="ズームレベルを指定。ズームレベルによって、内挿補完に使用する間隔値を決定する。",
        validators=[StandardValidators.POSITIVE_INTEGER_VALIDATOR],
        default_value=26,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    WIDTH_NAME = PropertyDescriptor(
        name="Width Name",
        description="幅を持つ属性名を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    HEIGHT_NAME = PropertyDescriptor(
        name="Height Name",
        description="高さを持つ属性名を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    START_DAY = PropertyDescriptor(
        name="Start Day",
        description="開始日を持つ属性名を指定。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    END_DAY = PropertyDescriptor(
        name="End Day",
        description="終了日を持つ属性名を指定。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )

    #:
    FEATURE_ID_COLUMN_NAME = PropertyDescriptor(
        name='Feature ID Column Name',
        description='地物IDが存在する場合はカラム名を設定',
        default_value='',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    #:
    GEOMETRY_TYPE = PropertyDescriptor(
        name='Geometry Type',
        description='Inputデータのジオメトリタイプ',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value='LINESTRING',
        allowable_values=['LINESTRING', 'POLYGON'],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    property_descriptors = [ZOOM_LEVEL, WIDTH_NAME, HEIGHT_NAME,
                            FEATURE_ID_COLUMN_NAME, START_DAY, END_DAY, GEOMETRY_TYPE]

    def __init__(self, **kwargs):

        self.width_name = "WIDTH_NAME"
        self.height_name = "HEIGHT_NAME"
        self.start_day = "START_DAY"
        self.end_day = "END_DAY"
        self.geometry_type = "GEOMETRY_TYPE"
        self.crs = "CRS"
        self.zoom_level = "ZOOM_LEVEL"
        self.interpolation_interval = "INTERPOLATION_INTERVAL"
        self.feature_id_column_name = "FEATURE_ID_COLUMN_NAME"

        # 存在しない場合のデフォルト値
        self.default_start_date = "1970/01/01"
        self.default_end_date = "2037/12/31"

        self.all_params = {}

        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def calculate_vertical_index_based_on_level(self, zoom_level):
        """
        ズームレベル25を基準に、指定したズームレベルに対して鉛直方向の補完間隔を計算する。

        :param zoom_level: ズームレベル
        :type zoom_level: int

        :return: ズームレベルに基づく補完間隔
        :rtype: float
        """
        interpolation_interval = np.float64(1 * (2 ** (25 - zoom_level)))
        return interpolation_interval

    def get_number_from_string(self, s: str, num_type: type = float):
        """
        文字列から指定された型の数値を取得する関数。
        数字に変換できない場合は ValueError を発生させる。

        :param s: 数値変換したい文字列
        :type s: str
        :param num_type: 変換したい型
        :type num_type: type

        :return: 変換後の値
        :rtype: num_type

        :raises Exception: 数字に変換できない場合、ValueErrorを発生させる。
        """
        try:
            # 指定された型に変換
            return num_type(s)
        except (ValueError, TypeError):
            # 数字に変換できない場合、ValueErrorを発生させる
            raise ValueError(f"'{s}' を {num_type.__name__} に変換できませんでした")

    def get_params(self, context, flowfile):
        """
        プロパティの値を取得し、辞書にして返す。

        :param context: NiFiの処理コンテキスト。プロパティを評価するために使用。
        :type context: org.apache.nifi.processor.ProcessContext
        :param flowfile: NiFiのFlowFile。属性を取得するために使用。
        :type flowfile: org.apache.nifi.flowfile.FlowFile

        :return: 属性情報とプロパティの値を格納した辞書
        :rtype: dict
        """

        def get_property_value(prop_descr):
            """
            PropertyDescriptorからその値を取得

            :param prop_descr: PropertyDescriptor
            :type prop_descr: org.apache.nifi.flowfile.FlowFile
            :return: PropertyDescriptorから取得した値
            :rtype: str
            """
            try:
                return (
                    context.getProperty(prop_descr)
                    .evaluateAttributeExpressions(flowfile)
                    .getValue()
                )
            except Exception as e:
                raise

        try:

            # FID
            self.all_params[self.feature_id_column_name] = get_property_value(
                self.FEATURE_ID_COLUMN_NAME)

            # ZoomLevel
            self.all_params[self.zoom_level] = int(
                self.get_number_from_string(get_property_value(self.ZOOM_LEVEL)))
            if self.all_params[self.zoom_level] < 0:
                raise ValueError(
                    "Specify an integer greater than or equal to 0 for Zoom Level.")

            # サイズ情報
            self.all_params[self.width_name] = get_property_value(
                self.WIDTH_NAME)
            self.all_params[self.height_name] = get_property_value(
                self.HEIGHT_NAME)

            # 開始日
            self.all_params[self.start_day] = get_property_value(self.START_DAY) if get_property_value(
                self.START_DAY) else self.start_day

            # 終了日
            self.all_params[self.end_day] = get_property_value(self.END_DAY) if get_property_value(
                self.END_DAY) else self.end_day

            # ジオメトリタイプ
            self.all_params[self.geometry_type] = get_property_value(self.GEOMETRY_TYPE) if get_property_value(
                self.GEOMETRY_TYPE) else self.geometry_type

            # CRS
            self.all_params[self.crs] = self.get_number_from_string(
                flowfile.getAttribute("crs")) or self.get_number_from_string(flowfile.getAttribute("CRS"))

            # 補完間隔
            self.all_params[self.interpolation_interval] = self.calculate_vertical_index_based_on_level(
                self.all_params[self.zoom_level])

            # 開始日列の名称
            self.start_day_col = self.all_params[self.start_day]

            # 終了日列の名称
            self.end_day_col = self.all_params[self.end_day]

            # サイズ属性列の名称
            self.width_name_col = self.all_params[self.width_name]
            self.height_name_col = self.all_params[self.height_name]

            # 補完間隔
            self.interval = self.all_params[self.interpolation_interval]

            return

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")

    def generate_rotated_wireframe(self, center_line, width, height):
        """
        中心線の各セグメントに基づいて、隣接セグメントの角度に応じて回転したワイヤーフレームを生成する。

        :param center_line: 中心線を構成する3D座標のリスト。中心線の各点を構成する3D座標がリスト形式で渡される。
        :type center_line: list[list or numpy.ndarray]

        :param width: ワイヤーフレームの横幅（左右方向のオフセット）。
        :type width: float

        :param height: ワイヤーフレームの高さ（上下方向のオフセット）。
        :type height: float

        :return: 各セグメントごとのワイヤーフレームの頂点を構成するリスト。各セグメントごとに8つの頂点が含まれる。
        :rtype: list[list]

        :raises Exception: 予期しないエラーが発生した場合。
        """
        try:
            # ワイヤーフレーム全体を格納するリスト
            all_wireframes = []

            # 前のセグメントで計算した"上方向のベクトル"を格納する変数
            previous_up_vector = None

            # 中心線の各セグメントを順番に処理
            for i in range(len(center_line) - 1):
                # 現在のセグメントの始点と終点を取得
                p1, p2 = center_line[i], center_line[i + 1]
                # 現在のセグメントの方向ベクトルを計算
                current_direction = np.array(p2, dtype=float) - np.array(p1, dtype=float)
                # ベクトルを正規化（長さを1にする）
                current_direction /= np.linalg.norm(current_direction)

                # 次のセグメントの方向を計算（最終セグメントの場合は単位行列を使用）
                if i < len(center_line) - 2:
                    next_direction = np.array(center_line[i + 2], dtype=float) - np.array(center_line[i + 1],
                                                                                          dtype=float)
                    next_direction /= np.linalg.norm(next_direction)  # 正規化
                    # 現在のセグメントと次のセグメントを回転させる行列を計算
                    rotation_matrix = calculate_rotation_matrix(current_direction, next_direction)
                else:
                    # 最終セグメントでは回転行列は単位行列を使用（回転なし）
                    rotation_matrix = np.eye(3)

                # 上方向の基準ベクトルを決定
                if np.abs(current_direction[2]) < 0.99:
                    # 現在のセグメントがほぼ水平な場合、上方向は[0, 0, 1]（Z軸方向）
                    up_vector = np.array([0, 0, 1], dtype=float)
                else:
                    # 現在のセグメントがほぼ垂直な場合、上方向は[X軸方向]
                    up_vector = np.array([1, 0, 0], dtype=float)

                # 前のセグメントの上方向を考慮して現在の上方向を調整
                if previous_up_vector is not None:
                    # 前のセグメントの上方向ベクトルを回転行列で現在のセグメントに適用
                    up_vector = np.dot(rotation_matrix, previous_up_vector)

                # サイドベクトルを計算
                # サイドベクトルは現在のセグメントの方向ベクトルと上方向ベクトルの外積で決まる
                side_vector = np.cross(current_direction, up_vector)
                side_vector /= np.linalg.norm(side_vector)  # 正規化

                # 上方向ベクトルを再計算（サイドベクトルとの外積で直交性を保証）
                up_vector = np.cross(side_vector, current_direction)

                # ワイヤーフレームの頂点を計算
                # 始点（p1）と終点（p2）から幅と高さを考慮して頂点を作成
                wireframe = [p1 + (-side_vector * width - up_vector * height),  # 始点左下
                             p1 + (side_vector * width - up_vector * height),  # 始点右下
                             p1 + (-side_vector * width + up_vector * height),  # 始点左上
                             p1 + (side_vector * width + up_vector * height),  # 始点右上
                             p2 + (-side_vector * width - up_vector * height),  # 終点左下
                             p2 + (side_vector * width - up_vector * height),  # 終点右下
                             p2 + (-side_vector * width + up_vector * height),  # 終点左上
                             p2 + (side_vector * width + up_vector * height)  # 終点右上
                             ]

                # 現在のセグメントのワイヤーフレームを追加
                all_wireframes.append(wireframe)

                # 現在の上方向ベクトルを保存（次のセグメントで使用）
                previous_up_vector = up_vector

            # すべてのセグメントのワイヤーフレームを返す
            return np.array(all_wireframes)

        except Exception as e:
            raise Exception(
                f"[generate_rotated_wireframe]: {str(e)}")

    def interpolate_faces_by_distance_ndarray(self, input_data, target_distance):
        """
        三角形面ごとに頂点を指定した距離に基づいて補完し、補完されたポイントを含む配列を返す。

        :param input_data: 補完対象の座標を含む2次元配列
        :type input_data: numpy.ndarray

        :param target_distance: 補完の基準となる距離
        :type target_distance: float

        :return: 補完された頂点データを含む2次元配列
        :rtype: numpy.ndarray

        :raises Exception: 補完処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            output_data = []

            # Findexに基づきジオメトリを分類
            for findex, p1, p2, p3 in input_data:
                # 各エッジの長さを計算してリストに追加
                edge_lengths = [np.linalg.norm(
                    p2 - p1), np.linalg.norm(p3 - p2), np.linalg.norm(p1 - p3)]

                # 各エッジの長さを計算
                # avg_edge_length = np.mean(edge_lengths)
                max_edge_length = max(edge_lengths)
                second_largest = max(
                    x for x in edge_lengths if x != max_edge_length)

                # 補完分割数を算出
                uniform_divisions = calculate_num_divisions(
                    second_largest, target_distance)

                # 補完点を生成
                interpolated_points = generate_barycentric_points(
                    p1, p2, p3, uniform_divisions)

                # 各補完点に findex を付加し、結果をリストに追加
                output_data.append(prepend_column_with_numba(
                    interpolated_points, findex))
            # numpy配列に一括変換
            output_data = np.vstack(output_data)

            return output_data

        except Exception as e:
            raise Exception(
                f"[interpolate_faces_by_distance_ndarray]: {str(e)}")

    def create_vector_dict(self, p1_list, p2_list):
        """
        numba用の辞書を作成（findexをキーに、p1とベクトルを値に）

        :param p1_list: ベクトルの始点を表すリスト。
        :type p1_list: list

        :param p2_list: ベクトルの終点を表すリスト。
        :type p2_list: list

        :return: findexをキーとし、ベクトルを値とする辞書。
        :rtype: dict[float, numpy.ndarray]
        """
        vector_dict = Dict.empty(key_type=types.float64, value_type=types.float64[:]  # NumPy配列の型を指定
                                 )
        for p1, p2 in zip(p1_list, p2_list):
            findex = p1[0]
            vector = np.array(p2[1]) - np.array(p1[1])  # ベクトル計算
            vector_dict[findex] = vector
        return vector_dict

    def copy_points(self, data, p1_list, p2_list, interval):
        """
        点群をp1からp2へのベクトル方向に拡張する

        :param data: 入力となる点群データ。各点が座標を持つ配列。
        :type data: numpy.ndarray
        :param p1_list: 始点となるp1のリスト。各始点は3D座標として提供される。
        :type p1_list: list or numpy.ndarray

        :param p2_list: 終点となるp2のリスト。各終点は3D座標として提供される。
        :type p2_list: list or numpy.ndarray

        :param interval: 増加間隔。点群をp1からp2に沿って拡張する際の間隔。
        :type interval: float

        :return: p1からp2に沿って拡張した点群データ。新しい点群は与えられた間隔で計算される。
        :rtype: numpy.ndarray
        """
        findex_column = np.int64(0)

        # p1_listとp2_list三角形同士のベクトルを計算
        vector_dict = self.create_vector_dict(p1_list, p2_list)

        expanded_data = expand_points_along_vector(
            data, vector_dict, interval, findex_column)

        # メモリ効率を上げるために結果を順次結合
        return np.vstack(expanded_data)

    def get_geondarray(self, df, limit=None):
        """
        GeoDataFrameから各PolygonやMultiPolygonの外周座標を独立したレコードとして抽出し、
        x, y, zの座標に変換する関数。

        :param df: geopandas.GeoDataFrame
            処理対象のGeoDataFrame。ポリゴンやマルチポリゴンの座標が格納されている。
        :param limit: int, optional
            抽出する件数を指定。デフォルトはNoneで、すべてのデータを抽出する。

        :return: numpy.ndarray
            各PolygonやMultiPolygonの外周座標を含むリスト。x, y, z座標のみを含む。
        """
        # 'Dwh'列が'coordinates'に一致する行を抽出
        coordinates_rows = df[df['Dwh'] == 'coordinates']

        # 'Value'列の値を取得
        value_list = coordinates_rows['Value'].iloc[0]

        # 件数制限を適用（limitがNoneの場合はすべてを取得）
        if limit is not None:
            value_list = value_list[:limit]

        # NumPy配列に変換して返す
        return np.array(value_list)

    def get_triangle(self, wireframes, triangle_indices):
        """
        各ワイヤーフレームに基づいて三角形の参照を生成する (findexをfloat64型)

        :param wireframes: 各ワイヤーフレームの頂点を含むリスト。各ワイヤーフレームは8つの頂点を持つ。
        :type wireframes: list[numpy.ndarray]

        :param triangle_indices: 三角形を構成するインデックス。インデックスはワイヤーフレームの頂点を参照する。
        :type triangle_indices: numpy.ndarray

        :return: 各三角形の情報を参照として格納したリスト。各三角形はfindex（浮動小数点型）、3つの頂点を含む。
        :rtype: list
        """
        triangle_list = []
        findex = np.float64(0)  # findexをnp.float64型で初期化

        for wireframe in wireframes:
            for indices in triangle_indices:
                # 三角形の頂点を参照として取得
                p1, p2, p3 = wireframe[indices[0]
                ], wireframe[indices[1]], wireframe[indices[2]]

                # 三角形データをリストに格納（参照のまま）
                triangle_list.append([findex, p1, p2, p3])
                findex += np.float64(1)
        return triangle_list

    def transform_xy_to_lonlat_batch(self, xyz_array, org_crs):
        """
        平面直角座標から経度緯度にバッチ変換する関数

        :param xyz_array: (N, 3) のNumPy配列。各行は [X, Y, Z]
        :type xyz_array: numpy.ndarray

        :param system_code: 平面直角座標系の系番号（1～19）
        :type system_code: int

        :return: (N, 3)のNumPy配列。各行は[lon, lat, Z]
        :rtype: numpy.ndarray
        """

        # 日本測地系2011（JGD2011）の地理座標系（緯度・経度）に変更
        geo_epsg = 6668  # WGS84のEPSGコード

        # 座標変換オブジェクトを作成
        transformer = pyproj.Transformer.from_crs(
            f"EPSG:{org_crs}", f"EPSG:{geo_epsg}", always_xy=True)

        # 平面直角座標から経度緯度に変換
        xyz_array[:, 0], xyz_array[:, 1] = transformer.transform(
            xyz_array[:, 0], xyz_array[:, 1])

    def calc_index(self, xyz_list, zoom_level):
        """
        与えられた座標リストとズームレベルを基にインデックスを計算する。

        :param xyz_list: (N, 3)のNumPy配列。各行は[X, Y, Z]座標。
        :type xyz_list: numpy.ndarray

        :param zoom_level: ズームレベル（整数値）。
        :type zoom_level: int

        :return: 計算されたインデックスの結果。
        :rtype: numpy.ndarray
        """
        n = np.int64(2 ** zoom_level)
        vertical_default = 2 ** 25
        n_v_ratio = np.float64(n / vertical_default)
        pi_reciprocal = np.float64(1 / np.pi)

        # x, y座標を直接取得し、h_listにはZ値をそのまま保持
        x_list = xyz_list[:, 0]
        y_list = xyz_list[:, 1]
        h_list = xyz_list[:, 2]

        # 緯度のリストをラジアンに変換
        lat_rad_list = np.radians(y_list)

        # x, y, zインデックスを計算（numbaで高速化）
        return calculate_indices(x_list, lat_rad_list, h_list, n, n_v_ratio, pi_reciprocal)

    def generate_spatial_strings(self, zoom_level, x_index_list, y_index_list, f_index_list, fid, df):
        """
        指定された情報から空間ID文字列を効率的に生成する。

        :param zoom_level: 固定ズームレベル（intまたはstr）
        :type zoom_level: int or str

        :param x_index_list: X座標のリスト
        :type x_index_list: list or numpy.ndarray

        :param y_index_list: Y座標のリスト
        :type y_index_list: list or numpy.ndarray

        :param f_index_list: F値のリスト
        :type f_index_list: list or numpy.ndarray

        :param fid: FIDの値
        :type fid: str or int

        :param df: 開始日、終了日、FIDを含むDataFrame
        :type df: pandas.DataFrame

        :return: 生成された空間ID文字列のリスト
        :rtype: list[str]
        """

        # リスト長の確認
        list_len = len(f_index_list)
        assert len(x_index_list) == list_len and len(
            y_index_list) == list_len, "リストの長さが一致しません。"

        # (x, y, f)を列方向に結合
        combined = np.stack((x_index_list, y_index_list, f_index_list), axis=1)

        # ユニークな行を抽出
        unique_combinations = np.unique(combined, axis=0)

        # リスト長の更新
        list_len = len(unique_combinations)

        # Zを固定値で埋める
        z_str_array = np.full(list_len, str(zoom_level), dtype=np.object_)

        # x,y,f文字列に変換
        f_str_array = unique_combinations[:, 2].astype(str)
        x_str_array = unique_combinations[:, 0].astype(str)
        y_str_array = unique_combinations[:, 1].astype(str)

        # 開始日列
        start_date_array = (
            df[df["Dwh"].str.contains(self.start_day_col, na=False)]['Value'].fillna(self.default_start_date).astype(
                str).to_numpy() if self.start_day_col in df["Dwh"].values else np.full(list_len,
                                                                                       self.default_start_date,
                                                                                       dtype=np.object_))

        # 終了日列
        end_date_array = (
            df[df["Dwh"].str.contains(self.end_day_col, na=False)]['Value'].fillna(self.default_end_date).astype(
                str).to_numpy() if self.end_day_col in df["Dwh"].values else np.full(list_len, self.default_end_date,
                                                                                     dtype=np.object_))

        # 空間IDの組み立て
        spatial_ids = z_str_array + "/" + f_str_array + \
                      "/" + x_str_array + "/" + y_str_array

        # FID列の作成(同一FIDなのでレコード数分コピーする
        fid_str_array = [np.array(fid) for _ in range(len(x_str_array))]

        # 全体の組み立て
        results = ('"' + spatial_ids + '","' + fid_str_array + '","' +
                   start_date_array + '","' + end_date_array + '"')

        return results.tolist()

    def generate_connection_vectors(self, center_line, width, height, tolerance=1e-2):
        """
        中心線の接続部分で外向きのベクトル線を生成する（Numba対応）。

        :param center_line: (N, 3)の2次元配列, 中心線を構成する3D座標の配列。
        :type center_line: list or numpy.ndarray
        :param width: ベクトルの横方向の長さ。
        :type width: float
        :param height: ベクトルの縦方向の長さ。
        :type height: float
        :param tolerance: 直線判定の誤差範囲。
        :type tolerance: float
        :return: (M, 2, 3) の2次元配列, 各接続部分のベクトル線（始点と終点のリスト）。
        :rtype: numpy.ndarray
        """
        n_points = center_line.shape[0]
        connection_vectors = np.empty((n_points - 2, 2, 3), dtype=np.float64)
        count = 0

        for i in range(1, n_points - 1):
            p_prev = center_line[i - 1]
            p_curr = center_line[i]
            p_next = center_line[i + 1]

            # 前後のセグメントのベクトルを計算
            vec_a = p_curr - p_prev
            vec_b = p_curr - p_next

            # ベクトルを正規化
            norm_a = calculate_norm(vec_a)
            norm_b = calculate_norm(vec_b)

            if norm_a < tolerance or norm_b < tolerance:
                # セグメントが非常に短い場合はスキップ
                continue

            vec_a /= norm_a
            vec_b /= norm_b

            # 外向きベクトルを計算
            outward_vector = vec_a + vec_b
            norm_outward = calculate_norm(outward_vector)

            if norm_outward < tolerance:
                # ベクトルが無効な場合（直線的または誤差範囲内）
                continue

            # 外向きベクトルを正規化
            outward_vector /= norm_outward

            # サイドベクトル（横方向）と上方向ベクトル（縦方向）を計算
            side_vector = np.cross(outward_vector, [0, 0, 1])
            side_vector /= np.linalg.norm(side_vector)
            up_vector = np.cross(side_vector, outward_vector)
            up_vector /= np.linalg.norm(up_vector)

            # 横方向と縦方向のオフセットを適用
            outward_vector = side_vector * width + up_vector * height

            # ベクトル線の始点と終点を生成
            start_point = p_curr
            end_point = p_curr + outward_vector

            connection_vectors[count, 0] = start_point
            connection_vectors[count, 1] = end_point
            count += 1

        return connection_vectors[:count]

    def generate_3d_point_cloud(self, geondaay_list, width_list, height_list, interpolation_interval):
        """
        与えられた座標データとサイズ情報を基に3D点群を生成する。

        :param geondaay_list: (N, 4) のNumPy配列。各行は[findex, x, y, z]の座標データ。
        :type geondaay_list: numpy.ndarray

        :param width_list: 各findexに対応する幅情報のリスト。
        :type width_list: tuple(float)

        :param height_list: 各findexに対応する高さ情報のリスト。
        :type height_list: tuple(float)

        :param interpolation_interval: 補間する間隔（数値）。
        :type interpolation_interval: numpy.float64

        :return: 生成された3D点群。
        :rtype: numpy.ndarray
        """

        # Findexのリストを作成
        findex_list = geondaay_list[:, 0]

        # ユニークなidxとそれぞれの開始位置を取得
        unique_indices, start_positions = np.unique(
            findex_list, return_index=True)

        all_points = []

        # 各ユニークなidxごとに処理
        for i, start in enumerate(start_positions):
            # 次のインデックスまでのデータをスライス
            end = start_positions[i + 1] if i + \
                                            1 < len(start_positions) else len(geondaay_list)
            line_string = geondaay_list[start:end]  # このidxに属する座標

            # 該当findexのx, y, z座標を抽出
            points = line_string[:, 1:4]  # x, y, z列を抽出

            # サイズを取得
            width = width_list[int(findex_list[start])] / 2

            # サイズを取得
            height = height_list[int(findex_list[start])] / 2

            # 不要な点を省く
            adjust_line = adjust_line_coords(points)

            # ワイヤーフレーム取得
            upper_right_info, lower_right_info, lower_left_info, upper_left_info = NCP.get_wire_frame_info(
                adjust_line, height, width)

            # ARC取得
            upper_arc_right, upper_arc_left, lower_arc_right, lower_arc_left = [
                info[2] for info in (upper_right_info, upper_left_info, lower_right_info, lower_left_info)
            ]

            # 平行線取得
            upper_right_lines, upper_left_lines, lower_right_lines, lower_left_lines = [
                info[1] for info in (upper_right_info, upper_left_info, lower_right_info, lower_left_info)
            ]

            points = np.empty((0, 3))

            wireframes = []

            # ワイヤーフレームを作成
            for i in range(len(lower_right_lines)):
                wireframe = np.array([upper_right_lines[i][0],  # 右上始点
                                      lower_right_lines[i][0],  # 右下始点
                                      lower_left_lines[i][0],  # 左下始点
                                      upper_left_lines[i][0],  # 左上始点
                                      upper_right_lines[i][1],  # 右上終点
                                      lower_right_lines[i][1],  # 右下終点
                                      lower_left_lines[i][1],  # 左下終点
                                      upper_left_lines[i][1],  # 左上終点
                                      ])
                wireframes.append(wireframe)

            # ワイヤーフレームをNumPy配列に変換
            wireframes = np.array(wireframes)

            # 始点側の三角形2枚
            p1_list = self.get_triangle(wireframes, [[0, 3, 2], [0, 1, 3]])

            # 終点側の三角形2枚
            p2_list = self.get_triangle(wireframes, [[4, 7, 6], [4, 5, 7]])

            # 補完点作成
            interpolated_faces = self.interpolate_faces_by_distance_ndarray(p1_list, interpolation_interval)

            # 5. 底面の点群をコピーして、z方向に追加する
            new_points = self.copy_points(interpolated_faces, p1_list, p2_list, interpolation_interval)

            # pointsに新しい点を追加
            points = np.vstack((points, new_points))

            # 右側の円弧部分から面を作成し座標を追加
            right_arc = self.create_arc_points(lower_arc_right, upper_arc_right)
            if right_arc is not None and len(right_arc) > 0:
                points = np.vstack((points, right_arc))

            # 左側の円弧部分から面を作成し座標を追加
            left_arc = self.create_arc_points(lower_arc_left, upper_arc_left)
            if left_arc is not None and len(left_arc) > 0:
                points = np.vstack((points, left_arc))

            # 先頭にfindexを追加し、全体に追加
            all_points.append(prepend_column_with_numba(
                points, findex_list[start]))
            
        return np.vstack(all_points)

    def create_arc_points(self, lower_arc, upper_arc):
        """
        円弧に対し、内挿補完を行う。

        :param lower_arc: 下側円弧の座標リスト
        :type geondaay_list: numpy.ndarray

        :param upper_arc: 上側円弧の座標リスト
        :type width_list: numpy.ndarray

        :return: 生成された3D点群。
        :rtype: numpy.ndarray
        """
        # 空は無視
        if len(lower_arc) == 0 or len(upper_arc) == 0:
            return

        # 数が異なるのはエラー
        if len(lower_arc) != len(upper_arc):
            raise ValueError("lower_arc or upper_arc is empty. Cannot generate arc points.")

        all_points = []
        findex = np.float64(0)

        for i in range(len(lower_arc)):
            polygon_coords_left = np.array([lower_arc[i][0],  # 下部左アークの始点
                                            lower_arc[i][1],  # 下部左アークの終点
                                            upper_arc[i][1],  # 上部左アークの終点
                                            upper_arc[i][0],  # 上部左アークの始点
                                            ])

            start_triangles = [[findex, polygon_coords_left[0], polygon_coords_left[1], polygon_coords_left[3]],
                               [findex, polygon_coords_left[1], polygon_coords_left[2], polygon_coords_left[3]], ]

            interpolated_faces = self.interpolate_faces_by_distance_ndarray(start_triangles, 0.5)

            if interpolated_faces is not None and len(interpolated_faces) > 0:
                all_points.append(interpolated_faces[:, 1:4])

            findex += np.float64(1)

        if len(all_points) == 0:
            raise ValueError("No points generated during arc interpolation.")

        points = np.vstack(all_points)
        return points

    def create_Delaunay_triangle(self, points, depth):
        """
        与えられた点群に対してドロネー分割を行い、三角形の頂点インデックスを返す。

        :param points: (N, 3)のNumPy配列。各行は[x, y, z]の座標を表す。
        :type points: numpy.ndarray
        :param depth: Z方向のオフセット量
        :type depth: float
        :return: 分割された三角形の頂点座標(start_triangles, end_triangles)。
        :rtype: tuple[numpy.ndarray, numpy.ndarray]
        """
        if points.shape[1] != 3:
            raise ValueError("ポイントの配列は(N, 3)形式である必要があります")

        # ドロネー分割
        delaunay = Delaunay(points[:, :2])  # x, yだけで分割
        triangles = points[delaunay.simplices]  # 分割された三角形の頂点座標を取得

        # ポリゴン内部の三角形だけ返却する
        triangles = extract_inner_triangles(points, triangles)

        # オフセットした三角形
        off_triangles = self.offset_triangles(triangles, depth)

        # 各三角形に対して[findex, p1, p2, p3]の形式でデータを作成
        start_triangles = []
        end_triangles = []

        findex = np.float64(0)  # findexを初期化
        for original, offset in zip(triangles, off_triangles):
            # オリジナルの三角形
            start_triangles.append([findex, original[0], original[1], original[2]])
            # オフセットされた三角形
            end_triangles.append([findex, offset[0], offset[1], offset[2]])
            findex += np.float64(1)

        return start_triangles, end_triangles

    def offset_triangles(self, triangles, offset):
        """
        各三角形のZ座標にオフセットを適用して新しい三角形を生成する。

        :param triangles: (M, 3, 3) の三角形配列。各行は三角形の3頂点を表す。
        :type triangles: numpy.ndarray
        :param offset: Z方向のオフセット量。
        :type offset: float
        :return: オフセットが適用された三角形配列。
        :rtype: numpy.ndarray
        """
        offset_array = np.array([0, 0, offset])
        return triangles + offset_array

    def generate_3d_point_cloud_for_polygon(self, geondaay_list, height_size, interpolation_interval):
        """
        与えられた座標データと高さ情報を基に3D点群を生成する。

        :param geondaay_list: (N, 4) のNumPy配列。各行は [findex, x, y, z] の座標データ。
        :type geondaay_list: numpy.ndarray

        :param height_size: 各findexに対応する高さ情報のリスト。
        :type height_size: list or numpy.ndarray

        :param interpolation_interval: 補間する間隔（数値）。
        :type interpolation_interval: float

        :return: 生成された3D点群。
        :rtype: numpy.ndarray
        """

        # Findexのリストを作成
        findex_list = geondaay_list[:, 0]

        # ユニークなidxとそれぞれの開始位置を取得
        unique_indices, start_positions = np.unique(
            findex_list, return_index=True)

        all_points = []

        # 各ユニークなidxごとに処理
        for i, start in enumerate(start_positions):
            # 次のインデックスまでのデータをスライス
            end = start_positions[i + 1] if i + \
                                            1 < len(start_positions) else len(geondaay_list)
            polygon = geondaay_list[start:end]  # このidxに属する座標

            # 該当findexのx, y, z座標を抽出
            points = polygon[:, 1:4]  # x, y, z列を抽出

            # 不要な点を省く
            polygon_coords = adjust_line_coords(points)

            # サイズを取得
            depth = height_size[int(findex_list[start])]

            # ドロネー分割を作成
            start_triangles, end_triangles = self.create_Delaunay_triangle(polygon_coords, -depth)

            # 補完点作成
            interpolated_faces = self.interpolate_faces_by_distance_ndarray(start_triangles,
                                                                            interpolation_interval)

            # 5. 底面の点群をコピーして、z方向に追加する
            points = self.copy_points(
                interpolated_faces, start_triangles, end_triangles, interpolation_interval)

            # 先頭にfindexを追加し、全体に追加
            all_points.append(prepend_column_with_numba(
                points, findex_list[start]))

        return np.vstack(all_points)

    def generate_spatial_index(self, x_index_list, y_index_list, f_index_list, all_points, df):
        """
        与えられた座標情報を基に空間インデックスを生成する。

        :param x_index_list: X座標のリスト。
        :type x_index_list: list or numpy.ndarray

        :param y_index_list: Y座標のリスト。
        :type y_index_list: list or numpy.ndarray

        :param f_index_list: F値のリスト。
        :type f_index_list: list or numpy.ndarray

        :param all_points: 点群データ。各行は [findex, x, y, z] の座標情報を含む。
        :type all_points: numpy.ndarray

        :param df: FIDと他の関連情報を含むDataFrame。
        :type df: pandas.DataFrame

        :return: 空間インデックスのリスト。
        :rtype: list
        """
        # idx列（0列目）を取得
        findex_list = all_points[:, 0]

        # ユニークなidxとそれぞれの開始位置を取得
        unique_indices, start_positions = np.unique(findex_list, return_index=True)

        # fidのリストを作成
        fid_list = self.get_fid_list(df)

        # start_positionsとendのタプルをfidに紐付ける
        fid_to_positions = {}
        for i, start in enumerate(start_positions):
            end = start_positions[i + 1] if i + 1 < len(start_positions) else len(x_index_list)
            fid = fid_list[i]
            if fid not in fid_to_positions:
                fid_to_positions[fid] = []
            fid_to_positions[fid].append((start, end))

        # 各fidごとに処理
        all_index = []
        for fid, ranges in fid_to_positions.items():
            fid_results = []
            for start, end in ranges:
                x_list = x_index_list[start:end]
                y_list = y_index_list[start:end]
                f_list = f_index_list[start:end]

                # generate_spatial_stringsを呼び出して結果を収集
                calc_result = self.generate_spatial_strings(self.all_params[self.zoom_level], x_list, y_list, f_list,
                                                            fid, df)
                fid_results.extend(calc_result)

            # FID値の単位で重複を排除してからall_indexに追加
            # 文字列なので、np.uniqueではなくsetを使用
            unique_results = list(set(fid_results))
            all_index.append(unique_results)

        return all_index

    def get_size_list(self, df):
        """
        サイズ属性（幅と高さ）を取得する。

        :param df: 対象のDataFrame。
        :type df: pandas.DataFrame

        :return: 幅と高さのリスト（タプル形式）。
        :rtype: tuple[list[float], list[float]]

        :raises ValueError: 必要なデータが存在しない場合や、データ形式が不正な場合。
        """
        # 幅と高さの条件に該当する行を取得
        width_df = df[df['Dwh'] == self.width_name_col]
        height_df = df[df['Dwh'] == self.height_name_col]

        # geometryがPolygonの時はチェックしない
        if width_df.empty and not self.all_params[self.geometry_type] == "POLYGON":
            self.logger.error(f"幅の属性「{self.width_name_col}」がありません。")
            return None, None
        if height_df.empty:
            self.logger.error(f"高さの属性「{self.height_name_col}」がありません。")
            return None, None

        # 幅と高さのデータを取得
        width_data = None
        if not width_df.empty:
            width_data = width_df["Value"].iloc[0]
        height_data = height_df["Value"].iloc[0]

        try:
            width_list = None
            if width_data:
                width_list = list(zip(*width_data))[1]  # 2列目を取得
            height_list = list(zip(*height_data))[1]  # 2列目を取得

            # 幅や高さの値に0以下の値がある場合
            if width_list:
                if any(size <= 0 for size in width_list):
                    self.logger.error(f"幅の属性{self.width_name_col}の中に、値が0以下のデータがあります。")
                    return None, None
            if any(size <= 0 for size in height_list):
                self.logger.error(f"高さの属性{self.height_name_col}の中に、値が0以下のデータがあります。")
                return None, None

            return width_list, height_list
        except IndexError as e:
            raise ValueError(f"幅または高さのデータが不正な形式です: {width_data}, {height_data}") from e

    def get_fid_list(self, df):
        """
         FIDリストを取得する。

        :param df: 対象のDataFrame。
        :type df: pandas.DataFrame

        :return: FIDリスト（タプル形式）。
        :rtype: list[tuples]

        :raises ValueError: 必要なデータが存在しない場合や、データ形式が不正な場合。
        """
        # 条件に該当する行を取得
        fid_data = df.loc[df['Dwh'] ==
                          self.all_params[self.feature_id_column_name], 'Value'].iloc[0]

        try:
            fid_list = list(zip(*fid_data))[0]  # 1列目を取得
            return fid_list
        except IndexError as e:
            raise ValueError(f"fidデータが不正な形式です: {fid_data}") from e

    def transform(self, context, flowfile):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param context: コンテキスト
        :param flowfile: FieldSetFileを持つフローファイル

        :return: 処理結果を含んだFlowFileTransformResult
        :rtype: FlowFileTransformResult

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティ値取得
            self.get_params(context, flowfile)

            # フローファイルのコンテンツを取得
            input_field_set_file = flowfile.getContentsAsBytes()

            # Dataframeとして読込み
            df = CU.field_set_file_to_dataframe(input_field_set_file)

            # Dwh列をスラッシュで分割し、最後の要素（attribute）を抽出
            df['Dwh'] = df['Dwh'].apply(lambda x: x.split('/')[-1])

            # 座標のリストを作成
            geondarray_list = self.get_geondarray(df)

            # サイズ属性を取得
            width_list, height_list = self.get_size_list(df)

            # 入力がラインの場合
            if self.all_params[self.geometry_type] == "LINESTRING":  # 入力がラインの場合
                all_points = self.generate_3d_point_cloud(
                    geondarray_list, width_list, height_list, np.float64(self.all_params[self.interpolation_interval]))
            else:  # 入力がポリゴンの場合
                all_points = self.generate_3d_point_cloud_for_polygon(
                    geondarray_list, height_list, np.float64(self.all_params[self.interpolation_interval]))

            # 全座標からxyzだけをスライス
            all_xyz = all_points[:, 1:]

            # 日本測地系2011（JGD2011）の地理座標系（緯度・経度）に変更
            self.transform_xy_to_lonlat_batch(
                all_xyz, self.all_params[self.crs])

            # 空間ID計算
            x_index_list, y_index_list, f_index_list = self.calc_index(
                all_xyz, self.all_params[self.zoom_level])

            all_index = self.generate_spatial_index(
                x_index_list, y_index_list, f_index_list, all_points, df)

            # 一つの文字列にマージ
            flattened_string = "\n".join(
                ["\n".join(inner_list) for inner_list in all_index])
            return FlowFileTransformResult(relationship="success", contents=f"{flattened_string}")

        except Exception as e:
            raise Exception(f"[GenerateSpatialID Exception]: {str(e)}")
