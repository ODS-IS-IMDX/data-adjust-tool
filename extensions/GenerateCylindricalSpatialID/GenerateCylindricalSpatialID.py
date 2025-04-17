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

@jit("i8(f8, f8)", nopython=True, cache=True)
def calculate_num_divisions(edge_length, target_distance):
    """
    avg_edge_length / target_distance の商を切り上げ、分割数を求める。
    ただし、0.01cm未満の誤差は切り捨てる。

    :param edge_length: 線分の全長 (単位: m)
    :type edge_length: float

    :param target_distance: 各区間の目標距離 (単位: m)
    :type target_distance: float

    :return: 線分の分割数
    :rtype: int
    """
    # 0.01cm未満の誤差を無視するための丸め込み
    rounded_edge_length = np.floor(edge_length * 10000) / 10000  # 小数第4位に切り捨て
    rounded_target_distance = np.floor(
        target_distance * 10000) / 10000  # 小数第4位に切り捨て

    # 分割数を計算し、切り上げ
    return int(np.ceil(rounded_edge_length / rounded_target_distance))


class GenerateCylindricalSpatialID(FlowFileTransform):
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
    CIRCLE_RADIUS = PropertyDescriptor(
        name="Circle Radius",
        description="円の半径を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    CIRCLE_DIVISIONS = PropertyDescriptor(
        name="Circle Divisions",
        description="円周の分割数を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
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

    property_descriptors = [ZOOM_LEVEL, CIRCLE_RADIUS, CIRCLE_DIVISIONS,
                            FEATURE_ID_COLUMN_NAME, START_DAY, END_DAY]

    def __init__(self, **kwargs):

        self.circle_radius = "CIRCLE_RADIUS"
        self.circle_divisions = "CIRCLE_DIVISIONS"
        self.start_day = "START_DAY"
        self.end_day = "END_DAY"
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
            self.all_params[self.circle_radius] = get_property_value(
                self.CIRCLE_RADIUS)

            # 分割数
            self.all_params[self.circle_divisions] = get_property_value(
                self.CIRCLE_DIVISIONS )

            # 開始日
            self.all_params[self.start_day] = get_property_value(self.START_DAY) if get_property_value(
                self.START_DAY) else self.start_day

            # 終了日
            self.all_params[self.end_day] = get_property_value(self.END_DAY) if get_property_value(
                self.END_DAY) else self.end_day

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
            self.circle_radius_col = self.all_params[self.circle_radius]

            # 補完間隔
            self.interval = self.all_params[self.interpolation_interval]

            return

        except Exception as e:
            raise Exception(f"[get_params]: {str(e)}")


    def interpolate_faces_by_distance_ndarray(self, input_data, target_distance):
        """
        三角形面ごとに頂点を指定した距離に基づいて補完し、補完されたポイントを含む配列を返す。

        :param input_data: 補完対象の三角形のリスト（split_circle_triangles）
        :type input_data: list or numpy.ndarray

        :param target_distance: 補完の基準となる距離
        :type target_distance: float

        :return: 補完された頂点データを含む2次元配列
        :rtype: numpy.ndarray

        :raises Exception: 補完処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            output_data = []
            # 多重リスト（ネストされたリスト）を展開して処理
            for face_group in input_data:
                for triangle_list in face_group:
                    for triangle in triangle_list:
                        p1, p2, p3 = triangle  # 各三角形の3頂点

                        # 各エッジの長さを計算
                        edge_lengths = [np.linalg.norm(p2 - p1),
                                        np.linalg.norm(p3 - p2),
                                        np.linalg.norm(p1 - p3)]

                        max_edge_length = max(edge_lengths)
                        second_largest = max(x for x in edge_lengths if x != max_edge_length)

                        # 補完分割数を算出
                        uniform_divisions = calculate_num_divisions(second_largest, target_distance)

                        # 補完点を生成
                        interpolated_points = generate_barycentric_points(p1, p2, p3, uniform_divisions)

                        # 補完されたポイントをリストに追加
                        output_data.append(interpolated_points)
            # numpy 配列に変換して返す
            return np.vstack(output_data) if output_data else np.array([])

        except Exception as e:
            raise Exception(f"[interpolate_faces_by_distance_ndarray]: {str(e)}")

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

    def transform_xy_to_lonlat_batch(self, xyz_array, org_crs):
        """
        平面直角座標から経度緯度にバッチ変換する関数

        :param xyz_array: (N, 3) のNumPy配列。各行は [X, Y, Z]
        :type xyz_array: numpy.ndarray

        :param system_code: 平面直角座標系の系番号（1～19）
        :type system_code: int

        :return: (N, 3) のNumPy配列。各行は [lon, lat, Z]
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

        :param xyz_list: (N, 3) のNumPy配列。各行は [X, Y, Z] 座標。
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

        # (x, y, f) を列方向に結合
        combined = np.stack((x_index_list, y_index_list, f_index_list), axis=1)

        # ユニークな行を抽出
        unique_combinations = np.unique(combined, axis=0)

        # リスト長の更新
        list_len = len(unique_combinations)

        # Zを固定値で埋める
        z_str_array = np.full(list_len, str(zoom_level), dtype=np.object_)

        # x,y,f 文字列に変換
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

    def generate_3d_point_cloud(self, geondarray_list, radius_list, interpolation_interval):
        """
        与えられた座標データとサイズ情報を基に3D点群を生成する。

        :param geondarray_list: (N, 4) のNumPy配列。各行は [findex, x, y, z] の座標データ。
        :type geondarray_list: numpy.ndarray

        :param radius_list: 各findexに対応する半径情報のリスト。
        :type radius_list: tuple(float)

        :param interpolation_interval: 補間する間隔（数値）。
        :type interpolation_interval: numpy.float64

        :return: 生成された3D点群。
        :rtype: numpy.ndarray
        """

        # Findexのリストを作成
        findex_list = geondarray_list[:, 0]

        # ユニークなidxとそれぞれの開始位置を取得
        unique_indices, start_positions = np.unique(
            findex_list, return_index=True)

        all_points = []

        # 各ユニークなidxごとに処理
        for i, start in enumerate(start_positions):
            # 次のインデックスまでのデータをスライス
            end = start_positions[i + 1] if i + \
                                            1 < len(start_positions) else len(geondarray_list)
            line_string = geondarray_list[start:end]  # このidxに属する座標

            # 該当findexのx,y,z座標を抽出
            points = line_string[:, 1:4]  # x, y, z列を抽出

            # サイズを取得
            radius = radius_list[int(findex_list[start])] / 2

            # 不要な点を省く
            adjust_line = adjust_line_coords(points)

            split_circle_triangles = NCP.generate_cylindrical_multipatch_split_triangles(adjust_line, radius, int(self.all_params[self.circle_divisions]), interpolation_interval)

            interpolated_faces = []

            processed_triangles = self.interpolate_faces_by_distance_ndarray(
                split_circle_triangles, interpolation_interval
            )

            # `processed_triangles`を`interpolated_faces`に追加
            if processed_triangles is not None and processed_triangles.size > 0:
                interpolated_faces.append(processed_triangles)

            if interpolated_faces:
                interpolated_faces = np.vstack(interpolated_faces)  # リストのリストを結合
            else:
                interpolated_faces = np.empty((0, 3))  # 空のNumPy配列を作成

            if 'points' not in locals():
                points = interpolated_faces  # 初回はそのまま代入
            else:
                points = np.vstack((points, interpolated_faces))  # 追加

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

        # start_positions と end のタプルを fid に紐付ける
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

                # generate_spatial_strings を呼び出して結果を収集
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
        サイズ属性（半径）を取得する。

        :param df: 対象のDataFrame。
        :type df: pandas.DataFrame

        :return: 半径のリスト（タプル形式）。
        :rtype: tuple[list[float], list[float]]

        :raises ValueError: 必要なデータが存在しない場合や、データ形式が不正な場合。
        """
        # 半径の条件に該当する行を取得
        width_df = df[df['Dwh'] == self.circle_radius_col]

        # 半径のデータを取得
        width_data = None
        if not width_df.empty:
            width_data = width_df["Value"].iloc[0]

        # 転置操作を zip で実現（タプルに対応）
        try:
            radius_list = None
            if width_data:
                radius_list = list(zip(*width_data))[1]  # 2列目を取得

            # 半径の値に0以下の値がある場合
            if radius_list:
                if any(size <= 0 for size in radius_list):
                    self.logger.error(f"半径の属性{self.circle_radius_col}の中に、値が0以下のデータがあります。")
                    return None, None


            return radius_list
        except IndexError as e:
            raise ValueError(f"半径のデータが不正な形式です: {width_data}") from e

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

        # 転置操作を zip で実現（タプルに対応）
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
            radius_list = self.get_size_list(df)

            # 点群発生
            all_points = self.generate_3d_point_cloud(
                geondarray_list, radius_list, np.float64(self.all_params[self.interpolation_interval]))

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
            raise Exception(f"[GenerateCylindricalSpatialID Exception]: {str(e)}")
