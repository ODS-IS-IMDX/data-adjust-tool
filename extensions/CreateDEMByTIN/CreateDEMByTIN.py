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
import io
import traceback
import zipfile
import os
import pathlib

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope


base_path = pathlib.Path(__file__).resolve().parent
proj_path = base_path.parent.parent/'api'/'venv_gdal'/'Lib'/'site-packages'/'osgeo'/'data'/'proj;'
os.environ["PROJ_LIB"] = str(proj_path)

jit = import_module("numba").jit
f8 = import_module("numba").f8
b1 = import_module("numba").b1
i8 = import_module("numba").i8
prange = import_module("numba").prange
pd = import_module("pandas")
np = import_module("numpy")
gdal = import_module("osgeo.gdal")
osr = import_module("osgeo.osr")

ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


@jit(b1(f8[:],f8[:,:]),nopython=True,cache=True,nogil=True)
def is_include(correction_target_feature,correction_target_tin_numpy_array):
    """
    点が三角形内部に含まれているか判定（線上を含む）

    Parameters:
        correction_target_feature: 判定対象の点座標（1次元配列 [x, y] または [x, y, z]）
        correction_target_tin_numpy_array: 三角形の頂点座標（2次元配列）

    Returns:
        bool: 点が三角形内（または辺上）にあれば True、そうでなければ False
    """

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


# ----------------------------------------------------------------
#  三角形に対して四角形が重複するかの判定処理
# ----------------------------------------------------------------
@jit(b1(f8[:,:],f8[:,:]),nopython=True,cache=True,nogil=True)
def is_intersect_or_include(triangle_array, square_array):
    """
    三角形と四角形が重複（交差または包含）しているか判定
    判定基準：
    1. 四角形の頂点が三角形内部にある場合は重複と判定
    2. 頂点が含まれなくても三角形の辺と四角形の辺が交差していれば重複と判定
    3. 四角形が三角形を完全に覆っている場合も重複と判定

    Parameters:
        triangle_array: 三角形の頂点座標配列
        square_array: 四角形の頂点座標配列

    Returns:
        bool: 三角形と四角形が重複していれば True、そうでなければ False
    """

    #########################################################################
    # 三角形が、四角形の頂点のいずれかを内包していれば重複していると判定する
    #########################################################################
    # 四角形の構成点のいずれかが三角形の内側に存在する場合、内包していると判定
    if np.any(np.array([is_include(square_array[si], triangle_array) for si in range(len(square_array))])):
        return True

    # 四角形の構成点が内包されていない場合後続の交差判定へ処理が移る
    else:
        pass

    #########################################################################
    # 四角形の構成点が三角形に内包されていないが、辺同士が交差している場合は、三角形に対して四角形が重複していると判定
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
    # 四角形が三角形を完全に覆いつくす場合重複していると判定する
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


@jit(f8(f8[:,:], f8[:]), nopython=True, cache=True, nogil=True)
def get_z_value_on_3points_plane(tin_array ,p_array):
    """
    TIN平面上に存在するxyが明確な点PのZ値を算出
    三角形の3頂点のXYZ座標から構成される平面上に、
    XY座標が明確な点Pがあるとき、その点のZ座標を線形補間により求める

    参考: https://mathscience-teach.com/koukoumath-kuukanbekutoru3-5/

    Parameters:
        tin_array: 三角形の3頂点座標(2次元配列)
        p_array:  計算対象の点PのXY座標(1次元配列)

    Returns:
        z_value: 点PのZ座標値
    """

    # abcが為す平面上に点pが存在するときのpの表し方
    # p = ra + sb + tc (r+s+t=1)
    # 変数の数を2つへ
    # p = (1-s-t)a + sb + tc
    # (1,3,z) = (1-s-t)(3,2,0) + s(3,1,3) + t(1,4,-3)

    # -----------------------------------------------------
    # X座標係数
    # -----------------------------------------------------
    # (1-s-t)*3 + s * 3  + t * 1 = 1

    # xを表すs係数の和
    # -3s + 3s
    s1 = -tin_array[0,0] + tin_array[1,0]

    # xを表すt係数の和
    # -3t + t
    t1 = -tin_array[0,0] + tin_array[2,0]

    # xを表す定数の和
    # 1 + (-3)
    u1 = p_array[0] - tin_array[0,0]
    # -----------------------------------------------------

    # -----------------------------------------------------
    # Y座標係数
    # -----------------------------------------------------
    #(1-s-t)*2 + s * 1  + t * 4 = 3

    # yを表すs係数の和
    # -2s + 1s
    s2 = -tin_array[0,1] + tin_array[1,1]

    # yを表すt係数の和
    # -2t + 4t
    t2 = -tin_array[0,1] + tin_array[2,1]

    # yを表す定数の和
    #3 + (-2)
    u2 = p_array[1] - tin_array[0,1]
    # -----------------------------------------------------

    # st係数2次元配列
    st_array = np.array([[s1, t1]
                        ,[s2, t2]])

    # 定数1次元配列
    u_array = np.array([u1, u2])

    # xyのみでst算出
    result_array = np.linalg.solve(st_array, u_array)

    # stよりz値算出
    # z = (1-s-t)*0 + s * 3  + t * (-3)
    z_value = (1 - result_array[0] - result_array[1]) * tin_array[0,2]\
                                   + result_array[0]  * tin_array[1,2]\
                                   + result_array[1]  * tin_array[2,2]

    return z_value


@jit(f8[:](f8[:,:], f8[:,:,:],i8[:]), nopython=True, cache=True, nogil=True)
def get_z_array_on_3points_plane(center_corrdinate_array, tin_xyz_array, tin_index_array):
    """
    各ピクセルの中心座標が属するTIN上にあると仮定し、そのZ値を一括で算出
    対応するTINが見つからない（tin_index_array[i] == -1）の場合、該当位置のZ値は初期値（0.0）のまま
    出力は1次元であり、通常は元のラスタ形状（縦×横）に reshape して使用

    Parameters:
        center_corrdinate_array: 対象ピクセルの中心XY座標の配列(2次元配列)
                                 各行が1つのピクセルの [x, y] 座標を示す

        tin_xyz_array: TIN（三角形）を構成する頂点の座標配列(3次元配列)

        tin_index_array: 各ピクセルに対応するTINのインデックス番号の配列
                         値が `-1` の場合は対応TINが存在しないと見なされ、Z値はスキップ

    Returns:
        result_z_array: 各ピクセルに対応するZ値（標高）を格納した一次元配列
    """

    # 結果格納用array
    result_z_array = np.zeros(len(center_corrdinate_array), dtype = np.float64)

    for i in prange(len(center_corrdinate_array)):
        if tin_index_array[i] == -1:
            continue

        else:
            result_z_array[i]\
                = get_z_value_on_3points_plane( tin_xyz_array[tin_index_array[i]]
                                            , center_corrdinate_array[i]
                                            )

    return result_z_array

class CreateDEMByTIN(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        入力されたTINの座標配列を基に、指定された領域のDEMを生成する。
                        ①input: TINの座標配列が格納された、1行のFieldSetFile。
                        ②output: GeoTIFF形式のDEMラスタ画像(TIFF互換)のバイナリデータ。
                      """
        tags = ['Python']


    # 出力画像の原点x座標
    OUTPUT_TARGET_ORIGIN_X = PropertyDescriptor(
        name = "Output Target Origin X",
        description = "出力画像の左上ピクセルのX座標（単位: m）",
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力画像の原点y座標
    OUTPUT_TARGET_ORIGIN_Y = PropertyDescriptor(
        name = "Output Target Origin Y",
        description = "出力画像の左上ピクセルのY座標（単位: m）",
        sensitive=False,
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力画像全体の縦方向の距離
    OUTPUT_TARGET_HEIGHT = PropertyDescriptor(
        name = "Output Target Height",
        description="出力画像全体の縦方向の距離（単位: m）",
        sensitive = False,
        required = True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力画像全体の横方向の距離
    OUTPUT_TARGET_WIDTH = PropertyDescriptor(
        name = "Output Target Width",
        description = "出力画像全体の横方向の距離（単位: m）",
        sensitive = False,
        required = True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 1ピクセルの一辺の長さ
    OUTPUT_TARGET_UNIT_PIXEL = PropertyDescriptor(
        name = "Output Target Unit Pixel",
        description = "1ピクセルの一辺の長さ（単位: m）",
        sensitive = False,
        required = True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # ZIP圧縮するかどうかのフラグ（圧縮するまたは圧縮しない）デフォルトは"圧縮する"
    OUTPUT_ZIP_FLAG = PropertyDescriptor(
        name = "Output ZIP Flag",
        description = """
                    圧縮しない場合、大容量データではNiFiのJava側でメモリ不足やBase64エラーが発生する可能性が高いため、
                    基本的には『圧縮する』を選択することを推奨します。
                    """,
        default_value = ZIP_COMPRESSION_ENABLED,
        allowable_values = [ZIP_COMPRESSION_ENABLED, ZIP_COMPRESSION_DISABLED],
        required = True,
        sensitive = False,
        expression_language_scope = ExpressionLanguageScope.NONE
    )

    property_descriptors = [OUTPUT_TARGET_ORIGIN_X,
                            OUTPUT_TARGET_ORIGIN_Y,
                            OUTPUT_TARGET_HEIGHT,
                            OUTPUT_TARGET_WIDTH,
                            OUTPUT_TARGET_UNIT_PIXEL,
                            OUTPUT_ZIP_FLAG]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors


    def get_property(self, context, flowfile):
        """
            プロパティで入力した値を取得

        Parameters:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        Returns:
            output_target_origin_x: 出力画像の原点x座標
            output_target_origin_y: 出力画像の原点y座標
            output_target_height: 出力画像の縦方向の総距離
            output_target_width: 出力画像の横方向の総距離
            output_target_unit_pixel: 1ピクセルの一辺の長さ
            output_zip_flag: ZIP圧縮するかどうかのフラグ
        """

        # プロパティで設定した値を取得
        output_target_origin_x = float(context.getProperty(
            self.OUTPUT_TARGET_ORIGIN_X).evaluateAttributeExpressions(flowfile).getValue())

        output_target_origin_y = float(context.getProperty(
            self.OUTPUT_TARGET_ORIGIN_Y).evaluateAttributeExpressions(flowfile).getValue())

        output_target_height = float(context.getProperty(
            self.OUTPUT_TARGET_HEIGHT).evaluateAttributeExpressions(flowfile).getValue())

        output_target_width = float(context.getProperty(
            self.OUTPUT_TARGET_WIDTH).evaluateAttributeExpressions(flowfile).getValue())

        output_target_unit_pixel = float(context.getProperty(
            self.OUTPUT_TARGET_UNIT_PIXEL).evaluateAttributeExpressions(flowfile).getValue())

        output_zip_flag\
            = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()

        return output_target_origin_x, output_target_origin_y, output_target_height, output_target_width, output_target_unit_pixel, output_zip_flag


    def _get_target_tin_index_by_rectangle_area_coordinates_array(self,
                                                                  rectangle_coordinate_array,
                                                                  tin_array):
        """
        指定した矩形領域の座標配列に交差または包含されるTIN（三角形）のインデックス配列を返す

        Parameters:
            rectangle_coordinate_array: 4点の矩形座標配列
            tin_array: TINの配列

        Returns:
            target_tin_index_array: 指定矩形に交差・包含されるTINの真偽値配列
        """

        # 指定範囲の矩形に交差、含まれるtinのインデックスを生成
        target_tin_index_array = np.array([is_intersect_or_include(tin_array[i, :, 1:], rectangle_coordinate_array)\
                                        for i in range(len(tin_array))])

        return target_tin_index_array


    def _get_pixel_center_coordinate_array(self,
                                           raster_x,
                                           raster_y,
                                           pixel_unit,
                                           origin_point):
        """
        ピクセル中心の座標配列を作成
        与えられたラスタサイズと、ピクセルサイズ、左上原点座標を元に、
        各ピクセルの中心座標を (x, y) 形式の 2 次元配列で返す

        Parameters:
            raster_x: ラスタの横方向のピクセル数（幅）
            raster_y: ラスタの縦方向のピクセル数（高さ）
            pixel_unit: 各ピクセルの長さ (x方向, y方向)
            origin_point: 左上隅の座標 (x, y)

        Returns:
            coordinate_array: 座標配列(中心X座標, 中心Y座標)
                              RGB値をflatten()したときと同じ並び
        """

        # 指定pixel数のx,yメッシュ配列作成
        x_grid_array, y_grid_array = np.meshgrid(np.arange(raster_x), np.arange(raster_y))

        # 左上のpixelの中心位置
        pixel_origin_point_array=np.array([ origin_point[0] + pixel_unit[0] / 2
                                        , origin_point[1] + pixel_unit[1] / 2]
                                        )

        # pixelの中心を座標に表す
        coordinate_array = np.stack([ x_grid_array.flatten() * pixel_unit[0] + pixel_origin_point_array[0]
                                    , y_grid_array.flatten() * pixel_unit[1] + pixel_origin_point_array[1]]
                                    , 1
                                )

        return coordinate_array


    def _get_pixel_center_coordinate_array_id(self,
                                              raster_x,
                                              raster_y,
                                              pixel_unit,
                                              origin_point):
        """
        各ピクセルの中心座標に対応するユニークIDとXY座標を取得
        各ピクセルの中心位置を、左上の原点とピクセルサイズから算出し、
        各ピクセルに一意のIDを付与した2次元配列として返す

        Parameters:
            raster_x: ラスタの横方向のピクセル数（幅）
            raster_y: ラスタの縦方向のピクセル数（高さ）
            pixel_unit: 各ピクセルの長さ (x方向, y方向)
            origin_point: 左上隅の座標 (x, y)

        Returns:
            coordinate_array: 座標配列(ピクセルID, 中心X座標, 中心Y座標)
                              RGB値をflatten()したときと同じ並び
        """

        # ピクセルのxy座標配列生成
        coordinate_array = self._get_pixel_center_coordinate_array(raster_x,
                                                                   raster_y,
                                                                   pixel_unit,
                                                                   origin_point)

        # pixelの中心点分のid生成
        id_array = np.arange(len(coordinate_array)).reshape((len(coordinate_array),1))

        # pixelの中心を座標に表す
        coordinate_array = np.concatenate([id_array, coordinate_array], 1)

        return coordinate_array


    def generate_geotiff_bytes(self, height_array, tfw_file_list, crs, tiff_datatype=gdal.GDT_Float32):
        """
        指定された標高配列・空間参照情報からGeoTIFF画像を生成し、バイナリ形式で返す

        Parameters:
            height_array: 標高データを格納した2次元配列
            tfw_file_list: TFWに対応する6要素のリスト: [pixel size x方向, rotation x, rotation y, pixel size y方向, x座標の原点, y座標の原点]
                           ※GDAL仕様に合わせて順序を変換して使用する[origin_x, pixel_width, 0, origin_y, 0, pixel_height]
            crs: EPSGコードで指定する座標参照系（例: 6677）
            tiff_datatype: 出力GeoTIFFに使用するGDALデータ型（例: gdal.GDT_Float32）
                           デフォルトは32bit浮動小数点（標高データ向け）

        Returns:
            tiff_bytes: 生成されたバイナリ形式のGeoTIFFファイル
        """

        # メモリ内ファイル名を指定（vsimemで仮想ファイル作成）
        mem_path = '/vsimem/temp_geotiff.tif'

        try:
            # 新規GeoTIFFファイルを作成
            gdal_object = gdal.GetDriverByName('GTiff').Create(
                mem_path,
                height_array.shape[1],
                height_array.shape[0],
                1,
                tiff_datatype
            )

            # TFWファイルの値からGeoTransformを設定（GDAL仕様に合わせて順序を変換）
            # 参考: https://gdal.org/en/stable/tutorials/geotransforms_tut.html
            # GT(0)左上ピクセルの左上隅の x 座標
            # GT(1)ピクセル解像度/ピクセル幅
            # GT(2)行の回転（通常はゼロ）
            # GT(3)左上ピクセルの左上隅の y 座標
            # GT(4)列の回転（通常はゼロ）
            # GT(5)ピクセル解像度 / ピクセル高さ
            gdal_tfw_file_list = (
                tfw_file_list[4],
                tfw_file_list[0],
                0.0,
                tfw_file_list[5],
                0.0,
                tfw_file_list[3]
            )
            gdal_object.SetGeoTransform(gdal_tfw_file_list)

            # CRSを設定
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(crs)
            gdal_object.SetProjection(srs.ExportToWkt())

            # 配列を書き込み
            gdal_object.GetRasterBand(1).WriteArray(height_array)
            gdal_object.FlushCache()

            # メモリ開放
            gdal_object = None

            # メモリ上のファイルを読み込み
            f = gdal.VSIFOpenL(mem_path, "rb")

            size = gdal.VSIStatL(mem_path).size

            tiff_bytes = gdal.VSIFReadL(1, size, f)

            gdal.VSIFCloseL(f)

            return tiff_bytes

        finally:
            # メモリ上のファイルを削除
            gdal.Unlink(mem_path)

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):

        try:
            # -----------------------------------------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # -----------------------------------------------------------------------------------------------------------
            output_target_origin_x, \
                output_target_origin_y, \
                output_target_height, \
                output_target_width, \
                output_target_unit_pixel, \
                output_zip_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # flowfileの値を取得しFieldSetFileをDataFrameに加工
            # -----------------------------------------------------------------------------------------------------------
            _, \
                _, \
                _, \
                target_value\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            # TIN配列の整形処理
            # -----------------------------------------------------------------------------------------------------------
            # 内包判定用TINarray [TIN単位]>[TINの構成点3点]>[id+xy]
            tin_xy_array = target_value[:,:3].reshape(int(len(target_value)/3), 3, 3)

            # pixel中心座標算出用TINarray [TIN単位]>[TINの構成点3点]>[id+xyz]
            tin_xyz_array = target_value[:,:].reshape(int(len(target_value)/3), 3, 4)

            # -----------------------------------------------------------------------------------------------------------
            # DEMの矩形座標
            # -----------------------------------------------------------------------------------------------------------
            # 左上の座標
            left_up_array = np.array([output_target_origin_x,
                                      output_target_origin_y])

            # 右下の座標
            right_down_array = ([output_target_origin_x+output_target_width,
                                 output_target_origin_y-output_target_height])

            # DEMの矩形座標
            rectangle_coordinates_array\
                = np.array([[left_up_array[0], left_up_array[1]],
                            [right_down_array[0], left_up_array[1]],
                            [right_down_array[0], right_down_array[1]],
                            [left_up_array[0], right_down_array[1]]])

            # -----------------------------------------------------------------------------------------------------------
            # 使用するTINだけを抽出
            # -----------------------------------------------------------------------------------------------------------
            # 指定した矩形領域の座標配列に交差または包含されるTIN（三角形）のインデックス配列を取得
            target_tin_index_array\
                = WM.calc_func_time(self.logger)(self._get_target_tin_index_by_rectangle_area_coordinates_array)(rectangle_coordinates_array,
                                                                                                                 tin_xy_array)

            # 使用TINがなければログに出力して終了
            if np.any(target_tin_index_array)==False:
                self.logger.warn(
                    """
                    [CreateDEMByTIN] 指定された出力範囲に該当するTIN（三角形）が見つかりませんでした。
                    プロパティで指定した範囲（OriginX/Y, Width, Height）がTINの分布から外れている可能性があります。
                    設定を見直してください。"""
                )
                return FlowFileTransformResult(relationship="failure")

            else:
                #使用TIN抽出
                tin_xy_array\
                    = tin_xy_array[target_tin_index_array]

                tin_xyz_array\
                    = tin_xyz_array[target_tin_index_array]

            # -----------------------------------------------------------------------------------------------------------
            # 中央の座標生成 id+xy
            # -----------------------------------------------------------------------------------------------------------
            # 縦横のpixel数算出
            width_pixel_counts = int(np.floor(output_target_width / output_target_unit_pixel))
            height_pixel_counts = int(np.floor(output_target_height / output_target_unit_pixel))
            pixel_unit_xy_array = np.array([output_target_unit_pixel, -output_target_unit_pixel])

            # 戻り値id付き
            center_corrdinate_array\
                = self._get_pixel_center_coordinate_array_id(width_pixel_counts,
                                                             height_pixel_counts,
                                                             pixel_unit_xy_array,
                                                             left_up_array)

            # 構成点idを一番後ろに付与（後続の内包判定が一番最後の列に構成点dを要求しているため）
            center_corrdinate_array\
                =np.concatenate([center_corrdinate_array, center_corrdinate_array[:,0:1]], 1)

            # -----------------------------------------------------------------------------------------------------------
            # 内包判定
            # -----------------------------------------------------------------------------------------------------------
            tin_index_array\
                = WM.calc_func_time(self.logger)(NCP.get_point_id_and_tin_index)(center_corrdinate_array,
                                                                                 tin_xy_array)

            # -----------------------------------------------------------------------------------------------------------
            # pixel中心座標に対してZ値付与
            # -----------------------------------------------------------------------------------------------------------
            z_array\
                = WM.calc_func_time(self.logger)(get_z_array_on_3points_plane)(center_corrdinate_array[:,1:3],
                                                                               tin_xyz_array[:,:,1:],
                                                                               tin_index_array[:,1])

            # TINの外側に存在するpixelの値は0で更新する
            target_center_corrdinate_bool=tin_index_array[:,1] == -1
            z_array[target_center_corrdinate_bool] = 0

            z_array = z_array.reshape((height_pixel_counts, width_pixel_counts)).astype(np.float32)

            # -----------------------------------------------------------------------------------------------------------
            # DEM作成
            # -----------------------------------------------------------------------------------------------------------
            # flowfileの属性からcrsを取得(geotiffに設定するため)
            crs = int(flowfile.getAttribute("crs"))

            # TFWに対応する空間参照情報6要素のリスト
            twf_file_list = [output_target_unit_pixel,
                             0.0,
                             0.0,
                             -1 * output_target_unit_pixel,
                             output_target_origin_x,
                             output_target_origin_y]

            # 指定された標高配列・空間参照情報からGeoTIFF画像を生成し、バイナリ形式で取得
            tiff_bytes\
                = WM.calc_func_time(self.logger)(self.generate_geotiff_bytes)(z_array,
                                                                              twf_file_list,
                                                                              crs)

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            filename = flowfile.getAttribute("filename")

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # CSV形式の文字列をZIP圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(filename, tiff_bytes)

                # ZIPデータを取得
                tiff_bytes = zip_buffer.getvalue()

            return FlowFileTransformResult(relationship="success",
                                           contents=tiff_bytes)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
