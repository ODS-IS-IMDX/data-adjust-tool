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

# -----------------------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# 座標データとデジタル標高モデル（DEM）を組み合わせて、地理空間データの標高値を更新するためのツール。
# 座標データの位置をDEMデータ上のピクセルにマッピングし、対応する標高値を取得。
# 指定された更新方法に基づき、座標データの標高値をDEMデータの標高値で更新する。
# 更新方法：既存の値に加算する add 、既存の値を上書き overwrite
# ---------------------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import zipfile
import pickle
import base64
import os
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiRasterioPackage as NRP
import nifiapi.NifiCustomPackage.DigilineCommonPackage as DCP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
rasterio = import_module("rasterio")

ADD = "加算"
OVERWRITE = "上書き"
ZIP_COMPRESSION_ENABLED = "圧縮する"
ZIP_COMPRESSION_DISABLED = "圧縮しない"


class ConvertCoordinatesToCoordinatesWithDrape(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        ドレープ処理（DEMファイルに基づき、Z座標に標高データを反映させる）を行う。処理結果をZIP圧縮することも可能。
                        ①input: 座標配列を持った1行のFieldSetFile。
                        ②output: 座標配列を持った1行のFieldSetFile。
                    """
        tags = ["Coordinates", "DEM", "tiff", "tif", "ZIP", "Unpack", "Python"]

    # coordinatesのDwh名を指定
    COORDINATES_DWH_NAME = PropertyDescriptor(
        name="Coordinates DWH Name",
        description="入力データの座標配列のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 更新方法を指定（加算または上書き）
    UPDATE_METHOD = PropertyDescriptor(
        name="Update Method",
        description="更新方法を指定（加算または上書き）'",
        allowable_values=[ADD, OVERWRITE],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # tiffが格納されているフォルダを指定
    TIFF_FOLDER = PropertyDescriptor(
        name="Tiff folder",
        description="tiffが格納されているフォルダパスを指定",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 拡張子
    TARGET_EXTENT = PropertyDescriptor(
        name="Target Extent",
        description="ファイルの拡張子(「.」ドット)無し",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # ZIP圧縮するかどうかのフラグ（圧縮するまたは圧縮しない）デフォルトは"圧縮しない"
    OUTPUT_ZIP_FLAG = PropertyDescriptor(
        name="Output ZIP Flag",
        description="出力結果をZIP圧縮するかどうかのフラグ",
        default_value=ZIP_COMPRESSION_DISABLED,
        allowable_values=[ZIP_COMPRESSION_ENABLED, ZIP_COMPRESSION_DISABLED],
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    property_descriptors = [COORDINATES_DWH_NAME,
                            UPDATE_METHOD,
                            TIFF_FOLDER,
                            TARGET_EXTENT,
                            OUTPUT_ZIP_FLAG]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):

        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティで入力した値を取得する関数

        引数:
            context: プロセッサの設定値が格納されたデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            coordinates_dwh_name: coordinatesのDwh名
            update_method: 更新方法を指定（加算または上書き）
            tiff_folder: tiffファイルがあるフォルダパス
            dem_information_dwh_name: DEMから取得する最大最小DWHの指定された頭の単語
            output_zip_flag: ZIP圧縮するかどうかのフラグ
        """

        # 各プロパティで設定した値を取得
        coordinates_dwh_name = context.getProperty(
            self.COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        update_method = context.getProperty(
            self.UPDATE_METHOD).evaluateAttributeExpressions(flowfile).getValue()

        tiff_folder = context.getProperty(
            self.TIFF_FOLDER).evaluateAttributeExpressions(flowfile).getValue()

        target_extent\
            = context.getProperty(self.TARGET_EXTENT).evaluateAttributeExpressions(flowfile).getValue()

        output_zip_flag\
            = context.getProperty(self.OUTPUT_ZIP_FLAG).evaluateAttributeExpressions(flowfile).getValue()

        return coordinates_dwh_name, update_method, tiff_folder, target_extent, output_zip_flag

    def get_flowfile_and_transform_each_list(self, flowfile, coordinates_dwh_name):
        """
        概要:
            Flowfileとプロセッサ設定から座標データとDWH情報を抽出する関数

        引数:
            flowfile: プロセッサに入ってくるデータ
            coordinates_dwh_name: 処理対象のDwh名

        戻り値:
            coordinates_array: 処理対象の Value 列のデータ
            dwh_list: 全 Dwh のリスト
            value_list: 全 Value のリスト
            coordinates_dwh_list: 処理対象の Dwh 列のデータ
            coordinates_type_list: 処理対象の Type 列のデータ
        """

        # FlowfileからCSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_data_frame = pd.read_csv(
            io.StringIO(input_field_set_file))

        # coordinates_dwh_nameに一致するDwhのValue列を取得し、pickleからデシリアライズ
        coordinates_array = pickle.loads(base64.b64decode(
            field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == coordinates_dwh_name, "Value"].values[0]))

        # coordinates_dwh_nameに一致する Dwh と Type のリストを取得
        coordinates_dwh_list = field_set_file_data_frame.loc[
            field_set_file_data_frame["Dwh"] == coordinates_dwh_name, "Dwh"].tolist()

        coordinates_type_list = field_set_file_data_frame.loc[
            field_set_file_data_frame["Dwh"] == coordinates_dwh_name, "Type"].tolist()

        return coordinates_array, coordinates_dwh_list, coordinates_type_list

    def get_index_that_is_within_pixel_index_range(self, width_pixel_num, height_pixel_num, geotiff_z_value_array, pixel_x_index_array, pixel_y_index_array):
        """
        概要:
            座標データのZ値をGeoTIFFのZ値で更新する関数

        引数:
            width_pixel_num: 幅のpixel数
            height_pixel_num: 高さのpixel数
            geotiff_z_value_array: GeoTIFFから取得したZ値の配列
            pixel_x_index_array: 座標データのX座標に基づくピクセルXインデックスの配列
            pixel_y_index_array: 座標データのY座標に基づくピクセルYインデックスの配列

        戻り値:
            target_index_array: 処理後のindex
        """

        # ピクセルインデックスの範囲内であるインデックスを取得
        target_index_array = np.where(
            (pixel_x_index_array >= 0) &
            (pixel_x_index_array < width_pixel_num) &
            (pixel_y_index_array >= 0) &
            (pixel_y_index_array < height_pixel_num)
        )

        # 値が存在しない（nan）ピクセルを除外
        target_index_array = target_index_array[0][
            ~np.isnan(
                geotiff_z_value_array[
                    pixel_y_index_array[target_index_array],
                    pixel_x_index_array[target_index_array]
                ]
            )
        ]

        return target_index_array

    def update_z_value(self, coordinates_array, geotiff_z_value_array, pixel_x_index_array, pixel_y_index_array, target_index_array, update_method):
        """
        概要:
            座標データのZ値をGeoTIFFのZ値で更新する関数

        引数:
            coordinates_array: 座標データの配列 [ID, X, Y, Z]
            geotiff_z_value_array: GeoTIFFから取得したZ値の配列
            pixel_x_index_array: 座標データのX座標に基づくピクセルXインデックスの配列
            pixel_y_index_array: 座標データのY座標に基づくピクセルYインデックスの配列
            target_index_array: 更新対象のインデックス配列
            update_method: 更新方法:'add'（加算）または 'overwrite'（上書き）

        戻り値:
            基本的になし
        """

        # 座標データのZ値をGeoTIFFのZ値で更新
        # 加算の場合
        if update_method == ADD:

            coordinates_array[target_index_array, 3] = \
                coordinates_array[target_index_array, 3] + \
                geotiff_z_value_array[pixel_y_index_array[target_index_array],
                                      pixel_x_index_array[target_index_array]]

        # 上書きの場合
        elif update_method == OVERWRITE:

            coordinates_array[target_index_array, 3] = \
                geotiff_z_value_array[pixel_y_index_array[target_index_array],
                                      pixel_x_index_array[target_index_array]]

    def transform(self, context, flowfile):

        try:

            # flowfileの属性からfilenameを取得(writestrに渡すため)
            Attribute_filename = flowfile.getAttribute("filename")

            # 各プロパティで設定した値を取得
            coordinates_dwh_name, \
                update_method, \
                tiff_folder, \
                target_extent, \
                output_zip_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context,
                                                                    flowfile)

            # flowfile から CSV 形式の FieldSetFile を取得し、DataFrame として読み込む
            # coordinates の Dwh の Value 列と全 Dwh・Value のリストを取得
            # coordinates の Dwh と Type のリストを取得(出力の際 FieldSetFile 作成に使用)
            coordinates_array, \
                coordinates_dwh_list, \
                coordinates_type_list\
                = WM.calc_func_time(self.logger)(self.get_flowfile_and_transform_each_list)(flowfile,
                                                                                            coordinates_dwh_name)

            # --------------------------------------------------------------------------
            # フォルダ直下をpathlibで検索 ファイル名（拡張子あり）取得
            # --------------------------------------------------------------------------
            path_list\
                = WM.calc_func_time(self.logger)(DCP.get_file_path)(tiff_folder, target_extent
                                                                    )

            # --------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # GDALでGEOTIFFを読み込んで四隅の情報取得
            # --------------------------------------------------------------------------
            min_x_array, max_x_array, min_y_array, max_y_array, file_name_array\
                = WM.calc_func_time(self.logger)(DCP.get_geotiff_coordinates_information_array)(path_list)
            # --------------------------------------------------------------------------

            lines_points = coordinates_array[:, 1:3].tolist()

            # file_name_arrayと同じインデックスの最大最小xyをそれぞれ抽出するために、file_name_arrayをループ
            for index, filename in enumerate(file_name_array):
                # -----------------------------------------------------------------------------------------------------------
                # 構成点が存在するtiffのみ処理を行うため、処理対象の座標データが tiff データの範囲内に存在するか確認
                found_point_within_bounds = False

                # 各座標データ (x, y) に対して、ポリゴンの範囲内に収まるかどうかチェック
                for point in lines_points:

                    # 現在の座標データを取得
                    x, y = point

                    # 座標がポリゴンの範囲内に収まっているかをチェック
                    if min_x_array[index] <= x <= max_x_array[index] and min_y_array[index] <= y <= max_y_array[index]:

                        # 座標がポリゴンの範囲内に存在する場合、フラグをTrueに設定し、ループを終了
                        found_point_within_bounds = True

                        break
                # -----------------------------------------------------------------------------------------------------------

                # フラグ True (座標がポリゴンの範囲内に存在する)場合
                if found_point_within_bounds:
                    # フルパスを作成
                    file_path = os.path.join(tiff_folder, filename)

                    # TIFF ファイルを読み込み、データを取得
                    with open(file_path, 'rb') as file:
                        geotiff_value = file.read()

                    # GeoTIFFのデータから情報を取得
                    geotiff_z_value_array, \
                        tfw_value_list, \
                        width_pixel_num, \
                        height_pixel_num\
                        = WM.calc_func_time(self.logger)(NRP.get_geotiff_information)(geotiff_value)

                    # 座標データのX、Yを基にピクセルのインデックスを取得
                    pixel_x_index_array, \
                        pixel_y_index_array\
                        = WM.calc_func_time(self.logger)(NRP.get_grid_index)(coordinates_array[:, 1],
                                                                             coordinates_array[:, 2],
                                                                             tfw_value_list)

                    # ピクセルインデックスの範囲内であるインデックスを取得し、値が存在しない（nan）ピクセルを除外
                    target_index_array = WM.calc_func_time(self.logger)(self.get_index_that_is_within_pixel_index_range)(width_pixel_num,
                                                                                                                         height_pixel_num,
                                                                                                                         geotiff_z_value_array,
                                                                                                                         pixel_x_index_array,
                                                                                                                         pixel_y_index_array)

                    # 座標データのZ値をGeoTIFFのZ値で更新
                    WM.calc_func_time(self.logger)(self.update_z_value)(coordinates_array,
                                                                        geotiff_z_value_array,
                                                                        pixel_x_index_array,
                                                                        pixel_y_index_array,
                                                                        target_index_array,
                                                                        update_method)

                # フラグ False (座標がポリゴンの範囲内に存在しない)場合、次のtiffへ
                else:
                    continue

            # set_field_set_file 関数で FieldSetFile(CSV形式) に加工する際 list 形式を求めるため list に変換
            output_list = [coordinates_array]

            # 更新した座標データを含む CSV 形式のFieldSetFileを生成
            output_field_set_file = PBP.set_field_set_file(
                coordinates_dwh_list, coordinates_type_list, output_list)

            if output_zip_flag == ZIP_COMPRESSION_ENABLED:

                # CSV形式の文字列をZIP圧縮
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zip_file:
                    zip_file.writestr(Attribute_filename,
                                      output_field_set_file)

                # ZIPデータを取得
                output_field_set_file = zip_buffer.getvalue()

            return FlowFileTransformResult(relationship="success",
                                           attributes={
                                               "DWH": coordinates_dwh_name},
                                           contents=output_field_set_file)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
