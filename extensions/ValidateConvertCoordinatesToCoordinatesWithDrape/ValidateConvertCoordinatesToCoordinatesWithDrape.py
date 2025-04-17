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

# Python標準モジュール
import base64
import pickle
import io
import pathlib

# 外部モジュール
from importlib import import_module

# Nifiライブラリ
from data_processing.common.data_processing_base_validate_processor import DataProcessingBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from common.error_code_list import ErrorCodeList
import cad.common.cad_utils as CU

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")
rasterio = import_module("rasterio")
osgeo = import_module("osgeo")
gdal = import_module("osgeo.gdal")
gdalconst = import_module("osgeo.gdalconst")

ADD = "加算"
OVERWRITE = "上書き"


class ValidateConvertCoordinatesToCoordinatesWithDrape(DataProcessingBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """
                ConvertCoordinatesToCoordinatesWithDrapeで必要な座標配列を持っているか、
                プロパティで指定したフォルダパスは有効か検証。
            """
        )
        tags = ["validate", "python"]

    # 座標配列のDWH名
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

    # tiffが格納されているフォルダパス
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

    property_descriptors = [COORDINATES_DWH_NAME,
                            UPDATE_METHOD,
                            TIFF_FOLDER,
                            TARGET_EXTENT]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_data(self, context, flowfile):
        """
        入力データの検証を実行する

        :param context: プロセッサの設定値が格納されているデータ
        :param flowfile: プロセッサに渡されるデータ

        :return 検証結果 正常="success"、異常="failure"
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # データの空検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式検証
            result, df = self.validate_fsf_format(input_data)
            if not result:
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            input_field_set_file = input_data.decode("utf-8")

            # flowfileのデータを、パスとして認識させる
            field_set_file_stringio = io.StringIO(input_field_set_file)

            # DataFrameに変換
            field_set_file_data_frame = pd.read_csv(field_set_file_stringio)

            try:
                # flowfileから"crs"属性の内容を取得する
                crs = flowfile.getAttribute("crs")
            except Exception:
                self.validate_logger.write_log(
                    error_code=ErrorCodeList.ED00039)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 座標配列のDWHファイル名を取得
            coordinates_dwh_name \
                = context.getProperty(self.COORDINATES_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # coordinates_dwh_nameがFieldSetFileに存在するか検証
            if not self.validate_target_in_fsf(coordinates_dwh_name, field_set_file_data_frame):
                return self.RESULT_FAILURE

            # coordinates_dwh_nameに一致するDwhのValue列を取得し、pickleからデシリアライズ
            coordinates_array = pickle.loads(base64.b64decode(
                field_set_file_data_frame.loc[field_set_file_data_frame["Dwh"] == coordinates_dwh_name, "Value"].values[0]))

            # 座標配列の形か検証
            if not self.validate_input_type(coordinates_array, "coordinates_array"):

                result = False

                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # tiff_folderを取得
            tiff_folder\
                = context.getProperty(self.TIFF_FOLDER).evaluateAttributeExpressions(flowfile).getValue()

            # 拡張子を取得
            target_extent\
                = context.getProperty(self.TARGET_EXTENT).evaluateAttributeExpressions(flowfile).getValue()

            # 指定したフォルダが存在するのか検証
            if not self.validate_is_valid_directory(tiff_folder):
                return self.RESULT_FAILURE

            # pathlib_objectに加工
            pathlib_object = pathlib.Path(tiff_folder)
            path_list = list(pathlib_object.glob('*.' + target_extent))

            if path_list:
                pass

            else:
                args = {"error_code": ErrorCodeList.ED00050,
                        "Target Extent": target_extent}
                self.validate_logger.write_log(**args)
                result = False
                return self.RESULT_FAILURE

            for path in path_list:
                try:
                    gdal_object = gdal.Open(str(path), gdalconst.GA_ReadOnly)

                    # 座標の情報取得
                    tfw_file_tuple = gdal_object.GetGeoTransform()

                    # 明示的メモリの解放
                    gdal_object = None

                except Exception:
                    args = {"error_code": ErrorCodeList.ED00051,
                            "Tiff folder": path}
                    self.validate_logger.write_log(**args)
                    result = False
                    return self.RESULT_FAILURE

            geodataframe = CU.field_set_file_to_geodataframe(input_data, crs)

            if not self.validate_gdf_shape(geodataframe, data_name="GeoDataFrame"):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS

            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
