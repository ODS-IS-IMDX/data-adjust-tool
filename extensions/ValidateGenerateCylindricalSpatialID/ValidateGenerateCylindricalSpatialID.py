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
from importlib import import_module

# 外部ライブラリの動的インポート
LineString = getattr(import_module("shapely.geometry"), "LineString")
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
np = import_module("numpy")

import cad.common.cad_utils as CU
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from common.error_code_list import ErrorCodeList

from nifiapi.properties import (
    PropertyDescriptor,
    ExpressionLanguageScope,
    StandardValidators,
)


class ValidateGenerateCylindricalSpatialID(CadBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """プロセッサー「GenerateSpatialID」の入力データのチェックを行う"""
        tags = ["fieldsetfile", "python", "validate"]

    #:
    RADIUS_NAME = PropertyDescriptor(
        name="Radius Name",
        description="半径情報を持つ属性名を指定。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    FEATURE_ID_COLUMN_NAME = PropertyDescriptor(
        name='Feature ID Column Name',
        description='地物IDのカラム名を指定する。',
        default_value='',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [RADIUS_NAME, FEATURE_ID_COLUMN_NAME]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_data(self, context, flowfile):
        """
        入力データのチェックを実行する

        :param context: プロセッサの設定値が格納されているデータ
        :param flowfile: プロセッサに渡されるデータ

        :return: チェック結果 正常="success"、異常="failure"
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            byte_data = flowfile.getContentsAsBytes()
            radius_name = context.getProperty(self.RADIUS_NAME).getValue()
            fid_col_name = context.getProperty(self.FEATURE_ID_COLUMN_NAME).getValue()
            crs = flowfile.getAttribute("crs")

            # データの存在チェック
            if not self.validate_empty_data(byte_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式であること
            result, df = self.validate_fsf_format(byte_data)
            if not result:
                return self.RESULT_FAILURE

            coordinates_rows = df[df['Dwh'].str.split('/').str[-1] == 'coordinates']

            # 'Value'列の値を取得
            value_list = coordinates_rows['Value'].iloc[0]
            ndarray = np.array(value_list)
            
            if not self.validate_no_nan(ndarray):
                return self.RESULT_FAILURE
            if not self.validate_exist_attribute(df, [fid_col_name, radius_name]):
                return self.RESULT_FAILURE

            # 地物IDの重複チェックを行う
            if not self.validate_duplicates_fid_data(df, fid_col_name):
                return self.RESULT_FAILURE

            # geometryがlineであること
            gdf = CU.field_set_file_to_geodataframe(byte_data, crs)
            if not self.validate_geometry_type(gdf):
                return self.RESULT_FAILURE
        
            # ジオメトリ有効チェック
            if not self.validate_geodataframe_geometry(gdf):
                return self.RESULT_FAILURE
            
            # GeoDataFrameの行数・列数チェック
            if not self.validate_gdf_shape(gdf):
                return self.RESULT_FAILURE

            return self.RESULT_SUCCESS
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")

    def validate_geometry_type(self, gdf):
        """
        ジオメトリがLineStringかどうかチェックする

        :param gdf: GeoDataFrame
        :type gdf: geopandas.GeoDataFrame

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        result = True
        for geom in gdf.geometry:
            if not isinstance(geom, LineString):
                result = False
                self.validate_logger.write_log(error_code=ErrorCodeList.EC00008, **{"ジオメトリタイプ:": geom.geom_type})
                if self.mode_value == self.MODE_STOP:
                    return result
        return result

    def validate_duplicates_fid_data(self, df, fid_col_name):
        """
        地物ID情報に重複がないかをチェックする
        
        :param df: DataFrame
        :type df: pandas.DataFrame

        :param fid_col_name: 地物ID情報のカラム名
        :type fid_col_name: str

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        result = True

        for i, row in df.iterrows():
            if row["Dwh"].split("/")[1] != fid_col_name:
                # 対象レコードが地物ID情報以外の場合はコンティニュー
                continue
            # 地物ID情報の重複チェックを行う
            if not self.validate_data_uniqueness(row["Value"], 0)[0] or not self.validate_data_uniqueness(row["Value"], 1)[0]:
                result = False
                args = {
                    "error_code": ErrorCodeList.EC00013,
                    "対象": row["Dwh"]
                }
                self.validate_logger.write_log(**args)
                if self.mode_value == self.MODE_STOP:
                    return result

        return result
