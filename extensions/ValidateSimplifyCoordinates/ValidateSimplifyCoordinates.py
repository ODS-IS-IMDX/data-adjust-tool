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

from importlib import import_module

np = import_module("numpy")
BaseGeometry = getattr(import_module("shapely.geometry.base"), "BaseGeometry")
gpd = import_module("geopandas")

from cad.common import cad_utils as CU
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from common.error_code_list import ErrorCodeList
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators


class ValidateSimplifyCoordinates(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「SimplifyCoordinates」の入力データのチェックを行う'''
        tags = ['fieldsetfile', 'numpy', 'GeoNdarray', 'python', 'validate']

    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name='Geometry Name',
        description='対象となるジオメトリ情報の名前を指定する。',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [GEOMETRY_NAME]

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
            # プロパティ取得
            geom_name = context.getProperty(self.GEOMETRY_NAME).getValue()

            fsf = flowfile.getContentsAsBytes()

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(fsf)
            if not result:
                return self.RESULT_FAILURE

            # DataFrameから対象のジオメトリデータのみを取得
            geometry_df = df[
                (df["Type"] == "geometry")
                & (df["Dwh"].str.split("/").str[0] == geom_name)
            ]

            # 対象のジオメトリデータがあるかチェック
            if geometry_df.empty:
                args = {
                    "error_code": ErrorCodeList.EC00010,
                    "対象": "Dwhのデータ名：geom_name、Type：geometry"
                }
                self.validate_logger.write_log(**args)
                return self.RESULT_FAILURE

            # Value列の型がBaseGeometryまたはnp.ndarrayかチェック
            for _, row in geometry_df.iterrows():
                geometry = row["Value"]
                if isinstance(geometry[0], BaseGeometry):
                    pass
                elif isinstance(geometry, np.ndarray):
                    # GeoNdarray形式かどうかチェック
                    if geometry.ndim != 2:
                        args = {
                            "error_code": ErrorCodeList.EC00006,
                            "対象": "GeoNdarray"
                        }
                        self.validate_logger.write_log(**args)
                        return self.RESULT_FAILURE

                    # 配列の列数をチェック
                    if geometry.shape[1] not in [3, 4]:
                        args = {
                            "error_code": ErrorCodeList.EC00006,
                            "期待値": "3列または4列の配列"
                        }
                        self.validate_logger.write_log(**args)
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE

                    # GeoNdarrayにnanが含まれているかチェック
                    if not self.validate_no_nan(geometry):
                        return self.RESULT_FAILURE

                    # 配列要素がfloat型かチェック
                    if not all(self.validate_data_types(value, float) for row in geometry for value in row):
                        return self.RESULT_FAILURE

                else:
                    args = {
                        "error_code": ErrorCodeList.EC00008,
                        "期待値": "shapely.BaseGeometry or numpy.ndarray"
                    }
                    self.validate_logger.write_log(**args)
                    return self.RESULT_FAILURE

            # 行数列数のValidateLogを生成
            for index, row in geometry_df.iterrows():
                # Dwhの値を「/」で分割する
                dwh_left, dwh_right = row["Dwh"].split("/")

                # ジオメトリデータからGeoDataFrameを生成する
                geometry = row["Value"]
                if isinstance(geometry[0], BaseGeometry):
                    gdf = gpd.GeoDataFrame(geometry=geometry)
                else:
                    gdf = CU.geo_ndarray_to_geodataframe(geometry)

                # 生成したGeoDataFrameのジオメトリ有効チェックを行う
                if not self.validate_geodataframe_geometry(gdf):
                    return self.RESULT_FAILURE

                # 同データ名で複数のジオメトリデータが存在する場合は、Dwhの右側の値を括弧書きで追加する
                data_name = dwh_left if len(geometry_df) == 1 else f"{dwh_left}({dwh_right})"

                # 行数列数のValidateLogを生成
                if not self.validate_gdf_shape(gdf, data_name):
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")

