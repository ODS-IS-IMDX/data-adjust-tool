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

from cad.common import cad_utils as CU
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from common.error_code_list import ErrorCodeList


class ValidateTransformGeoNdarrayToGeoDataFrame(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「TransformGeoNdarrayToGeoDataFrame」の入力データのチェックを行う'''
        tags = ['GeoNdarray', 'python', 'validate']

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
            input_data = flowfile.getContentsAsBytes()

            if not flowfile.getAttribute("crs"):
                args = {
                        "error_code": ErrorCodeList.EC00014,
                        "attribute_name": "crs",
                    }
                self.validate_logger.write_log(**args)
                return self.RESULT_FAILURE

            # インプットがシリアライズされているかチェック
            result, ndarray = self.validate_serialized_data(input_data)
            if not result:
                return self.RESULT_FAILURE

            # ndarray型であるかどうかをチェック
            if not self.validate_data_types(ndarray, np.ndarray):
                return self.RESULT_FAILURE

            # GeoNdarray形式かどうかチェック
            if ndarray.ndim != 2:
                args = {
                    "error_code": ErrorCodeList.EC00006,
                    "対象": "GeoNdarray"
                }
                self.validate_logger.write_log(**args)
                return self.RESULT_FAILURE

            # 配列の列数をチェック
            if ndarray.shape[1] not in [3, 4]:
                args = {
                    "error_code": ErrorCodeList.EC00006,
                    "期待値": "3列または4列の配列"
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # GeoNdarrayにnanが含まれているかチェック
            if not self.validate_no_nan(ndarray):
                return self.RESULT_FAILURE

            # 配列要素がfloat型かチェック
            if not all(self.validate_data_types(value, float) for row in ndarray for value in row):
                return self.RESULT_FAILURE

            # GeoNdarray->GeoDataFrame変換し、ジオメトリの有効チェックを行う
            gdf = CU.geo_ndarray_to_geodataframe(ndarray)
            if not self.validate_geodataframe_geometry(gdf):
                return self.RESULT_FAILURE

            # 行数列数のValidateLogを生成
            if not self.validate_gdf_shape(gdf, "GeoNdarray"):
                return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE
            
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
