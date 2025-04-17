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

from common.error_code_list import ErrorCodeList
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor


class ValidateTransformGeoDataFrameToPixel(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「TransformGeoDataFrameToPixel」の入力データのチェックを行う'''
        tags = ['geodataframe', 'geopandas', 'python', 'validate']

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
            # インプットデータ取得
            gdf = flowfile.getContentsAsBytes()

            # インプットデータの検証
            if not self.validate_empty_data(gdf):
                return self.RESULT_FAILURE

            # インプットがGeoDataFrameに変換できるかチェックする
            result, gdf = self.validate_serialized_data(gdf)
            if not result:
                return self.RESULT_FAILURE

            # 必須フィールド（layer）が存在するかチェックする
            if not self.validate_exist_attribute(gdf, attribute_names=["layer"]):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE
            
            # ジオメトリタイプが、Point、LineString、Polygonかどうかチェックする
            for idx, geom in enumerate(gdf.geometry):
                geom_type = geom.geom_type
                if geom_type not in ["Point", "LineString", "Polygon"]:
                    kwargs = {
                        "ジオメトリタイプ": geom_type
                    }
                    self.validate_logger.write_log(error_code=ErrorCodeList.EC00017, record_number=idx, **kwargs)
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE
            
            # ジオメトリ有効チェック
            if not self.validate_geodataframe_geometry(gdf):
                return self.RESULT_FAILURE
            
            # GeoDataFrameの行数・列数チェック
            if not self.validate_gdf_shape(gdf):
                return self.RESULT_FAILURE
            
            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
