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
import pickle

# 外部ライブラリの動的インポート
LineString = getattr(import_module("shapely.geometry"), "LineString")

import cad.common.cad_utils as CU
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
from common.error_code_list import ErrorCodeList


class ValidateExtractLineCoordinates(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「ExtractLineCoordinates」の入力データのチェックを行う'''
        tags = ['pickle', 'python', 'validate']

    #:
    CRS = PropertyDescriptor(
        name='crs',
        description='使用する座標参照系(CRS)を指定する。',
        default_value='6677',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    property_descriptors = [CRS]

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
            input_data = flowfile.getContentsAsBytes()
            crs = context.getProperty(self.CRS).getValue()
            
            # GeoDataFrameがInputの場合、LineStringが含まれていること
            if self.validate_serialized_data(input_data)[0]:
                gdf = pickle.loads(input_data)
                if self.validate_geometry_type(gdf):
                    # ジオメトリ有効チェック
                    if not self.validate_geodataframe_geometry(gdf):
                        return self.RESULT_FAILURE
                    
                    # GeoDataFrameの行数・列数チェック
                    if not self.validate_gdf_shape(gdf):
                        return self.RESULT_FAILURE
                else:
                    return self.RESULT_FAILURE
            else:
                result, df = self.validate_fsf_format(input_data)
                if not result:
                    return self.RESULT_FAILURE
                
                # FieldSetFileがInputの場合、LineStringが含まれていること
                gdf = CU.field_set_file_to_geodataframe(input_data, crs)
                
                if self.validate_geometry_type(gdf):
                    # バリデータログ作成
                    if not self.validate_log_of_fsf(input_data, df):
                        return self.RESULT_FAILUER
                else:
                    return self.RESULT_FAILURE
            
            
            return self.RESULT_SUCCESS

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")


    def validate_geometry_type(self, gdf):
        """
        ジオメトリがLineStringかどうかチェックする
        
        :param gdf: GeoDataFrame
        :return: チェック結果 正常=true、異常=False
        """
        result = True
        for geom in gdf.geometry:
            if not isinstance(geom, LineString):
                result = False
                self.validate_logger.write_log(error_code=ErrorCodeList.EC00008, **{"ジオメトリタイプ:": geom.geom_type})
                if self.mode_value == self.MODE_STOP:
                    return result
        return result
