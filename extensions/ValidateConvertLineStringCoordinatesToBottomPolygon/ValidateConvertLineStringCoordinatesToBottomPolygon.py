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
np = import_module("numpy")
LineString = getattr(import_module("shapely.geometry"), "LineString")

from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from common.error_code_list import ErrorCodeList
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators


class ValidateConvertLineStringCoordinatesToBottomPolygon(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「ConvertLineStringCoordinatesToBottomPolygon」の入力データのチェックを行う'''
        tags = ['FieldSetFile', 'python', 'validate']

    #:
    WIDTH_NAME = PropertyDescriptor(
        name='Width Name',
        description='線分に対して生成する底面ポリゴンの辺と線分の距離を指定する。',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [WIDTH_NAME]

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
            width_name = context.getProperty(self.WIDTH_NAME).getValue()

            # crs属性存在チェック
            if not flowfile.getAttribute("crs"):
                args = {
                    "error_code": ErrorCodeList.EC00014,
                    "attribute_name": "crs",
                }
                self.validate_logger.write_log(**args)
                return self.RESULT_FAILURE

            fsf = flowfile.getContentsAsBytes()

            # インプットデータの検証
            if not self.validate_empty_data(fsf):
                return self.RESULT_FAILURE

            # FieldSetFileの形式をチェック
            result, df = self.validate_fsf_format(fsf)
            if not result:
                return self.RESULT_FAILURE

            # ジオメトリが存在し、空でないこと + GeoNdarrayにnanが存在しないことを確認
            if not self.validate_exist_geometry(df):
                return self.RESULT_FAILURE

            # 属性情報が含まれているかチェック
            if not self.validate_exist_attribute(df, [width_name]):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # Value列の型がLineStringまたはnumpy.ndarrayかチェック
            geometry_df = df[(df["Type"] == "geometry")]
            for _, row in geometry_df.iterrows():
                geometry = row["Value"]
                if isinstance(geometry[0], LineString):
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

                    # 配列要素がfloat型かチェック
                    if not all(self.validate_data_types(value, float) for row in geometry for value in row):
                        return self.RESULT_FAILURE

                else:
                    args = {
                        "error_code": ErrorCodeList.EC00008,
                        "期待値": "LineString or numpy.ndarray"
                    }
                    self.validate_logger.write_log(**args)
                    return self.RESULT_FAILURE

            # バリデータログ作成
            if not self.validate_log_of_fsf(fsf, df):
                return self.RESULT_FAILUER
            
            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")

