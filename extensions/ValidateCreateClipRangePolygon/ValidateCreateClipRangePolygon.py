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

from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
from common.error_code_list import ErrorCodeList


class ValidateCreateClipRangePolygon(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「CreateClipRangePolygon」の入力データのチェックを行う'''
        tags = ['fieldsetfile', 'python', 'validate']

    #:
    DATA_NAME = PropertyDescriptor(
        name="Data Name",
        description="存在範囲の情報が含まれるデータ名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    SUFFIX = PropertyDescriptor(
        name="Suffix",
        description="存在範囲の情報が含まれるデータの属性名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [DATA_NAME, SUFFIX]

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
            data_name = context.getProperty(self.DATA_NAME).getValue()
            suffix = context.getProperty(self.SUFFIX).getValue()

            # インプットデータ取得
            fsf = flowfile.getContentsAsBytes()

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(fsf)
            if not result:
                return self.RESULT_FAILURE

            # 列名称が存在するかチェック
            if any((df["Dwh"] == f"{data_name}/{suffix}") & (df["Type"] == "geometry")):
                # Dwhが{geom_name}/{attr_name}のValueを取得
                geometry_list = df.loc[df["Dwh"] == f"{data_name}/{suffix}", "Value"].values[0]
                # 取得したジオメトリがLineStringかどうかチェック
                if not all(self.validate_data_types(geom, LineString) for geom in geometry_list):
                    return self.RESULT_FAILURE
            else:
                args = {
                    "error_code": ErrorCodeList.EC00009,
                    "列名称": f"Dwh：{data_name}/{suffix}、Type：geometry"
                }
                self.validate_logger.write_log(**args)
                return self.RESULT_FAILURE

            # ValidateLogを生成
            if not self.validate_log_of_fsf(fsf, df, LineString):
                return self.RESULT_FAILURE

            return self.RESULT_SUCCESS
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
