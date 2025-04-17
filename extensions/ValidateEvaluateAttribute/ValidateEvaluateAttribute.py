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

from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from common.error_code_list import ErrorCodeList
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope


class ValidateEvaluateAttribute(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「EvaluateAttribute」の入力データのチェックを行う'''
        tags = [
            "fieldsetfile",
            "dataframe",
            'python',
            'validate'
        ]

    #:
    GEOMETRY_NAME = PropertyDescriptor(
        name="GeometryName",
        description="対象のジオメトリデータ名。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    #:
    ATTRIBUTE_NAME = PropertyDescriptor(
        name="AttributeName",
        description="対象の属性データ名。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )

    property_descriptors = [GEOMETRY_NAME, ATTRIBUTE_NAME]

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
            attr_name = context.getProperty(self.ATTRIBUTE_NAME).getValue()

            # インプットデータ取得
            fsf = flowfile.getContentsAsBytes()

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(fsf)
            if not result:
                return self.RESULT_FAILURE

            # Dwh名が{geom_name}/{attr_name}のValueを取得
            if any(df["Dwh"] == f"{geom_name}/{attr_name}"):
                geo_att_value = df.loc[df["Dwh"] == f"{geom_name}/{attr_name}", "Value"].values[0]
            else:
                args = {
                    "error_code": ErrorCodeList.EC00009,
                    "列名称": f"{geom_name}/{attr_name}"
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # Valueの型がlistか、list内の要素の型がtupleかチェック
            if self.validate_data_types(geo_att_value, list):
                for val in geo_att_value:
                    if not self.validate_data_types(val, tuple):
                        return self.RESULT_FAILURE
            else:
                return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
