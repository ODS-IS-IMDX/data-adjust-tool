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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# IDと属性値が格納されたtupleを格納したlistの属性値をすべて既定値に置き換えるプロセッサ。
# 新しいfieldsetfileを作成するプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準モジュール
import traceback

# Nifi自作モジュール
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

STR = "文字列型"
INT = "整数型"
FLOAT = "浮動小数点型"


class ConvertAllFieldsValuesToDefaultValue(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        コンテンツのフィールドを指定した値に一律で置き換える。
                        ①input: 任意のフィールドを持った1行のFieldSetFile。
                        ②output: 任意のフィールドを持った1行のFieldSetFile。
                      """
        tags = ['Replace', 'Python']

    # 置換後の既定値を指定
    DEFAULT_VALUE = PropertyDescriptor(
        name="Replacement Word",
        description="置換後の文字",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True
    )

    # 置換後の既定値を指定
    DEFAULT_VALUE_TYPE = PropertyDescriptor(
        name="Replacement Type",
        description="置換後の文字の型",
        required=True,
        allowable_values=[INT, FLOAT, STR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
    )

    # 新しいDWHファイル名をプロパティで入力。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [DEFAULT_VALUE,
                            DEFAULT_VALUE_TYPE,
                            OUTPUT_DWH_NAME]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要
            プロパティで入力した値を取得する関数
        引数
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ
        戻り値
            default_value: 既定値
            default_value_type: 既定値の型
            output_dwh_name: 出力データのDWH
        """
        # プロパティから既定値を取得
        default_value = context.getProperty(self.DEFAULT_VALUE).getValue()

        # 既定値の型
        default_value_type = context.getProperty(
            self.DEFAULT_VALUE_TYPE).getValue()

        # output用のDWH名をプロパティの値から取得
        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return default_value, default_value_type, output_dwh_name

    def replace_second_element(self, target_list, default_value_type, default_value):
        """
        概要
            属性リスト内のタプルの2番目の要素を既定値に置き換える
        引数
            target_list - Findexと属性が格納されたlist
            default_value_type - 既定値の型
            default_value: 既定値
        戻り値
            converted_target_list: 置換後のlist
        """
        # 各型で属性を既定値に置換
        if default_value_type == STR:
            converted_target_list = [(tuple[0], default_value)
                                     for tuple in target_list]

        elif default_value_type == INT:
            converted_target_list = [(tuple[0], int(default_value))
                                     for tuple in target_list]

        elif default_value_type == FLOAT:
            converted_target_list = [
                (tuple[0], float(default_value)) for tuple in target_list]

        return converted_target_list

    def create_output_field_set_file(self, output_dwh_name, target_dwh, converted_target_list):
        """
        概要
            output_field_set_fileを作成する関数。
        引数
            output_dwh_name: 出力のDWH名
            target_dwh: 入力のDWH名
            converted_target_list: 置換後の属性データ
        戻り値
            output_field_set_file: 出力のフィールドセットファイル
            attributes_dict: DWH名が格納されたdict
        """
        # 型を取得
        output_type = type(converted_target_list[0][1]).__name__

        # set_field_set_file用にリスト化
        output_dwh_name_list = [output_dwh_name]
        target_dwh_list = [target_dwh]
        output_type_list = [output_type]
        output_target_list = [converted_target_list]

        # 新しいDWH名が空の場合はgeometryのDWH名を使う
        if output_dwh_name == "" or not output_dwh_name:
            output_field_set_file = WM.calc_func_time(self.logger, False)(
                PBP.set_field_set_file)(target_dwh_list, output_type_list, output_target_list)
            attributes_dict = {"DWH": target_dwh}

        else:
            output_field_set_file = WM.calc_func_time(self.logger, False)(
                PBP.set_field_set_file)(output_dwh_name_list, output_type_list, output_target_list)
            attributes_dict = {"DWH": output_dwh_name}

        return output_field_set_file, attributes_dict

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティから default_value default_value_type output_dwh_nameを取得
            default_value, \
                default_value_type, \
                output_dwh_name\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # フィールドセットファイルからdwhと属性データlistを取得
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                target_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 属性値を既定値に置き換える。
            converted_target_list\
                = WM.calc_func_time(self.logger)(self.replace_second_element)(target_list, default_value_type, default_value)

            # 出力のフィールドセットファイルを作成
            output_field_set_file, \
                attributes_dict\
                = WM.calc_func_time(self.logger)(self.create_output_field_set_file)(output_dwh_name,
                                                                                    target_dwh,
                                                                                    converted_target_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
