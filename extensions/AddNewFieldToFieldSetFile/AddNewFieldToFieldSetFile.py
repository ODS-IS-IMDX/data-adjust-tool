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
# 1行のFieldSetFileに属性を追加するプロセッサ。
# ○○/FIDのときのみ処理が変わる。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

TYPE_INT = "整数型"
TYPE_FLOAT = "浮動小数点型"
TYPE_STR = "文字列型"


class AddNewFieldToFieldSetFile(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''
                        FieldSetFileに指定したフィールドを追加する。
                        ①input: 任意のフィールドを持った1行のFieldSetFile。
                        ②output: 新規のフィールドを持った1行のFieldSetFile。
                      '''
        tags = ["Append", "Python"]

    # 新規属性のDwh名
    NEW_FIELD_DWH_NAME = PropertyDescriptor(
        name='New Field DWH Name',
        description='FieldSetFileに追加する新規のフィールドのDWH名',
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
    )

    NEW_FIELD_DEFAULT_VALUE = PropertyDescriptor(
        name='New Field Default Value',
        description='追加する新規のフィールドの既定値',
        required=True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
    )

    NEW_FIELD_VALUE_TYPE = PropertyDescriptor(
        name='New Field Value Type',
        description='追加する新規フィールドの既定値のデータ型',
        required=True,
        allowable_values=[TYPE_INT, TYPE_FLOAT, TYPE_STR],
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
    )

    property_descriptors = [NEW_FIELD_DWH_NAME,
                            NEW_FIELD_DEFAULT_VALUE,
                            NEW_FIELD_VALUE_TYPE]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要:
            プロパティの値を取得

        引数:
            context: フローファイルの中身のデータ
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            new_field_dwh_name: 新規のフィールド名
            new_field_default_value: 入力した数値
            new_field_value_type: 入力したタイプ
        """

        new_field_dwh_name = context.getProperty(
            self.NEW_FIELD_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        new_field_default_value = context.getProperty(
            self.NEW_FIELD_DEFAULT_VALUE).evaluateAttributeExpressions(flowfile).getValue()

        new_field_value_type = context.getProperty(
            self.NEW_FIELD_VALUE_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        return new_field_dwh_name, new_field_default_value, new_field_value_type

    def get_type(self, new_field_value_type):
        """
        概要:
            タイプをpythonの型に変える。

        引数:
            new_field_value_type: 入力したタイプ

        戻り値:
            new_type: pythonの型
        """
        if new_field_value_type == TYPE_INT:
            new_type = int
        elif new_field_value_type == TYPE_FLOAT:
            new_type = float
        else:
            new_type = str

        return new_type

    def create_new_value(self, base_dwh_name, new_type, new_field_default_value, base_field_list):
        """
        概要:
            新規属性の行を作成する。

        引数:
            base_dwh_name: 基準となるDWH
            new_type: 新規属性のデータ型
            new_field_default_value: 規定値
            base_field_list: 基準となる属性のリスト

        戻り値:
            new_field_list: 新規属性のリスト
        """

        parts_list = base_dwh_name.split('/')

        # FIDの場合は右側がFindex
        if parts_list[-1] == "FID":
            new_field_list = [(findex, new_type(new_field_default_value))
                              for _, findex in base_field_list]

        # それ以外は左がFindex
        else:
            new_field_list = [(findex, new_type(new_field_default_value))
                              for findex, _ in base_field_list]

        return new_field_list

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            new_field_dwh_name, \
                new_field_default_value, \
                new_field_value_type\
                = WM.calc_func_time(self.logger)(self.get_property)(context,
                                                                    flowfile)

            # タイプをpythonのタイプに変換
            new_type = WM.calc_func_time(self.logger)(
                self.get_type)(new_field_value_type)

            # 1行のfieldsetfileを取得
            field_set_file_data_frame, \
                base_dwh_name, \
                base_type, \
                base_field_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # 新規属性のリスト
            new_field_list\
                = WM.calc_func_time(self.logger)(self.create_new_value)(base_dwh_name,
                                                                        new_type,
                                                                        new_field_default_value,
                                                                        base_field_list)

            # fieldsetfile用のリストに新しい行のデータを格納
            dwh_list = [new_field_dwh_name]
            type_list = [new_type.__name__]
            value_list = [new_field_list]

            # fieldsetfileに変換
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                         type_list,
                                                                         value_list)

            if new_field_dwh_name == "":
                attributes_dict = {}

            else:
                attributes_dict = {"DWH": new_field_dwh_name}

            # 成功リレーションと共に処理結果を返す
            return FlowFileTransformResult(relationship='success', contents=output_field_set_file, attributes=attributes_dict)

            # エラーが発生した場合、失敗リレーションを返す
        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship='failure')
