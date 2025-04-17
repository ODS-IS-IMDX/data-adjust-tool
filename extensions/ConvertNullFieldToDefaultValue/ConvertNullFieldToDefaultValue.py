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
# 1つの属性のNull値を既定値に置き換えるプロセッサ。
# --------------------------------------------------------------------------------------------

# Python標準モジュール
import traceback
import math

# Nifi自作モジュール
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

TYPE_INT = "整数型"
TYPE_FLOAT = "浮動小数点型"
TYPE_STR = "文字列型"

DELETE_TRUE = "上書きする"
DELETE_FALSE = "上書きしない"


class ConvertNullFieldToDefaultValue(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        フィールドが持つNull値を既定値で上書きする。
                        ①input: 任意のフィールドを持った1行のFieldSetFile。
                        ②output: 入力したフィールドを持った1行のFieldSetFile。
                      """
        tags = ["Null", "None", "nan", "Python"]

    # 置換後の既定値を指定
    DEFAULT_VALUE = PropertyDescriptor(
        name="Default Value",
        description="Null値を置換した後の既定値",
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=True
    )

    # 置換後の既定値の型
    DEFAULT_VALUE_TYPE_IF_ALL_NULL = PropertyDescriptor(
        name="Default Value Type If All Null",
        description="全てがNull値だった時の、既定値のデータ型",
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        allowable_values=[TYPE_INT, TYPE_FLOAT, TYPE_STR],
    )

    # 適切な値の範囲に収まる最大値
    MAX_VALUE_IN_RANGE = PropertyDescriptor(
        name="Max Value In Range",
        description="適切な値の範囲に収まる最大値（未入力時は最大値を定めない。）",
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        required=False
    )

    # 適切な値の範囲に収まる最小値
    MIN_VALUE_IN_RANGE = PropertyDescriptor(
        name="Min Value In Range",
        description="適切な値の範囲に収まる最小値（未入力時は最小値を定めない。）",
        required=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
    )

    # 新しいDWHファイル名をプロパティで入力。
    OUTPUT_DWH_NAME = PropertyDescriptor(
        name="Output DWH Name",
        description="出力データのDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 文字列のNoneを削除したいかどうか。
    DELETE_STRING_NULL_FLAG = PropertyDescriptor(
        name="Delete String Null Flag",
        description="文字列のNull,NULL,None,NONE,nan,Nan,NaN,#N/Aも削除するかどうかのフラグ",
        required=True,
        expression_language_scope=ExpressionLanguageScope.NONE,
        sensitive=False,
        default_value=DELETE_TRUE,
        allowable_values=[DELETE_TRUE, DELETE_FALSE],
    )

    property_descriptors = [DEFAULT_VALUE,
                            DEFAULT_VALUE_TYPE_IF_ALL_NULL,
                            MAX_VALUE_IN_RANGE,
                            MIN_VALUE_IN_RANGE,
                            OUTPUT_DWH_NAME,
                            DELETE_STRING_NULL_FLAG]

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
            default_value: 入力した数値
            default_value_type_if_all_null: 全ての値がNullの時のタイプ
            max_value_in_range: 適切な値の範囲に収まる最大値
            min_value_in_range: 適切な値の範囲に収まる最小値
            output_dwh_name: 処理後のDWH名
            delete_string_null_flag
        """

        default_value = context.getProperty(
            self.DEFAULT_VALUE).evaluateAttributeExpressions(flowfile).getValue()

        default_value_type_if_all_null = context.getProperty(
            self.DEFAULT_VALUE_TYPE_IF_ALL_NULL).evaluateAttributeExpressions(flowfile).getValue()

        max_value_in_range = context.getProperty(
            self.MAX_VALUE_IN_RANGE).evaluateAttributeExpressions(flowfile).getValue()

        # 未入力時は無限大
        if max_value_in_range == "" or max_value_in_range is None:
            max_value_in_range = math.inf
        else:
            max_value_in_range = float(max_value_in_range)

        min_value_in_range = context.getProperty(
            self.MIN_VALUE_IN_RANGE).evaluateAttributeExpressions(flowfile).getValue()

        # 未入力時は負の無限大
        if min_value_in_range == "" or min_value_in_range is None:
            min_value_in_range = -math.inf
        else:
            min_value_in_range = float(min_value_in_range)

        output_dwh_name = context.getProperty(
            self.OUTPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        delete_string_null_flag = context.getProperty(
            self.DELETE_STRING_NULL_FLAG).evaluateAttributeExpressions(flowfile).getValue()

        if delete_string_null_flag == DELETE_TRUE:
            delete_string_null_flag = True
        else:
            delete_string_null_flag = False

        return default_value, default_value_type_if_all_null, max_value_in_range, min_value_in_range, output_dwh_name, delete_string_null_flag

    def get_new_type(self, value_type):
        """
        概要:
            タイプをpythonの型に変える。

        引数:
            new_field_value_type: 入力したタイプ

        戻り値:
            new_type: pythonの型
        """
        if value_type == TYPE_INT:
            new_type = int
        elif value_type == TYPE_FLOAT:
            new_type = float
        else:
            new_type = str

        return new_type

    def check_bad_value(self, value, max_value, min_value, delete_string_null_flag):
        """
        概要:
            値が有効なものかを確認、無効な値（None, NaN, Inf, 空文字列など）をチェック。

        引数:
            value: チェックする値
            max_value: 最大許容値
            min_value: 最小許容値
            delete_string_null_flag: 文字列Null値を削除するフラグ

        戻り値:
            bool: 値が有効ならTrue、無効ならFalse
        """

        # 異常な型（整数、浮動小数点以外）
        if not isinstance(value, (int, float, str, list, dict)):  # ここに適切な型を追加
            return False

        # Noneチェック
        if value is None:
            return False

        # NaNチェック
        if isinstance(value, float) and math.isnan(value):
            return False

        # Infチェック
        if isinstance(value, float) and math.isinf(value):
            return False

        # 空の値チェック（空文字列、空リスト、空辞書）
        if value == "" or value == [] or value == {}:
            return False

        # 半角スペースのみ、全角スペースのみ
        if isinstance(value, str) and (value.replace(" ", "") == "" or value.replace("　", "") == ""):
            return False

        # 範囲外の数値を除外
        if (isinstance(value, int) or isinstance(value, float)) and not (min_value <= value <= max_value):
            return False

        # 文字列のNull値を消すなら動作。
        if delete_string_null_flag:
            null_value_list = ["Null", "NULL", "None",
                               "NONE", "nan", "Nan", "NaN", "NAN", "#N/A"]
            for null_value in null_value_list:
                if value == null_value:
                    return False

        return True

    def get_type(self, value_list, default_value_type_if_all_null, max_value_in_range, min_value_in_range, delete_string_null_flag):
        """
        概要:
            入力された値からデータ型を取得。Null値を除外して型を判別。

        引数:
            value_list: 対象の値リスト
            default_value_type_if_all_null: 全ての値がNullの場合のデフォルトのデータ型
            max_value_in_range: 最大値
            min_value_in_range: 最小値
            delete_string_null_flag: 文字列Null値を削除するかのフラグ

        戻り値:
            default_value_type: データ型
        """

        # Null値ではない値からデータ型を取得する。
        for one_tuple in value_list:

            value = one_tuple[1]

            if WM.calc_func_time(self.logger, False)(self.check_bad_value)(value, max_value_in_range, min_value_in_range, delete_string_null_flag) == False:
                flag = True
                pass
            else:
                print(one_tuple)
                default_value_type = type(value)
                flag = False
                break

        if flag == True:
            default_value_type\
                = WM.calc_func_time(self.logger, False)(self.get_new_type)(default_value_type_if_all_null)

        return default_value_type

    def create_target_list(self, default_value, default_value_type, target_value_list, max_value_in_range, min_value_in_range, delete_string_null_flag):
        """
        概要:
            値が有効な場合、Null値をデフォルト値で置き換える。

        引数:
            default_value: Null値の置換デフォルト値
            default_value_type: 置き換えるデータ型
            target_value_list (list): 置き換える対象の値リスト
            max_value_in_range (float): 最大値
            min_value_in_range (float): 最小値
            delete_string_null_flag (bool): 文字列Null値を削除するかのフラグ

        戻り値:
            target_list: 置き換え後のターゲットリスト
        """

        # 属性値を既定値に置き換える。
        target_list = [(findex, default_value_type(default_value))
                       if WM.calc_func_time(self.logger, False)
                       (self.check_bad_value)(value, max_value_in_range,
                                              min_value_in_range, delete_string_null_flag) == False
                       else (findex, value)
                       for findex, value in target_value_list]

        return target_list

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------

    def transform(self, context, flowfile):
        try:

            # プロパティの値を取得
            default_value, \
                default_value_type_if_all_null, \
                max_value_in_range, \
                min_value_in_range, \
                output_dwh_name, \
                delete_string_null_flag\
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)

            # FieldSetFileのデータを取得
            field_set_file_dataframe, \
                target_dwh, \
                target_type, \
                target_value_list\
                = WM.calc_func_time(self.logger)(PBP.get_dataframe_and_value_from_field_set_file)(flowfile)

            # Null値ではない値からデータ型を取得する。
            default_value_type\
                = WM.calc_func_time(self.logger)(self.get_type)(target_value_list,
                                                                default_value_type_if_all_null,
                                                                max_value_in_range,
                                                                min_value_in_range,
                                                                delete_string_null_flag)

            # 属性値を既定値に置き換える。
            target_list\
                = WM.calc_func_time(self.logger)(self.create_target_list)(default_value,
                                                                          default_value_type,
                                                                          target_value_list,
                                                                          max_value_in_range,
                                                                          min_value_in_range,
                                                                          delete_string_null_flag)

            # プロパティ未入力時は元のDWHを使用
            if output_dwh_name is None or output_dwh_name == "":
                output_dwh_name = target_dwh

            dwh_list = [output_dwh_name]
            type_list = [target_type]
            value_list = [target_list]

            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(dwh_list,
                                                                         type_list,
                                                                         value_list)

            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes={"DWH": output_dwh_name})

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
