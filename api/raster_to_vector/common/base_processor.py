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
import sys
from functools import wraps
import traceback
import io
import importlib

# 外部ライブラリの動的インポート
pd = importlib.import_module("pandas")

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from raster_to_vector.common.custom_processor_executor import CustomProcessorExecutor
from raster_to_vector.common.field_set_file_converter import FieldSetFileConverter
from raster_to_vector.common.basic_processor_executor import BasicProcessorExecutor
from raster_to_vector.common.base_raster_vector_logic import BaseRasterVectorLogic


class BaseProcessor(FlowFileTransform):
    columns_to_field_set_file = ['Dwh', 'Type', 'Value']

    def __init__(self, **kwargs):
        """
        初期化メソッドです。特に初期化処理は行いませんが、継承されたクラスでオーバーライドされることを
        想定しています。

        Parameters
        ----------
        kwargs : dict
            任意のキーワード引数を受け取ることが可能です。
        """
        pass
    __init__.__doc__ = ""  # __init__のdocstringは出力しない

    def calc_func_time(self, func):
        """
        関数の実行時間を測定し、ログに記録します。

        渡された関数をラップし、関数の開始と終了時にログを出力します。
        また、例外が発生した場合にはエラーログを記録し、引数やエラー内容もログに残します。

        Parameters
        ----------
        func : callable
            ラップ対象の関数です。

        Returns
        -------
        callable
            実行時間が記録されるようにラップされた関数を返します。
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            self.logger.info(func.__name__ + ' 実行開始')
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                self.logger.error(f'{func.__name__} 異常終了')
                for index, arg in enumerate(args):
                    self.logger.error(f'{func.__name__}【args】arg[{index}] = {arg}')

                for key, value in kwargs.items():
                    self.logger.error(f'{func.__name__}【kwargs】{key} : {value}')
                self.logger.error(traceback.format_exc())
                raise ValueError from e

            self.logger.info(func.__name__ + ' 実行終了')
            return result

        return wrapper

    def wrap_with_timing(self, func):
        """
        関数を'calc_func_time'メソッドでラップします。

        Parameters
        ----------
        func : callable
            ラップ対象の関数です。

        Returns
        -------
        callable
            'calc_func_time'メソッドでラップされた関数を返します。
        """
        return self.calc_func_time(func)

    def wrap_methods_in_class(self, cls):
        """
        指定されたクラス内のメソッドをすべて'wrap_with_timing'でラップします。

        ただし、メソッド名がアンダースコアで始まらないもののみを対象とします。

        Parameters
        ----------
        cls : type
            メソッドをラップする対象のクラスです。

        Returns
        -------
        None
        """
        for attr_name in dir(cls):
            if not attr_name.startswith("_"):
                attr = getattr(cls, attr_name)
                if callable(attr):
                    wrapped_attr = self.wrap_with_timing(attr)
                    setattr(cls, attr_name, wrapped_attr)

    def wrap_functions_in_module(self, module):
        """
        指定されたモジュール内の関数をすべて'wrap_with_timing'でラップします。

        アンダースコアで始まらない関数のみを対象とし、'main'関数やビルトイン型、モジュールは除外されます。

        Parameters
        ----------
        module : module
            関数をラップする対象モジュールです。

        Returns
        -------
        None
        """
        module_name = module.__name__
        for attr_name in dir(module):
            attr = getattr(module, attr_name)

            # クラス、ビルトイン型、モジュールなどは除外し、純粋な関数のみ対象とする
            if (callable(attr) and
                    not attr_name.startswith("test") and
                    not attr_name.startswith("_") and
                    attr_name != 'main' and
                    not isinstance(attr, type) and
                    not isinstance(attr, type(sys))):
                wrapped_attr = self.wrap_with_timing(attr)
                setattr(module, attr_name, wrapped_attr)

        # del sys.modules[module_name]

        # sys.modulesを更新して、モジュール内の関数が正しく参照されるようにする
        sys.modules[module_name] = module

        # メソッドをラップした後にモジュールを再読み込みし、キャッシュを強制的に更新
        # importlib.reload(module)

    @classmethod
    def set_logic_class(cls, logic_class_name):
        """
        ロジッククラスを設定するためのデコレータです。

        指定されたロジッククラス内のメソッドを自動的にラップし、使用できるようにします。

        Parameters
        ----------
        logic_class_name : str
            ロジッククラスの完全修飾名です（モジュール名とクラス名をドットで区切った形式）。

        Returns
        -------
        callable
            指定されたロジッククラス内のメソッドをラップしたデコレータを返します。
        """

        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                module_name, class_name = logic_class_name.rsplit('.', 1)
                self.logic_module = importlib.import_module(module_name)
                logic_class = getattr(self.logic_module, class_name)
                self.logic_instance = logic_class()

                # ラップ処理をここで自動的に実行
                self.wrap_methods_in_class(self.logic_instance.__class__)
                self.wrap_functions_in_module(self.logic_module)

                return func(self, *args, **kwargs)

            return wrapper

        return decorator

    def process_content(self, content, attribute, properties, method=None):
        """
        指定されたコンテンツ、属性、プロパティを使用してロジックインスタンスのメソッドを呼び出します。

        Parameters
        ----------
        content : bytes
            処理対象のコンテンツデータです。
        attribute : dict
            フロー属性の辞書です。
        properties : dict
            プロパティの辞書です。
        method : callable, optional
            呼び出すメソッドです。指定がない場合はロジックインスタンスの`__call__`メソッドが使用されます。

        Returns
        -------
        tuple
            新しいコンテンツと属性を含むタプルを返します。
        """
        if method is None:
            method = self.logic_instance.__call__

        return self.wrap_with_timing(method)(content, attribute, properties)

    def create_executor(self, content, attribute):
        executor = BasicProcessorExecutor(self.logic_instance, self.process_content)
        if not issubclass(self.logic_instance.__class__, BaseRasterVectorLogic):
            # raster vector team以外はBasicProcessorExecutorのみを使用する
            return executor

        try:
            content_str = content.decode('utf-8')
            df_content = pd.read_csv(io.StringIO(content_str))
            all_exist = all(col in df_content.columns for col in BaseProcessor.columns_to_field_set_file)
            if all_exist:
                executor = CustomProcessorExecutor(self.logic_instance, self.process_content)

        except Exception as e:
            # 例外が発生した場合はBasicProcessorExecutor
            pass

        return executor

    def transform(self, context, flowfile):
        """
        FlowFileのコンテンツと属性を取得し、それらを基に拡張プロセッサのロジックを実行します。

        実行結果を新しいFlowFileに格納して返します。

        Parameters
        ----------
        context : Context
            NiFiプロセッサの実行コンテキストです。
        flowfile : FlowFile
            処理対象のFlowFileです。

        Returns
        -------
        FlowFileTransformResult
            実行結果を含む新しいFlowFileTransformResultを返します。
        """
        BaseProcessor.logger = self.logger

        try:
            content = flowfile.getContentsAsBytes()
        except Exception as e:
            self.logger.error(f'{self.__class__.__name__} contentデータの取得に失敗しました')
            raise e

        try:
            attributes = dict(flowfile.getAttributes().items())
        except AttributeError as e:
            self.logger.error(f'{self.__class__.__name__} attributeデータの取得に失敗しました')
            raise e

        try:
            # PropertyDescriptorのインスタンスを自動的に取得して辞書に格納
            properties = {}
            for key, value in self.__class__.__dict__.items():
                if isinstance(value, PropertyDescriptor):
                    # expressionLanguageScopeにExpressionLanguageScope.FLOWFILE_ATTRIBUTESが設定されている場合、evaluateAttributeExpressionsを使用して取得する
                    if value.expressionLanguageScope == ExpressionLanguageScope.FLOWFILE_ATTRIBUTES:
                        properties[key] = context.getProperty(value).evaluateAttributeExpressions(flowfile).getValue()
                    else:
                        # contextを使ってプロパティの値を取得
                        properties[key] = context.getProperty(value).getValue()
        except AttributeError as e:
            self.logger.error(f'{self.__class__.__name__} propertyデータの取得に失敗しました')
            raise e

        converter = FieldSetFileConverter()
        content = converter.convert_img_to_field_set_file(content, attributes)

        executor = self.create_executor(content, attributes)

        # カスタムプロセッサの実行を行う
        new_content, new_attribute = executor.execute(content, attributes, properties)

        result = FlowFileTransformResult(relationship="success", contents=new_content, attributes=new_attribute)

        return result
