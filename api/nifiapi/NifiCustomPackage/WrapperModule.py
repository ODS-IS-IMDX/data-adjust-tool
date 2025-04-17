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

from functools import wraps

import traceback


# 実行するメソッド前後に開始終了LOGを付与する
# 実行するメソッドが
def calc_func_time(mylogger, tracebackflag=True):
    def _calc_func_time(f):

        # args：位置引数 タプルの状態で渡された引数をunpack
        # kwargs：キーワード引数 １つの辞書型にまとめられる
        @wraps(f)
        def wrapper(*args, **kwargs):
            # logger.info(sys._getframe().f_code.co_name + ' 実行開始')

            # logクラスの開始ログ用メソッドを実行する予定
            try:

                result = f(*args, **kwargs)

            except Exception:
                mylogger.error(f'{f.__name__} 異常終了')

                # argsの値
                for index, arg in enumerate(args):
                    mylogger.error(f'{f.__name__}【args】arg[{index}] = {arg}')

                # kwargsの値
                for key, value in kwargs.items():
                    mylogger.error(f'{f.__name__}【kwargs】{key} : {value}')

                if tracebackflag:
                    mylogger.error(traceback.format_exc())

                else:
                    pass

                raise Exception

            # logクラスの終了ログ用メソッドを実行する予定
            return result
        return wrapper
    return _calc_func_time
