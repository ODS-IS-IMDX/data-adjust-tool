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

from importlib import import_module
import pickle
import datetime

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")

import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class AnalyzeDataFrameLogic:

    def get_title(self):
        """
        処理日時とプロセッサ名を取得

        :return: 処理日時とプロセッサ名
        :rtype: str
        """
        # 処理日時とプロセッサ名
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # ミリ秒部分を3桁に調整

        return f"[{timestamp}] AnalyzeDataFrame\n"

    def __call__(self, byte_data, attributes, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、処理結果としてフィールドセットファイルを返す。

        :param byte_data: 入力としてのDataFrameまたはGeoDataFrameのバイトデータ
        :type byte_data: bytes

        :param attributes: フローファイルの属性情報を格納する辞書
        :type attributes: dict

        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のテキストデータと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            column_name = properties["COLUMN_NAME"]
            if column_name is not None:
                column_name = column_name.strip()

            key_name = properties["KEY_NAME"]
            data_name = properties["DATA_NAME"]

            new_content = self.get_title()

            input_data = pickle.loads(byte_data)

            if isinstance(input_data, pd.DataFrame):
                # INPUTデータがデータフレームであれば
                new_content += NSP.count_dataframe_values(input_data, column_name)
                new_content += "\n-----------------------------------------------\n"
                new_content += NSP.count_dataframe_duplicate_keys(input_data, key_name)
                new_content += "\n-----------------------------------------------\n"
                new_content += NSP.generate_data_definition(input_data, data_name)
            else:
                # INPUTデータがデータフレームではない場合
                new_content += "インプットデータがデータフレームではありません。"

            return new_content, attributes
        except Exception as e:
            return self.get_title() + str(e), attributes
