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

import pickle
import cad.common.cad_utils as CU

class ConvertFromFieldSetFileToGeoDataFrameLogic:

    def __call__(self, byte_data, attribute, properties):
        """
        FieldSetFileを受け取り、GeoDataFrameに変換する。

        :param byte_data: 入力としてのバイトデータ
        :type byte_data: bytes
        :param attribute: フローファイルの属性情報を格納する辞書
        :type attribute: dict
        :param properties: プロパティ情報を含む辞書
        :type properties: dict

        :return: 出力用のGeoDataFrameと、更新された属性情報を含むタプル
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # プロパティ値取得
            crs = properties['CRS']

            # FieldSetFileをGeoDataFrameに変換
            gdf = CU.field_set_file_to_geodataframe(byte_data, str(crs))
            output_contents = pickle.dumps(gdf)
            return output_contents, attribute
        
        except Exception as e:
            raise(f"[ERROR transform]: {str(e)}")