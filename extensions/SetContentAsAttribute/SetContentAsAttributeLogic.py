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


class SetContentAsAttributeLogic:

    def __init__(self):
        pass

    def __call__(self, byte_data, attribute, properties):
        """
        バイトデータの内容をデシリアライズし、設定した属性の値にそのまま設定する。

        :param byte_data: 入力としてのシリアライズされたバイトデータ。
        :type byte_data: bytes

        :param attribute: FlowFileの属性情報を格納する辞書。
        :type attribute: dict

        :param properties: プロパティ情報を含む辞書。
        :type properties: dict

        :return: シリアライズされたバイトデータと、更新された属性情報を含むタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # プロパティ値の取得（属性名をプロパティから取得）
            attribute_name = properties['ATTRIBUTE_NAME']

            # デシリアライズ
            serialize_data = pickle.loads(byte_data)

            # プロパティで指定された属性名で属性を設定
            attribute = {attribute_name: str(serialize_data)}

            return byte_data, attribute

        except Exception as e:
            raise Exception(f"[transform_Exception]: {str(e)}")
