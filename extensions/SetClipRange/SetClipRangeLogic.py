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

import json


class SetClipRangeLogic:

    def __init__(self, **kwargs):
        pass

    def __call__(self, byte_data, attribute, properties):
        """
        バイトデータおよびプロパティ情報を受け取り、指定された切り取り範囲を基に属性情報を追加したデータを返す。

        :param byte_data: 入力としての任意のバイナリ形式のデータ
        :type byte_data: bytes

        :param attribute: FlowFileの属性情報を格納する辞書
        :type attribute: dict

        :param properties: プロパティ情報を格納する辞書
        :type properties: dict

        :return: 出力用のバイトデータと、追加または更新された属性情報を格納した辞書をタプルで返す。
                 'ClipAreas'キーに切り取り範囲のリスト（JSON形式）、'MaxClipAreas'キーにクリップ範囲の最大数が含まれる。
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            clip_range = properties['CLIP_RANGE']

            # 改行でclip_rangeを分割し、それぞれをリストに変換
            clip_ranges = [
                ((float(values[0].strip()), float(values[1].strip())),
                 (float(values[2].strip()), float(values[3].strip())))
                for values in (line.split(',') for line in clip_range.strip().split('\n'))
            ]

            Max_clip_areas = len(clip_ranges) - 1
            attributes = {
                'ClipAreas': json.dumps(clip_ranges),
                'MaxClipAreas': str(Max_clip_areas)
            }

            return byte_data, attributes

        except Exception as e:
            raise Exception(f"[__call__ Exception]: {str(e)}")