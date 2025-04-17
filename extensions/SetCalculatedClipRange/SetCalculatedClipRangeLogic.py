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

import ast
import cad.common.cad_utils as CU


class SetCalculatedClipRangeLogic:

    def calculated_crip_range(self, x_ratio, y_ratio, reference_point, bounding_box):
        """
        SetCalculatedClipRangeFromDxfプロセッサのメイン処理を行う関数。
        指定した基準点と調整比率に基づいて、バウンディングボックスの切り取り範囲を計算する。

        :param x_ratio: バウンディングボックスの幅に対するX軸方向の縮尺比率（0から1の間の値）。
        :type x_ratio: float

        :param y_ratio: バウンディングボックスの高さに対するY軸方向の縮尺比率（0から1の間の値）。
        :type y_ratio: float

        :param reference_point: 切り取り範囲の基準となる位置。'top-left', 'top-right', 'bottom-left', 'bottom-right'のいずれか。
        :type reference_point: str

        :param bounding_box: 元のバウンディングボックスを指定するタプル。形式は ((min_x, min_y), (max_x, max_y))。
        :type bounding_box: tuple(tuple(float, float))

        :return: 指定した基準点と比率に基づいて計算された、新しい切り取り範囲のバウンディングボックス。
        :rtype: tuple(tuple(float, float))

        :raises Exception:
            切り取り中にエラーが発生した場合に例外をスローする。
        """
        try:
            (min_x, min_y), (max_x, max_y) = bounding_box

            # 全体のバウンディングボックスから、指定された値で取得するバウンディングボックスの座標を100分率で求める
            if reference_point == 'top-left':
                new_max_x = min_x + (max_x - min_x) * x_ratio
                new_min_y = max_y - (max_y - min_y) * y_ratio
                new_bounding_box = ((min_x, new_min_y), (new_max_x, max_y))
            elif reference_point == 'top-right':
                new_min_x = max_x - (max_x - min_x) * x_ratio
                new_min_y = max_y - (max_y - min_y) * y_ratio
                new_bounding_box = ((new_min_x, new_min_y), (max_x, max_y))
            elif reference_point == 'bottom-left':
                new_max_x = min_x + (max_x - min_x) * x_ratio
                new_max_y = min_y + (max_y - min_y) * y_ratio
                new_bounding_box = ((min_x, min_y), (new_max_x, new_max_y))
            elif reference_point == 'bottom-right':
                new_min_x = max_x - (max_x - min_x) * x_ratio
                new_max_y = min_y + (max_y - min_y) * y_ratio
                new_bounding_box = ((new_min_x, min_y), (max_x, new_max_y))

            return new_bounding_box

        except Exception as e:
            raise Exception(f'[calculated_crip_range_Exception]:{e}')

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータと属性情報を受け取り、切り取り範囲を適用した結果を返す。

        :param byte_data: 入力データとしてのGeoDataFrameのバイト列。
        :type byte_data: bytes

        :param attribute: DXFデータに関連する属性情報を格納した辞書。ここでは 'BoundingBox' キーが含まれる必要がある。
        :type attribute: dict

        :param properties: プロセスに必要なプロパティ情報を含む辞書。
        :type properties: dict

        :return: 更新されたバイトデータと、'ClipBoundingBox' を含む属性情報を格納した辞書のタプル。
        :rtype: tuple(bytes, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            # プロパティの値取得
            x_ratio = CU.get_number_from_string(properties['XDR'])
            y_ratio = CU.get_number_from_string(properties['YDR'])
            reference_point = properties['CORNER_SETTINGS']

            bounding_box = ast.literal_eval(attribute.get("BoundingBox", None))

            new_bounding_box = self.calculated_crip_range(x_ratio, y_ratio, reference_point, bounding_box)

            attributes = {'ClipBoundingBox': str(new_bounding_box)}

            return byte_data, attributes

        except Exception as e:
            raise Exception(f'[__call__ Exception]:{e}')
