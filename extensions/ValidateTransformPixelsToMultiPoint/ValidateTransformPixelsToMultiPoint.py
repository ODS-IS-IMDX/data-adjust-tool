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
from importlib import import_module

np = import_module("numpy")

from common.error_code_list import ErrorCodeList
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor


class ValidateTransformPixelsToMultiPoint(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「TransformPixelsToMultiPoint」の入力データのチェックを行う'''
        tags = ['geodataframe', 'geopandas', 'python', 'validate']

    def validate_pixel_data_of_point(self, pixel_data):
        """
        対象のピクセルデータが有効かどうかをチェックする

        :param pixel_data: 対象のピクセルデータ
        :type pixel_data: list of tuple

        :return: 処理結果（True：正常／False：異常）
        :rtype: bool
        """
        for point_list in pixel_data:
            # 座標情報を１点以上持つかどうかを確認
            if not (isinstance(point_list, list) and len(point_list) > 0):
                return False

            if all(isinstance(point, tuple) for point in point_list):
                # ピクセルデータが[(0, 0), (1, 1), (2, 2), ...]という形式の場合
                # タプルの中身が数値かつX・Y（・Z）座標を持つかどうかを確認
                is_valid_coords = all(
                    len(point) in (2, 3)
                    and all(isinstance(val, (int, float, np.integer, np.floating)) for val in point)
                    for point in point_list
                )
                if not is_valid_coords:
                    return False
            elif isinstance(point_list[0], tuple) and isinstance(point_list[1], (int, float, np.integer, np.floating)):
                # ピクセルデータが[(22, 28), 8]という形式の場合
                # タプルの中身が座標データの形式かどうかをチェックする
                is_valid_coords = all(
                    isinstance(val, (int, float, np.integer, np.floating))
                    for val in point_list[0]
                )
                if not (len(point_list[0]) in (2, 3) and is_valid_coords):
                    return False
            else:
                return False

        return True

    def validate_data(self, context, flowfile):
        """
        入力データのチェックを実行する

        :param context: プロセッサの設定値が格納されているデータ
        :param flowfile: プロセッサに渡されるデータ

        :return: チェック結果 正常="success"、異常="failure"
        :rtype: str

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()

            # インプットデータの検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFile形式かどうかチェックする
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE
            
            # デシリアライズできるデータが存在すること
            is_deserializable = False
            for idx in range(len(df)):
                row_value = df.loc[idx, 'Value']
                if isinstance(row_value, list):
                    for item in row_value:
                        if isinstance(item, tuple) and len(item) > 1 and isinstance(item[1], bytes):
                            # ピクセルデータの中身がマルチポイントを構成する形式か確認
                            pixel_data = pickle.loads(item[1])
                            if not self.validate_pixel_data_of_point(pixel_data):
                                result = False
                                args = {
                                    "error_code": ErrorCodeList.EC00006,
                                    "対象": (item[0], pixel_data)
                                }
                                self.validate_logger.write_log(**args)
                                if self.mode_value == self.MODE_STOP:
                                    return self.RESULT_FAILURE
                            is_deserializable = True
                    if is_deserializable:
                        break
            if not is_deserializable:
                result = False
                self.validate_logger.write_log(ErrorCodeList.EC00016)

            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
