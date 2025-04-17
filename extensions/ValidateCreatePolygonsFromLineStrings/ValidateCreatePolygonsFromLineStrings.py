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
from collections import Counter
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")
LineString = getattr(import_module("shapely.geometry"), "LineString")

from cad.common.cad_base_validate_processor import CadBaseValidateProcessor
from common.error_code_list import ErrorCodeList


class ValidateCreatePolygonsFromLineStrings(CadBaseValidateProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            """プロセッサー「CreatePolygonsFromLineStrings」の入力データのチェックを行う"""
        )
        tags = ["fieldsetfile", "python", "validate"]

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

            # FieldSetFileの形式チェック
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            # geometryの有無をチェック
            check_df = df[df["Type"] == "geometry"]
            if check_df.empty:
                self.validate_logger.write_log(ErrorCodeList.EC00002)
                return self.RESULT_FAILURE

            # Valueがndarrayであることをチェック
            geom_list = check_df["Value"].values.tolist()[0]
            if not self.validate_data_types(geom_list, np.ndarray):
                return self.RESULT_FAILURE

            # GeoNdarray形式かどうかチェック
            if geom_list.ndim != 2:
                args = {
                    "error_code": ErrorCodeList.EC00006,
                    "期待値": "GeoNdarray"
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # GeoNdArray要素数が3つ以上
            if geom_list.shape[1] not in [3, 4]:
                args = {
                    "error_code": ErrorCodeList.EC00006,
                    "期待値": "3列または4列の配列"
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 同じIDのリストが2つ以上
            id_counts = Counter(geom_list[:, 0].tolist())
            if not any(count >= 2 for count in id_counts.values()):
                args = {
                    "error_code": ErrorCodeList.EC00006,
                    "期待値": "2つ以上の座標情報を持つLineString"
                }
                self.validate_logger.write_log(**args)
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # ValidateLogを生成
            if not self.validate_log_of_fsf(input_data, df, LineString):
                return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")
