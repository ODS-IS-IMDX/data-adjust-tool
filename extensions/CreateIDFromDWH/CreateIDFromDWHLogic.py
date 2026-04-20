# MIT License
#
# Copyright (c) 2026 NTT InfraNet
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
import uuid
from importlib import import_module

# 外部ライブラリの動的インポート
np = import_module("numpy")

# NiFi 自作ライブラリ
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import cad.common.cad_utils as CU

class CreateIDFromDWHLogic:

    def __init__(self):
        pass

    def extract_findex_list(self, base_dwh_name, input_dwh_name, base_value_list):
        """
        入力の値リスト (左, 右) 配列からFindexを抽出する。
        - DWH末尾が '.../FID' → 右がFindex
        - その他                  → 左がFindex
        base_dwh_name が空なら input_dwh_name を採用。
        base_dwh_name が指定され、かつ input と異なるなら failure 扱い。

        :param base_dwh_name: 採用するDWH列名
        :type base_dwh_name: str
        :param input_dwh_name: 入力DWH名
        :type input_dwh_name: str
        :param base_value_list: 入力FieldSetFileのValue列

        :return: 使用するDWH名, 抽出したFindex
        :rtype: str, list
        :raises Exception:　エラーが発生した場合に発生する。
        """
        try:
            target_dwh = input_dwh_name if (base_dwh_name == "" or base_dwh_name is None) else base_dwh_name

            is_fid = str(target_dwh).split("/")[-1] == "FID"

            if is_fid:
                # 右が Findex
                findex_list = [findex for _, findex in base_value_list]
            else:
                # 左が Findex
                # Geometryの場合
                if isinstance(base_value_list, np.ndarray):
                    base_value_list = [
                        (int(row[0]), (float(row[1]), float(row[2])))
                        for row in base_value_list
                    ]
                findex_list = [findex for findex, _ in base_value_list]

            return target_dwh, findex_list
        except Exception as e:
            raise Exception(f"[ERROR extract_findex_list]: {str(e)}")

    def generate_ids(self, gen_type, n):
        """
        個数 n に対して、gen_type に応じた配列を返す。

        :param gen_type: UUID,連番生成方（UUID: 文字列UUID v4, SEQ*: float64 連番）
        :type gen_type: str
        :param n: FieldSetFile,Value列の要素数
        :type n: int

        :return: UUIDまたは連番, FieldSetFileのType列用文字
        :rtype: list, str

        :raise　Exception: エラーが発生した場合に発生する。
        """
        try:
            if gen_type == "UUID":
                return [str(uuid.uuid4()) for _ in range(n)], "string"

            # 連番（float64想定）
            if gen_type == "SEQ_ASC_ONE":
                seq = [int(i) for i in range(1, n + 1)]
            elif gen_type == "SEQ_ASC_ZERO":
                seq = [int(i) for i in range(0, n)]
            elif gen_type == "SEQ_DES_ONE":
                seq = [int(i) for i in range(n, 0, -1)]
            else:  # SEQ_DES_ZERO
                seq = [int(i) for i in range(n - 1, -1, -1)]

            return seq, "int64"
        except Exception as e:
            raise Exception(f"[ERROR generate_ids]: {str(e)}")

    def build_output_fieldset(self, out_dwh_name, findex_list, id_list, type_label):
        """
        (Findex, 生成ID) の配列を out_dwh_name の1行として FieldSetFile化。

        :param out_dwh_name: 出力DWH名
        :type out_dwh_name: str
        :param findex_list: Findexの配列
        :type findex_list: list
        :param id_list: idの配列
        :type id_list: list
        :param type_label: FieldSetFileのType列用文字
        :type type_label: str

        :return: FieldSetFile
        :rtype: FieldSetFile

        :raise Exception:エラーが発生した場合に発生する。
        """
        try:
            out_pairs = list(zip(findex_list, id_list))
            dwh_list = [out_dwh_name]
            type_list = [type_label]  # "string" or "int64"
            value_list = [out_pairs]

            return PBP.set_field_set_file(dwh_list, type_list, value_list)
        except Exception as e:
            raise Exception(f"[ERROR build_output_fieldset]: {str(e)}")

    def get_base_dwh_name(self, df, base_name):
        """
        flowfileからCSV形式のFieldSetFileを取得し、任意のDwh名単体のDataFrameに加工。Value列の値をデコードデシリアライズしlist もしくは、arrayにする。
        Dwh列からDWHファイル名を、Type列からTypeの種類を取得。

        :param df: 入力データのDataframe
        :type df: pandas.DataFrame
        :param base_name: 　処理対象出DWH名
        :type base_name: str

        :return: DWH名, 対象Value
        :rtype: str, str
        """
        try:
            filtered_df = df[df["Dwh"] == base_name]

            # FieldSetFileのDwh列を抽出
            target_dwh = filtered_df["Dwh"].iloc[0]

            # FieldSetFileのValue列
            target_value = filtered_df["Value"].iloc[0]

            return target_dwh, target_value
        except Exception as e:
            raise Exception(f"[ERROR get_base_dwh_name]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        FieldSetFileの任意の1行に対し、UUIDまたは連番を振る。

        :param byte_data:シリアライズされたFieldSetFile
        :type byte_data: bytes
        :param attribute: 属性
        :type attribute: dict
        :param properties: プロパティ
        :type properties: dict

        :return: 処理結果のフィールドセットファイル、属性
        :rtype: tuple(str, dict)

        :raises Exception: エラーが発生した場合に発生する。
        """
        try:
            # プロパティ取得
            base_dwh_name = properties["BASE_DWH_NAME"]
            out_dwh_name = properties["OUTPUT_DWH_NAME"]
            gen_type = properties["GENERATOR_TYPE"]

            # データフレーム作成
            df = CU.field_set_file_to_dataframe(byte_data)

            # 入力 FieldSetFile を取得
            input_dwh_name, value_list = self.get_base_dwh_name(df, base_dwh_name)

            # Findex 抽出
            target_dwh, findex_list = \
                self.extract_findex_list(base_dwh_name, input_dwh_name, value_list)

            # ID 生成
            ids, type_label = self.generate_ids(gen_type, len(findex_list))

            # 出力 FieldSetFile 作成
            output_field_set_file = \
                self.build_output_fieldset(out_dwh_name, findex_list, ids, type_label)

            # 属性更新（DWH を出力DWHへ）
            attributes = {"DWH": out_dwh_name}

            return output_field_set_file, attributes

        except Exception as e:
            raise Exception(f"[transform_Exception]:{e}")
