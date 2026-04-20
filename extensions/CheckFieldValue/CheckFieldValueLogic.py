# MIT License
#
# Copyright (c) 2026 NTT InfraNet
# Copyright (c) 2026 NTT DATA Japan Co., Ltd.
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

import math, re
from decimal import Decimal
import cad.common.cad_utils as cu
import nifiapi.NifiCustomPackage.NifiSimplePackage as nsp

# -----------------------------
# 比較演算子テーブル（モジュール定数）
# -----------------------------
_OPS = {
    "=": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
}
_ORDER_OPS = {">", "<", ">=", "<="}  # 真偽値では使用不可


# -------- 内部ユーティリティ --------

def _is_null_like(v) -> bool:
    """
    Python内部の欠損（Null相当）を判定する。
    対象: None / NaN（float/Decimal 等）/ NaT（numpy.datetime64/pandas.NaT）
    """
    # None
    if v is None:
        return True

    # NaN系 (float('nan'), numpy.nan, pandas.NA/NaT など)
    try:
        if v != v:  # 自分自身と等しくないものは NaN/NaT の類
            return True
    except Exception:
        pass

    # Decimal NaN
    if isinstance(v, Decimal) and (v.is_qnan() or v.is_snan()):
        return True

    # numpy.NaT 判定
    try:
        import numpy as np
        if isinstance(v, (np.datetime64, np.timedelta64)):
            if np.isnat(v):
                return True
    except Exception:
        pass

    # pandas.NaT 判定
    try:
        import pandas as pd
        if v is pd.NaT:
            return True
    except Exception:
        pass

    return False


def _to_number(x):
    """
    値を数値(float)へ変換する。NaNは例外とする。

    :param x: 変換対象の値
    :type x: Any

    :return: 変換後の浮動小数点数
    :rtype: float

    :raises ValueError: x が NaN の場合、または数値へ変換できない場合
    """
    if isinstance(x, (int, float)):
        if isinstance(x, float) and math.isnan(x):
            raise ValueError("NaN")
        return float(x)
    return float(str(x).strip())


def _to_bool(x):
    """
    値を真偽値へ変換する。"True"/"False"（大文字小文字不問）のみ受理する。

    :param x: 変換対象の値
    :type x: Any

    :return: 変換後の真偽値
    :rtype: bool

    :raises ValueError: "True"/"False" 以外の文字列や変換不能な値の場合
    """
    t = str(x).strip().lower()
    if t == "true":
        return True
    if t == "false":
        return False
    raise ValueError("bool型変換: 'True'か'False'のみ受理される。")


def _both_try(fn, lhs, rhs):
    """
    指定関数を左右の値に適用し、両方成功したときのみタプルで返す。

    :param fn: 適用する関数（例: _to_number, _to_bool）
    :type fn: Callable
    :param lhs: 左辺の値
    :type lhs: Any
    :param rhs: 右辺の値
    :type rhs: Any

    :return: (fn(lhs), fn(rhs)) のタプル。いずれかが例外の場合は None。
    :rtype: Optional[Tuple[Any, Any]]

    :raises Exception: 予期せぬ内部例外が発生した場合にスローする
    """
    try:
        return fn(lhs), fn(rhs)
    except Exception:
        return None


class CheckFieldValueLogic:
    """
    DWH属性の値に対して、正規表現または通常比較による検証を行うロジックである。
    1回のみ DWH 値を抽出し、欠損カウント→正規化→評価の順で処理する。
    """

    # -----------------------------
    # プロパティ解析（整形と既定値付与のみ）
    # -----------------------------
    def parse_properties(self, properties: dict) -> dict:
        """
        NiFi プロセッサのプロパティを取得し、文字列トリムと既定値付与のみ行う。

        :param properties: NiFi のプロパティ辞書
        :type properties: dict

        :return: 解析済みプロパティ（DWH, 演算子, パターン, 期待方向, 出力設定 等）
        :rtype: dict

        :raises Exception: 予期せぬ内部例外が発生した場合にスローする
        """

        def _get(key: str, default: str = "") -> str:
            # プロパティ未設定 or None → 既定値
            if not properties:
                return default
            v = properties.get(key, default)
            # None は既定値、文字列はトリム、空文字（""）や空白のみは既定値へフォールバック
            if v is None:
                return default
            s = str(v).strip()
            return default if s == "" else s

        params = {}
        params["DWH"] = _get("DWH")
        params["COMPARISON_OPERATOR"] = _get("COMPARISON_OPERATOR")
        params["CHECK_REG_EX_PATTERN"] = _get("CHECK_REG_EX_PATTERN")
        params["SUFFIX"] = _get("SUFFIX")
        params["EXPECT_TRUE"] = _get("EXPECT_TRUE", "true").lower() == "true"
        params["NULL_HANDLING"] = _get("NULL_HANDLING", "check").lower()
        params["OUTPUT_ROW_LIST"] = _get("OUTPUT_ROW_LIST", "false").lower() == "true"
        params["OUTPUT_ROW_LIST_SPLIT_CHAR"] = _get("OUTPUT_ROW_LIST_SPLIT_CHAR", ",")
        params["OUTPUT_ATTRIBUTE_PREFIX"] = _get("OUTPUT_ATTRIBUTE_PREFIX", "CheckResult")
        return params

    # -----------------------------
    # DWH値取得
    # -----------------------------
    def _get_att_value(self, df, geo_att):
        """
        DataFrame から Dwh=<geom>/<attr> に一致する Value 列（list[tuple] 前提）を1件取得する。

        :param df: 入力 FieldSetFile を変換した DataFrame
        :type df: pandas.DataFrame
        :param geo_att: 'Geometry/Attribute' 形式の DWH 識別子
        :type geo_att: str

        :return: (fid, value) 形式のタプルを要素とするリスト
        :rtype: list[tuple]

        :raises Exception: 対象 DWH が存在しない、Value が想定型でない等の場合
        """
        try:
            if df[df["Dwh"] == geo_att].shape[0] == 0:
                raise ValueError(f"データ名/属性 が見つからない: {geo_att}")
            val = df.loc[df["Dwh"] == geo_att, "Value"].values[0]
            if not isinstance(val, list):
                raise ValueError(f"Value列がリスト型ではない: {type(val)}")
            for t in val:
                if not isinstance(t, tuple):
                    raise ValueError(f"リスト内がタプル型ではない: {type(t)}")
            return val
        except Exception as e:
            raise Exception(f"[get_att_value]: {str(e)}")

    def invert_value_pairs(self, fid_value):
        """
        (fid, value) の各タプル要素を入れ替える（FID 専用ワークアラウンド）。

        :param fid_value: (a, b) 形式のタプルを要素にもつリスト
        :type fid_value: list[tuple]

        :return: (b, a) へ入れ替えた新しいリスト
        :rtype: list[tuple]

        :raises Exception: 予期せぬ内部例外が発生した場合にスローする
        """
        try:
            return [(b, a) for a, b in fid_value]
        except Exception as e:
            raise Exception(f"[invert_value]: {str(e)}")

    def normalize_null(self, value):
        """
        検索・比較前の正規化を行う。None/NaN を文字列化し、既存の文字列は変更しない。

        - Python の None は "None"（文字列）へ
        - NaN（Python float / numpy.float系）は "NaN"（文字列）へ
        - "null"/"nan"/"none"/"" など既に文字列の値はそのまま

        :param value: 正規化対象の値
        :type value: Any

        :return: 正規化後の値（多くは元の値。None/NaN は文字列化）
        :rtype: Any

        :raises Exception: 予期せぬ内部例外が発生した場合にスローする
        """
        try:
            if _is_null_like(value):
                # None
                if value is None:
                    return "None"
                # NumPy / Pandas NaT 判定
                try:
                    import numpy as np
                    if isinstance(value, (np.datetime64, np.timedelta64)) and np.isnat(value):
                        return "NaT"
                except Exception:
                    pass
                try:
                    import pandas as pd
                    if value is pd.NaT:
                        return "NaT"
                except Exception:
                    pass
                return "NaN"
            return value
        except Exception as e:
            raise Exception(f"[normalize_null]: {str(e)}")

    # -----------------------------
    # NULLカウント
    # -----------------------------
    def count_missing_values(self, geo_att_value):
        """
        None/NaN のみを欠損としてカウントする（文字列 "nan" 等は含めない）。

        :param geo_att_value: (fid, value) タプルを要素に持つリスト
        :type geo_att_value: list[tuple]

        :return: 欠損件数（None または NaN の数）
        :rtype: int

        :raises Exception: 予期せぬ内部例外が発生した場合にスローする
        """
        try:
            count = 0
            for _, v in geo_att_value:
                if _is_null_like(v):
                    count += 1
            return count
        except Exception as e:
            raise Exception(f"[count_null_or_nan]: {str(e)}")

    # -----------------------------
    # 比較ロジック
    # -----------------------------
    def check_regex_pattern(self, comparison_operator, comp_val_x, comp_val_y):
        """
        比較ロジックを実行する。正規表現（~ / !~）と通常比較（=, !=, <, >, <=, >=）に対応する。

        :param comparison_operator: 比較演算子（~,!~,=,!=,<,>,<=,>=）
        :type comparison_operator: str
        :param comp_val_x: 左辺値
        :type comp_val_x: Any
        :param comp_val_y: 右辺値（正規表現パターン、または通常比較の右辺）
        :type comp_val_y: Any

        :return: 比較の真偽
        :rtype: bool

        :raises ValueError: 未対応演算子、真偽値に対する順序演算など不正条件
        :raises Exception: 予期せぬ内部例外が発生した場合にスローする
        """
        try:
            op = str(comparison_operator).strip()

            # --- 正規表現 ---
            if op in ("~", "!~"):
                pattern = "" if comp_val_y is None else str(comp_val_y)
                rx = re.compile(pattern)
                s = "" if comp_val_x is None else str(comp_val_x)
                matched = rx.search(s) is not None
                return matched if op == "~" else (not matched)

            # --- 通常比較 ---
            if op not in _OPS:
                raise ValueError(f"未対応の演算子: {op}")

            rhs_txt = str(comp_val_y).strip().lower()
            rhs_is_text_none = rhs_txt in ("none", "nan", "nat")
            rhs_is_real_none = comp_val_y is None
            rhs_is_real_nan = isinstance(comp_val_y, float) and math.isnan(comp_val_y)
            force_string = rhs_is_text_none or rhs_is_real_none or rhs_is_real_nan

            # 1) number
            pair = None if force_string else _both_try(_to_number, comp_val_x, comp_val_y)
            if pair is not None:
                a, b = pair
                return _OPS[op](a, b)

            # 2) boolean（順序演算は禁止）
            pair = None if force_string else _both_try(_to_bool, comp_val_x, comp_val_y)
            if pair is not None:
                if op in _ORDER_OPS:
                    raise ValueError("ブール型では順序演算子は不可である。")
                a, b = pair
                return _OPS[op](a, b)

            # 3) string（最終フォールバック）
            a = "" if comp_val_x is None else str(comp_val_x)
            b = "" if comp_val_y is None else str(comp_val_y)
            return _OPS[op](a, b)

        except Exception as e:
            raise Exception(f"[check_regex_pattern]: {str(e)}")

    # -----------------------------
    # 評価（高速化：正規表現コンパイル・右辺変換の事前計算）
    # -----------------------------
    def evaluate(self, op, rhs, geo_att_value):
        """
        取得済みの DWH 値に対して、演算子と右辺を用い一括評価を行う。
        正規表現は事前コンパイル、通常比較は右辺の型変換を事前に行い、ループ内の分岐を最小化する。

        :param op: 比較演算子（~,!~,=,!=,<,>,<=,>=）
        :type op: str
        :param rhs: 右辺値（正規表現パターン、または通常比較の右辺）
        :type rhs: Any
        :param geo_att_value: (fid, value) タプルのリスト（正規化済みを想定）
        :type geo_att_value: list[tuple]

        :return: (行番号, 判定結果) のリスト
        :rtype: list[tuple[float, bool]]

        :raises ValueError: 未対応演算子、真偽値に対する順序演算など不正条件
        :raises Exception: 予期せぬ内部例外が発生した場合にスローする
        """
        try:
            op = str(op).strip()
            rhs_str = "" if rhs is None else str(rhs)

            # 正規表現パス
            if op in ("~", "!~"):
                rx = re.compile(rhs_str)
                search = rx.search
                if op == "~":
                    return [(float(i), search("" if v is None else str(v)) is not None)
                            for i, (_, v) in enumerate(geo_att_value)]
                else:
                    return [(float(i), search("" if v is None else str(v)) is None)
                            for i, (_, v) in enumerate(geo_att_value)]

            # 通常比較パス
            if op not in _OPS:
                raise ValueError(f"未対応の演算子: {op}")
            cmp_fn = _OPS[op]

            rhs_txt = rhs_str.strip().lower()
            force_string = (rhs_txt in ("none", "nan", "nat"))

            rhs_num_ok = False
            rhs_num = None
            rhs_bool_ok = False
            rhs_bool = None
            if not force_string:
                try:
                    rhs_num = _to_number(rhs_str)
                    rhs_num_ok = True
                except Exception:
                    pass
                try:
                    rhs_bool = _to_bool(rhs_str)
                    rhs_bool_ok = True
                except Exception:
                    pass

            out = []
            append = out.append
            str_ = str
            for i, (_, v) in enumerate(geo_att_value):
                # 数値比較
                if rhs_num_ok and not force_string:
                    try:
                        lv = _to_number(v)
                        append((float(i), cmp_fn(lv, rhs_num)))
                        continue
                    except Exception:
                        pass

                # 真偽比較（順序演算は禁止）
                if rhs_bool_ok and not force_string:
                    if op in _ORDER_OPS:
                        raise ValueError("ブール型では順序演算子は不可である。")
                    try:
                        lvb = _to_bool(v)
                        append((float(i), cmp_fn(lvb, rhs_bool)))
                        continue
                    except Exception:
                        pass

                # 文字列比較（フォールバック）
                a = "" if v is None else str_(v)
                b = rhs_str
                append((float(i), cmp_fn(a, b)))

            return out

        except Exception as e:
            raise Exception(f"[evaluate]: {str(e)}")

    # -----------------------------
    # FieldSetFile
    # -----------------------------
    def create_fsf(self, fsf_value, properties):
        """
        評価結果を FSF(FieldSetFile) として生成する。

        :param fsf_value: (行番号, 判定結果) リスト
        :type fsf_value: list[tuple[float, bool]]
        :param properties: プロパティ辞書（SUFFIX・DWH を利用）
        :type properties: dict

        :return: 生成済み FSF のバイト列
        :rtype: bytes

        :raises Exception: FSF 生成に失敗した場合にスローする
        """
        try:
            dwh = f"{properties['DWH']}"
            geo_name = dwh.split("/")[0]
            suffix_name = f"{properties['SUFFIX']}"
            dwh_name = "/".join([geo_name, suffix_name])
            field_type = ["bool"]
            field_val = [cu.encode_value(fsf_value)]
            return nsp.get_field_set_file(field_type, field_val, dwh_name)
        except Exception as e:
            raise Exception(f"[create_fsf]: {str(e)}")

    # -----------------------------
    # メイン処理
    # -----------------------------
    def __call__(self, byte_data, attribute, properties):
        """
        入力 FieldSetFile を検査し、条件に合致するかを評価する。

        :param byte_data: 入力 FieldSetFile のバイト列
        :type byte_data: bytes
        :param attribute: 入出力属性辞書（RowNumList/Count/NullCount/EvaluateResult を設定）
        :type attribute: dict
        :param properties: NiFi プロパティ辞書
        :type properties: dict

        :return: (出力 FieldSetFile のバイト列, 更新済み属性辞書)
        :rtype: Tuple[bytes, dict]

        :raises ValueError: NULL_HANDLING='failure' かつ欠損が検出された場合
        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # 1) プロパティ整形
            params = self.parse_properties(properties)

            # 2) DataFrame 取得
            df = cu.field_set_file_to_dataframe(byte_data)

            # 3) DWH値を一度だけ取得
            geo_att = params["DWH"]
            geo_att_value = self._get_att_value(df, geo_att)
            geo_att_name = geo_att.split("/")[-1]
            if geo_att_name == "FID":
                geo_att_value = self.invert_value_pairs(geo_att_value)

            # 4) Nullカウント
            null_handling = params["NULL_HANDLING"]
            if null_handling in ("check", "failure"):
                non_value = self.count_missing_values(geo_att_value)
                if null_handling == "failure" and non_value >= 1:
                    raise ValueError("Null値を許可しない設定のため、処理を中断した。")
            else:
                non_value = 0

            # 5) Null正規化
            geo_att_value = [(fid, self.normalize_null(val)) for fid, val in geo_att_value]

            # 6) 評価
            evaluated = self.evaluate(params["COMPARISON_OPERATOR"], params["CHECK_REG_EX_PATTERN"], geo_att_value)

            # 7) 期待方向の反転
            if not params["EXPECT_TRUE"]:
                evaluated = [(i, not flag) for i, flag in evaluated]

            # 8) 属性付与
            prefix = params["OUTPUT_ATTRIBUTE_PREFIX"]
            if params["OUTPUT_ROW_LIST"]:
                output_row_list = [int(i) for i, flag in evaluated if flag]
                split_char = params["OUTPUT_ROW_LIST_SPLIT_CHAR"]
                attribute[f"{prefix}.RowNumList"] = split_char.join(map(str, output_row_list))
            else:
                attribute[f"{prefix}.RowNumList"] = ""

            # 9) 判定集計
            all_ok = all(flag for _, flag in evaluated)
            result_true = sum(1 for _, flag in evaluated if flag)
            attribute["EvaluateResult"] = "True" if all_ok else "False"
            attribute[f"{prefix}.Count"] = str(result_true)
            if null_handling == "check":
                attribute[f"{prefix}.NullCount"] = str(non_value)

            # 10) FSF生成
            out_fsf = self.create_fsf(evaluated, params)
            return out_fsf, attribute

        except Exception as e:
            raise Exception(f"[transform]: {str(e)}")
