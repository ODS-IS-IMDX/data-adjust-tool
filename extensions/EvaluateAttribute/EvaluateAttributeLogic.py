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

import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class EvaluateAttributeLogic:

    def get_fsf_values(self, df, geo_att, is_results, geo_fid):
        """
        フィールドセットバリューからDwhの値を指定してValue列の値を取得

        :param df: FieldSetFileをDataFrameに変換したもの。
        :type df: pandas.DataFrame
        :param geo_att: <GeometryName>/<AttributeName>
        :type geo_att: str
        :param is_results: プロパティのResultの値
        :type is_results: bool
        :param geo_fid: <GeometryName>/FID
        :type geo_fid: str

        :return: <GeometryName>/<AttributeName>のValue列の値、<GeometryName>/FIDまたはNone
        :rtype: tuple(list[tuple(str, int)], list[tuple(str, int)])

        :raises Exception:
            処理中にエラーが発生した場合に例外を送出する。
        """
        try:
            if df[df["Dwh"] == geo_att].shape[0] == 0:
                # Dwhの値が{geometry_name}/{attribute_name}である行数が0の場合(処理対象の行が存在しない場合は処理不可)
                raise ValueError(
                    f"GeometryName or AttributeName does not exist: {geo_att}"
                )

            # Dwh名が{geometry_name}/{attribute_name}のValueを取得
            geo_att_value = df.loc[df["Dwh"] == geo_att, "Value"].values[0]

            if isinstance(geo_att_value, list):
                # Value全体がlistである場合
                for val in geo_att_value:
                    if not isinstance(val, tuple):
                        # ValueがTupleのリストでない場合は処理不可
                        raise ValueError(
                            f"Expected tuple in Value column, but got: {type(val)}"
                        )
            else:
                # Value全体がlistでない場合は処理不可
                raise ValueError(f"Expected list Value, but got: {type(geo_att_value)}")

            if is_results:
                # resultsがtrueの場合はDwhの値が{geometry_name}/FIDと比較
                if df[df["Dwh"] == geo_fid].shape[0] == 0:
                    # Dwhの値が{geometry_name}/FIDである行数が0の場合(処理対象の行が存在しない場合は処理不可)
                    raise ValueError(
                        f"GeometryName or AttributeName does not exist: {geo_fid}"
                    )

                geo_fid_value = df.loc[df["Dwh"] == geo_fid, "Value"].values[0]
            else:
                geo_fid_value = None

            return geo_att_value, geo_fid_value
        except Exception as e:
            raise Exception(f"[get_fsf_values]: {str(e)}")

    def compare_value(self, comparison_operator, comp_val_x, comp_val_y):
        """
        インプットデータからの値とプロパティの値を比較し、比較結果を返す

        :param comparison_operator: 比較演算子
        :type comparison_operator: str
        :param comp_val_x: 比較する値(インプットデータからの値)
        :type comp_val_x: int
        :param comp_val_y: 比較する値(プロパティの値)
        :type comp_val_y: int

        :return: 比較結果
        :rtype: bool

        :raises Exception:
            処理中にエラーが発生した場合に例外を送出する。
        """
        try:
            operators = {
                "=": lambda x, y: x == y,
                "!=": lambda x, y: x != y,
                ">": lambda x, y: x > y,
                "<": lambda x, y: x < y,
                ">=": lambda x, y: x >= y,
                "<=": lambda x, y: x <= y,
            }

            if (
                    not isinstance(comp_val_x, (int, float))
                    or comp_val_y == "None"
                    or comp_val_y == "nan"
            ):
                # 数値でない、または数値であってもCOMPARISON_VALUEがNoneやnanでなければ文字列で比較
                comp_x = str(comp_val_x)
                comp_y = comp_val_y

            else:
                # 数値の場合(COMPARISON_VALUEがNoneやnanを除く)は数値に変換して比較
                comp_x = comp_val_x
                comp_y = type(comp_val_x)(comp_val_y)

            return operators[comparison_operator](comp_x, comp_y)

        except Exception as e:
            raise Exception(f"[compare_value]: {str(e)}")

    def evaluate_attributes(self, df, properties):
        """
        属性を比較した結果を返す

        :param df: FieldSetFileをDataFrameに変換したもの。
        :type df: pandas.DataFrame
        :param properties: プロパティ情報の辞書
        :type properties: dict

        :return: 右辺が比較結果となるタプルのリスト
        :rtype: list[tuple(float, bool)]

        :raises Exception:
            処理中にエラーが発生した場合に例外を送出する。
        """
        try:
            # Dwhの値
            geo_att = f"{properties['GEOMETRY_NAME']}/{properties['ATTRIBUTE_NAME']}"
            geo_fid = f"{properties['GEOMETRY_NAME']}/FID"

            is_results = properties["RESULTS"] == "true"

            # inputデータに問題がないかチェックし値を取得
            geo_att_value, geo_fid_value = self.get_fsf_values(
                df, geo_att, is_results, geo_fid
            )

            if is_results:
                # resultsがtrueの場合はDwhが{geometry_name}/FIDの中で、Dwh名が{geometry_name}/{attribute_name}の左辺のFIndexが存在するものをTrueとする
                result_list = []
                for _, fidx in geo_fid_value:
                    is_exists = False
                    for main, sub in geo_att_value:
                        if sub is None:
                            # results形式のlistの場合でNoneの場合
                            break
                        elif fidx == main:
                            is_exists = True
                            break
                    result_list.append((fidx, is_exists))
            else:
                # resultsがfalseの場合はtupleの右辺を比較
                result_list = [
                    (
                        float(i),
                        self.compare_value(
                            properties["COMPARISON_OPERATOR"],
                            val[1],
                            properties["COMPARISON_VALUE"],
                        ),
                    )
                    for i, val in enumerate(geo_att_value)
                ]

            return result_list
        except Exception as e:
            raise Exception(f"[evaluate_attributes]: {str(e)}")

    def create_fsf(self, fsf_value, properties):
        """
        FieldSetFileを作成する。

        :param fsf_value: FieldSetFileのValue。
        :type fsf_value: list[tuple(int, bool)]
        :param properties: プロパティの値の辞書。
        :type properties: dict

        :return: FieldSetFile。
        :rtype: str

        :raises Exception:
            処理中にエラーが発生した場合に例外を送出する。
        """
        try:
            # FieldSetFileのDwh
            dwh_file_name_list = [
                f"{properties['GEOMETRY_NAME']}/{properties['SUFFIX']}"
            ]

            # FieldSetFileのType
            field_type_list = ["bool"]

            # FieldSetFileのValue
            field_value_list = [CU.encode_value(fsf_value)]

            # FieldSetFileの形式に変換
            return NSP.get_field_set_file(
                field_type_list, field_value_list, dwh_file_name_list
            )

        except Exception as e:
            raise Exception(f"[create_fsf]: {str(e)}")

    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。バイトデータとプロパティを受け取り、処理結果としてFieldSetFileを返す。

        :param byte_data: 入力としてのCSV形式のFieldSetFileのバイトデータ
        :type byte_data: bytes
        :param attribute: FlowFileの属性の辞書
        :type attribute: dict
        :param properties: プロパティ情報の辞書
        :type properties: dict

        :return: 出力用のFieldSetFileと、更新された属性情報を含むタプル
        :rtype: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # FieldSetFileをDataFrameに変換
            df = CU.field_set_file_to_dataframe(byte_data)

            # 属性比較処理
            evaluate_value = self.evaluate_attributes(df, properties)

            # 出力用のFieldSetFileを作成
            output_fsf = self.create_fsf(evaluate_value, properties)

            return output_fsf, attribute

        except Exception as e:
            raise Exception(f"[transform]: {str(e)}")
