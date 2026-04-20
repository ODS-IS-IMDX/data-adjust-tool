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

import cad.common.cad_utils as CU
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP


class CheckFieldDuplicationLogic:

    def get_fsf_values(self, df, dwh, att_name):
        """
        フィールドセットバリューからDwhの値を指定してValue列の値を取得

        :param df: FieldSetFileをDataFrameに変換したもの。
        :type df: pandas.DataFrame
        :param dwh: dwh
        :type dwh: str
        :param att_name: <AttributeName>
        :type att_name: str

        :return: <GeometryName>/<AttributeName>のValue列の値
        :type: tuple(list[tuple(str, int)], list[tuple(str, int)]), list[string]

        :raises Exception:
            処理中にエラーが発生した場合に例外を送出する。
        """
        try:
            if df[df["Dwh"] == dwh].shape[0] == 0:
                # Dwhの値が{geometry_name}/{attribute_name}である行数が0の場合(処理対象の行が存在しない場合は処理不可)
                raise ValueError(f"GeometryName or AttributeName does not exist: {dwh}")
            
            # Dwh名が{geometry_name}/{attribute_name}のValueを取得
            geo_att_value = df.loc[df["Dwh"] == dwh, "Value"].values[0]
            if isinstance(geo_att_value, list):
                if att_name != "geometry":
                    for val in geo_att_value:
                        if not isinstance(val, tuple):
                            raise ValueError(f"Expected tuple in Value column, but got: {type(val)}")
            else:
                raise ValueError(f"Expected list Value, but got: {type(geo_att_value)}")
            
            return geo_att_value
        except Exception as e:
            raise Exception(f"[get_fsf_values]: {str(e)}")


    def __call__(self, byte_data, attribute, properties):
        """
        プロセスのエントリーポイントとなる関数。
        バイトデータとプロパティを受け取り、指定したDwhのValue値の重複確認を行う。

        :param byte_data: 入力としてのCSV形式のFieldSetFileのバイトデータ
        :type byte_data: bytes
        :param attribute: FlowFileの属性の辞書
        :type attribute: dict
        :param properties: プロパティ情報の辞書
        :type properties: dict

        :return: 入力されたFieldSetFileと、更新された属性情報を含むタプル
        :type: tuple(str, dict)

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする
        """
        try:
            # FieldSetFileをDataFrameに変換
            df = CU.field_set_file_to_dataframe(byte_data)

            # プロパティの値を設取得
            dwh = properties['DWH']
            
            if '/' not in dwh:
                raise ValueError(f"Invalid DWH format: '{dwh}' (expected 'geometry_name/attribute_name')")
            
            # attributeNameを取得
            _, att_name = dwh.split('/')
            
            # valueを取得
            geo_att_value = self.get_fsf_values(
                df, dwh, att_name
            )

            # 重複チェック対象のvalueを取得
            if att_name == "FID":
                # (value, Findex)のvalue(左辺)だけを取り出す
                values = [left for left, _ in geo_att_value]
            elif att_name == "geometry" :
                # wkt文字列を取得
                values = [str(v) for v in geo_att_value]
            else:
                # (index, value)のvalue(右辺)だけを取り出す
                values = [right for _, right in geo_att_value]
                
            # 重複チェック
            duplicates = [v for v in set(values) if values.count(v) > 1]
            
            # 重複チェックの結果を属性に追加
            result = "false" if duplicates else "true"
            attribute.update({
                "duplication_status":result,
            })
            return byte_data, attribute 

        except Exception as e:
            raise Exception(f"[transform]: {str(e)}")
