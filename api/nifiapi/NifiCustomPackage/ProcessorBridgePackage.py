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
import base64
import io

from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")


def get_dataframe_and_value_from_field_set_file(flowfile):
    """
    概要:
        【前提条件】FieldSetFileはpartitionrecordプロセッサにて、カラムの行を除いた1行しかないものとする。
        flowfileからCSV形式のFieldSetFileを取得し、DataFrameに加工。Value列の値をデコードデシリアライズしlist もしくは、arrayにする。
        Dwh列からDWHファイル名を、Type列からTypeの種類を取得。

    引数:
        flowfile: processorに入ってくるデータ

    戻り値:
        field_set_file_dataframe: field_set_fileをデータフレームに変換したデータ
        target_dwh: Dwh列から抽出したDWHファイル名
        target_type: Type列から抽出したtarget_valueのデータの型
        target_value: デコード、デシリアライズされたValue列データ
    """

    # flowfileから、CSV形式のFieldSetFileを取得
    input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

    # flowfile_contentsをパスとして扱う
    field_set_file_stringio = io.StringIO(input_field_set_file)

    # DataFrameに加工
    field_set_file_dataframe = pd.read_csv(field_set_file_stringio)

    # FieldSetFileの１行目のDwh列を抽出
    target_dwh = field_set_file_dataframe.loc[0, "Dwh"]

    # FieldSetFileの１行目のType列を抽出
    # (例:geometry)
    target_type = field_set_file_dataframe.loc[0, "Type"]

    # FieldSetFileの1行目のValue列をデコードデシリアライズする
    # target_value = list or arrayが入る
    target_value = pickle.loads(base64.b64decode(
        field_set_file_dataframe.loc[0, "Value"]))

    return field_set_file_dataframe, target_dwh, target_type, target_value


def set_field_set_file(dwh_list, type_list, values_list):
    """
    概要:
        flowfileのcontentsには、配列やリストは入らない為、シリアライズ、エンコード、バイト列を文字列にしたいのでutf-8でデコードする必要がある。
        そのデータが何のデータなのかを識別するためにdwhとtypeをFieldSetFileに書き込む。
        引数で取得した値を、dataframe内の該当の列に、配置し、csv化。

    引数:
        dwh_list: 加工したデータに関連するDwhファイル名
        type_list: 加工したデータの型
        values_list: 加工したデータ（リストまたは配列）

    戻り値:
        field_set_file: FlowFileに送るために作成されたCSV形式のFieldSetFile
    """

    # list or array を、シリアライズ、base64でエンコード、バイト列を文字列にしたいのでutf-8でデコード
    encoded_values_list = [base64.b64encode(pickle.dumps(
        value)).decode("utf-8") for value in values_list]

    # FieldSetFile用のdata_dictを作成
    data_dict = {
        "Dwh": dwh_list,
        "Type": type_list,
        "Value": encoded_values_list
    }

    # data_dictをFieldSetFile(CSV形式)に加工
    result_dataframe = pd.DataFrame(data_dict)
    field_set_file = result_dataframe.to_csv(index=False)

    return field_set_file


def get_value_from_field_Set_file_or_serialized(flowfile):
    """
    概要:
        FlowFileからCSV形式のfield_set_fileを取得し、その中のValue列をデコード・デシリアライズして返す
        CSV形式ではない(シリアライズされたGeoDataFrame)場合は、シリアライズされたバイトデータをそのまま復元して返す

    引数:
        flowfile: processorに入ってくるデータ

    戻り値:
        target_value: 復元されたデータ
    """

    try:
        # flowfileから、CSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # flowfile_contentsをパスとして扱う
        field_set_file_stringio = io.StringIO(input_field_set_file)

        # DataFrameに加工
        field_set_file_dataframe = pd.read_csv(field_set_file_stringio)

        # FieldSetFileの1行目のValue列をデコードデシリアライズする
        target_value = pickle.loads(base64.b64decode(
            field_set_file_dataframe.loc[0, "Value"]))

    except UnicodeDecodeError:

        # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得する
        serialize_dataframe = flowfile.getContentsAsBytes()

        # バイトデータからGeoDataFrameを復元する
        target_value = pickle.loads(serialize_dataframe)

    return target_value
