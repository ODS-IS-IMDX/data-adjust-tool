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
from importlib import import_module

# 外部ライブラリのインポート
LineString = getattr(import_module("shapely.geometry"), "LineString")

import cad.common.cad_utils as CU
from common.error_code_list import ErrorCodeList
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
from cad.common.cad_base_validate_processor import CadBaseValidateProcessor


class ValidateAdjustEndpoints(CadBaseValidateProcessor):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''プロセッサー「AdjustEndpoints」の入力データのチェックを行う'''
        tags = ['geodataframe', 'geopandas', 'python', 'validate']

    #:
    CRS = PropertyDescriptor(
        name='crs',
        description='使用する座標参照系(CRS)を指定する。',
        default_value='6677',
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False
    )

    #:
    FEATURE_ID_COLUMN_NAME = PropertyDescriptor(
        name='Feature ID Column Name',
        description='地物IDのカラム名を指定する。',
        default_value='',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True
    )

    property_descriptors = [CRS, FEATURE_ID_COLUMN_NAME]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_exist_geometry_type(self, gdf):
        """
        ジオメトリにLineStringが存在するかチェックする
        
        :param gdf: GeoDataFrame
        :type gdf: geopandas.GeoDataFrame

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        exists = False
        for geom in gdf.geometry:
            if isinstance(geom, LineString):
                exists = True
            if not exists:
                self.validate_logger.write_log(error_code=ErrorCodeList.EC00010, **{"ジオメトリタイプ:": "LineString"})
        return exists

    def validate_duplicates_fid_data(self, row):
        """
        地物ID情報に重複がないかをチェックする

        :param row: DataFrameの1レコード分の情報
        :type row: pandas.Series

        :return: チェック結果 正常=True、異常=False
        :rtype: bool
        """
        no_duplicates = False  # 重複チェックフラグ

        # 両辺の値についてそれぞれ重複チェックを行う
        if self.validate_data_uniqueness(row["Value"], 0)[0] and self.validate_data_uniqueness(row["Value"], 1)[0]:
            no_duplicates = True
        else:
            args = {
                "error_code": ErrorCodeList.EC00013,
                "対象": row["Dwh"]
            }
            self.validate_logger.write_log(**args)

        return no_duplicates

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
            crs = context.getProperty(self.CRS).getValue()
            fid_col_name = context.getProperty(self.FEATURE_ID_COLUMN_NAME).getValue()

            # DataDefinition属性が存在しているかをチェック
            if not flowfile.getAttribute("DataDefinition"):
                args = {
                        "error_code": ErrorCodeList.EC00014,
                        "attribute_name": "DataDefinition",
                    }
                self.validate_logger.write_log(**args)
                return self.RESULT_FAILURE

            # インプットデータの検証
            if not self.validate_empty_data(input_data):
                return self.RESULT_FAILURE

            # FieldSetFileの形式をチェックする
            result, df = self.validate_fsf_format(input_data)
            if not result:
                return self.RESULT_FAILURE

            # バリデータログ作成
            if not self.validate_log_of_fsf(input_data, df):
                return self.RESULT_FAILURE

            # 必須フィールド（geometry、layer、fid_col_name）が存在するかチェックする
            if not self.validate_exist_geometry(df):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if not self.validate_exist_attribute(df, attribute_names=[fid_col_name]):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            # 地物IDの重複チェックを行う
            for i, row in df.iterrows():
                if row["Dwh"].split("/")[1] != fid_col_name:
                    # 対象レコードが地物ID情報以外の場合はコンティニュー
                    continue
                # 地物ID情報の重複チェックを行う
                if not self.validate_duplicates_fid_data(row):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE

            # ジオメトリタイプのLineStringが含まれていることをチェックする
            gdf = CU.field_set_file_to_geodataframe(input_data, crs)
            if not self.validate_exist_geometry_type(gdf):
                result = False
                if self.mode_value == self.MODE_STOP:
                    return self.RESULT_FAILURE

            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE

        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")

