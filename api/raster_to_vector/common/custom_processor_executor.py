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

# 外部ライブラリの動的インポート
pd = import_module("pandas")

from raster_to_vector.common.field_set_file_converter import FieldSetFileConverter
from raster_to_vector.common.processor_results import ProcessorResults
from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class CustomProcessorExecutor:
    def __init__(self, instance, processors):
        self.instance = instance
        self.processors = processors
        self.converter = FieldSetFileConverter()

    def execute_raster_vector_one_row(self, row, attributes, properties):
        # ラスベクチームの場合はrow['content']をカスタムプロセッサのcontentに設定する
        df = None
        content_data = row['content']

        # FSFの項目はContent, Findex以外はattributeに書き込む
        for key, value in row.items():
            if key not in ('content', 'Findex'):
                attributes[key] = value

        attributes['ColorSpace'] = row['color_space']

        new_content, new_attribute = self.processors(content_data, attributes, properties)
        row['color_space'] = new_attribute['ColorSpace']

        if isinstance(new_content, ProcessorResults):
            for data in new_content.results:
                column_name = data['column_name']
                col_type = data['col_type']
                value = data['value']

                row[column_name] = value
                # df_content.loc[index, column_name] = value

                self.converter.type_dict[column_name] = col_type
        elif isinstance(new_content, pd.DataFrame):
            # 複数行返ってきた場合
            df = new_content
        else:
            # dict以外の場合はそのままcontentを更新する
            row['content'] = new_content
            # df_content.loc[index, 'content'] = new_content

        return row, new_attribute, df

    def exexute_one_row(self, row, attributes, properties):
        df = None
        attributes['ColorSpace'] = row.get('color_space', '')
        new_row, new_attribute = self.processors(row, attributes, properties)
        if isinstance(new_row, list):
            df = pd.concat(new_row, axis=1).T

        return new_row, new_attribute, df

    def create_one_to_n_data(self, index, row, df):
        row_columns = set(row.index)
        df_columns = set(df.columns)
        missing_columns = row_columns - df_columns  # df_contentにない列を検出

        for col in missing_columns:
            df[col] = row[col]

        df['Findex'] = df.index

        return df

    def execute(self, content_data, attributes, properties):
        df_content_field_set_file = self.converter.content_to_field_set_file(content_data)
        df_content = self.converter.field_set_file_to_table(df_content_field_set_file)

        # 1 to Nでレコードが増加した場合使用するdataframe
        df_content_new = None

        if self.instance.__class__.__name__ == 'ImageMergeLogic':
            # 同じDataFrame内に複数のlayerがある場合もあるので分けてマージする
            group_row = df_content.groupby('layer').agg(lambda x: list(x)).reset_index()
            df_content = group_row.copy()

        for index, row in df_content.iterrows():
            if not issubclass(self.instance.__class__, FlexibleRasterVectorLogic):
                # BaseRasterVectorLogicを継承している場合
                new_row, new_attribute, df = self.execute_raster_vector_one_row(row, attributes, properties)
            else:
                # FlexibleRasterVectorLogicを継承している場合
                new_row, new_attribute, df = self.exexute_one_row(row, attributes, properties)

            if df is not None:
                # 1 to Nの場合
                df_one_to_n = self.create_one_to_n_data(index, row, df)
                df_content_new = pd.concat([df_content_new, df_one_to_n], ignore_index=True)
            else:
                # rowとdf_contentの列に差分がある場合はdf_contentに新規列を追加する
                row_columns = set(new_row.index)
                df_columns = set(df_content.columns)
                missing_columns = row_columns - df_columns  # df_contentにない列を検出

                # df_contentに存在しない列があれば、Noneで初期化して追加
                for col in missing_columns:
                    df_content[col] = None

                # 更新した行をDataFrameに戻す
                df_content.loc[index] = row

        if df_content_new is not None:
            # 1 to Nでレコードが増加した場合
            df_content_new['Findex'] = df_content_new.groupby('layer').cumcount()
            df_content = df_content_new
        
        if self.instance.__class__.__name__ == 'ImageMergeLogic':
            # 不要なcolumnを削除
            df_content = df_content.drop(columns=[new_attribute['offset_coordinate_x'], new_attribute['offset_coordinate_y']])

        df_field_set_file = self.converter.table_to_field_set_file(df_content)
        field_set_file = df_field_set_file.to_csv(index=False)

        return field_set_file, new_attribute



