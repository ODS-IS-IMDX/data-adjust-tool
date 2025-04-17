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
import base64
import pickle
import io
from importlib import import_module

# 外部ライブラリの動的インポート
pd = import_module("pandas")
np = import_module("numpy")
Image = import_module('PIL.Image')


class FieldSetFileConverter:
    def __init__(self):
        self.data_name = None
        self.type_dict = {}

    def table_to_field_set_file(self, df):
        dwh_list = []
        type_list = []
        value_list = []

        layer_List = df['layer'].unique()
        for layer in layer_List:
            df_filtered = df[df['layer'] == layer]
            for column in df_filtered.columns:
                if column in ('Findex', 'layer'):
                    continue

                column_data = [(row['Findex'], row[column]) for _, row in df_filtered.iterrows()]

                # content全体をpickle.dumps()する
                # [(0, xxxx), (1, yyyy),...., (n, zzzz)]
                column_data_bytes = pickle.dumps(column_data)

                # Base64でエンコード
                encoded_column_data = base64.b64encode(column_data_bytes).decode('utf-8')

                data_type = type(df.loc[0, column]).__name__
                dtype = self.type_dict.get(column, data_type)

                dwh_list.append(f"{layer}/{column}")
                type_list.append(dtype)
                value_list.append(encoded_column_data)

        df_new = pd.DataFrame({
            "Dwh": dwh_list,
            "Type": type_list,
            "Value": value_list
        })

        return df_new

    def content_to_field_set_file(self, content):
        try:
            content_str = content.decode('utf-8')
        except UnicodeDecodeError:
            raise Exception("contentのデコードに失敗しました。UTF-8エンコードされたバイトデータが必要です。")

        try:
            df = pd.read_csv(io.StringIO(content_str))
            return df
        except pd.errors.EmptyDataError:
            raise Exception("FieldSetFileの読み込みに失敗しました。データが空、または形式が正しくありません。")

    def field_set_file_to_table(self, df_field_set_file):
        new_columns = [dwh.split('/')[-1] for dwh in df_field_set_file['Dwh']]
        new_columns = list(dict.fromkeys(new_columns))
        new_columns.append('layer')
        new_columns.append('Findex')
        df_restored = pd.DataFrame(columns=new_columns)

        # self.data_name = df_field_set_file['Dwh'][0].split('/')[0]
        layer_list = df_field_set_file['Dwh'].apply(lambda x: x.split('/')[0]).unique()

        for layer in layer_list:
            df_filtered = df_field_set_file[df_field_set_file['Dwh'].str.contains(layer)]
            row_count = len(df_restored)

            for _, row in df_filtered.iterrows():
                column_name = row['Dwh'].split('/')[-1]
                column_type = row['Type']
                self.type_dict[column_name] = column_type

                encoded_value = row['Value']
                decoded_data = base64.b64decode(encoded_value)
                column_data = pickle.loads(decoded_data)

                for findex, value in column_data:
                    if len(df_restored) < len(column_data) + row_count:
                        df_restored = pd.concat(
                            [
                                df_restored,
                                pd.DataFrame([{column_name: value, 'layer': layer, "Findex": findex}])
                            ],
                            ignore_index=True)
                    else:
                        df_restored.at[findex + row_count, column_name] = value

        return df_restored

    def create_fsf_new_row_from_image(self, img, content, attribute):
        img_numpy = np.array(img)
        img_pickle = pickle.dumps(img_numpy)
        img_numpy = np.array(img)

        if img.mode == '1' or (np.unique(img_numpy).size == 2 and img_numpy.min() == 0 and img_numpy.max() == 255):
            # 2値画像であってもimg.mode: '1'にならない場合があるので上記の条件分としている
            # gray scaleで0, 255ということも考えられるが、確率が低いのでとりあえずこの実装としている
            color_space = 'BINARY'
        elif img.mode in ('L', 'F'):
            color_space = 'GRAYSCALE'
        else:
            color_space = img.mode  # 他のモード (例: RGB, RGBAなど)

        df = pd.DataFrame(columns=["content", "color_space", 'layer', "Findex"])
        new_row = {
            "content": img_pickle if img.format != 'TIFF' else content,
            "color_space": color_space,
            "layer": attribute['filename'],
            "Findex": 0
        }

    def convert_img_to_field_set_file(self, content, attribute):
        new_content = content
        ext = attribute.get('filename', '').split('.')[-1]
        if ext.lower() not in ('jpg', 'jpeg', 'tiff', 'png', 'pdf'):
            return new_content

        df = pd.DataFrame(columns=["content", "color_space", 'layer', "Findex"])

        if ext == 'pdf':
            # PDFの場合
            new_row = {
                "content": new_content,
                "color_space": 'RGB',
                "layer": attribute['filename'],
                "Findex": 0
            }

        else:
            # 画像の場合
            try:
                img = Image.open(io.BytesIO(content))
            except Exception as e:
                return new_content

            img_numpy = np.array(img)
            img_pickle = pickle.dumps(img_numpy)

            if img.mode in ('L', 'F'):
                color_space = 'GRAYSCALE'
                # 画素が0と255のみの場合をBINARYとする
                if np.unique(img_numpy).size == 2 and img_numpy.min() == 0 and img_numpy.max() == 255:
                    color_space = 'BINARY'
            else:
                color_space = img.mode  # 他のモード (例: 24bit画像:RGB, 32bit画像:RGBAなど)

            new_row = {
                "content": img_pickle if img.format != 'TIFF' else content,
                "color_space": color_space,
                "layer": attribute['filename'],
                "Findex": 0
            }

        df.loc[len(df)] = new_row
        df_field_set_file = self.table_to_field_set_file(df)
        new_content = df_field_set_file.to_csv(index=False).encode('utf-8')

        return new_content





