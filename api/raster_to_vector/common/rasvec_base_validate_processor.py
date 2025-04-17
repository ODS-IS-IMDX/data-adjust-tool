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
import pickle
from importlib import import_module
from abc import abstractmethod

# 外部ライブラリの動的インポート
np = import_module("numpy")
pd = import_module("pandas")

from common.base_validate_processor import BaseValidateProcessor
from common.error_code_list import ErrorCodeList
from raster_to_vector.common.field_set_file_converter import FieldSetFileConverter

class RasVecBaseValidateProcessor(BaseValidateProcessor):

    @abstractmethod
    def validate_data(self, context, flowfile):
        pass
    
    def validate_wrong_file_extension(self, file_name):
        """
        file_nameに'.jpg', '.jpeg', '.tiff', '.png', '.csv', '.pkl'か含まれるかチェックする
        :param file_name: フロー属性のファイルネーム
        :return bool: チェック結果 正常=True、異常=False
        """
        result = True
        # 画像の拡張子をfield_set_file_converter.convert_img_to_field_set_fileと合わせる
        # 処理できる拡張子'pkl', 'csv'かどうかも判定
        check_list = ('.jpg', '.jpeg', '.tiff', '.png', '.csv', '.pkl', '.pdf')
        if not file_name.lower().endswith(check_list):
            args = {
                    "error_code": ErrorCodeList.ER00001,
                    "file_name": file_name
                }
            self.validate_logger.write_log(**args)
            result = False
        
        return result
    
    def validate_img_to_fsf(self, input_data, attributes):
        """
        入力画像がFieldSetFile形式に変換できるかチェックする
        :param input_data: 入力画像
        :param attributes: 入力属性
        :return bool: チェック結果 正常=True、異常=False
        """
        result = True
        try:
            # 画像データの場合はFSFに変換
            self.converter = FieldSetFileConverter()
            field_set_file = self.converter.convert_img_to_field_set_file(input_data, attributes)
            df_content_field_set_file = self.converter.content_to_field_set_file(field_set_file)
            df_table = self.converter.field_set_file_to_table(df_content_field_set_file)
        except:
            args = {
                    "error_code": ErrorCodeList.ER00002,
                    "file_name": attributes['filename']
                }
            self.validate_logger.write_log(**args)
            result = False
            df_table = None
        
        return result, df_table

    def validate_empty_input_data(self, input_data, fsf_target_src):
        """
        FSFに処理対象が設定されているかチェックする
        :param input_data: FSF形式データ
        :param fsf_target_src: FSF項目名
        :return bool: チェック結果 正常=True、異常=False
        """
        result = True
        if fsf_target_src not in input_data.columns:
            args = {
                    "error_code": ErrorCodeList.ER00003,
                    "FSF Target Src": fsf_target_src + "がFieldSetFileに含まれていません。",
                    "columns": input_data.columns
                }
            self.validate_logger.write_log(**args)
            result = False
        
        return result
    
    def validate_empty_color_space(self, input_data):
        """
        color_spaceが設定されているかチェックする
        :param input_data: FSF形式データ
        :return bool: チェック結果 正常=True、異常=False
        """
        result = True
        if 'color_space' not in input_data.columns:
            args = {
                    "error_code": ErrorCodeList.ER00004,
                    "columns": input_data.columns
                }
            self.validate_logger.write_log(**args)
            result = False
        
        return result

    def validate_input_data_contents(self, input_data, file_name, fsf_target_src, index, is_fsf):
        """
        contentの中身が処理できる形式かチェックする
        :param input_data: PKL、FSF形式データ
        :param file_name: フロー属性のファイルネーム
        :param fsf_target_src: FSF項目名
        :param index: FSFの列番号
        :param is_fsf: FSF形式データ=True、PKL形式データ=False
        :return bool: チェック結果 正常=True、異常=False
        """
        result = True
        if is_fsf:
            content_name = "FSF Target Src → " + fsf_target_src
            if fsf_target_src in input_data.index:
                input_data = input_data[fsf_target_src]
            else:
                # ModeがContinueの場合でもエラーコードを出力
                args = {
                    "error_code": ErrorCodeList.ER00003,
                    "FSF Target Src": fsf_target_src + "がFieldSetFileに含まれていません。",
                    "columns": input_data.index,
                    "row_number": index
                }
                self.validate_logger.write_log(**args)
                return False
        else:
            content_name = "入力データ → " + file_name

        if not file_name.lower().endswith(('.tiff', '.pdf')):
            content = pickle.loads(input_data)
        else:
            content = input_data
        
        # .tiffの時はbytes型となる
        check_list = (np.ndarray, list, tuple, bytes)
        if type(content) not in check_list:
            args = {
                    "error_code": ErrorCodeList.ER00005,
                    "content_name": content_name,
                    "data_type": type(content),
                    "row_number": index
                }
            self.validate_logger.write_log(**args)
            result = False  
             
        return result
    
    def validate_none_contents(self, input_data, file_name, fsf_target_src, index, is_fsf):
        """
        contentの中身が処理できる形式かチェックする
        :param input_data: PKL、FSF形式データ
        :param file_name: フロー属性のファイルネーム
        :param fsf_target_src: FSF項目名
        :param index: FSFの列番号
        :param is_fsf: FSF形式データ=True、PKL形式データ=False
        :return bool: チェック結果 正常=True、異常=False
        """
        result = True
        if is_fsf:
            content_name = "FSF Target Src → " + fsf_target_src
            if fsf_target_src in input_data.index:
                input_data = input_data[fsf_target_src]
            else:
                # ModeがContinueの場合でもエラーコードを出力
                args = {
                    "error_code": ErrorCodeList.ER00003,
                    "FSF Target Src": fsf_target_src + "がFieldSetFileに含まれていません。",
                    "columns": input_data.index,
                    "row_number": index
                }
                self.validate_logger.write_log(**args)
                return False
        else:
            content_name = "入力データ → " + file_name
            
        if not file_name.lower().endswith(('.tiff', '.pdf')):
            content = pickle.loads(input_data)
        else:
            content = input_data

        if content is None:
            args = {
                    "error_code": ErrorCodeList.ER00007,
                    "content_name": content_name,
                    "content": content,
                    "row_number": index
                }
            self.validate_logger.write_log(**args)
            result = False 

        elif type(content) is np.ndarray:
            if content.size == 0:
                args = {
                    "error_code": ErrorCodeList.ER00006,
                    "content_name": content_name,
                    "content_size": content.size,
                    "row_number": index
                }
                self.validate_logger.write_log(**args)
                result = False 

            if np.any(np.isnan(content)):
                args = {
                    "error_code": ErrorCodeList.ER00008,
                    "content_name": content_name,
                    "row_number": index
                }
                self.validate_logger.write_log(**args)
                result = False
        
        # .tiffの時はbytes型となる
        elif type(content) is bytes:
            if len(content) == 0:
                args = {
                    "error_code": ErrorCodeList.ER00006,
                    "content_name": content_name,
                    "content_size": len(content),
                    "row_number": index
                }
                self.validate_logger.write_log(**args)
                result = False 
            
        elif type(content) is list or type(content) is tuple:
            if not content:
                args = {
                    "error_code": ErrorCodeList.ER00006,
                    "content_name": content_name,
                    "content_size": content,
                    "row_number": index
                }
                self.validate_logger.write_log(**args)
                result = False 

        # ModeがContinueの場合でもエラーコードを出力
        else:
            args = {
                    "error_code": ErrorCodeList.ER00005,
                    "content_name": content_name,
                    "data_type": type(content),
                    "row_number": index
                }
            self.validate_logger.write_log(**args)
            result = False
            
        return result