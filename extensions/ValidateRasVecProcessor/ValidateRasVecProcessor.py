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


from raster_to_vector.common.rasvec_base_validate_processor import RasVecBaseValidateProcessor
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

class ValidateRasVecProcessor(RasVecBaseValidateProcessor):
    """
    プロセッサ名
    --------------------------------
    ValidateRasVecProcessor

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    入力データの設定が適切か検証を行うプロセッサです。

    **処理概要:**

    - FieldSetFileが処理できる形式かチェックします。
    - FieldSetFileの検証対象の項目を取得し、データが適切に設定されていることをチェックします。

    タグ
    --------------------------------
    * validate

    プロパティ
    --------------------------------
    **Mode**

    **必須入力  リスト入力**

    - エラー発生時の動作モードを設定します。[Stop: Failureにする, Continue: successにする]

    **FSF Target Src**

    **必須入力  直接入力**

    - 検証したいデータが設定されたFieldSetFileの項目名を指定します。
    - 初期値はcontentです。

    リレーションシップ
    --------------------------------
    * success: 入力データ、attributesにエラーがない場合
    * failure: 入力データ、attributesに問題がある場合

    Reads Attributes
    --------------------------------
    * なし

    Writes Attributes
    --------------------------------
    * なし

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式のデータを持つFieldSetFile

    System Resource Considerations
    --------------------------------
    * なし
    """
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ValidateRasVecProcessor'

    FSF_TARGET_SRC = PropertyDescriptor(
        name="FSF Target Src",
        description="入力が正しいか検証したいFieldSetFile項目名を指定します。入力データが.pklファイルの場合だと使用されません。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    property_descriptors = [FSF_TARGET_SRC]

    def getPropertyDescriptors(self):
        parent_properties = super().getPropertyDescriptors()
        return parent_properties + [self.Mode]

    def validate_data(self, context, flowfile):
        try:
            # インプットデータ取得
            input_data = flowfile.getContentsAsBytes()
            attributes = dict(flowfile.getAttributes().items())
            file_name = attributes['filename']
            fsf_target_src = str(context.getProperty(self.FSF_TARGET_SRC).getValue())

            result = True
            # 拡張子のチェック
            if not self.validate_wrong_file_extension(file_name):
                return self.RESULT_FAILURE
                
            if file_name.lower().endswith('.pkl'):
                is_fsf = False
                # pklファイルの場合データ数は常に1つ(0番目)
                index = 0
                # pklのデータ形式チェック
                if not self.validate_input_data_contents(input_data, file_name, fsf_target_src, index, is_fsf):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE
                
                # contentが空かNoneかチェック
                if not self.validate_none_contents(input_data, file_name, fsf_target_src, index, is_fsf):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE
            else:
                is_fsf = True 
                # 画像からFSF変換のチェック
                resurt, df_table = self.validate_img_to_fsf(input_data, attributes)
                if not resurt:
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE
                
                # contentのチェック
                if not self.validate_empty_input_data(df_table, fsf_target_src):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE
            
                # color_spaceのチェック
                if not self.validate_empty_color_space(df_table):
                    result = False
                    if self.mode_value == self.MODE_STOP:
                        return self.RESULT_FAILURE
                
                # FieldSetFile内に複数のcontentに対応
                for index, row in df_table.iterrows():
                    # contentのデータ形式チェック
                    if not self.validate_input_data_contents(row, file_name, fsf_target_src, index, is_fsf):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE
                    
                    # contentが空かNoneかチェック
                    if not self.validate_none_contents(row, file_name, fsf_target_src, index, is_fsf):
                        result = False
                        if self.mode_value == self.MODE_STOP:
                            return self.RESULT_FAILURE
                        
                # df(FieldSetFile)の行数・列数チェック
                if not self.validate_gdf_shape(df_table, data_name="DataFrame"):
                    return self.RESULT_FAILURE
            
            if result:
                return self.RESULT_SUCCESS
            else:
                return self.RESULT_FAILURE
            
        except Exception as e:
            raise Exception(f"[validate_data]: {str(e)}")