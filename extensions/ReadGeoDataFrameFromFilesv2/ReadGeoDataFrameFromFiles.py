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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
#  パス指定でファイルを読み込みGeoDataFrameに変換する 変換後はcontentsに格納する
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import io
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")

class ReadGeoDataFrameFromFiles(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "2.0.0"
        description = """パス指定でファイルを読み込みgeodataframeに変換する 変換後はcontentsに格納する"""
        tags = ["python", "pandas", "geopandas"]

    # 出力後Attributeに設定する名前（フィールド集合ファイルではDWH名にも設定される）
    OUTPUT_NAME = PropertyDescriptor(
        name="output name",
        description="出力後attributeに設定する名前（フィールド集合ファイルではDWH名にも設定される）",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # GeoDataframeに設定するオプションCSV
    INPUT_OPTION_CSV = PropertyDescriptor(
        name="input_option_csv",
        description="GeoDataframeに設定するオプションCSV",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="OPTION,VALUE",
        sensitive=False,
        required=True
    )

    # CONTENTSに設定する形式
    CONTENTS_CODE = PropertyDescriptor(
        name="Contents Code",
        description="CONTENTSに設定する形式",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value=DDC.CONTENTS_CODE_GEODATAFRAME,
        allowable_values=DDC.CONTENTS_CODE_LIST,
        sensitive=False,
        required=True
    )

    property_descriptors = [OUTPUT_NAME,
                            INPUT_OPTION_CSV,
                            CONTENTS_CODE]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors
    
    #
    def get_geodataframe_from_stream(self, input_stream, input_option_stream):
        
        #geopandasに設定するオプションを取得
        input_option_dataframe = pd.read_csv(input_option_stream, quoting=3)
        
        if len(input_option_dataframe)==0:
            target_gdf = gpd.read_file(input_stream)
            return target_gdf
        
        option_list=input_option_dataframe["OPTION"].to_list()
        value_list=input_option_dataframe["VALUE"].to_list()
        
        #必須のファイルパスを引数に設定
        exec_read_code\
            ='target_gdf = gpd.read_file(input_stream'

        #オプションの文字列を結合
        for i in range(len(option_list)):

            option_code\
                =', {} = {}'.format(option_list[i],
                                    value_list[i])
            exec_read_code=exec_read_code+option_code
        
        #カッコで閉じる
        exec_read_code=exec_read_code+')'

        global_vars = globals().copy()
        local_vars = locals()
        
        #コードの実行
        exec(exec_read_code, global_vars, local_vars)
        
        #戻り値取得
        return local_vars['target_gdf']
        
    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # 出力後Attributeに設定する名前（フィールド集合ファイルではDWH名にも設定される）
            output_name\
                = context.getProperty(self.OUTPUT_NAME).evaluateAttributeExpressions(flowfile).getValue()

            # GeoDataframeに設定するオプションCSV
            input_option_csv\
                = context.getProperty(self.INPUT_OPTION_CSV).evaluateAttributeExpressions(flowfile).getValue()
            input_option_stream = io.StringIO(input_option_csv)
            
            # contentsに対する設定方法取得
            contents_code\
                = context.getProperty(self.CONTENTS_CODE).evaluateAttributeExpressions(flowfile).getValue()

            # flowfileからデータを取得プロパティから取得
            input_data = flowfile.getContentsAsBytes()

            # flowfileのデータをfile_objectへ変換
            input_stream = io.BytesIO(input_data)
            # ---------------------------------------------------------------------------

            # --------------------------------------------------------------------------
            # ファイルパスで取得
            # --------------------------------------------------------------------------
            input_geodataframe\
                = WM.calc_func_time(self.logger)\
                                   (self.get_geodataframe_from_stream)\
                                   (input_stream,
                                    input_option_stream)
            # --------------------------------------------------------------------------
            
            # GeoDataFrameを選択した場合、GeoDataFrameをpickle形式でシリアライズしてcontentsに設定する
            if contents_code == DDC.CONTENTS_CODE_GEODATAFRAME:
                output_value\
                    = pickle.dumps(input_geodataframe)

            # フィールド集合ファイルを選択した場合、GeoDataFrameをpickle形式でシリアライズしてValueに設定する。
            # Dwhは引数のファイル名に、Typeは'GeoDataFrame'とする
            elif contents_code == DDC.CONTENTS_CODE_FIELDSETFILE:

                target_dwh_list = []
                target_type_list = []
                target_value_list = []

                target_dwh_list, \
                    target_type_list, \
                    target_value_list\
                    = WM.calc_func_time(self.logger)(NSP.add_record_to_field_set_file_list)(target_dwh_list,
                                                                                            target_type_list,
                                                                                            target_value_list,
                                                                                            output_name,
                                                                                            DDC.CONTENTS_CODE_GEODATAFRAME,
                                                                                            input_geodataframe)

                # --------------------------------------------------------------------------
                # FieldSetFile構成要素の各リストをFieldSetFileに変換
                # --------------------------------------------------------------------------
                output_value\
                    = WM.calc_func_time(self.logger)(NSP.set_list_to_field_set_file)(target_dwh_list,
                                                                                     target_type_list,
                                                                                     target_value_list)
                # --------------------------------------------------------------------------

            else:
                raise

            # 結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=output_value,
                                           attributes={
                                               "crs": str(input_geodataframe.crs).replace("EPSG:", "")}
                                           )

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
