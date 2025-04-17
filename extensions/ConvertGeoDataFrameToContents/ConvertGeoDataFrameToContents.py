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

# --------------------------------------------------------------------------------------------
# 【プロセッサ概要】
# GeoDataFrameを受け取ってファイルに出力
# --------------------------------------------------------------------------------------------
# Python標準ライブラリ
import traceback
from importlib import import_module
import io
import pickle

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")

class ConvertGeoDataFrameToContents(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """GeoDataFrameを受け取ってContentsに格納する形式に変換"""
        tags = ["python", "pandas", "GeoDataFrame"]

    # GeoDataframeに設定するオプションCSV
    OUTPUT_OPTION_CSV = PropertyDescriptor(
        name="Output Option CSV",
        description="""GeoDataframeに設定するオプション（CSV形式）。
                      ※GeoPandasライブラリのto_fileメソッドで用いるオプションを指定
                    """,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="OPTION,VALUE",
        sensitive=False,
        required=True
    )

    property_descriptors = [OUTPUT_OPTION_CSV]

    def __init__(self, **kwargs):
        pass

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_contents_from_geodataframe(self, geodataframe, output_option_dataframe):
        
        option_list=output_option_dataframe["OPTION"].to_list()
        value_list=output_option_dataframe["VALUE"].to_list()

        # OPTION列にdriverがなければエラー
        if "driver" in option_list:
            pass
        else:
            return "driverを設定してください"

        temp_object = io.BytesIO()

        #driver取得
        driver_index=option_list.index('driver')
        driver_string=value_list[driver_index]
        
        #CSVの場合 to_csvのメソッド使用
        if len(driver_string)==5 and driver_string[1:4]=='CSV':
        
            exec_read_code\
                ='result = geodataframe.to_csv(temp_object'

            #オプションの文字列を結合
            for i in range(len(option_list)):
                
                if option_list[i]=='driver':
                    continue
                else:
                    pass
                option_code\
                    =', {} = {}'.format(option_list[i],
                                        value_list[i])
                exec_read_code=exec_read_code+option_code
        
        else:
            
            exec_read_code\
                ='result = geodataframe.to_file(temp_object'
        
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
        return temp_object.getvalue()

    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # GeoDataframeに設定するオプションCSV
            # quoting=3→クォーテーションはそのまま文字列として読み込み
            output_option_csv\
                = context.getProperty(self.OUTPUT_OPTION_CSV).evaluateAttributeExpressions(flowfile).getValue()
            output_option_stream = io.StringIO(output_option_csv)
            output_option_dataframe = pd.read_csv(output_option_stream, quoting=3)
            # ---------------------------------------------------------------------------

            # ---------------------------------------------------------------------------
            # flowfileからGeoDataFrame取得
            # ---------------------------------------------------------------------------
            geodataframe\
                = WM.calc_func_time(self.logger)(NSP.get_geodataframe_from_contents_or_field_set_file)(flowfile)
            # ---------------------------------------------------------------------------

            # ---------------------------------------------------------------------------
            # 指定した形式でファイルを出力
            # ---------------------------------------------------------------------------
            result_object\
                = WM.calc_func_time(self.logger)(self.get_contents_from_geodataframe)(geodataframe,
                                                                                      output_option_dataframe)
            # ---------------------------------------------------------------------------

            if result_object == "driverを設定してください":
                self.logger.error('driverを設定してください')
                return FlowFileTransformResult(relationship="failure")

            # 結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=result_object)

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
