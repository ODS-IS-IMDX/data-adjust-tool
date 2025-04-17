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

from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope
from raster_to_vector.common.base_processor import BaseProcessor


class ImageColorExtraction(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageColorExtraction

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像から特定の色の抽出を行うプロセッサクラスです。

    **処理概要:**

    - 入力された画像データに対して、指定された色情報で画素を抽出します。
    - 抽出された画素の画像をバイト列形式で返します。ColorSpaceはBINARYとなります。

    **注意事項:**

    - カラースペースはRGBである必要があります。異なる場合はエラーとなります。

    **赤、緑、青を抽出したい場合の例:**

    - プロパティの指定：
        Color Range: 20
        Color Name : red,       green,     blue
        RGB Value  : [235,0,0], [0,235,0], [0,0,235]

    - FieldSetFileへの出力結果：
        red項目  :RGB値が[215-255, 0-20, 0-20]の画素を抜き出したBINARY画像
        green項目:RGB値が[0-20, 215-255, 0-20]の画素を抜き出したBINARY画像
        blue項目 :RGB値が[0-20, 0-20, 215-255]の画素を抜き出したBINARY画像
        
    ※処理結果はColor Nameで指定した項目名に出力されます。

    タグ
    --------------------------------
    * image processing
    * extraction color
    * opencv

    プロパティ
    --------------------------------
    **Color Range**

    **必須入力  直接入力**

    - 画素抽出に使用する色情報の範囲(RGB Value)を±で指定します。
    - Color Rangeが20、RGB Valueが[10,20,240]の場合、RGB Valueは[0~30,0~40,220~255]の値をとります。(カンマ後ろの空白の有無は問いません。)
    - 初期値は20です。

    **Color Name**

    **必須入力  直接入力**

    - 抽出したい色の名前を入力してください。これはField_Set_Fileに登録する際にRGB Valueと連動したインデックスとなります。
    - 複数色指定したい場合はred,green,blueのように指定してください。(カンマ後ろの空白の有無は問いません。)
    - otherは指定した色以外がまとめて格納されるため、設定できません。
    - contentを指定した場合は入力画像が出力画像に上書きされます。
    - 初期値はredです。

    **RGB Value**

    **必須入力  直接入力**

    - 抽出したい色のRGB値を入力してください。[0,0,0]は左から赤、緑、青に対応しています。
    - 複数色指定したい場合は[255,0,0],[0,255,0],[0,0,255]のように指定してください。(カンマ後ろの空白の有無は問いません。)
    - 初期値は[255,0,0]です。

    **FSF Image Src**

    **任意入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。

    **Other Color Image Output**

    **任意入力  リスト入力**

    - Color Nameで指定した色以外の画素をother画像(RGB形式)として出力するかどうかを指定します。
    - Trueはother画像を出力し、Falseはother画像を出力しません。
    - 初期値はFalseです。

    リレーションシップ
    --------------------------------
    * success: 色抽出処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。RGBである必要があります。

    Writes Attributes
    --------------------------------
    * ColorSpace: 処理が正常に完了した場合にBINARYに設定されます。

    State management
    --------------------------------
    * なし

    Restricted
    --------------------------------
    * なし

    Input requirement
    --------------------------------
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(RGB形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ImageColorExtraction'

    color_range = PropertyDescriptor(
        name="Color Range",
        description="画素抽出に使用する色情報の範囲(RGB Value)を±で指定します。\
                    Color Rangeが20、RGB Valueが[10,20,240]の場合、\
                    RGB Valueは[0~30,0~40,220~255]の値をとります。(カンマ後ろの空白の有無は問いません。)\
                    初期値は20です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value=20,
        sensitive=False
        )
    
    color_name = PropertyDescriptor(
        name="Color Name",
        description="抽出したい色の名前を入力してください。これはField_Set_Fileに登録する際にRGB Valueと連動したインデックスとなります。\
                    複数色指定したい場合はred,green,blueのように指定してください。(カンマ後ろの空白の有無は問いません。)\
                    otherは指定した色以外がまとめて格納されるため、設定できません。\
                    contentを指定した場合は入力画像が出力画像に上書きされます。\
                    初期値はredです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value='red',
        sensitive=False
        )

    rgb_value = PropertyDescriptor(
        name="RGB Value",
        description="抽出したい色のRGB値を入力してください。[0,0,0]は左から赤、緑、青に対応しています。\
                    複数色指定したい場合は[255,0,0],[0,255,0],[0,0,255]のように指定してください。(カンマ後ろの空白の有無は問いません。)\
                    初期値は[255,0,0]です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value='[255,0,0]',
        sensitive=False
        )
    
    fsf_image_src = PropertyDescriptor(
        name="FSF Image Src",
        description="処理する画像を取得するFieldSetFileの項目名を指定します。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value='content',
        sensitive=False
        )

    other_color_image_output = PropertyDescriptor(
        name="Other Color Image Output",
        description="Color Nameで指定した色以外の画素をother画像(RGB形式)として出力するかどうかを指定します。\
                    Trueはother画像を出力し、Falseはother画像を出力しません。\
                    初期値はFalseです。",
        allowable_values=["True","False"],
        required=False,
        default_value='False',
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
        )

    # プロパティをリストに追加
    property_descriptors = [color_range, color_name, rgb_value, fsf_image_src, other_color_image_output]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageColorExtractionLogic.ImageColorExtractionLogic')
    def __init__(self, **kwargs):
        pass