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


class ImageMorphology(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageMorphology

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    モルフォロジー変換を行うプロセッサクラスです。

    **処理概要:**

    - 入力データを受け取り、指定された演算方法でモルフォロジー変換を行います。
    - 演算には、MORPH_OPEN（ノイズ除去や物体の分離に有効）とMORPH_CLOSE（小さな穴や隙間を埋めるのに有効）を選択できます。
    - 変換結果はバイト列形式で返されます。

    **注意事項:**

    - カラースペースはRGB、GRAYSCALE、BINARYのいずれかである必要があります。異なる場合はエラーとなります。
    - Kernel Sizeは、5,5のような形式で指定する必要があります。
    - Anchorは、3,3のような形式であり、それぞれの要素がKernel Sizeよりも小さい必要があります。


    タグ
    --------------------------------
    * image processing
    * morphological operations
    * opencv


    プロパティ
    --------------------------------
    **Morphology Method**

    **必須入力  リスト入力**

    - モルフォロジー変換の演算方法を指定します。
      #. MORPH_OPEN: Erosion（侵食）によって画像内の小さな物体を削り取り、その後、Dilation（膨張）によって残った物体を膨らませる手法です（ノイズ除去や物体の分離に有効）。
      #. MORPH_CLOSE: Dilation（膨張）によって画像内の小さな穴を埋め、その後、Erosion（侵食）によって物体の形状を元に戻す手法です（小さな穴や隙間を埋めるのに有効）。
    - 初期値はMORPH_OPENです。

    **Kernel Shape**

    **必須入力  リスト入力**

    - 演算に使用するカーネルの形状を指定します。
      #. MORPH_RECT: 直線的な道路や管路の処理に適しています。
      #. MORPH_ELLIPSE: 斜め45度を含む道路の交差点や、格子状に配置された地下埋設物の処理に適しています。
      #. MORPH_CROSS: 曲線的な道路形状や、円形のマンホール等の処理に適しています。
    - 初期値は MORPH_RECTです。

    **Kernel Size**

    **必須入力  直接入力**

    - 演算に使用するカーネルのサイズ(縦pixel,横pixel)を奇数値で指定します。
    - 大きいカーネルほど大きなノイズ除去や穴埋めをしますが、細かい構造も失われる可能性があります。
    - 細い線（1〜5pixel）を対象に処理したい場合:
        3,3 を推奨します。
    - 中程度以上の線（6pixel以上）を対象に処理したい場合:
        線幅の1/2程度の値を推奨します。（線幅が20pixelの場合、9,9〜11,11程度）
    - 円形を対象に処理したい場合:
        円の直径の1/4〜1/3程度の値を推奨します。（円の直径が40pixelの場合、11,11〜13,13程度）
    - 初期値は 5,5 です。

    **Anchor**

    **任意入力  直接入力**

    - 演算の基準点(カーネルのアンカー位置)を指定します。
    - モルフォロジーの効果を特定の方向に強調することができます。
    - -1,-1（デフォルト値）は、カーネルの中心を自動選択して使用する。
    - 通常はデフォルト値の使用を推奨します。
    - 初期値は -1,-1です。それぞれの要素がカーネルのサイズよりも小さい必要があります。

    **Iterations**

    **任意入力  直接入力**

    - 演算を繰り返す回数を指定します。
    - 回数が多いほど大きなノイズ除去や穴埋めをしますが、細かい構造も失われる可能性があり、形状が過剰な変形します。
    - 初期値は 1 です。

    **Border Type**

    **任意入力  リスト入力**

    - 演算を行う際の画像境界ピクセル種別を指定します。
    
    #. BORDER_CONSTANT: 
        画像外を背景（黒）として扱いたい場合に使用します。
        画像端の対象物の輪郭抽出に適しています。

    #. BORDER_REPLICATE: 
        境界pixel値を繰り返して埋めます。
        画像端まで続く線や図形を処理する場合に使用します。
        境界付近の対象物の連続性を保持したい場合に適しています。

    #. BORDER_REFLECT: 
        境界pixel値を反転して埋めます。
        画像端での急激な変化を避けたい場合に使用します。
        境界付近の対象物の形状を維持したい場合に適しています。

    #. BORDER_REFLECT_101: 
        境界の前のpixelから反転して埋めます。
        より自然な境界処理が必要な場合に使用します。

    #. BORDER_WRAP: 
        反対側の境界pixel値で埋めます。
        周期的なパターンや繰り返し模様の処理に適しています。
        画像を繰り返しパターンとして扱う場合に使用します。

    - 初期値はBORDER_CONSTANTです。

    **Border Value**

    **任意入力  直接入力**

    - 演算を行う際の画像境界値として使用する値を指定します。Border TypeがBORDER_CONSTANTの場合のみ有効です。
    - 有効範囲は 0～255 で、初期値は 0 です。

    **FSF Image Src**

    **必須入力  直接入力**

    - 処理する画像を取得するFieldSetFileの項目名を指定します。
    - 画像データが見つからない場合はエラーを出力し処理を停止します。
    - 初期値はcontentです。"

    **FSF Image Dst**

    **必須入力  直接入力**

    - 処理した画像を出力するFieldSetFileの項目名を指定します。
    - 既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。
    - 初期値はcontentです。"

    リレーションシップ
    --------------------------------
    * success: モルフォロジー変換が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。RGB, GRAYSCALE, BINARYのいずれかである必要があります。

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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(RGB、BINARY、GRAYSCALE形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ImageMorphology'

    morphology_method = PropertyDescriptor(
        name="Morphology Method",
        description="モルフォロジー変換の演算方法を指定します。\
                        MORPH_OPEN: Erosion（侵食）によって画像内の小さな物体を削り取り、その後、Dilation（膨張）によって残った物体を膨らませる手法です（ノイズ除去や物体の分離に有効）。\
                        MORPH_CLOSE: Dilation（膨張）によって画像内の小さな穴を埋め、その後、Erosion（侵食）によって物体の形状を元に戻す手法です（小さな穴や隙間を埋めるのに有効）。\
                    初期値はMORPH_OPENです。",
        allowable_values=["MORPH_OPEN","MORPH_CLOSE"],
        required=True,
        default_value="MORPH_OPEN",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    kernel_shape = PropertyDescriptor(
        name="Kernel Shape",
        description="演算に使用するカーネルの形状を指定します。\
                        MORPH_RECT: 直線的な道路や管路の処理に適しています。\
                        MORPH_ELLIPSE: 斜め45度を含む道路の交差点や、格子状に配置された地下埋設物の処理に適しています。\
                        MORPH_CROSS: 曲線的な道路形状や、円形のマンホール等の処理に適しています。\
                    初期値は MORPH_RECTです。",
        allowable_values=["MORPH_RECT","MORPH_ELLIPSE","MORPH_CROSS"],
        required=True,
        default_value="MORPH_RECT",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    kernel_size = PropertyDescriptor(
        name="Kernel Size",
        description="演算に使用するカーネルのサイズを指定します。\
                    大きいカーネルほど大きなノイズ除去や穴埋めをしますが、細かい構造も失われる可能性があります。\
                    細い線（1〜5px）: 3,3を推奨します。\
                    中程度以上の線（6px以上）: 線幅の1/2程度（例：20pxの場合、9,9〜11,11）を推奨します。\
                    円形: 円の直径の1/4〜1/3程度（例：40pxの場合、11,11〜13,13）を推奨します。\
                    初期値は 5,5です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="5,5",
        sensitive=False
    )

    anchor = PropertyDescriptor(
        name="Anchor",
        description="演算の基準点(カーネルのアンカー位置)を指定します。\
                    モルフォロジーの効果を特定の方向に強調することができます。\
                    -1,-1（デフォルト値）は、カーネルの中心を自動選択して使用します。\
                    通常はデフォルト値の使用を推奨します。\
                    初期値は -1,-1です。それぞれの要素がカーネルのサイズよりも小さい必要があります。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="-1,-1",
        sensitive=False
    )

    iterations = PropertyDescriptor(
        name="Iterations",
        description="演算を繰り返す回数を指定します。\
                    回数が多いほど大きなノイズ除去や穴埋めをしますが、細かい構造も失われる可能性があり、形状が過剰な変形します。\
                    初期値は 1 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value=1,
        sensitive=False
    )

    border_type = PropertyDescriptor(
        name="Border Type",
        description="演算を行う際の画像境界ピクセル種別を指定します。\
                        BORDER_CONSTANT: 画像外を黒で処理、輪郭抽出に適用できます。\
                        BORDER_REPLICATE: 境界のピクセルを繰り返し、線や図形の連続性を保持します。\
                        BORDER_REFLECT: 境界ピクセルを反転し、急激な変化を避けます。\
                        BORDER_REFLECT_101: 境界前のピクセルを反転し、自然な境界処理を実現します。\
                        BORDER_WRAP: 反対側の境界ピクセルで埋め、周期的なパターンや繰り返し模様に適用できます。\
                    初期値はBORDER_CONSTANTです。",
        allowable_values=["BORDER_CONSTANT", "BORDER_REPLICATE", "BORDER_REFLECT", "BORDER_REFLECT_101", "BORDER_WRAP"],
        required=False,
        default_value="BORDER_CONSTANT",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    border_value = PropertyDescriptor(
        name="Border Value",
        description="演算を行う際の画像境界値として使用する値を指定します。Border TypeがBORDER_CONSTANTの場合のみ有効です。\
                    有効範囲は 0～255 で、初期値は 0 です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value=0,
        sensitive=False
    )

    fsf_image_src = PropertyDescriptor(
        name="FSF Image Src",
        description="処理する画像を取得するFieldSetFileの項目名を指定します。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    fsf_image_dst = PropertyDescriptor(
        name="FSF Image Dst",
        description="処理した画像を出力するFieldSetFileの項目名を指定します。\
                    既存の項目名を指定した場合は上書きされ、新規の項目を指定した場合は項目が追加されます。\
                    初期値はcontentです。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
        default_value="content",
        sensitive=False
    )

    property_descriptors = [
        morphology_method,
        kernel_shape,
        kernel_size,
        anchor,
        iterations,
        border_type,
        border_value,
        fsf_image_src,
        fsf_image_dst
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageMorphologyLogic.ImageMorphologyLogic')
    def __init__(self, **kwargs):
        pass
