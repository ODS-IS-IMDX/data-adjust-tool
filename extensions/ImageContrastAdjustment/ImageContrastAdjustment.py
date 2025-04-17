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


class ImageContrastAdjustment(BaseProcessor):
    """
    プロセッサ名
    --------------------------------
    ImageContrastAdjustment

    バージョン
    --------------------------------
    1.0.0

    説明
    --------------------------------
    画像のコントラスト調整を行うプロセッサクラスです。

    **処理概要:**

    - 入力画像に対して、指定された方法でコントラストを調整します。
    - 利用できるコントラスト調整方法は4種類あります。

    #. AlphaBeta(アルファ・ベータ補正)
    #. Gamma(ガンマ補正)
    #. HistogramEqualization(ヒストグラム均等化)
    #. CLAHE(コントラスト制限付き適応ヒストグラム平坦化)

    - 調整結果画像をバイト列形式で返します。

    **注意事項:**

    - HistogramEqualization と CLAHEはカラースペースがGRAYSCALEである必要があります。異なる場合はエラーとなります。
    - AlphaBeta および Gamma はカラースペースがRGB、GRAYSCALEである必要があります。異なる場合はエラーとなります。
    - 入力画像のフォーマットと品質によって、調整結果が異なる場合があります。

    **コントラスト調整方法の説明**

    - AlphaBeta(アルファ・ベータ補正)：
        画像の明るさとコントラストを線形変換で調整する手法です。
        アルファ値でコントラスト制御、ベータ値で明るさ制御を行います。単純な計算で高速に処理できる特徴があります。

    - Gamma(ガンマ補正)：
        画像の明るさを非線形に変換する手法です。人間の視覚特性に合わせた自然な補正が可能です。

    - HistogramEqualization(ヒストグラム均等化)：
        画像全体のヒストグラムを均一に分布させる手法で、コントラストの自動的な改善が可能です。
        画像全体に対して同じ処理を適用するため、暗い画像の視認性を大幅に改善できますが、過度な強調になることがあります。

    - CLAHE(コントラスト制限付き適応ヒストグラム平坦化)：
        局所的な領域ごとにヒストグラム均等化を適用する手法です。過度なコントラスト強調を抑制できます。

    タグ
    --------------------------------
    * image processing
    * contrast adjustment
    * opencv

    プロパティ
    --------------------------------
    **Contrast Adjustment Method**

    **必須入力  リスト入力**

    - コントラスト調整の方法を選択します。

    #. AlphaBeta: アルファ・ベータ補正を行います。
        スキャン時の明るさムラや全体的な濃度調整が必要な図面に使用できます。

    #. Gamma: ガンマ補正を行います。
        薄い線や文字を見やすく調整したい図面に使用できます。特に鉛筆書きの図面や経年劣化で薄くなった線の視認性向上に適しています。

    #. HistogramEqualization: ヒストグラム均等化を行います。
        コピーを重ねて全体的に不鮮明になった図面や、極端に濃度差の少ない図面の改善に使用できます。
        ただし、線や文字が不自然に強調される可能性があります。

    #. CLAHE: コントラスト制限付き適応ヒストグラム平坦化を行います。
        図面の一部が影や汚れで見づらい場合や、スキャン時の光むらがある場合に使用できます。
        局所的な補正により、図面全体の視認性を均一に保つことができます。
      
    - 初期値はAlphaBetaです。

    **Alpha**

    **任意入力  直接入力**

    - AlphaBeta補正のアルファ補正値を設定します。
    - プロパティ(Contrast Adjustment Method)でAlphaBetaを指定した場合のみ有効です。
    - 初期値は1です。
    - 実用的な範囲は0.0～3.0程度であり、以下の効果があります。
        1.0未満：コントラスト減少
        1.0超過：コントラスト増加

    **Beta**

    **任意入力  直接入力**

    - AlphaBeta補正のベータ補正値を設定します。
    - プロパティ(Contrast Adjustment Method)でAlphaBetaを指定した場合のみ有効です。
    - 0 は変更なしです。初期値は0です。
    - 実用的な範囲は-100～100程度であり、以下の効果があります。
        0未満：暗くなる
        0超過：明るくなる

    **Gamma**

    **任意入力  直接入力**

    - Gamma補正のガンマ補正値を設定します。
    - プロパティ(Contrast Adjustment Method)でGammaを指定した場合のみ有効です。
    - 値が小さいと暗い部分が明るく、値が大きいと明るい部分が暗くなります。
    - 1.0 は変更なしです。初期値は1.0です。
    - 実用的な範囲は0.1～10.0程度程度であり、以下の効果がある。
        1.0未満：暗い部分が暗くなる
        1.0超過：暗い部分が明るくなる

    **CLAHE Clip Limit**

    **任意入力  直接入力**

    - CLAHEで使用するコントラスト制限値を設定します。
    - プロパティ(Contrast Adjustment Method)でCLAHEを指定した場合のみ有効です。
    - 値が大きいほどコントラストの強調が抑えられます。
    - 初期値は2.0です。
    - 実用的な範囲は1～40程度であり、以下の効果がある。
        小さい値：穏やかなコントラスト強調
        大きい値：強めのコントラスト強調
    
    **CLAHE Tile Grid Size**

    **任意入力  直接入力**

    - CLAHEで分割処理サイズ（縦pixel×横pixel）を設定します。
    - プロパティ(Contrast Adjustment Method)でCLAHEを指定した場合のみ有効です。
    - 画像をどれだけの大きさのグリッドに分割して処理を行うかを指定します。
    - 初期値は8,8です。
    - 実用的な範囲は 4,4 ～ 16,16 程度であり、以下の効果があります。
        小さい値：局所的な処理（細かい領域で補正）
        大きい値：広域的な処理（広い領域で補正）

    リレーションシップ
    --------------------------------
    * success: コントラスト調整処理が正常に完了した場合
    * failure: 処理中にエラーが発生した場合

    Reads Attributes
    --------------------------------
    * ColorSpace: 入力画像のカラースペース。HistogramEqualizationとCLAHEメソッドの場合はGRAYSCALEである必要があります。
    * ColorSpace: 入力画像のカラースペース。AlphaBeta および Gamma はカラースペースがRGB、GRAYSCALEである必要があります

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
    * バイトデータ形式の画像データ、またはバイトデータ形式の画像データを持つFieldSetFile(RGB形式、GRAYSCALE形式)である必要があります。

    System Resource Considerations
    --------------------------------
    * OpenCVライブラリを使用するため、画像サイズによってはメモリ使用量が増加する可能性があります。
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = 'ImageContrastAdjustment'

    contrast_adjustment_method = PropertyDescriptor(
        name="Contrast Adjustment Method",
        description="コントラスト調整の方法を選択します。\
                        AlphaBeta: スキャン時の明るさムラや全体的な濃度調整が必要な図面に使用できます。\
                        Gamma: 薄い線や文字を見やすく調整したい図面に使用できます。特に鉛筆書きの図面や経年劣化で薄くなった線の視認性向上に適しています。\
                        HistogramEqualization: コピーを重ねて全体的に不鮮明になった図面や、極端に濃度差の少ない図面の改善に使用できます。\
                        CLAHE: 局所的な領域ごとにヒストグラム均等化を適用する手法です。過度なコントラスト強調を抑制できます。\
                    初期値はAlphaBetaです。",
        allowable_values=["AlphaBeta", "Gamma", "HistogramEqualization", "CLAHE"],
        required=True,
        default_value="AlphaBeta",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    alpha = PropertyDescriptor(
        name="Alpha",
        description="AlphaBeta補正のアルファ補正値を設定します。\
                    値が大きいほどコントラストが強くなります。\
                    実用的な範囲は0.0～3.0程度であり、以下の効果がある。\
                    1.0未満：コントラスト減少\
                    1.0超過：コントラスト増加\
                    1.0 は変更なし、初期値は1.0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.0",
        sensitive=False
    )

    beta = PropertyDescriptor(
        name="Beta",
        description="AlphaBeta補正のベータ補正値を設定します。\
                    値が大きいほど画像が明るくなります。\
                    実用的な範囲は-100～100程度であり、以下の効果があります。\
                    0未満：暗くなる\
                    0超過：明るくなる\
                    0 は変更なしです。初期値は0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="0",
        sensitive=False
    )

    gamma = PropertyDescriptor(
        name="Gamma",
        description="Gamma補正のガンマ補正値を設定します。\
                    値が小さいと暗い部分が明るく、値が大きいと明るい部分が暗くなります。\
                    実用的な範囲は0.1～10.0程度程度であり、以下の効果があります。\
                    1.0未満：暗い部分が暗くなる\
                    1.0超過：暗い部分が明るくなる\
                    1.0 は変更なしです。初期値は1.0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="1.0",
        sensitive=False
    )

    clahe_clip_limit = PropertyDescriptor(
        name="CLAHE Clip Limit",
        description="CLAHEで使用するコントラスト制限値を設定します。\
                    値が大きいほどコントラストの強調が抑えられます。\
                    実用的な範囲は1～40程度であり、以下の効果があります。\
                    小さい値：穏やかなコントラスト強調\
                    大きい値：強めのコントラスト強調\
                    初期値は2.0です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="2.0",
        sensitive=False
    )

    clahe_tile_grid_size = PropertyDescriptor(
        name="CLAHE Tile Grid Size",
        description="CLAHEで分割処理サイズ（縦pixel×横pixel）を設定します。\
                    画像をどれだけの大きさのグリッドに分割して処理を行うかを指定します。\
                    実用的な範囲は 4,4 ～ 16,16 程度であり、以下の効果があります。\
                    小さい値：局所的な処理（細かい領域で補正）\
                    大きい値：広域的な処理（広い領域で補正）\
                    初期値は8,8です。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        default_value="8,8",
        sensitive=False
    )

    property_descriptors = [contrast_adjustment_method, alpha, beta, gamma, clahe_clip_limit, clahe_tile_grid_size]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('ImageContrastAdjustmentLogic.ImageContrastAdjustmentLogic')
    def __init__(self, **kwargs):
        pass
