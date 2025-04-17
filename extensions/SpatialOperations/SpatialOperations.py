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

from nifiapi.properties import (
    PropertyDescriptor,
    ExpressionLanguageScope,
    StandardValidators,
)

from raster_to_vector.common.base_processor import BaseProcessor


class SpatialOperations(BaseProcessor):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """FieldSetFileからジオメトリデータを取得し、指定された空間演算を実行するプロセッサ。\
        包含関係や交差関係などでジオメトリを関連付け、関連付けられたジオメトリに対して結合や差分取得などを行い、新しいジオメトリを生成する。\
        関連性を取得する空間演算と、関連性を基にジオメトリ生成を行う空間演算を組み合わせて処理を実行し、処理結果としてFieldSetFileを出力する。"""
        tags = [
            "geopandas",
            "join",
            "geodataframe",
            "dataframe",
            "spatial",
            "operations",
        ]

    # Operation Typeの定義
    OPERATION_INTERSECTS = "Intersects"
    OPERATION_CONTAINS = "Contains"
    OPERATION_WITHIN = "Within"
    OPERATION_TOUCHES = "Touches"
    OPERATION_CROSSES = "Crosses"
    OPERATION_OVERLAPS = "Overlaps"
    OPERATION_COVERS = "Covers"
    OPERATION_COVERED_BY = "Covered_by"
    OPERATION_EQUALS = "Equals"
    OPERATION_DISTANCE = "Distance"
    OPERATION_ISCIRCLE = "IsCircle"
    OPERATION_TOUCHESENDPOINT = "TouchesEndpoint"
    OPERATION_CASCADETOUCHES = "CascadeTouches"
    OPERATION_MOSTINTERSECTS = "MostIntersects"

    # Post Operation Typeの定義
    POST_OPERATION_NONE = "None"
    POST_OPERATION_UNION = "Union"
    POST_OPERATION_INTERSECTION = "Intersection"
    POST_OPERATION_DIFFERENCE = "Difference"
    POST_OPERATION_SYMMETRICAL_DIFFERENCE = "SymmetricalDifference"
    POST_OPERATION_CLIP = "Clip"

    # 空間演算の種類
    OPERATION_TYPE = PropertyDescriptor(
        name="Operation Type",
        description="実行する空間演算（ジオメトリ判定）の種類を指定する。",
        default_value=OPERATION_INTERSECTS,
        allowable_values=[
            OPERATION_INTERSECTS,
            OPERATION_CONTAINS,
            OPERATION_WITHIN,
            OPERATION_TOUCHES,
            OPERATION_CROSSES,
            OPERATION_OVERLAPS,
            OPERATION_COVERS,
            OPERATION_COVERED_BY,
            OPERATION_EQUALS,
            OPERATION_DISTANCE,
            OPERATION_ISCIRCLE,
            OPERATION_TOUCHESENDPOINT,
            OPERATION_CASCADETOUCHES,
            OPERATION_MOSTINTERSECTS
        ],
        required=True,
    )
    # 座標参照系（CRS）
    #:
    CRS = PropertyDescriptor(
        name="CRS",
        description="空間演算に使用する座標参照系(CRS)を指定する。",
        default_value=6677,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )
    # 演算対象メインジオメトリのデータ名
    #:
    MAIN_GEOMETRY_NAME = PropertyDescriptor(
        name="Main Geometry Name",
        description="演算対象メインジオメトリのデータ名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )
    # 演算対象サブジオメトリのデータ名
    #:
    SUB_GEOMETRY_NAME = PropertyDescriptor(
        name="Sub Geometry Name",
        description="演算対象サブジオメトリのデータ名を指定する。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )
    # メインジオメトリのフィルタリング属性名
    #:
    MAIN_ATTRIBUTE_NAME = PropertyDescriptor(
        name="Main Attribute Name",
        description="メインジオメトリのフィルタリング属性名を指定する。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )
    # サブジオメトリのフィルタリング属性名
    #:
    SUB_ATTRIBUTE_NAME = PropertyDescriptor(
        name="Sub Attribute Name",
        description="サブジオメトリのフィルタリング属性名を指定する。",
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )
    # 円形検出の許容誤差
    #:
    TOLERANCE = PropertyDescriptor(
        name="Tolerance",
        description="円形検出の許容誤差または接触判定の許容距離を指定する。0以上の数値を許容する。",
        default_value=0.01,
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
    )
    # 空の結果を許容するか
    #:
    INCLUDE_EMPTY_RESULTS = PropertyDescriptor(
        name="Include Empty Results",
        description="レコードに空の結果（None）を含めるかどうかを指定する。",
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value="True",
        allowable_values=["True", "False"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=False,
    )
    # 出力形式
    #:
    OUTPUT_FORMAT = PropertyDescriptor(
        name="Output Format",
        description='出力形式を指定する。① pairs: 1対1のペア  ② list: 1対多のペア',
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value="pairs",
        allowable_values=["pairs", "list"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )
    # 出力フィールド名のサフィックス
    #:
    SUFFIX = PropertyDescriptor(
        name="Suffix",
        description="FieldSetFileに出力する空間演算（ジオメトリ判定）処理結果レコードのデータ属性名を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )
    # 許容範囲値
    #:
    DISTANCE = PropertyDescriptor(
        name="Distance",
        description="探索範囲値を指定する。負の値の場合、サブジオメトリ全てを包括するジオメトリを探索基準として取得する。",
        validators=[StandardValidators.NUMBER_VALIDATOR],
        default_value=0,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )
    # 該当しないレコードを出力するかどうか
    #:
    IS_NOT = PropertyDescriptor(
        name="Is Not",
        description="演算タイプIsCircleに対応。非円形のジオメトリをTrueとするかどうかを指定する。",
        validators=[StandardValidators.BOOLEAN_VALIDATOR],
        default_value="False",
        allowable_values=["True", "False"],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.NONE,
        required=True,
    )

    # 実行する空間演算の種類
    #:
    POST_OPERATION_TYPE = PropertyDescriptor(
        name="Post Operation Type",
        description="実行する空間演算の種類（ジオメトリ生成）を指定する。",
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        default_value=POST_OPERATION_NONE,
        allowable_values=[
            POST_OPERATION_NONE,
            POST_OPERATION_UNION,
            POST_OPERATION_INTERSECTION,
            POST_OPERATION_DIFFERENCE,
            POST_OPERATION_SYMMETRICAL_DIFFERENCE,
            POST_OPERATION_CLIP,
        ],
        required=True,
    )

    # 結果として出力されるジオメトリデータの名称(PostOperationTypeがNoneでないとき)
    #:
    POST_GEOMETRY_NAME = PropertyDescriptor(
        name="Post Geometry Name",
        description="FieldSetFileに出力する空間演算（ジオメトリ生成）処理結果レコードのデータ名を指定する。Post Operation TypeがNoneの場合に機能する。",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        validators=[StandardValidators.NON_EMPTY_VALIDATOR],
        required=True,
    )

    property_descriptors = [
        OPERATION_TYPE,
        CRS,
        MAIN_GEOMETRY_NAME,
        SUB_GEOMETRY_NAME,
        MAIN_ATTRIBUTE_NAME,
        SUB_ATTRIBUTE_NAME,
        TOLERANCE,
        INCLUDE_EMPTY_RESULTS,
        OUTPUT_FORMAT,
        SUFFIX,
        DISTANCE,
        IS_NOT,
        POST_OPERATION_TYPE,
        POST_GEOMETRY_NAME,
    ]

    def getPropertyDescriptors(self):
        return self.property_descriptors

    @BaseProcessor.set_logic_class('SpatialOperationsLogic.SpatialOperationsLogic')
    def __init__(self, **kwargs):
        pass
