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
# 補正対象のジオメトリ（Point, LineString, Polygon）に対し、基準点の変化をもとに、
# ヘルマート変換を適用して空間位置を補正。
# 補正対象のジオメトリとTINで空間結合を行い、対象がTINのConvex Hull（凸包）内にあるかを確認して信頼できる補正のみを実施。
# 処理後は補正済みの座標をFieldSetFileとして出力。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import traceback
import io

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.WrapperModule as WM


# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
np = import_module("numpy")
gpd = import_module("geopandas")
Point = import_module("shapely").geometry.Point
LineString = import_module("shapely").geometry.LineString
Polygon = import_module("shapely").geometry.Polygon
MultiPoint = import_module("shapely").geometry.MultiPoint
box = import_module("shapely").geometry.box

GEOMETRY_TYPE_MAP = {
    "Point": "+1",
    "LineString": "+2",
    "Polygon": "+3"
}

class HelmertTransform(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        補正対象のジオメトリ（Point, LineString, Polygon）を、基準GCPと対象データGCPの変化をもとに、ヘルマート変換を行う
                        ①input: 3つの座標配列（位置基準点・補正対象点・補正対象）を持った3行のFieldSetFile。
                                 ※内周ポリゴンが存在する場合は内周の座標配列も必要。
                        ②output: 座標配列を持った1行のFieldSetFile。
                                 ※inputに内周ポリゴンが存在する場合は内周の座標配列も出力。
                      """

        tags = ["HelmertTransform", "TIN", "GCP", "Python"]

    TARGET_DWH_NAME = PropertyDescriptor(
        name="Target DWH Name",
        description="入力データの補正対象の座標配列のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    POLYGON_INTERIOR_DWH_NAME = PropertyDescriptor(
        name="Polygon Interior DWH Name",
        description="入力データのポリゴンの内周座標配列のDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=False,
        sensitive=False
    )

    TARGET_GEOMETRY_TYPE = PropertyDescriptor(
        name="Geometry Type",
        description="補正対象のジオメトリ",
        expression_language_scope=ExpressionLanguageScope.NONE,
        allowable_values=["Point", "LineString", "Polygon"],
        required=True,
        sensitive=False
    )

    TARGET_GCP_DWH_NAME = PropertyDescriptor(
        name="Target GCP DWH Name",
        description="入力データの補正対象点の座標配列のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    BASE_GCP_DWH_NAME = PropertyDescriptor(
        name="Base GCP DWH Name",
        description="入力データの位置基準点の座標配列のDWH名",
        required=True,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    OUTPUT_TARGET_DWH_NAME = PropertyDescriptor(
        name="Output Target DWH Name",
        description="出力データの補正対象の座標配列のDWH名",
        required=False,
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
    )

    OUTPUT_INTERIOR_DWH_NAME = PropertyDescriptor(
        name = "Output Polygon Interior DWH Name",
        description = "出力データのポリゴンの内周座標配列のDWH名",
        required = False,
        sensitive = False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [TARGET_DWH_NAME,
                            POLYGON_INTERIOR_DWH_NAME,
                            TARGET_GEOMETRY_TYPE,
                            TARGET_GCP_DWH_NAME,
                            BASE_GCP_DWH_NAME,
                            OUTPUT_TARGET_DWH_NAME,
                            OUTPUT_INTERIOR_DWH_NAME
                            ]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        プロパティで設定した値を取得

                Parameters:
            context: プロセッサの設定値
            flowfile: プロセッサに入るデータ

        Returns:
            target_dwh_name: 補正対象の座標配列のDWH名
            polygon_interior_dwh_name: 補正対象ポリゴンの内周座標配列のDWH名
            target_geometry_type: 補正対象のShapelyオブジェクトのタイプ
            target_gcp_dwh_name: 補正対象点の座標配列のDWH名
            base_gcp_dwh_name: 位置基準点の座標配列のDWH名
            output_target_dwh_name: 補正後の座標配列のDWH名
            output_interior_dwh_name: 補正対象ポリゴン(内周)補正後の座標配列のDWH名
        """

        # 補正対象の座標配列のDWH名
        target_dwh_name = context.getProperty(
            self.TARGET_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 補正対象ポリゴンの内周座標配列のDWH名
        polygon_interior_dwh_name = context.getProperty(
            self.POLYGON_INTERIOR_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 補正対象のShapelyオブジェクトのタイプ
        target_geometry_type = context.getProperty(
            self.TARGET_GEOMETRY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        # 補正対象点の座標配列のDWH名
        target_gcp_dwh_name= context.getProperty(
            self.TARGET_GCP_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 位置基準点の座標配列のDWH名
        base_gcp_dwh_name= context.getProperty(
            self.BASE_GCP_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 補正後の座標配列のDWH名
        output_target_dwh_name= context.getProperty(
            self.OUTPUT_TARGET_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 補正対象ポリゴン(内周)補正後の座標配列のDWH名
        output_interior_dwh_name= context.getProperty(
            self.OUTPUT_INTERIOR_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        return target_dwh_name, polygon_interior_dwh_name, target_geometry_type, target_gcp_dwh_name, base_gcp_dwh_name, output_target_dwh_name, output_interior_dwh_name

    def get_flowfile(self, flowfile):
        """
        FlowFileから"crs"属性の値を取得し、FieldSetFileをDataFrameに変換

        Parameters:
            flowfile: プロセッサに入ってくるデータ

        Returns:
            field_set_file_dataframe: FieldSetFileから作成したDataFrame
        """
        # flowfileから"crs"属性の内容を取得する
        crs = flowfile.getAttribute("crs")

        # flowfileから、CSV形式のFieldSetFileを取得
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
        field_set_file_dataframe = pd.read_csv(io.StringIO(input_field_set_file))

        return field_set_file_dataframe, crs

    def get_coordinates_array_from_field_set_file_dataframe(self,
                                                            field_set_file_dataframe,
                                                            target_dwh_name,
                                                            target_gcp_dwh_name,
                                                            base_gcp_dwh_name):
        """
        FieldSetFileで作成したdataframeから、プロパティで指定したDWH名を持つ行のValue列をデコード、デシリアライズし取得

        Parameters:
            field_set_file_dataframe: FieldSetFileをDataFrameに加工したデータ
            target_dwh_name: プロパティで指定した補正対象の座標配列のDWH名
            target_gcp_dwh_name: プロパティで指定した補正対象点の座標配列のDWH名
            base_gcp_dwh_name: プロパティで指定した位置基準点の座標配列のDWH名

        Returns:
            target_coordinates_array: 補正対象の座標配列
            target_gcp_coordinates_array: 補正対象点の座標配列
            target_gcp_coordinates_array: 位置基準点の座標配列
        """

        # 補正対象の座標配列を field_set_dataframeから取得
        target_coordinates_array = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, target_dwh_name)

        # 補正対象点の座標配列を field_set_dataframeから取得
        target_gcp_coordinates_array = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, target_gcp_dwh_name)

        #位置基準点の座標配列を field_set_dataframeから取得
        base_gcp_coordinates_array = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, base_gcp_dwh_name)

        return target_coordinates_array, target_gcp_coordinates_array, base_gcp_coordinates_array


    def assign_group_id(self, target_array, group_size):
        """
        配列に対して group_size ごとのグループIDを設定

        Parameters:
            target_array: 入力配列（2次元配列。先頭列にIDを書き換える）
            group_size: 各グループの行数（1グループに何行割り当てるか）

        Returns:
            result_array: グループIDを設定した配列
        """

        # 入力配列をコピー（元の配列を変更しないように）
        result_array = target_array.copy()

        # 総行数を取得
        row_length = result_array.shape[0]

        # グループIDを繰り返しで生成
        # 例: group_size=3, 行数=7 → [0,0,0,1,1,1,2]
        group_ids = np.repeat(np.arange((row_length + group_size - 1) // group_size), group_size)[:row_length]

        # 生成したグループIDを配列の1列目（ID列）に代入
        result_array[:, 0] = group_ids

        return result_array


    def tin_array_to_polygons(self, tin_array, geometry_type="+3", group_size=4):
        """
        TINの配列に終点(始点と同一座標)を追加

        Parameters:
            tin_array: TINの配列 (N, 3, 3 or 4)
            geometry_type: 補正対象のジオメトリ
            group_size: グループIDを付与する際の単位

        Returns:
            shapely_array: shapely.geometry.Polygonオブジェクトの配列
        """

        # ポリゴン作成のためドロネー3点に終点(始点と同一座標)を追加
        closed_tin_array=np.concatenate([tin_array, tin_array[:,0:1,:]], axis=1)

        # ジオメトリ作成関数使用のため、(N, 3, 3 or 4) → (N, 3 or 4)
        flattened_array = closed_tin_array.reshape(-1, closed_tin_array.shape[2])

        # ジオメトリ作成のため、ポリゴン構成点ごとに同一id振り直し
        assign_id_flattened_array = self.assign_group_id(flattened_array, group_size)

        # 座標配列をポリゴンジオメトリ(shaplyオブジェクト)へ変換
        shapely_array\
            = NSP.get_attribute_coordinates_by_geometry_items(assign_id_flattened_array[:,:4], geometry_type)

        return shapely_array

    def compute_helmert_params(self, Q, Qp):
        """
        ヘルマート変換パラメータ（回転、スケーリング、平行移動）を計算

        Parameters:
            Q: 変換元の座標 (N, 2)
            Qp: 変換先の座標 (N, 2)

        Returns:
            a : スケーリングと回転に関わる係数
            b : スケーリングと回転に関わる係数
            tx : x方向の平行移動量
            ty : y方向の平行移動量
        """

        A = []
        b_vec = []

        for (x, y), (xp, yp) in zip(Q, Qp):

            A.append([x, -y, 1, 0])
            b_vec.append(xp)

            A.append([y,  x, 0, 1])
            b_vec.append(yp)

        A = np.array(A)
        b_vec = np.array(b_vec)

        params, residuals, rank, s = np.linalg.lstsq(A, b_vec, rcond=None)
        a, b, tx, ty = params

        return a, b, tx, ty

    def transform_coordinates_array_by_helmert(self, coordinates_array, a_, b_, tx, ty, start, end):
        """
        指定範囲の座標にヘルマート変換を適用

        Parameters:
            coordinates_array: 補正対象の座標配列
            a_, b_, tx, ty: ヘルマート変換パラメータ
            start: 開始インデックス
            end: 終了インデックス

        Returns:
            r_x, r_y: 変換後のx座標配列、y座標配列
        """

        r_x = a_*coordinates_array[start: end + 1][: ,1]-b_*coordinates_array[start: end + 1][: ,2] + tx
        r_y = b_*coordinates_array[start: end + 1][: ,1]+a_*coordinates_array[start: end + 1][: ,2] + ty

        return r_x, r_y

    def apply_helmert_transform(self,
                                target_coordinates_array,
                                interior_coordinates_array,
                                target_gcp_tin_array,
                                base_gcp_tin_array,
                                target_index_list,
                                interior_index_list,
                                id_dict,
                                unique_ids_array,
                                has_interior_flag):
        """
        Parameters:
            target_coordinates_array: 補正対象ジオメトリの座標配列
            interior_coordinates_array: 補正対象ポリゴンの内周座標配列
            target_gcp_tin_array: 移動前GCPのTIN座標配列
            base_gcp_tin_array: 移動後GCPのTIN座標配列
            target_index_list: 補正対象の座標配列の始点・終点インデックスのリスト
            interior_index_list: 補正対象ポリゴンの内周座標配列の始点・終点インデックスのリスト
            id_dict: 各ジオメトリIDに対応するTIN IDの辞書
            unique_ids_array: ジオメトリごとのユニークID配列
            has_interior_flag: 補正対象がポリゴンの際、内周が存在するかどうかのフラグ

        Returns:
            target_coordinates_array_copy: ヘルマート変換後の外周座標配列
            interior_coordinates_array_copy: ヘルマート変換後の内周座標配列
        """

        # 元の配列を破壊しないようにコピーを作成（外周・内周）
        interior_coordinates_array_copy = interior_coordinates_array.copy()
        target_coordinates_array_copy = target_coordinates_array.copy()

        # 内周のインデックスのスライス位置を管理するためのポインタ
        interior_ptr = 0

        for i, uid in enumerate(unique_ids_array):

            # 内周がこのジオメトリにあるかのフラグ
            interior_match_flag = False

            # 外周の対象範囲（始点・終点インデックス）を取得
            target_start, target_end = target_index_list[i]

            # 内周フラグがTrueかつ未処理の内周がまだある場合に内周処理を試みる
            if has_interior_flag and interior_ptr < len(interior_index_list):

                # 内周インデックス（始点・終点）を取得
                interior_start, interior_end = interior_index_list[interior_ptr]

                # 内周座標の先頭要素のIDが現在処理中のジオメトリIDと一致するか確認
                if interior_coordinates_array_copy[interior_start:interior_end + 1][0, 0] == uid:

                    interior_ptr+=1
                    interior_match_flag = True

                else:
                    interior_match_flag = False

            # GCPとの交差情報が存在しない場合はスキップ
            if uid not in id_dict:

                continue

            # 補正対象ジオメトリと交差するTIN座標配列indexを取得
            gcp_index_list = id_dict[uid]

            # [gcp_index_list]: 補正対象ジオメトリと交差するTIN座標配列を取得
            # [:, :, 1:3]: id抜きの座標[x, y]のみ取得
            # .reshape(-1, 2): (N, N, 2) → (2, N)
            reshape_target_gcp_tin_array = target_gcp_tin_array[gcp_index_list][:, :, 1:3].reshape(-1, 2)
            reshape_base_gcp_tin_array = base_gcp_tin_array[gcp_index_list][:, :, 1:3].reshape(-1, 2)

            # 交差がなかった場合スキップ
            if reshape_target_gcp_tin_array.size == 0 or reshape_base_gcp_tin_array.size == 0:

                continue

            # ヘルマート変換パラメータ（回転、スケーリング、平行移動）を計算
            a_, b_, tx, ty\
                = self.compute_helmert_params(reshape_target_gcp_tin_array, reshape_base_gcp_tin_array)

            # 外周の対象座標に対してHelmert変換を適用
            target_corrected_x, target_corrected_y = self.transform_coordinates_array_by_helmert(target_coordinates_array, a_, b_, tx, ty, target_start, target_end)

            # 補正後のx, yをコピー配列に上書き
            target_coordinates_array_copy[target_start:target_end + 1][:,1] = target_corrected_x
            target_coordinates_array_copy[target_start:target_end + 1][:,2] = target_corrected_y

            # 内周の座標が存在し、一致する場合に限り補正を適用
            if has_interior_flag and interior_match_flag:

                # 内周の対象座標に対してHelmert変換を適用
                interior_corrected_x, interior_corrected_y = self.transform_coordinates_array_by_helmert(interior_coordinates_array, a_, b_, tx, ty, interior_start, interior_end)

                # 補正後のx, yをコピー配列に上書き
                interior_coordinates_array_copy[interior_start:interior_end + 1][:,1] = interior_corrected_x
                interior_coordinates_array_copy[interior_start:interior_end + 1][:,2] = interior_corrected_y


        return target_coordinates_array_copy, interior_coordinates_array_copy


    def convert_shapely_array_to_geodataframe(self, target_shapely_array, shapefile_crs):
        """
        Shapelyオブジェクト配列をGeoDataFrameに変換

        Parameters:
            target_shapely_array: Shapelyオブジェクト配列
            shapefile_crs: EPSGコード

        Returns:
            target_geodataframe: GeoDataFrameに変換されたジオメトリ
        """

        # 対象ジオメトリ → GeoDataFrame
        target_geodataframe = gpd.GeoDataFrame(
            geometry=target_shapely_array,
            crs=shapefile_crs
        )

        return target_geodataframe


    def get_min_max_xy(self, points):
        """
        指定された点群から最小・最大のx, y座標を取得

        Parameters:
            points: [id, x, y, z] の点配列

        Returns:
            min_x, min_y, max_x, max_y: 最小・最大のx, y
        """

        min_x = np.min(points[:, 1])
        max_x = np.max(points[:, 1])
        min_y = np.min(points[:, 2])
        max_y = np.max(points[:, 2])

        return min_x, min_y, max_x, max_y


    def get_bounding_box_polygons(self, target_array):
        """
        地物ごとの外接矩形ポリゴンを取得

        Parameters:
            target_array: [id, x, y, z] の座標配列

        Returns:
            bounding_box_array: 矩形ポリゴン（Shapely box）配列
            unique_ids_array: 地物IDの配列
        """

        # 地物ID（1列目）をユニークに抽出
        unique_ids_array = np.unique(target_array[:, 0])

        # 各地物ごとの座標配列の開始・終了インデックスを取得
        start_index_array, end_index_array = NCP.get_start_index_and_end_index(target_array)

        bounding_box_list = []

        for start, end in zip(start_index_array, end_index_array):

            # 現在処理中の地物の座標群を抽出
            points = target_array[start:end + 1]

            # 最小・最大のX, Yを取得（外接矩形のため）
            min_x, min_y, max_x, max_y = self.get_min_max_xy(points)

            # 補正対象ジオメトリを囲う矩形作成
            bounding_box_list.append(box(min_x, min_y, max_x, max_y))

        bounding_box_array = np.array(bounding_box_list)

        return bounding_box_array, unique_ids_array


    def check_geometry_inside_convex_hull(self, target_geometry, points):
        """
        点群から構成される凸包の内側にジオメトリが完全に含まれているかを判定

        Parameters:
            polygo: shapelyの対象ジオメトリ
            points: shapely Pointオブジェクトのリスト

        Returns:
            bool: True（完全内包） or False（はみ出し有）
        """

        # 点群が空ならFalse（何も包含できない）
        if not points:

            return False

        # 点群からMultiPointを生成し、そのconvex hull（凸包）を作成
        convex_hull = MultiPoint(points).convex_hull

        # ジオメトリが凸包の内部に完全に含まれているかを判定
        return convex_hull.contains(target_geometry)


    def spatial_join_geodataframe(self, left_geodataframe, right_geodataframe, predicate = "intersects", how = "left"):
        """
        2つのGeoDataFrameを空間結合

        Parameters:
            left_geodataframe : 結合の左側に使用するGeoDataFrame
            right_geodataframe : 結合の右側に使用するGeoDataFrame
            predicate : str, オプション
                結合に使用する空間的関係を指定。
                指定できるオプション例：
                - "intersects"（デフォルト）：ジオメトリが交差する場合に結合。
                - "contains"：左側のジオメトリが右側のジオメトリを包含する場合に結合。
                - "within"：左側のジオメトリが右側のジオメトリの中に含まれる場合に結合
            how : str, オプション
                結合に使用する結合方法を指定。指定できるオプション例：
                - "left"（デフォルト）：左側のGeoDataFrameの全ての行を保持し、右側のGeoDataFrameを結合
                - "inner"：空間的な関係が存在する行のみを保持
                - "right"：右側のGeoDataFrameの全ての行を保持し、左側を結合
                - "outer"：両方のGeoDataFrameの全ての行を保持

        Returns:
            joined_geodataframe: 空間的結合の結果を含むGeoDataFrame
        """

        joined_geodataframe = gpd.sjoin(left_geodataframe, right_geodataframe, predicate = predicate, how = how)

        return joined_geodataframe

    def filter_geometries_within_convex_hull(self, geodataframe, target_shapely_array, base_gcp_tin_array):
        """
        各ジオメトリが交差TINのconvex hull内にあるかを判定し、条件を満たすIDのみ抽出

        Parameters:
            geodataframe: targetとTINの空間結合結果
            target_shapely_array: 対象ポリゴンのshapely配列
            base_gcp_tin_array: TIN（三角形）座標配列

        Returns:
            dict: validなTarget_IDをキーに、対応するTIN_IDリストを値に持つ辞書
        """

        # 凸包内にあると判定されたジオメトリIDを格納するリスト
        valid_target_ids = []

        # Target_ID ごとにグループ化して処理
        grouped = geodataframe.groupby("Target_ID")

        for target_id, group in grouped:

            # 対象のshapelyジオメトリを取得
            target_geometry = target_shapely_array[target_id]

            # 交差しているGCP TINのIDを抽出（NaNを除き、int化してユニーク化）
            gcp_ids = group["Target_GCP_ID"].dropna().astype(int).unique()

            # GCP IDがない場合はスキップ
            if len(gcp_ids) == 0:
                continue

            # GCPに対応するTINの頂点座標（[X, Y]）をすべて取り出して平坦化
            tin_vertices = base_gcp_tin_array[gcp_ids][:, :, 1:3].reshape(-1, 2)

            # 各座標をshapelyのPointオブジェクトに変換
            points = [Point(xy) for xy in tin_vertices]

            # 対象ポリゴンが、TIN頂点の凸包内に完全に収まっているかを判定
            if self.check_geometry_inside_convex_hull(target_geometry, points):

                valid_target_ids.append(target_id)

        # 判定されたvalid_target_idsに該当する行のみ抽出（空間結合後のDataFrameから）
        joined_filtered = geodataframe[
            geodataframe["Target_ID"].isin(valid_target_ids)
        ]

        # 各Target_IDに対応するTIN IDリストを辞書として生成
        id_dict = joined_filtered.groupby("Target_ID")["Target_GCP_ID"].apply(
            lambda x: x.dropna().astype(int).tolist()
        ).to_dict()

        return id_dict

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # 以降は"has_interior_flag"で内周用処理を行うかを管理
            has_interior_flag = False

            # -----------------------------------------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # -----------------------------------------------------------------------------------------------------------
            target_dwh_name, \
                polygon_interior_dwh_name, \
                target_geometry_type, \
                target_gcp_dwh_name, \
                base_gcp_dwh_name, \
                output_target_dwh_name, \
                output_interior_dwh_name\
                = self.get_property(context, flowfile)

            # -----------------------------------------------------------------------------------------------------------
            #  flowfileをDataFrameに加工し、取得
            # -----------------------------------------------------------------------------------------------------------
            field_set_file_dataframe, \
                crs\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile)

            # -----------------------------------------------------------------------------------------------------------
            #  FieldSetFileで作成したdataframeから、プロパティで指定したDWH名のValue列を取得
            # -----------------------------------------------------------------------------------------------------------
            target_coordinates_array, \
                target_gcp_coordinates_array, \
                base_gcp_coordinates_array\
                = WM.calc_func_time(self.logger)(self.get_coordinates_array_from_field_set_file_dataframe)(field_set_file_dataframe,
                                                                                                           target_dwh_name,
                                                                                                           target_gcp_dwh_name,
                                                                                                           base_gcp_dwh_name)

            # -----------------------------------------------------------------------------------------------------------
            # 補正対象の座標配列をジオメトリ(shaplyオブジェクト)へ変換
            # -----------------------------------------------------------------------------------------------------------
            target_shapely_array\
                = NSP.get_attribute_coordinates_by_geometry_items(target_coordinates_array[:,:4], GEOMETRY_TYPE_MAP.get(target_geometry_type))

            if polygon_interior_dwh_name and isinstance(target_shapely_array[0], Polygon):

                # 内周用処理を行うかを管理
                has_interior_flag = True

                # -----------------------------------------------------------------------------------------------------------
                #  FieldSetFileで作成したdataframeから、プロパティで指定したDWH名のValue列(補正対象の内周座標配列)を取得
                # -----------------------------------------------------------------------------------------------------------
                interior_coordinates_array = NSP.get_value_from_field_set_file_dataframe(field_set_file_dataframe, polygon_interior_dwh_name)

            else:
                pass

            # -----------------------------------------------------------------------------------------------------------
            # GCP座標配列(N, 3 or 4) → ドロネー化された座標配列(N, 3, 3 or 4)
            # -----------------------------------------------------------------------------------------------------------
            target_gcp_tin_array, \
                base_gcp_tin_array\
                = NCP.create_TIN(target_gcp_coordinates_array, base_gcp_coordinates_array)

            # -----------------------------------------------------------------------------------------------------------
            # base_gcpの座標配列に終点(始点と同一座標)を追加し、ポリゴンジオメトリ(shaplyオブジェクト)へ変換
            # -----------------------------------------------------------------------------------------------------------
            base_gcp_shapely_array\
                = WM.calc_func_time(self.logger)(self.tin_array_to_polygons)(base_gcp_tin_array)

            # -----------------------------------------------------------------------------------------------------------
            # 補正対象ジオメトリを囲う矩形のユニークidと範囲を取得　bounding_box_array: [id, min_x, min_y, max_x, max_y]
            # -----------------------------------------------------------------------------------------------------------
            bounding_box_array, unique_ids_array\
                = WM.calc_func_time(self.logger)(self.get_bounding_box_polygons)(target_coordinates_array)

            # -----------------------------------------------------------------------------------------------------------
            # 対象TINのジオメトリ配列をGeoDataFrameに変換
            # -----------------------------------------------------------------------------------------------------------
            target_gcp_gdf\
                = WM.calc_func_time(self.logger)(self.convert_shapely_array_to_geodataframe)(base_gcp_shapely_array, crs)

            # -----------------------------------------------------------------------------------------------------------
            # 外接矩形のジオメトリ配列をGeoDataFrameに変換
            # -----------------------------------------------------------------------------------------------------------
            bbox_gdf\
                = WM.calc_func_time(self.logger)(self.convert_shapely_array_to_geodataframe)(bounding_box_array, crs)

            # -----------------------------------------------------------------------------------------------------------
            # 補正対象のジオメトリ配列をGeoDataFrameに変換
            # -----------------------------------------------------------------------------------------------------------
            target_gdf\
                = WM.calc_func_time(self.logger)(self.convert_shapely_array_to_geodataframe)(target_shapely_array, crs)

            # 各GeoDataFrameにid列追加
            target_gcp_gdf["Target_GCP_ID"] = np.arange(len(target_gcp_gdf))
            bbox_gdf["Bounding_Box_ID"] = unique_ids_array
            target_gdf["Target_ID"] = np.arange(len(target_gdf))

            # -----------------------------------------------------------------------------------------------------------
            # BoundingBoxのGeoDataFrameと対象TINのGeoDataFrameを空間結合
            # -----------------------------------------------------------------------------------------------------------
            joined_geodataframe\
                = WM.calc_func_time(self.logger)(self.spatial_join_geodataframe)(bbox_gdf, target_gcp_gdf)

            # 空間結合結果から NaN を除いた Target_GCP_ID を取得（重複除去）
            matched_target_gcp_ids = joined_geodataframe["Target_GCP_ID"].dropna().astype(int).unique()

            # 元の tin_gdf から一致する Target_GCP_ID を持つ行のみ抽出
            matched_tin_gdf = target_gcp_gdf[target_gcp_gdf["Target_GCP_ID"].isin(matched_target_gcp_ids)]

            # -----------------------------------------------------------------------------------------------------------
            # 補正対象のGeoDataFrameと対象TINのGeoDataFrameを空間結合
            # -----------------------------------------------------------------------------------------------------------
            joined_geodataframe = WM.calc_func_time(self.logger)(self.spatial_join_geodataframe)(target_gdf, matched_tin_gdf)

            # -----------------------------------------------------------------------------------------------------------
            # 補正対象が交差TINのconvex hull内にあるかを判定し、完全にTINに内包されたの補正対象IDのみ抽出
            # -----------------------------------------------------------------------------------------------------------
            id_dict\
                = WM.calc_func_time(self.logger)(self.filter_geometries_within_convex_hull)(joined_geodataframe,
                                                                                            target_shapely_array,
                                                                                            base_gcp_tin_array)

            # -----------------------------------------------------------------------------------------------------------
            # 補正対象座標配列の始点終点indexを取得
            # -----------------------------------------------------------------------------------------------------------
            target_start_index_array, \
                target_end_index_array\
                = WM.calc_func_time(self.logger)(NSP.get_start_index_and_end_index)(target_coordinates_array)

            # [(s0, e0), (s1, e1)...]形式に
            target_index_list = list(zip(target_start_index_array, target_end_index_array))

            if has_interior_flag:

            # -----------------------------------------------------------------------------------------------------------
            # 補正対象の内周座標配列の始点終点indexを取得
            # -----------------------------------------------------------------------------------------------------------
                interior_start_index_array, \
                    interior_end_index_array\
                    = WM.calc_func_time(self.logger)(NSP.get_start_index_and_end_index)(interior_coordinates_array)

                interior_index_list = list(zip(interior_start_index_array, interior_end_index_array))

            else:
                # 内周が存在しないケースに対応：未定義によるUnboundLocalErrorを回避するため初期化
                interior_index_list = []
                interior_coordinates_array = np.array([])

            # -----------------------------------------------------------------------------------------------------------
            # 外周・内周座標配列に対してTINに基づくヘルマート変換を適用
            # -----------------------------------------------------------------------------------------------------------
            output_target_coordinates_array, \
                output_interior_shapely_array\
                = WM.calc_func_time(self.logger)(self.apply_helmert_transform)(target_coordinates_array,
                                                                               interior_coordinates_array,
                                                                               target_gcp_tin_array,
                                                                               base_gcp_tin_array,
                                                                               target_index_list,
                                                                               interior_index_list,
                                                                               id_dict,
                                                                               unique_ids_array,
                                                                               has_interior_flag)

            # -----------------------------------------------------------------------------------------------------------
            # 出力用前処理
            # -----------------------------------------------------------------------------------------------------------
            # 出力タイプ（固定でジオメトリ）
            output_type = "geometry"

            if has_interior_flag:

                # 内周ありの場合は出力２行のため属性dictを空で初期化
                attributes_dict = {}

                # 出力タイプリストを外周・内周の2つで構成
                output_type_list = [output_type] * 2

                # 外周・内周の座標配列を出力リストとして準備
                output_value_list = [output_target_coordinates_array, output_interior_shapely_array]

                # プロパティでDWH名を入力した場合は上書き
                if output_target_dwh_name and output_interior_dwh_name:

                    # 指定されたDWH名を使用
                    output_dwh_name_list = [output_target_dwh_name, output_interior_dwh_name]

                else:

                    # 既存のDWH名を使用
                    output_dwh_name_list = [target_dwh_name, polygon_interior_dwh_name]

            else:

                # 出力タイプリストを外周のみで構成
                output_type_list = [output_type]

                # 外周の座標配列を出力リストとして準備
                output_value_list = [output_target_coordinates_array]

                # プロパティでDWH名を入力した場合は上書き
                if output_target_dwh_name:

                    # 指定されたDWH名を使用
                    output_dwh_name_list = [output_target_dwh_name]
                    # 属性にも設定
                    attributes_dict = {"DWH": output_target_dwh_name}

                else:

                    # 既存のDWH名を使用
                    output_dwh_name_list = [target_dwh_name]
                    # 属性にも設定
                    attributes_dict = {"DWH": target_dwh_name}

            # -----------------------------------------------------------------------------------------------------------
            # 更新した座標データを含むFieldSetFileを生成
            # -----------------------------------------------------------------------------------------------------------
            output_field_set_file\
                = WM.calc_func_time(self.logger)(PBP.set_field_set_file)(output_dwh_name_list, output_type_list, output_value_list)


            return FlowFileTransformResult(relationship="success",
                                           contents=output_field_set_file,
                                           attributes=attributes_dict)

        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
