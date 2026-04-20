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
# GeoDataFrameを受け取ってファイルに出力
# --------------------------------------------------------------------------------------------
# Python標準ライブラリ
import traceback
from importlib import import_module
import io

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.NifiSimplePackage as NSP
import nifiapi.NifiCustomPackage.DataDistributionConstant as DDC
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.NifiComplicationPackage as NCP

# NiFiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope

# 外部ライブラリの動的インポート
pd = import_module("pandas")
gpd = import_module("geopandas")
o3d = import_module("open3d")
np = import_module("numpy")


class Put3DModelFromGeoNdArray(FlowFileTransform):

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        GeoNdArrayを受け取り、Open3Dでファイルに出力する
                        ①input: GeoNdArrayを持った1行以上のFieldSetFile
                        ②output: 入力データのままのデータ（指定されたファイルに出力する）
                        """
        tags = ["Open3D", "GeoNdarray", "python"]

    # 入力データのDWH名
    INPUT_DWH_NAME = PropertyDescriptor(
        name="Input DWH Name",
        description="入力データのDWH名",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # Open3Dに設定するオプションCSV
    OUTPUT_OPTION_CSV = PropertyDescriptor(
        name="Output Option CSV",
        description="Open3Dに設定するオプションCSV",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        default_value="OPTION,VALUE",
        sensitive=False,
        required=True
    )

    # Open3Dで出力するファイルパス
    OUTPUT_FILE_PATH = PropertyDescriptor(
        name="Output File Path",
        description="Open3Dで出力するファイルパス",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        sensitive=False,
        required=True
    )

    # GeoNdarrayの変換する型を入力
    GEONDARRAY_TYPE = PropertyDescriptor(
        name="GeoNdarray Type",
        description="GeoNdarrayの変換する型を入力",
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        allowable_values=[DDC.POINT, DDC.LINESTRING, DDC.TRIANGLE_MESH, DDC.MULTIPATCH],
        sensitive=False,
        required=True
    )

    property_descriptors = [INPUT_DWH_NAME,
                            OUTPUT_OPTION_CSV,
                            OUTPUT_FILE_PATH,
                            GEONDARRAY_TYPE]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        プロパティで入力した値を取得する関数

        引数
            context: プロセッサの設定値が格納されているデータ
            flowfile: プロセッサに入ってくるデータ
        戻り値
            input_dwh_name: 入力データのDWH名
            output_option_csv: Open3Dに渡す出力用オプションCSV
            output_file_path: 出力ファイル名
            geondarray_type: GeoNdarrayを変換する型
        """
        input_dwh_name = context.getProperty(self.INPUT_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        output_option_csv = context.getProperty(
            self.OUTPUT_OPTION_CSV).evaluateAttributeExpressions(flowfile).getValue()

        output_option_stream = io.StringIO(output_option_csv)

        # quoting=3→クォーテーションはそのまま文字列として読み込み
        output_option_dataframe = pd.read_csv(output_option_stream, quoting=3)

        output_file_path = context.getProperty(
            self.OUTPUT_FILE_PATH).evaluateAttributeExpressions(flowfile).getValue()

        geondarray_type = context.getProperty(
            self.GEONDARRAY_TYPE).evaluateAttributeExpressions(flowfile).getValue()

        return input_dwh_name, output_option_dataframe, output_file_path, geondarray_type

    def get_flowfile(self, flowfile):
        """
        input_field_set_fileをデータフレームに加工する関数

        flowfile: プロセッサに入ってくるデータ

        field_set_file_data_frame: field_set_fileをデータフレームに加工した物
        """
        # flowfileから、csv形式のfield_set_fileを取得。
        input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

        # データフレームに加工
        field_set_file_data_frame = pd.read_csv(io.StringIO(input_field_set_file))

        return field_set_file_data_frame

    def convert_options_dataframe_to_dict(self, output_option_dataframe):
        """
        DataFrameを dict に変換しつつ、
        "True"/"False" を bool に変換
        """

        if output_option_dataframe is None:
            return {}

        if "OPTION" not in output_option_dataframe.columns:
            return {}

        if "VALUE" not in output_option_dataframe.columns:
            # VALUE列が無いなら空dict返す
            return {}

        options_dict = {}

        for option, value in zip(
            output_option_dataframe["OPTION"],
            output_option_dataframe["VALUE"]
        ):

            if isinstance(value, str):
                lower_val = value.lower()

                if lower_val == "true":
                    value = True
                elif lower_val == "false":
                    value = False

            options_dict[option] = value

        return options_dict


    def point_array_to_open3d_object(self, point_array):
        point_cloud = o3d.geometry.PointCloud()

        # IDを除いた全行をポイントとしてOpen3DObjectにする。
        point_cloud.points = o3d.utility.Vector3dVector(point_array[:, 1:4].astype(float))
        
        return point_cloud

    def line_array_to_open3d_object(self, line_array):

        si_array, ei_array = NCP.get_start_end_index_by_1d_array(line_array[:, 0])

        # 頂点は一括取得 
        vertices = line_array[:, 1:4].astype(np.float64)

        # lines をまとめて作る
        line_segments = []

        for si, ei in zip(si_array, ei_array):
            
            idx = np.arange(si, ei)
            seg = np.stack([idx, idx + 1], axis=1)
            line_segments.append(seg)

        if len(line_segments) > 0:
            lines = np.vstack(line_segments)
        else:
            lines = np.empty((0, 2))

        # ===== Open3Dオブジェクト作成 =====
        lineset = o3d.geometry.LineSet()
        lineset.points = o3d.utility.Vector3dVector(vertices)
        lineset.lines = o3d.utility.Vector2iVector(lines)

        return lineset

    def polygon_array_to_open3d_object(self, polygon_array):
        """
        ポリゴン配列をOpen3DのTriangleMeshに変換する。

        Parameters
        ----------
        polygon_array : np.ndarray
            shape = (N, 4), 列: [ID, X, Y, Z]

        Returns
        -------
        mesh : o3d.geometry.TriangleMesh
        """

        # ID列から各ポリゴンの開始・終了インデックスを取得（IDはソート済み前提）
        si_array, ei_array = NCP.get_start_end_index_by_1d_array(polygon_array[:, 0])

        # 頂点は一括取得
        vertices = polygon_array[:, 1:4].astype(np.float64)

        # 三角形リストを作成
        triangles = []

        # 各ポリゴンの先頭3点を三角形として追加
        vertex_offset = 0
        for si, ei in zip(si_array, ei_array):
            n_points = ei - si + 1

            # 最初の3点で三角形を作る
            triangles.append([vertex_offset, vertex_offset + 1, vertex_offset + 2])

            vertex_offset += n_points  # ポリゴンごとにオフセット更新

        # TriangleMesh作成
        mesh = o3d.geometry.TriangleMesh()
        mesh.vertices = o3d.utility.Vector3dVector(vertices)
        mesh.triangles = o3d.utility.Vector3iVector(np.array(triangles))
        mesh.compute_vertex_normals()

        return mesh

    def multi_patch_to_open3d_object(self, multi_patch_array):
        """
        MultiPatch配列をOpen3DのTriangleMeshに変換する。

        Parameters
        ----------
        multi_patch_array : np.ndarray
            shape = (N, M), 列: [ID, X, Y, Z, Nx, Ny, Nz, partID, ...]

        Returns
        -------
        mesh : o3d.geometry.TriangleMesh
        """

        # 頂点、三角形、頂点法線、三角形法線用リスト
        vertices_list = []
        triangles_list = []
        vertex_normals_list = []
        triangle_normals_list = []

        vertex_offset = 0

        # IDごとの地物ごとにループ
        si_array, ei_array = NCP.get_start_end_index_by_1d_array(multi_patch_array[:, 0])

        for si_geom, ei_geom in zip(si_array, ei_array):
            geom_array = multi_patch_array[si_geom:ei_geom+1]

            # partID列（7列目）から各TINを取得
            part_ids = np.unique(geom_array[:, 7])
            for part in part_ids:
                # partに属する点だけをスライス
                part_mask = geom_array[:, 7] == part
                part_points = geom_array[part_mask][:, 1:4].astype(np.float64)
                part_normals = geom_array[part_mask][:, 4:7].astype(np.float64)

                # 頂点の先頭3点だけを使用
                vertices_list.append(part_points[:3])
                vertex_normals_list.append(part_normals[:3])

                # 三角形作成（先頭3点）
                triangles_list.append([vertex_offset, vertex_offset + 1, vertex_offset + 2])

                # 三角形法線は頂点法線の先頭1つを使用
                triangle_normals_list.append(part_normals[0])

                vertex_offset += 3  # オフセット更新

        # NumPy配列化
        vertices = np.vstack(vertices_list) if vertices_list else np.empty((0, 3))
        triangles = np.array(triangles_list, dtype=np.int32) if triangles_list else np.empty((0, 3), dtype=np.int32)
        vertex_normals = np.vstack(vertex_normals_list) if vertex_normals_list else np.empty((0, 3))
        triangle_normals = np.vstack(triangle_normals_list) if triangle_normals_list else np.empty((0, 3))

        # TriangleMesh作成
        mesh = o3d.geometry.TriangleMesh()
        mesh.vertices = o3d.utility.Vector3dVector(vertices)
        mesh.triangles = o3d.utility.Vector3iVector(triangles)
        mesh.vertex_normals = o3d.utility.Vector3dVector(vertex_normals)
        mesh.triangle_normals = o3d.utility.Vector3dVector(triangle_normals)

        return mesh

    def array_to_open3d_object(self, geo_ndarray, geometry_type):
        """
        メソッド呼び出し関数
        配列 → Open3Dオブジェクトに変換する。

        入力配列の形式:
        1. 通常地物 (4列): id, x, y, z
        - Point / Line / Polygon
        2. マルチパッチ (8列): id, x, y, z, nx, ny, nz, part_id
        - part_idで分けて三角形Mesh化

        geometry_type: Point, LineString, 三角形メッシュ(法線無し), マルチパッチ
        """

        if geometry_type == DDC.POINT:

            # pointのGeoNdArrayをOpen3DObjectに変換
            open3d_object = self.point_array_to_open3d_object(geo_ndarray)

        # --------------------
        # LineSet
        # --------------------
        elif geometry_type == DDC.LINESTRING:
            # LineのGeoNdArrayをOpen3DObjectに変換
            open3d_object = self.line_array_to_open3d_object(geo_ndarray)

        # --------------------
        # TriangleMesh (Polygon)
        # --------------------
        elif geometry_type == DDC.TRIANGLE_MESH:

            # PolygonのGeoNdArrayをOpen3DObjectに変換
            open3d_object = self.polygon_array_to_open3d_object(geo_ndarray)

        # ----------------------------------------
        # 8列: マルチパッチ地物
        # ----------------------------------------
        elif geometry_type == DDC.MULTIPATCH:
            # マルチパッチをOpen3DObjectに変換
            open3d_object = self.multi_patch_to_open3d_object(geo_ndarray)

        else:
            raise Exception(f"geometry_type は {DDC.POINT} / {DDC.LINESTRING} / {DDC.TRIANGLE_MESH} /{DDC.MULTIPATCH} のいずれかを指定してください")

        return open3d_object

    def put_open3d_object(self, open3d_object, geometry_type, output_path, options_dict):
        """
        メソッド呼び出し関数
        Open3Dオブジェクトを、型にあったメソッドで出力する。

        geometry_type: Point, LineString, 三角形メッシュ(法線無し), マルチパッチ
        output_path: 出力ファイルパス
        options_dict: Open3Dに渡す出力用オプションDict
        """

        if geometry_type == DDC.POINT:

            output_open3d_method = o3d.io.write_point_cloud

        elif geometry_type == DDC.LINESTRING:
            output_open3d_method = o3d.io.write_line_set

        elif geometry_type == DDC.TRIANGLE_MESH or geometry_type == DDC.MULTIPATCH:
            output_open3d_method = o3d.io.write_triangle_mesh

        else:
            raise Exception(f"geometry_type は {DDC.POINT} / {DDC.LINESTRING} / {DDC.TRIANGLE_MESH} /{DDC.MULTIPATCH} のいずれかを指定してください")

        # STL / PLY / OBJ などで書き出す
        result = \
            output_open3d_method(
                output_path,
                open3d_object,
                **options_dict
            )

        return result

    def transform(self, context, flowfile):

        try:

            # ---------------------------------------------------------------------------
            # プロパティで入力した値を取得
            # ---------------------------------------------------------------------------
            # GeoDataframeに設定するオプションCSV

            # 地物IDのカラム名とデータ定義の区切り文字を取得する
            input_dwh_name, \
                output_option_dataframe, \
                output_file_path, \
                geondarray_type \
                = WM.calc_func_time(self.logger)(self.get_property)(context, flowfile)
            # ---------------------------------------------------------------------------

            # ---------------------------------------------------------------------------
            # プロパティで入力したファイルパスが正しいか検証
            # ---------------------------------------------------------------------------
            WM.calc_func_time(self.logger)(NSP.validate_output_path)(output_file_path)
            # ---------------------------------------------------------------------------

            # ---------------------------------------------------------------------------
            # output_option_csvを出力用のdictに変換
            # ---------------------------------------------------------------------------
            options_dict = self.convert_options_dataframe_to_dict(output_option_dataframe)
            # ---------------------------------------------------------------------------

            # ---------------------------------------------------------------------------
            # flowfileからFieldSetFile取得し、GeoNdarrayを取得
            # ---------------------------------------------------------------------------
            field_set_file_data_frame = self.get_flowfile(flowfile)

            geo_ndarray = NSP.get_value_from_field_set_file_dataframe(
                field_set_file_data_frame, input_dwh_name
            )
            # ---------------------------------------------------------------------------

            # ---------------------------------------------------------------------------
            # 指定したファイルに出力
            # ---------------------------------------------------------------------------
            open3d_object = self.array_to_open3d_object(
                geo_ndarray, geondarray_type
            )

            result = self.put_open3d_object(
                open3d_object,
                geondarray_type,
                output_file_path,
                options_dict
            )

            # 出力に失敗しても落ちない為、エラーを追加。
            if result:
                pass
            else:
                self.logger.error(f"出力に失敗しました。出力ファイルパス、又はGeoNdarrayを確認してください。")
                return FlowFileTransformResult(relationship="failure")

            # ---------------------------------------------------------------------------
            
            # 結果を返す
            return FlowFileTransformResult(relationship="success")

        except Exception as e:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
