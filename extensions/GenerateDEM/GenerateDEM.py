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
# LASファイルから、FieldSetFileを生成するためのプロセッサ。
# LASファイルを受け取り、データ定義を取得し、フィールドの情報を処理している。
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
from io import StringIO, BytesIO
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.LasPackage as LP

# Nifiライブラリ
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators

# 外部ライブラリの動的インポート
np = import_module("numpy")
laspy = import_module("laspy")
pd = import_module("pandas")
Image = import_module("PIL.Image")
griddata = import_module("scipy.interpolate").griddata
ndimage = import_module("scipy").ndimage


class GenerateDEM(FlowFileTransform):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileTransform"]

    class ProcessorDetails:
        version = "1.0.0"
        description = """
                        LASを入荷したFieldSetFile or LASファイルからDEMを作成する
                        ①input: Bytes形式のLasもしくは、ポイントの座標配列とclassificationをもった2行以上のFieldSetFile
                        ②output: Bytes形式のDEM(tiffファイル)
                      """

        tags = ["LAS", "DEM", "Image", "tiff", "Python"]

    # 点群座標配列のDWH名
    POINT_ARRAY_DWH_NAME = PropertyDescriptor(
        name = "Point Array DWH Name",
        description = "点群座標配列のDWH名",
        sensitive = False,
        required = False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 地表分類のDWH名
    CLASSIFICATION_DWH_NAME = PropertyDescriptor(
        name = "Classification DWH Name",
        description = "classification情報のDWH名",
        sensitive = False,
        required = False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 立方体の一辺(0.05=5cm)
    CUBE_EDGE_LENGTH = PropertyDescriptor(
        name = "Cube Edge Length",
        description = "立方体の一辺(0.05=5cm)の長さ",
        sensitive = False,
        required = True,
        validators = [StandardValidators.NUMBER_VALIDATOR],
        expression_language_scope=ExpressionLanguageScope.NONE
    )

    # 地面として判別する分類コード
    LAS_CLASSIFICATION_CODES = PropertyDescriptor(
        name = "Las Classification Codes",
        description='''
                        地面として判別する分類コード。
                        点群の classification 属性の中で、このコードに該当する点を「地面」とみなします。
                        複数コードを指定する場合はカンマ区切りで渡してください。（例: 2 や 2, 11）
                    ''',
        default_value = 2,
        sensitive = False,
        required = True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    # 出力ファイル名を指定
    OUTPUT_FILE_NAME = PropertyDescriptor(
        name = "Output File Name",
        description = "出力時のファイル名を入力 (拡張子は除く)",
        sensitive = False,
        required = True,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
    )

    property_descriptors = [POINT_ARRAY_DWH_NAME,
                            CLASSIFICATION_DWH_NAME,
                            CUBE_EDGE_LENGTH,
                            OUTPUT_FILE_NAME,
                            LAS_CLASSIFICATION_CODES]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def get_property(self, context, flowfile):
        """
        概要
            プロパティで設定した値を取得

        引数
            context: プロセッサの設定値
            flowfile: プロセッサに入るデータ

        戻り値
            point_array_dwh_name: 点群座標配列のDWH名
            classification_dwh_name: 地表分類のDWH名
            cube_edge_length: 立方体の一辺の長さ
            output_file_name: 出力ファイル名

        """

        # 点群座標配列のDWH名を取得
        point_array_dwh_name = context.getProperty(
            self.POINT_ARRAY_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 地表分類のDWH名を取得
        classification_dwh_name\
            = context.getProperty(self.CLASSIFICATION_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 立方体の一辺の長さを取得
        cube_edge_length\
            = float(context.getProperty(self.CUBE_EDGE_LENGTH).getValue())

        # 地表分類のDWH名を取得
        classification_dwh_name\
            = context.getProperty(self.CLASSIFICATION_DWH_NAME).evaluateAttributeExpressions(flowfile).getValue()

        # 出力ファイル名を取得
        output_file_name\
            = context.getProperty(self.OUTPUT_FILE_NAME).evaluateAttributeExpressions(flowfile).getValue()

        las_classification_codes = context.getProperty(
            self.LAS_CLASSIFICATION_CODES).evaluateAttributeExpressions(flowfile).getValue()

        return point_array_dwh_name, classification_dwh_name, cube_edge_length, output_file_name, las_classification_codes

    def get_flowfile(self, flowfile):
        """
        概要
            flowfileがFieldSetFileの場合、DataFrameに加工、FieldSetFileではない場合はバイトデータを取得する。

        引数:
            flowfile: プロセッサに入ってくるデータ

        戻り値:
            target_value: FieldSetDataFrame or LASファイルのバイトデータ
            bytes_flag: inputの型で後続処理が違う為、型を識別した時のフラグ
        """

        # flowfileがLASファイルのバイトデータの場合とFieldSetFileの場合では、後続の処理が違う為
        # フラグ管理をしておく。
        bytes_flag = False

        try:

            # flowfileから、CSV形式のFieldSetFileを取得
            input_field_set_file = flowfile.getContentsAsBytes().decode("utf-8")

            # StringIOオブジェクトとしてCSV形式のデータをDataFrameとして読み込む
            target_value = pd.read_csv(StringIO(input_field_set_file))

        except UnicodeDecodeError:

            # flowfileからLASファイルのバイトデータを取得
            target_value = flowfile.getContentsAsBytes()

            # バイトデータのためフラグはON
            bytes_flag = True

        return target_value, bytes_flag

    def get_coordinates_array_and_classification_from_field_set_file_dataframe(self,
                                                                               field_set_dataframe,
                                                                               point_array_dwh_name,
                                                                               classification_dwh_name):
        """
        概要
            field_set_dataframeから、プロパティで指定したDWH名の持つ行のValue列をデコード、デシリアライズし取得

        引数:
            field_set_dataframe: FieldSetFileをDataFrameに加工したデータ
            point_array_dwh_name: プロパティで指定した点群座標配列のDWH名
            classification_dwh_name: プロパティで指定した地表分類のDWH名

        戻り値:
            coordinates_array: 点群座標配列
            classification_list: 地表分類を持ったlist
        """

        # 点群座標配列を field_set_dataframeから取得
        coordinates_array = LP.get_value_from_field_set_file_dataframe(field_set_dataframe, point_array_dwh_name)

        # 地表分類を持ったlistを field_set_dataframeから取得
        classification_list = LP.get_value_from_field_set_file_dataframe(field_set_dataframe, classification_dwh_name)


        return coordinates_array, classification_list

    def extract_interpolate_result(self, result_array, expansion_index):
        """
        概要
            内挿補間結果を抽出し、Intensityの値が0のメッシュは透過させる
        
        引数
            result_array: 内挿補間された配列
            expansion_index: 膨張した後の存在インデックス
        
        戻り値
            interpolated_result_array: 透過処理された画像配列
        """
        # 内挿補間結果を抽出する。
        # 値が存在する場合は元の値、存在しない場合は0となる
        interpolated_result_array = result_array * expansion_index

        # Intensityの値が0のメッシュは透過させるために0のメッシュにnanを設定する。
        interpolated_result_array[(interpolated_result_array == 0)] = np.nan

        return interpolated_result_array

    def interpolate_mesh_array(self, structured_data_array, pil_array, grid_data_mesh_list, grid_data_value_list):
        """
        概要
            scipy.interpolate.griddataで、内挿補間を行う
        
        引数
            structured_data_array: classification付き構造化配列
            pil_array: 画像配列
            grid_data_mesh_list: メッシュIDが格納されたlist
            grid_data_value_list: メッシュに設定したZ値が格納されたlist
        
        戻り値
            result_array.astype(np.float32): 内挿補完後の配列
        """
        # 内挿補間処理 scipy.interpolate.griddataを用いる
        # 構造化配列のx_mesh_idの最大値を取得
        x_mesh_max = int(LP.extract_max_value_from_structured_data_array(structured_data_array, "x_mesh_id", np.int64))

        # 構造化配列のy_mesh_idの最大値を取得
        y_mesh_max = int(LP.extract_max_value_from_structured_data_array(structured_data_array, "y_mesh_id", np.int64))

        # 2D メッシュグリッドを生成
        xx, yy = np.meshgrid(np.arange(x_mesh_max + 1), np.arange(y_mesh_max + 1))

        grid_data_mesh_array = np.array(grid_data_mesh_list)
        grid_data_value_array = np.array(grid_data_value_list)

        # メッシュIDのxかyがすべて同じ値なら内挿補間しない
        # 内挿補間は2次元配列でなければならない1列または1行の場合1次元配列とみなしエラーとなる
        result_array = griddata(points=grid_data_mesh_array, values=grid_data_value_array, xi=(xx, yy), method='linear')

        # 2次元の場合は2
        rank = 2

        # 4近傍（上下左右）の場合は1
        connectivity = 1

        # 膨張の仕様を取得
        expansion_struct = ndimage.generate_binary_structure(rank, connectivity)

        # Intensityが登録されたgridのnumpyを膨張させる
        # 取得される値は膨張した後の存在インデックス
        # 値が存在するメッシュの上下左右を出力対象とする　これを2回繰り返す
        expansion_index = ndimage.binary_dilation(pil_array, structure=expansion_struct,iterations=2).astype(pil_array.dtype)

        # 内挿補間結果を抽出する
        # 値が存在する場合は元の値、存在しない場合は0となる
        # Intensityの値が0のメッシュは透過させるために0のメッシュにnanを設定する
        result_array = self.extract_interpolate_result(result_array, expansion_index)
        
        return result_array.astype(np.float32)

    def convert_array_for_pil(self, structured_data_array, mesh_id_indices_array, mesh_id_sorted_xyz_array, las_classification_codes):
        """
        概要
            numpy配列を、画像配列に変換

        引数
            structured_data_array: classification付き構造化配列
            mesh_id_indices_array: メッシュIDの情報が入った配列[ユニークID, インデックス, 件数]
            mesh_id_sorted_xyz_array: メッシュIDで並び替えられた、XYZ座標配列を持つ二次元配列
            las_classification_codes: 地面とみなすclassificationのコード 例)2
        
        戻り値
            interpolated_pil_dem_array: 内挿補間された画像配列
        """

        grid_data_mesh_list = []
        grid_data_value_list = []

        las_classification_codes = [int(num.strip()) for num in las_classification_codes.split(',')]

        y_mesh_max = LP.extract_max_value_from_structured_data_array(structured_data_array, "y_mesh_id", np.int64)
        x_mesh_max = LP.extract_max_value_from_structured_data_array(structured_data_array, "x_mesh_id", np.int64)

        # PIL引数用配列初期化
        pil_ground_height_array = np.zeros((int(y_mesh_max) + 1,
                                            int(x_mesh_max) + 1),dtype='float32')

        # メッシュ内の最小値最大値格納用配列初期化
        for i ,(mesh_id, mesh_id_index, mesh_id_count) in enumerate(zip(mesh_id_indices_array[0], mesh_id_indices_array[1], mesh_id_indices_array[2])):

            z_value_temp_array, \
                classification_temp_array\
                = LP.extract_mesh_data(mesh_id_sorted_xyz_array, structured_data_array, mesh_id_index, mesh_id_count)

            mesh_z_value\
                = LP.get_min_z(classification_temp_array, las_classification_codes, z_value_temp_array)

            # メッシュIDを'@'で分割してxyのメッシュIDをそれぞれ取得
            mesh_list = mesh_id.split('@')

            # Z値をメッシュに設定する
            pil_ground_height_array[int(mesh_list[1])][int(mesh_list[0])] = mesh_z_value

            # メッシュIDをlistに格納→内挿補間で使用する
            grid_data_mesh_list.append([int(mesh_list[0]),int(mesh_list[1])])

            # メッシュに設定したZ値をlistに格納→内挿補間で使用する
            grid_data_value_list.append(mesh_z_value)

        # 内挿補間を行う
        interpolated_pil_dem_array =\
            self.interpolate_mesh_array(structured_data_array,
                                        pil_ground_height_array,
                                        grid_data_mesh_list,
                                        grid_data_value_list)

        return interpolated_pil_dem_array

    def verify_property_entered(self, verification_dwh_list, verification_property_list):
        """
        概要
            プロパティが入力されているか検証 入力されていない場合は、エラーを返す

        引数
            verification_dwh_list: プロパティで入力されたDWHが格納されたlist
            verification_property_list: プロパティ名が格納されたlist

        戻り値
            error_flag_list:プロパティが入力されているかの処理結果が格納されたlist
        """

        # 検証処理結果を格納するlistを定義
        error_flag_list = []

        # プロパティで入力したDWHをひとつづつ検証
        for i, dwh in enumerate(verification_dwh_list):

            # 入力されていた場合は listにTrueを格納
            if dwh:
                error_flag_list.append(True)

            # 入力されていなかった場合は、エラーを出力し、listにFalseを格納
            else:
                self.logger.error(f"inputがFieldSetFileです。プロパティ「{verification_property_list[i]}」を入力してください。")
                error_flag_list.append(False)

        return error_flag_list

    def verify_dwh_in_field_set_data_frame(self, field_set_data_frame, verification_dwh_list):
        """
        概要
            プロパティで入力されたDWH名がFieldSetFile内に存在するのか検証

        引数
            field_set_data_frame: データフレームに変換されたFieldSetFile
            verification_dwh_list: プロパティで入力されたDWH名が格納されたlist
        
        戻り値
            result(bool型): bool型の検証結果 True:問題なし False:問題あり
        """
        field_set_dwh_list = set(field_set_data_frame["DWH"])

        missing_dwh_names = [dwh_name for dwh_name in verification_dwh_list if dwh_name not in field_set_dwh_list]

        # 検証結果のboolを定義
        result = True

        # missing_dwh_namesが存在する場合エラーを返す
        if missing_dwh_names:
            self.logger.error(f"入力されたDWH名がFieldSetFile内に存在しません。 存在しないDWH名:{missing_dwh_names}")

            # 検証結果を更新
            result = False

            # 検証結果のboolを返す
            return result
        
        # missing_dwh_namesが空の場合は、問題ない
        else:

            # 検証結果を返す
            return result

    # ---------------------------------------------------------------------------------------------------
    # ★メイン処理★
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # プロパティで入力した値を取得
            point_array_dwh_name, \
                classification_dwh_name, \
                cube_edge_length, \
                output_file_name ,\
                las_classification_codes = \
                self.get_property(context, flowfile)

            # flowfileを解析し、FieldSetFile or lasファイルを取得
            target_value, \
                bytes_flag\
                = WM.calc_func_time(self.logger)(self.get_flowfile)(flowfile)

            # inputデータがbytes形式だった場合
            if bytes_flag:

                # bytes形式のLASを読み込み、laspyのオブジェクトに変換
                las_object\
                    = WM.calc_func_time(self.logger)(LP.read_las)(target_value)

                # LASオブジェクトから座標配列に変換
                coordinates_array\
                    = WM.calc_func_time(self.logger)(LP.get_coordinates_from_las_object)(las_object)

                # classificationを取得
                index_array = coordinates_array[:, 0]

                classification_list = list(zip(index_array.copy(), las_object["classification"]))

            # inputデータが文字列型だった場合
            else:

                # プロパティで入力した値と、プロパティ名を検証する為に、listに格納
                verification_dwh_list = [point_array_dwh_name, classification_dwh_name]
                verification_property_list = ["Point Array DWH Name", "Classification DWH Name"]

                # プロパティが入力されているか検証　入力されていない場合は、エラーを返す
                error_flag_list = self.verify_property_entered(verification_dwh_list,
                                                               verification_property_list)

                # どちらかが入力されていない場合は強制終了する。
                if False in error_flag_list:
                    return FlowFileTransformResult(relationship="failure")
                else:
                    pass

                # プロパティで入力されたDWH名がFieldSetFileのDWH名に存在しない場合、エラーを返す
                result_bool = self.verify_dwh_in_field_set_data_frame(target_value, verification_dwh_list)
                
                # FieldSetFileの中にプロパティで入力されたDWH名が存在する為、処理続行
                if result_bool:
                    pass

                # FieldSetFileの中にプロパティで入力されたDWH名が存在しない為、処理中止
                else:
                    return FlowFileTransformResult(relationship="failure")

                # 座標配列とclassificationのlistをFieldSetFileから取得
                coordinates_array, \
                    classification_list\
                    = WM.calc_func_time(self.logger)(self.get_coordinates_array_and_classification_from_field_set_file_dataframe)(target_value,
                                                                                                                                  point_array_dwh_name,
                                                                                                                                  classification_dwh_name)

            # LAS内のx座標最小値 LAS内のy座標最大値 取得
            xyz_array, \
                las_min_x, \
                las_max_y\
                = WM.calc_func_time(self.logger)(LP.get_min_x_max_y)(coordinates_array)

            # numpyからメッシュIDごとに並び替えクラス変数に格納する処理→属性指定でデータを取り出す処理使用
            structured_data_array, \
                mesh_id_indices_array, \
                mesh_id_sorted_xyz_array=\
                LP.get_mesh_array(xyz_array.copy(),
                                  las_min_x.copy(),
                                  las_max_y.copy(),
                                  classification_list,
                                  cube_edge_length)

            # numpy配列をpil用に変換する処理
            interpolated_pil_dem_array =\
                self.convert_array_for_pil(structured_data_array,
                                           mesh_id_indices_array,
                                           mesh_id_sorted_xyz_array.copy(),
                                           las_classification_codes)

            # 配列を画像にする
            tiff_bytes = \
                LP.convert_pil_array_to_tiff_bytes(interpolated_pil_dem_array.copy())

            # 出力されるTIFFに対応するワールドファイルの要素をattributeにまとめる
            output_attributes_dict\
                = WM.calc_func_time(self.logger)(LP.generate_tfw_dict)(las_min_x, 
                                                                       las_max_y,
                                                                       cube_edge_length
                                                                       )

            # 出力ファイル名をattributeにまとめる
            output_attributes_dict["filename"] = output_file_name + ".tiff"

            # 成功リレーションと共に処理結果を返す
            return FlowFileTransformResult(relationship="success",
                                           contents=tiff_bytes,
                                           attributes=output_attributes_dict
                                           )

        # エラーが発生した場合、失敗リレーションを返す
        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship="failure")
