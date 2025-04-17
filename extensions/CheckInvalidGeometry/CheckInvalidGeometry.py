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
#  ジオメトリオブジェクトが幾何学的に有効であるかの判断とgeometry列の値がnullのレコード削除を行う
# --------------------------------------------------------------------------------------------

# Python標準ライブラリ
import pickle
import traceback

from importlib import import_module

# NiFi自作ライブラリ
import nifiapi.NifiCustomPackage.WrapperModule as WM
import nifiapi.NifiCustomPackage.ProcessorBridgePackage as PBP

# Nifiライブラリ
from nifiapi.properties import PropertyDescriptor, ExpressionLanguageScope, StandardValidators
from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

# 外部ライブラリの動的インポート
np = import_module("numpy")
explain_validity = import_module("shapely.validation").explain_validity
Point = getattr(import_module("shapely.geometry"), "Point")
LineString = import_module("shapely").geometry.LineString
Polygon = getattr(import_module("shapely.geometry"), "Polygon")
MultiPolygon = import_module("shapely").geometry.MultiPolygon


class CheckInvalidGeometry(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.0.0'
        description = '''
                        不正なジオメトリと、Nullジオメトリをチェック及び、削除する。
                        ①input: GeoDataFrameかGeoDataFrameを持ったFieldSetFile。
                        ②output: 入力データと同じ形式のデータ。
                      '''
        tags = ['GeoDataFrame', 'Python']

    # LineStringの頂点間距離を求める際の許容距離
    PERMISSION_DISTANCE = PropertyDescriptor(
        name="Permission Distance",
        description="1ジオメトリ内の頂点間距離を求める際の許容距離",
        default_value=0.0,
        validators=[StandardValidators.NUMBER_VALIDATOR],
        sensitive=False,
        expression_language_scope=ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        required=True,
    )


    property_descriptors = [PERMISSION_DISTANCE]

    def __init__(self, **kwargs):
        super().__init__()

    def getPropertyDescriptors(self):
        return self.property_descriptors

    def check_point_distances(self, coords, permission_distance):
        """
        頂点間の距離のチェック

        :param coords: 対象ジオメトリ
        :type coords: numpy.array
        :param permission_distance: 指定距離
        :type permission_distance: float

        :return: 指定した距離以下の線分が1つでもあればFalse、
                 すべて指定した距離より大きければTrue

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:
            for i in range(len(coords) - 1):
                # ジオメトリのポイント同士の距離が対象距離以下かどうか
                distance = np.linalg.norm(coords[i] - coords[i + 1])

                if (0 <= distance <= permission_distance):
                    return False
            return True
        except Exception as e:
            raise Exception(f"[check_point_distances_Exception]:{e}")

    def get_coordinates_array_from_shapely(self, geom):
        """
        shapelyオブジェクトからxyまたはxyzの座標配列を取得

        :param geometry: 対象ジオメトリ
        :type geometry: shapely.geometry.Polygon or
                        shapely.geometry.LineString or 
                        shapely.geometry.Point

        :return: 対象ジオメトリ
        :rtype: numpy.array

        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """

        try:
            # Polygonの場合は外郭のみ取得
            if isinstance(geom, Polygon):
                coordinates_array=np.array(geom.exterior.coords)
            else:
                coordinates_array=np.array(geom.coords)
            return coordinates_array
        except Exception as e:
            raise Exception(f"[get_coordinates_array_from_shapely]:{e}")


    def get_segments_bool_string(self,geom,permission_distance):
        """
        構成点間の距離と線分同士に関数チェックをおこなう
        チェック結果とメッセージを返す

        :param geom: 対象ジオメトリ
        :type geom: shapely.geometry.Polygon or
                        shapely.geometry.LineString or
                        shapely.geometry.Point
        :param permission_distance: 指定距離
        :type permission_distance: float

        :return: 検証結果判定
        :rtype: bool
        :return: 検証結果理由
        :rtype: string
        :raises Exception: 処理中にエラーが発生した場合に例外をスローする。
        """
        try:

            # 座標配列取得
            coords=self.get_coordinates_array_from_shapely(geom)

            # LineStringの場合
            if isinstance(geom, LineString):

                # 構成点間距離のチェックのみ実施
                valid_bool=self.check_point_distances(coords,permission_distance)

                if valid_bool:
                    valid_reason_string='Valid Geometry'
                else:
                    valid_reason_string='構成点間の距離が近すぎます。'

            # Polygonの場合
            elif isinstance(geom, Polygon):

                # 構成点間距離と自己交差判定結果をチェック
                segments_bool=self.check_point_distances(coords,permission_distance)

                if segments_bool:
                    segments_reason = ''
                else:
                    segments_reason = '構成点間の距離が近すぎます。'

                # is_validの結果をその理由取得
                # is_validは2Dの時だけ
                if len(coords[0])==2:
                    is_valid_bool = geom.is_valid
                    if is_valid_bool:
                        is_valid_reason = ''
                    else:
                        is_valid_reason = explain_validity(geom)
                else:
                    is_valid_bool=True
                    is_valid_reason = ''

                # どちらの判定もTrueならOK
                if segments_bool & is_valid_bool:
                    valid_bool=True
                    valid_reason_string='Valid Geometry'

                else:
                    valid_bool=False
                    valid_reason_string=segments_reason + is_valid_reason

            # それ以外の場合（ポイントなので線分の要素はない）
            else:
                valid_bool=True
                valid_reason_string='Valid Geometry'
            return valid_bool, valid_reason_string
        except Exception as e:
            raise Exception(f"[get_segments_bool_string]:{e}")


    def validate_geometries(self, geodataframe, permission_distance):
        """
        概要:
            GeoDataFrame内のジオメトリが幾何学的に有効であるかの判断を行う関数

        引数:
            geodataframe: チェック対象のジオメトリを含むGeoDataFrame
            permission_distance: 指定距離

        戻り値:
            invalid_geometries: GeoDataFrameの各行に対する検証結果を格納したGeoDataFrame
        """

        # 検証結果と理由を入れるリストをそれぞれ用意
        is_valid_list = []
        validity_reason_list = []

        # すべてのジオメトリに対してチェック
        for geom in geodataframe['geometry']:

            # 座標をnumpy配列へ変換
            coordinates_array=self.get_coordinates_array_from_shapely(geom)

            # 少なくとも1つは不正な座標（nan,inf）を持つジオメトリの判定
            # 1つでもあればNG 理由を明記し次のジオメトリへ
            # なければ続行
            if np.any(np.isnan(coordinates_array)):
                is_valid_list.append(False)
                validity_reason_list.append('不正な座標が含まれています（nan）')
                continue
            else:
                pass

            if np.any(np.isinf(coordinates_array)):
                is_valid_list.append(False)
                validity_reason_list.append('不正な座標が含まれています（inf）')
                continue
            else:
                pass

            # nan,infがないことがわかったら構成点間の距離+自己交差の判定へで
            validity_bool,\
            reason=self.get_segments_bool_string(geom,permission_distance)

            is_valid_list.append(validity_bool)
            validity_reason_list.append(reason)

        # GeoDataFrame内にis_valid列とvalidity_reason列を追加
        geodataframe["is_valid"] = is_valid_list
        geodataframe["validity_reason"] = validity_reason_list

        # 無効なレコードだけ出力
        invalid_geometries = geodataframe[~geodataframe['is_valid']]

        return invalid_geometries

    def filter_null_geometry(self, geodataframe):
        """
        概要:
            geometry 列の値が null でないレコードをフィルタリングする関数

        引数:
            geodataframe: バイトデータから還元された geodataframe

        戻り値:
            geodataframe: CRSをターゲットEPSGに変換したもの
            null_geometry_count: 削除されるレコード数
        """

        # 削除されるレコードを取得
        remove_bool_series=geodataframe.geometry.isnull() | geodataframe.geometry.is_empty
        removed_records = geodataframe[remove_bool_series]

        # 削除されるレコード数
        null_geometry_count = len(removed_records)

        # geometry 列の値が null でないレコードをフィルタリング
        geodataframe = geodataframe[~remove_bool_series]

        return geodataframe, null_geometry_count

    def serialize_geodataframe(self, geodataframe):
        """
        概要:
            GeoDataFrameをpickle形式でシリアライズする関数

        引数:
            geodataframe: pickle形式でシリアライズするGeoDataFrameオブジェクト

        戻り値:
            serialize_dataframe: pickle形式でシリアライズしたバイトデータ
        """

        # GeoDataFrameをpickle形式でシリアライズ
        serialize_dataframe = pickle.dumps(geodataframe)

        return serialize_dataframe

    # ---------------------------------------------------------------------------------------------------
    # メイン処理
    # ---------------------------------------------------------------------------------------------------
    def transform(self, context, flowfile):
        try:

            # flowfileからシリアライズされたGeoDataFrameのバイトデータを取得し、バイトデータからGeoDataFrameを復元
            geodataframe = WM.calc_func_time(self.logger)(
                PBP.get_value_from_field_Set_file_or_serialized)(flowfile)


            # 許容される距離をプロパティから取得
            permission_distance = context.getProperty(
                self.PERMISSION_DISTANCE).evaluateAttributeExpressions(flowfile).getValue()
            permission_distance = float(permission_distance)

            # -----------------------------------------------------------------------------------------------

            # input時の地物数
            input_geodataframe_count = len(geodataframe)

            # geometry 列の値が null でないレコードをフィルタリング
            geodataframe, \
                null_geometry_count = WM.calc_func_time(self.logger)(
                    self.filter_null_geometry)(geodataframe)

            # ジオメトリの有効性をチェック
            invalid_geometries = WM.calc_func_time(self.logger)(
                self.validate_geometries)(geodataframe, permission_distance)

            # エラージオメトリ数を出力
            error_count = 0

            # 無効なジオメトリが存在する場合の処理
            if not invalid_geometries.empty:
                for index, row in invalid_geometries.iterrows():

                    # エラー詳細をログ出力
                    error_details = (
                        f"インデックス: {index}, ジオメトリ: {row['geometry']}, 理由: {row['validity_reason']}")
                    self.logger.warn(f'InvalidGeometry: {error_details}')

                error_count = (~geodataframe['is_valid']).sum()

                # 無効ジオメトリを削除
                geodataframe = geodataframe[geodataframe['is_valid']]

            output_geodataframe_count = len(geodataframe)

            # 各地物数をログに出力
            self.logger.info("\r\n".join([
                "",
                f"インプット時の地物数:{input_geodataframe_count}",
                f"アウトプット時の地物数:{output_geodataframe_count}",
                f"nullジオメトリの地物数:{null_geometry_count}",
                f"不正なジオメトリの地物数:{error_count}"
            ]))

            # GeoDataFrame の中に is_valid列 と validity_reason列 があった場合は削除する。
            geodataframe = geodataframe.drop(columns=[col for col in ['is_valid', 'validity_reason'] if col in geodataframe.columns])

            # GeoDataFrame を pickle 形式でシリアライズ
            serialize_dataframe = WM.calc_func_time(self.logger)(
                self.serialize_geodataframe)(geodataframe)

            # 成功フローに送る
            return FlowFileTransformResult(relationship='success', contents=serialize_dataframe)
        except Exception:
            self.logger.error(traceback.format_exc())
            return FlowFileTransformResult(relationship='failure')
