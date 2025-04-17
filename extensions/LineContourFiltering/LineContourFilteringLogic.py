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

# Python標準ライブラリ
import pickle
from decimal import Decimal
from importlib import import_module

# 外部ライブラリの動的インポート
cv2 = import_module("cv2")
np = import_module("numpy")
pandas = import_module("pandas")

from raster_to_vector.common.base_raster_vector_logic import FlexibleRasterVectorLogic


class LineContourFilteringLogic(FlexibleRasterVectorLogic):
    def __init__(self):
        # フィルタリング条件による計算式を格納
        self.operators = {
                '<>': lambda target, conditional_values, conditional_values_2, target_objects: target != conditional_values,
                '=': lambda target, conditional_values, conditional_values_2, target_objects: target == conditional_values,
                '>': lambda target, conditional_values, conditional_values_2, target_objects: target > conditional_values,
                '<': lambda target, conditional_values, conditional_values_2, target_objects: target < conditional_values,
                'max': lambda target, conditional_values, conditional_values_2, target_objects: target == max(target_objects),
                'min': lambda target, conditional_values, conditional_values_2, target_objects: target == min(target_objects),
                'average': lambda target, conditional_values, conditional_values_2, target_objects: target == round(np.mean(target_objects)),
                'between': lambda target, conditional_values, conditional_values_2, target_objects: conditional_values_2 <= target <= conditional_values
        }

    def input_check(self, byte_data, attribute):
        """
        入力データと属性の妥当性を確認します。

        Parameters
        ----------
        byte_data : bytes
            入力される画像データのバイト列です。
        attribute : dict
            画像の属性情報を含む辞書です。ColorSpace 属性が含まれている必要があります。

        Raises
        ------
        Exception
            byte_data が None の場合や ColorSpace が設定されていない場合に例外を発生させます。
        """
        if byte_data is None:
            raise Exception('入力データが設定されていません')
        if not attribute or attribute.get('ColorSpace') is None:
            raise Exception('attributeにColorSpaceが設定されていません')

    def __call__(self, byte_data, attribute, properties):
        """
        入力データが線分の場合は線分の座標から長さを計算し、フィルタリングした結果を入力データと同じデータ形式で返します。
        線分でフィルタリング対象が太さの場合は太さでフィルタリングを行い、結果を入力データと同じデータ形式で返します。
        入力データが輪郭の場合は輪郭から面積を取得し、フィルタリングした結果を入力データと同じデータ形式で返します。
        フィルタリング結果によっては空リストのバイト列を返します。

        Parameters
        ----------
        byte_data : bytes
            バイトデータ形式の座標データを持つFieldSetFileである必要があります。
        attribute : dict
            画像データに関連する属性情報の辞書です。ColorSpace の指定はありません。
        properties : dict
            プロパティ設定を含む辞書です。
            - conditional_params : string
                フィルタリングの条件が設定されます。

            - conditional_values : int
                フィルタリングの条件値が設定されます。
                
            - conditional_values_2 : int
                フィルタリングの条件値2が設定されます。
            
            - filtering_object : int
                線分をフィルタリングする際の対象が設定されます。

            - output_line_thickness : bool
                線分フィルタリング後の出力に太さ情報を含めるかのフラグです。
            
            - fsf_filtering_target : string
                フィルタリングする線分や輪郭が格納されているField Set Fileの項目名が指定されます。
            
        Returns
        -------
        tuple
            new_byte_data : bytes
                フィルタリング結果のバイト列です。

            dict
                更新された属性情報を含む辞書です。ColorSpace は 元の値を引き継ぎます。

        Raises
        ------
        Exception
            - 入力データや ColorSpace が無効な場合や、プロパティ設定が正しくない場合に発生します。

        ValueError
            - プロパティ取得時や画像データの取得に失敗した場合に発生します。
        """
        self.input_check(byte_data, attribute)

        # プロパティから値を取得
        conditional_params = properties.get('conditional_params', '<>')
        filtering_object = properties.get('filtering_object', 'Length')
        output_line_thickness = properties.get('output_line_thickness', 'False')
        fsf_filtering_target = properties.get('fsf_filtering_target', 'content')

        if (conditional_params not in self.operators) is True:
            raise ValueError(f'conditional_paramsに無効な値が設定されています: {conditional_params}')
        if filtering_object != 'Length' and filtering_object != 'Thickness':
            raise Exception(f'filtering_objectはLengthまたはThicknessである必要があります: {properties.get("filtering_object")}')
        if output_line_thickness != 'True' and output_line_thickness != 'False':
            raise Exception(f'output_line_thicknessはTrueまたはFalseである必要があります: {properties.get("output_line_thickness")}')

        try:
            conditional_values = float(properties.get('conditional_values', 100))
        except Exception as e:
            raise ValueError(f'conditional_valuesに無効な値が設定されています: {properties.get("conditional_values")}')
        try:
            conditional_values_2 = float(properties.get('conditional_values_2', 0))
        except:
            raise ValueError(f'conditional_values_2に無効な値が設定されています: {properties.get("conditional_values_2")}')
        if conditional_values < 0 or conditional_values_2 < 0:
            raise ValueError("conditional_valuesとconditional_values_2は0以上である必要があります")
        
        
        # 入力データ取得
        if type(byte_data) is pandas.core.series.Series:
            try:
                detection_list = pickle.loads(byte_data[fsf_filtering_target])
            except:
                raise ValueError(f'fsf_filtering_targetに無効な値が設定されています: {properties.get("fsf_filtering_target")}')
        else:
            detection_list = pickle.loads(byte_data)

        # 'between'でconditional_valuesとconditional_values_2の値の大きさは順不同のため
        # 常にconditional_values_2 <= target <= conditional_valuesが成り立つようにする
        if conditional_params == 'between':
            if conditional_values_2 > conditional_values:
                conditional_values, conditional_values_2 = conditional_values_2, conditional_values

        # フィルタリング結果のリスト
        filtering_result = []
        transformed_list = []
        
        # 線分と輪郭用にfloatをintにキャスト
        if filtering_object == 'Length':
            conditional_values = int(conditional_values)
            conditional_values_2 = int(conditional_values_2)
        else:
            # 線分の太さの場合は小数第3位を四捨五入
            conditional_values = round(conditional_values, 2)
            conditional_values_2 = round(conditional_values_2, 2)
        
        # 入力が線分の場合
        if type(detection_list) is list:
            # 線分の長さを計算、太さを格納する
            filtering_target = self.line_length_thickness_append(detection_list, filtering_object)
            # 太さのフィルタリングの場合、小数点以下を比較できるようにする
            if filtering_object == 'Thickness':
                conditional_values = Decimal(str(conditional_values))
                conditional_values_2 = Decimal(str(conditional_values_2))
                filtering_target = [Decimal(str(decimal)) for decimal in filtering_target]
            # 線分の長さ、太さをフィルタリング
            filtering_result = self.filtering_process(detection_list, conditional_values, conditional_values_2, filtering_target, conditional_params)
            # 次元数を後のフローに合わせて削減
            transformed_list = filtering_result[0][:]
            # 出力に太さ情報が必要ない場合
            if output_line_thickness == 'False':
                transformed_list = [sublist[:2] for sublist in transformed_list]
                
        # 入力が輪郭の場合
        elif type(detection_list) is tuple:
            # 輪郭の面積を取得
            contour_area = self.line_contour_area_calculation(detection_list)
            # 輪郭の面積をフィルタリング
            filtering_result = self.filtering_process(detection_list, conditional_values, conditional_values_2, contour_area, conditional_params)
            # 次元数を後のフローに合わせて削減
            transformed_list = tuple(filtering_result[0][:])
        else:
            raise TypeError(f'線分の場合はlist形式、輪郭の場合はtuple形式である必要があります: {type(detection_list)}')
        
        if type(byte_data) is pandas.core.series.Series:
            byte_data[fsf_filtering_target] = pickle.dumps(transformed_list)
            new_content = byte_data
        else:
            new_content = pickle.dumps(transformed_list)

        return new_content, attribute
    
    def filtering_process(self, detection_list, conditional_values, conditional_values_2, target_objects, conditional_params):
        filtering_result = []
        target_indexes = []
        # 辞書から入力のconditional_paramsに基づいたインデックスを取得する
        for index, target in enumerate(target_objects):
            if self.operators[conditional_params](target, conditional_values, conditional_values_2, target_objects) is True:
                target_indexes.append(index)
        
        # 取得したインデックスから線分をフィルタリング
        filtering_result.append([detection_list[index] for index in target_indexes])

        return filtering_result
    
    def line_length_thickness_append(self, detection_list, filtering_object):
        target = []
        if filtering_object == 'Length':
            # 線分の長さを計算
            try:
                for line_points in detection_list:
                    # リストに太さのデータが存在しても計算できるようにする
                    line_satrt, line_end = np.array(line_points[:2])
                    target.append(round(np.linalg.norm(line_end - line_satrt)))
            except:
                raise IndexError(f'リストの構造は[(x, y), (x2, y2)]である必要があります: {line_points}')
        else:
            # 線分の太さを格納
            try:
                for line_points in detection_list:
                    # 小数第3位を四捨五入
                    target.append(round(line_points[2], 2))
            except:
                raise IndexError(f'リストの構造は[(x, y), (x2, y2), thickness]である必要があります: {line_points}')

        return target
    
    def line_contour_area_calculation(self, detection_list):
        contour_area = []
        # 輪郭の面積を取得
        contour_area = [round(cv2.contourArea(contour)) for contour in detection_list]

        return contour_area
    

