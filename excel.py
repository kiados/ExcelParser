import os
import copy
import pandas
from loguru import logger


class ExcelParser:

    def __init__(self):
        self.default_filepath = 'resources/excel_file.xlsx'
        self.main_dataframe = None
        self.frames = []
        self.solution = ''

        self.level_names = ['company', 'id']
        self.i = 1
        self.multiheaders_by_number = {
            1: 'Факт или прогноз',
            2: 'Тип Q',
            3: 'Дата'
        }

    def main(self):
        self.read_excel_file()
        self.divide_by_companies()
        self.divide_to_separate_frames()
        self.counting_separate_frames()
        dataset = self.create_dataset()
        return dataset

    def create_dataset(self):

        dataset = []
        try:
            for frame in self.frames:
                frame.update({'Значения': frame['Фрейм'].values.tolist()})
                dataset.append(frame)
            return dataset
        except Exception as er:
            logger.error(f'Ошибка при создании датасета: {er}')
            raise

    def counting_separate_frames(self):

        solution_string = ''
        try:
            for item in self.frames:

                frame_total = self.count_frame_total(item["Фрейм"])
                conditions = ', '.join([item[key] for key in item if key not in ['Компания', 'Фрейм']])
                solution_string += f'Расчетный тотал для компании {item["Компания"]}' \
                                  f' с условиями {conditions} составил {frame_total}\n'
            self.solution += solution_string
            print(self.solution)
        except Exception as er:
            logger.error(f'Ошибка при подсчете фрейма: {er}')
            raise

    def read_excel_file(self, filepath=None):

        if not filepath:
            filepath = self.default_filepath

        if not os.path.exists(filepath):
            logger.error(f'Excel-файл не найден! Путь: {filepath}')
            raise

        try:
            self.main_dataframe = pandas.read_excel(filepath, header=[0,1,2])
        except Exception as er:
            logger.error(f'Произошла ошибка при считывании файла: {er}')
            raise

        return

    def divide_by_companies(self):

        try:
            df = self.main_dataframe

            # Преобразование что уйти от безымянных мультииндексов
            companies_list = [item[0] for item in df.company.values.tolist()]
            id_list = [item[0] for item in df.id.values.tolist()]
            df = df.drop(df.columns[[0, 1]], axis=1)
            df.insert(0, ('id', 'id', 'id'), '')
            df.insert(1, ('company', 'company', 'company'), '')
            df['id', 'id', 'id'] = id_list
            df['company', 'company', 'company'] = companies_list

            # Разделение на фреймы по компаниям
            companies = self.get_companies(df)
            for comp in companies:
                self.frames.append({
                    "Компания": comp,
                    "Фрейм": df[df[('company', 'company', 'company')] == comp]
                })

            return

        except Exception as er:
            logger.error(f'Ошибка при обработке файла: {er}')
            raise

    def divide_to_separate_frames(self):

        try:
            for k in range(self.main_dataframe.columns.nlevels):
                is_last_iter = k == self.main_dataframe.columns.nlevels-1
                list_copy = copy.deepcopy(self.frames)
                list_to_insert = []

                for item in list_copy:
                    frame = item['Фрейм']

                    if is_last_iter:
                        level_names = [item for item in frame.columns if not "Unnamed" in item]
                    else:
                        level_names = [item for item in list(frame.columns.levels[0]) if item not in self.level_names and not "Unnamed" in item]

                    for name in level_names:
                        item_copy = copy.deepcopy(item)
                        item_copy[self.multiheaders_by_number[self.i]] = name
                        item_copy['Фрейм'] = frame[name]
                        list_to_insert.append(item_copy)
                        del item_copy

                if not is_last_iter:
                    frame.columns = frame.columns.droplevel()

                self.level_names += level_names
                self.frames = copy.deepcopy(list_to_insert)
                self.i += 1
                del list_copy, list_to_insert

        except Exception as er:
            logger.error(f'Ошибка при разделении на фреймы: {er}')
            raise

    @staticmethod
    def count_frame_total(frame):
        return frame.sum()

    @staticmethod
    def get_companies(df) -> list:
        dfc = df['company']
        dfc.columns = dfc.columns.droplevel()
        dfc.columns = ['company']
        companies = [item[0] for item in dfc.drop_duplicates().values]

        return companies


if __name__ == '__main__':
    ExcelParser().main()
        