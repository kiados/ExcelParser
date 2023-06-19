import copy
import os

import pandas
from loguru import logger


class ExcelParser:

    def __init__(self):
        self.default_filepath = 'resources/excel_file.xlsx'
        self.main_dataframe = None
        self.frames = []
        self.solution = ''

        self.level_names = ['company', 'id']  # Заголовки, не требующие обработки или уже обработанные
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
            logger.info(f'Чтение файла {filepath}')
            self.main_dataframe = pandas.read_excel(filepath, header=[0,1,2])
            logger.info('Файл успешно прочитан')
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

            # Проход по каждому уровню вложенности
            levels_count = self.main_dataframe.columns.nlevels
            for k in range(levels_count):

                # Проверка на последнюю итерацию
                last_iter = True if k == levels_count - 1 else False

                frames_to_prepare = copy.deepcopy(self.frames)
                frames_result = []

                # Разделение фрейма на меньшие фреймы
                for item in frames_to_prepare:
                    frame = item['Фрейм']
                    cols = frame.columns

                    # Последний уровень вложенности считывается иначе
                    if not last_iter:
                        columns_names = cols.levels[0]
                        levels_names_cleared = [name for name in columns_names
                                                if name not in self.level_names and "Unnamed" not in name]
                    else:
                        levels_names_cleared = [name for name in cols if "Unnamed" not in item]

                    # Получаем фрейм из каждой колонки
                    for name in levels_names_cleared:
                        item_copy = copy.deepcopy(item)
                        header = self.multiheaders_by_number[self.i]
                        item_copy[header] = name
                        item_copy['Фрейм'] = frame[name]

                        # Добавляем фрейм в результат
                        frames_result.append(item_copy)

                # Если итерация не последняя - удаляем верхний уровень вложенности
                if not last_iter:
                    frame.columns = frame.columns.droplevel()

                # Добавляем уже прочитанные имена в список, чтобы не обрабатывать их повторно
                self.level_names += levels_names_cleared

                # Подменяем фреймы более высокого уровня фреймами более низкого уровня
                self.frames = copy.deepcopy(frames_result)
                self.i += 1

            return

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
        