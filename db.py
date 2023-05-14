from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, MetaData, insert
from loguru import logger
from datetime import datetime


class Database:

    def __init__(self):
        self.engine = None
        self.connect = None
        self.metadata = None
        self.table = None

    def main(self, datarows):
        self.set_connection()
        self.create_table()
        for row in datarows:
            self.insert_one_row(row)

    def set_connection(self):
        try:
            self.engine = create_engine('sqlite:///main_database.db')
            self.connect = self.engine.connect()
            self.metadata = MetaData()
        except Exception as er:
            logger.error(f'Ошибка при коннекте к БД: {er}')
            raise

    def create_table(self):

        self.table = Table('parsing', self.metadata,
                    Column('id', Integer(), primary_key=True),
                    Column('datetime_written', DateTime()),
                    Column('company', String()),
                    Column('fact', String()),
                    Column('q_type', String()),
                    Column('data', String()),
                    Column('values', String()))
        self.metadata.create_all(self.engine)

    def insert_one_row(self, row):

        try:
            company = row['Компания']
            fact = row['Факт или прогноз']
            q_type = row['Тип Q']
            _data = row['Дата']
            values = str(row['Значения'])
            datetime_written = datetime.now()

            smt = (
                insert(self.table).
                values(datetime_written=datetime_written,
                       company=company,
                       fact=fact,
                       q_type=q_type,
                       data=_data,
                       values=values)
            )

            self.connect.execute(smt)
            self.connect.commit()
        except Exception as er:
            logger.error(f'Ошибка при записи строки в БД: {er}')
            raise

