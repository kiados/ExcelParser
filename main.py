from db import Database
from excel import ExcelParser


def main():
    dataset = ExcelParser().main()
    Database().main(dataset)


if __name__ == '__main__':
    main()
