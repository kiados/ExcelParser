from excel import ExcelParser
from db import Database


def main():
    dataset = ExcelParser().main()
    Database().main(dataset)


if __name__ == '__main__':
    main()
