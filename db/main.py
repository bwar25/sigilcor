from src.auxiliary.connection import connect_to_db
from src.auxiliary.create import create_price_data_table
from src.auxiliary.insert import insert_price_data

from src.pricedata.csv_files import csv_filepaths


# ---------- Main Function (Create Tables & Insert Data) ---------- #
def database_main():
    with connect_to_db() as db_connection:
        print("Successful: connection...")
        create_price_data_table(db_connection)
        print('Successful: table creation...')
        insert_price_data(db_connection, csv_filepaths)
        print("Successful: insertion...")


if __name__ == '__main__':
    database_main()