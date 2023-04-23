from database.connection import connect_to_db
from database.create import create_price_data_table
from database.insert import insert_price_data

from pricedata.csv_files import csv_filepaths


# ---------- Main Function (Create Tables & Insert Data) ---------- #
# def database_main():
#     with connect_to_db() as db_connection:
#         create_price_data_table(db_connection)
#         insert_price_data(db_connection, csv_filepaths)



if __name__ == '__main__':
    pass
    # database_main()