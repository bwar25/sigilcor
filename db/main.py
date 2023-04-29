from src.auxiliary.connection import connect_to_db
from src.auxiliary.create import create_price_data_table
from src.auxiliary.insert import insert_price_data

from src.processing.process import separate_time_columns, open_eth, open_rth, high_eth, low_eth, high_rth, low_rth, eliminate_holidays, session_close

from src.pricedata.csv_files import csv_filepaths


# ---------- Main Function (Create Tables & Insert Data) ---------- #
def database_main():
    with connect_to_db() as db_connection:
        print("Successful: connection...")
        create_price_data_table(db_connection)
        print('Successful: table creation...')
        insert_price_data(db_connection, csv_filepaths)
        print("Successful: insertion...")

def processing_main():
    with connect_to_db() as db_connection:
        # separate_time_columns(db_connection)
        # open_eth(db_connection)
        # open_rth(db_connection)
        # high_eth(db_connection)
        # low_eth(db_connection)
        # high_rth(db_connection)
        # low_rth(db_connection)
        # eliminate_holidays(db_connection)
        # session_close(db_connection)
        pass

# if __name__ == '__main__':
    # database_main()
    # processing_main()