import os
from dotenv import load_dotenv

import pyodbc


load_dotenv()

db_connection_str = os.getenv('db_connection')


# ---------- DB Connection ---------- #
def connect_to_db(db_connection_str: str = db_connection_str) -> pyodbc.Connection:
    """ 
    Example Usage: 
    with connect_to_db(db_connection_str) as db_connection:
        ...
    """
    return pyodbc.connect(db_connection_str)