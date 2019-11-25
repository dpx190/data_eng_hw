import csv
import logging
import os
from operator import itemgetter
from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parent
DATA = ROOT / 'dataset'
LOGGER = logging.getLogger(__file__)
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

first = itemgetter(0)

CREATE_MARKETING = """
CREATE TABLE IF NOT EXISTS marketing (
    event_id VARCHAR(50),
    phone_id VARCHAR(50),
    ad_id VARCHAR(25),
    provider VARCHAR(25),
    placement VARCHAR(25),
    length VARCHAR(25),
    event_ts VARCHAR(50)
)"""

CREATE_USER = """
CREATE TABLE IF NOT EXISTS users (
    event_id VARCHAR(50),
    user_id VARCHAR(50),
    phone_id VARCHAR(50),
    property VARCHAR(25),
    value VARCHAR(25),
    event_ts VARCHAR(50)
)"""

def make_connection():
    """
    Returns
    ---------
    connection: psycopg2.Connection
        A connection object tied to a Postgres database
    """
    connection = psycopg2.connect(
        user='postgres',
        # normally we wouldn't have the password here but for the sake of the exercise
        # and this being a dummy database we can hardcode the password in here
        password='password',
        host='localhost',
        port=5432
        )
    return connection


def get_data_file_names():
    """
    Returns
    --------
    filenames: list
        A list of strings which correspond to datafile names
    """
    fns = []

    for _, _, fn in os.walk(str(DATA)):
        fns = [f for f in fn if f.endswith('.csv')]
    return fns


def upload_to_db(files, connection):
    """
    Uploads the given file to a Postgres database

    Parameters
    -----------
    files: iterable of path-like objects
        iterable of file names that need to be uploaded to the database
    
    connection: class or sub-class of psycopg2.Connection
        a class that establishes a connection to a Postgres database
    
    Returns
    ---------
    None
    """
    with connection as conn, conn.cursor() as cursor:
        cursor.execute(CREATE_MARKETING)
        LOGGER.info('Created the marketing table')

        cursor.execute(CREATE_USER)
        LOGGER.info('Created the users table')
        for fin in files:
            table = str(fin).split('_')[0]  # parse table name from file name
            if table == 'user':
                table = 'users'
            fp = str(DATA / fin)
            with open(fp, 'r') as f:
                next(f)
                cursor.copy_from(
                    file=f,
                    table=table,
                    sep=',',
                    null='NULL'
                    )


def preprocess(files):
    """
    Parses data files and catches bad lines. Currently there are two types
    of malformation present:
        1: \x00 NULL byte characters present in data files
        2: Line spilling over to the next line
    Parameters
    -----------
    files: iterable of path-like objects
        iterable of file names that need to be uploaded to the database
    
    Returns
    ---------
    None
    """
    for fin in files:
        malformed = dict()
        fp = str(DATA / fin)
        tmp = fp + '_tmp'

        with open(fp, 'rb') as fin, open(tmp, 'w') as fout:
            reader = csv.reader(
                line.replace(b'\x00', b'').decode('utf-8') for line in fin)
            writer = csv.writer(fout)
            writer.writerows(reader)

        os.replace(tmp, fp)
        
        with open(fp, 'r') as fin:
            reader = csv.reader(fin)
            headers = reader.__next__()
            length = len(headers)

            for idx, line in enumerate(reader, 2):  # start from 2 since we consumed headers
                check = len(list(filter(None, line)))
                if check < length and check > 0:
                    malformed[idx] = line
                    LOGGER.info('Line {n} in {f} is malformed'.format(
                        n=idx, f=fp))
                else:
                    continue
                    
            if len(malformed) > 0:
                LOGGER.info('Fixing malformed lines in {f}'.format(
                    f=fp))
                fix_bad_line(fp, malformed, length)


def fix_bad_line(file_path, corrupt_map, num_cols):
    """
    Takes in a file path of a data file with malformed data and a mapping
    pointing to malformed lines. Writes out a new CSV which attempts to fix
    the case of lines spilling over.

    Parameters
    -----------
    file_path: path-like object
        path-like object pointing to a data file
    corrupt_map: dict
        a dictionary where keys are indices of corrupt lines and values are
        the actual corrupt line
    num_cols: integer
        an integer representing the number of columns in the dataset
    
    Returns
    ---------
    None
    """
    tmp = file_path + '_tmp'
    with open(file_path, 'r') as fin, open(tmp, 'w') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        for idx, line in enumerate(reader, 1):
            if idx not in corrupt_map and line:
                writer.writerow(line)
            
            elif len(line) == 0:  # empty row from CSV
                LOGGER.info('Line {l} in file {f} is empty'.format(
                    l=idx, f=file_path))
                continue
            
            else:
                this_line = corrupt_map.get(idx)
                next_line = corrupt_map.get(idx + 1)
                
                if this_line and next_line:
                    a = list(filter(None, this_line))
                    b = list(filter(None, next_line))
                    
                    if len(this_line) + len(next_line) == num_cols:
                        to_write = this_line + next_line
                        writer.writerow(to_write)
                        LOGGER.info('Successfully combined lines {a} and {b} \
                            from file {f}'.format(a=idx, b=idx + 1, f=file_path))
    
    os.replace(tmp, file_path)


def etl(conn):
    files = get_data_file_names()
    preprocess(files)
    upload_to_db(files, conn)



def question1(connection):
    query = """
    SELECT COUNT(DISTINCT user_id)
    FROM users
    """
    with connection as conn, conn.cursor() as cursor:
        cursor.execute(query)
        res = first(cursor.fetchone())
        LOGGER.info('There are {r} unique users'.format(r=res))


def question2(connection):
    query = """
    SELECT DISTINCT provider
    from marketing
    """
    with connection as conn, conn.cursor() as cursor:
        cursor.execute(query)
        res = list(map(first, cursor.fetchall()))
        LOGGER.info('The distinct ad providers are {r}'.format(r=res))

def question3(connection):
    query = """
    SELECT property, COUNT(1) AS counts
    FROM users
    GROUP BY 1
    ORDER BY 2 DESC
    """
    with connection as conn, conn.cursor() as cursor:
        cursor.execute(query)
        res = first(cursor.fetchone())
        LOGGER.info('The most changed property is {r}'.format(r=res))

def question4(connection):
    query = """
    SELECT COUNT(1)
    FROM marketing
    WHERE provider = 'Snapchat'
    AND event_ts::DATE = '2019-07-03'::DATE
    """
    with connection as conn, conn.cursor() as cursor:
        cursor.execute(query)
        res = first(cursor.fetchone())
        LOGGER.info('{r} users were shown a Snapchat ad on July 3rd 2019'.format(r=res))

def question5(connection):
    query = """
    SELECT a.ad_id, COUNT(1) AS counts
    FROM marketing AS a
    JOIN users AS b
    ON a.phone_id = b.phone_id
    WHERE UPPER(b.property) = 'POLITICS'
    AND UPPER(b.value) = 'MODERATE'
    GROUP BY 1
    ORDER BY 2 DESC
    """
    with connection as conn, conn.cursor() as cursor:
        cursor.execute(query)
        res = first(cursor.fetchone())
        LOGGER.info('The most shown ad to moderates is {r}'.format(r=res))


def question6(connection):
    query = """
    SELECT a.ad_id, COUNT(DISTINCT a.phone_id) AS counts
    FROM marketing AS a
    JOIN users AS b
    ON a.phone_id = b.phone_id
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 5
    """
    with connection as conn, conn.cursor() as cursor:
        cursor.execute(query)
        res = list(map(first, cursor.fetchall()))
        LOGGER.info('The 5 most succesful ads are {r}'.format(r=res))


if __name__ == '__main__':
    conn = make_connection()
    etl(conn)
    question1(conn)
    question2(conn)
    question3(conn)
    question4(conn)
    question5(conn)
    question6(conn)
