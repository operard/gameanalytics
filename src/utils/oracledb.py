import cx_Oracle
import requests
import yaml
import os
from pathlib import Path
home = str(Path.home())

def process_yaml():
	with open("config.yaml") as file:
		return yaml.safe_load(file)


class OracleATPDatabaseConnection:
    def __init__(self, data=process_yaml()):
        # wallet location (default is HOME/wallets/wallet_X)
        os.environ['TNS_ADMIN'] = '{}/{}'.format(home, process_yaml()['WALLET_DIR'])
        print(os.environ['TNS_ADMIN'])
        self.pool = cx_Oracle.SessionPool(data['db']['username'], data['db']['password'], data['db']['dsn'],
            min=1, max=4, increment=1, threaded=True,
            getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT
        )
        print('Connection successful.')



    def close_pool(self):
        self.pool.close()
        print('Connection pool closed.')



    def insert(self, cursor_object):
        connection = self.pool.acquire()
        connection.autocommit = True
        cursor = connection.cursor()

        try:
            # print(sql_insert)
            cursor.execute(cursor_object)
            print('[DBG] INSERT {} OK'.format(cursor_object))
        except Exception as e:
            print('[DBG] INSERT ERR: {}'.format(e))

        self.pool.release(connection)
        return 1


    # select
    def select(self, query_sql):
        assert 'select' in query_sql.lower()
        connection = self.pool.acquire()
        connection.autocommit = True
        cursor = connection.cursor()
        return_list = list()
        try:
            cursor.execute(query_sql)
            print('[DBG] SELECT OK')
        except Exception as e:
            print('[DBG] SELECT ERR {}'.format(e))

        rows = cursor.fetchall()
        if rows:
            for x in rows:
                return_list.append(x)
        self.pool.release(connection)
        return return_list


    # update
    def update(self, query_sql, row, debug=None):
        assert 'update' in query_sql.lower()
        connection = self.pool.acquire()
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(query_sql, row)
            if debug:
                print('[DBG][{}] UPDATE {} OK'.format(debug, row))
            else:
                print('[DBG] UPDATE {} OK'.format(row))
        except Exception as e:
            if debug:  
                print('[DBG][{}] UPDATE ERR {}'.format(debug, e))
            else:
                print('[DBG] UPDATE ERR {}'.format(e))

        self.pool.release(connection)
        return 1



def test_class():
    object = OracleATPDatabaseConnection()
    print(object.pool)
    object.close_pool()



if __name__ == '__main__':
    test_class()
