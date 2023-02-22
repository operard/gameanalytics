from oracledb import OracleATPDatabaseConnection
import threading
import os
import argparse
import time

TABLES = [
	{
		'table_name': 'admin.user_sentiment',
		'translation_object': 'tweet',
		'identifier': 'tweet_id'
	},
	{
		'table_name': 'admin.public_sentiment',
		'translation_object': 'tweet',
		'identifier': 'tweet_id'
	},
	{
		'table_name': 'twitch.chat',
		'translation_object': 'message',
		'identifier': 'message_id'
	},
	{
		'table_name': 'twitch.vod_chat',
		'translation_object': 'message',
		'identifier': 'message_id'
	}
]


parser = argparse.ArgumentParser()
parser.add_argument('-t', '--threads', help='Number of threads', required=True)
args = parser.parse_args() 


NUM_THREADS = int(args.threads)


def thread_function(table, base, offset):
    os.system('python libre_translate.py --table "{}" --offset "{}" --base "{}"'.format(table, offset, base))



def find_number_rows(db, x):
    sql_query = """select count(*) from (select distinct {}, language, {} from {} where language not in ('en', 'UNKNOWN', 'und') and translated=0)""".format(x['translation_object'], x['identifier'], x['table_name'])
    result = db.select(sql_query)    
    print(result[0][0])
    print('Obtained {} rows to process'.format(result[0][0]))
    return int(result[0][0])



def main():
    db = OracleATPDatabaseConnection()

    for x in TABLES:
        offset = 0
        threads = list()
        total_rows = find_number_rows(db, x)
   
        rows_per_thread = int(total_rows / NUM_THREADS) + 1
        print('[STAT] Table: {} NUM_THREADS: {} Total rows: {} Base: {} Offset: {}'.format(
            x['table_name'], NUM_THREADS, total_rows, rows_per_thread, offset
        ))
        time.sleep(3) # allow seeing stats
        for y in range(NUM_THREADS):
            thread = threading.Thread(target=thread_function, args=(x['table_name'], rows_per_thread, offset))
            threads.append(thread)
            thread.start()
            offset += rows_per_thread
        
        for y in threads:
            y.join()
    
    db.close_pool()



if __name__ == '__main__':
    main()