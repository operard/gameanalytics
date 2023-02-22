from googletrans import Translator
from oracledb import OracleATPDatabaseConnection
import yaml

'''
CODE DEPRECATED, GOOGLETRANS LIBRARY NOT WORKING ATM.
'''

# Load config file
def load_config_file():
	with open('config.yaml') as file:
		return yaml.safe_load(file)



TABLES = [
	{
		'table_name': 'admin.user_sentiment',
		'translation_object': 'tweet'
	},
	{
		'table_name': 'admin.public_sentiment',
		'translation_object': 'tweet'
	},
	{
		'table_name': 'twitch.chat',
		'translation_object': 'message'
	},
	{
		'table_name': 'twitch.vod_chat',
		'translation_object': 'message'
	}
]


def translate(obj, text, source_language, destination_language):
    translation = obj.translate(text, src=source_language, dest=destination_language)
    print(translation)
    #print(translation.src, translation.text, translation.extra_data)
    return translation.src, translation.text
    



def run():
    translator = Translator()
    db = OracleATPDatabaseConnection()

    for x in TABLES:
        x['table_name']
        x['translation_object']

        sql_query = """select distinct {}, language from {} where language != 'en'""".format(x['translation_object'], x['table_name'])
        messages_to_translate = db.select(sql_query)



        for x in messages_to_translate:
            translate(translator, x[0], x[1], 'en')
 

    db.close_pool()



def main():
    run()



if __name__ == '__main__':
    main()


'''
for x in TWITTER_LANGUAGES:
    translation = translator.translate('안녕하세요.', dest='en')
    print(translation)
'''