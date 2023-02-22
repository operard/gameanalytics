import pandas as pd

file = '../docs/premium.xls'
file_2 = '../docs/sprinklr.xls'

df = pd.read_excel(file)
df_2 = pd.read_excel(file_2)

df = pd.DataFrame(df, columns=['Twitter'])
df_2 = pd.DataFrame(df_2, columns=['Twitter'])
print(df)
print(df_2)

# Eliminate users with no twitter handle since we are doing Twitter analysis

df = df.dropna(subset=['Twitter'])
df_2 = df_2.dropna(subset=['Twitter'])

def get_twitter_handle(original):
	return original.split('twitter.com/')[1]

df['final_twitter'] = df['Twitter'].apply(get_twitter_handle)
df_2['final_twitter'] = df_2['Twitter'].apply(get_twitter_handle)
print(df['final_twitter'])
print(df_2['final_twitter'])

df['final_twitter'].to_csv('../data/twitter.handles')
df_2['final_twitter'].to_csv('../data/twitter.handles', mode='a') # append