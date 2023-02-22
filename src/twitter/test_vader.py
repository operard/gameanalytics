'''
NLTK already has a built-in, pretrained sentiment analyzer called VADER (Valence Aware Dictionary and sEntiment Reasoner).
Since VADER is pretrained, you can get results more quickly than with many other analyzers. However, VADER is best suited for language used in social media, like short sentences with some slang and abbreviations. It’s less accurate when rating longer, structured sentences, but it’s often a good launching point.
To use VADER, first create an instance of nltk.sentiment.SentimentIntensityAnalyzer, then use .polarity_scores() on a raw string:
'''

from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

def nltk_package_check():
    try:
        SentimentIntensityAnalyzer()
    except LookupError:
        nltk.download('vader_lexicon')


nltk_package_check()
TEXT = "This is a very happy day"
sia = SentimentIntensityAnalyzer()
result = sia.polarity_scores(TEXT)
print(result.get('compound'), result.get('pos'), result.get('neu'), result.get('neg'))
# {'neg': 0.0, 'neu': 0.295, 'pos': 0.705, 'compound': 0.8012}

'''
The negative, neutral, and positive scores are related:
They all add up to 1 and can’t be negative. The compound score is calculated differently.
It’s not just an average, and it can range from -1 to 1.
'''