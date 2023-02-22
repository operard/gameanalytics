from oci.config import from_file
import oci

def init_oracle_lang_service():
	config = from_file()
	# Initialize service client with default config file
	ai_language_client = oci.ai_language.AIServiceLanguageClient(config)
	return ai_language_client


def detect_language(ai_language_client):
    text = 'hola que tal? esto es un ejemplo de textos que se pueden detectar los lenguajes hello this is a second example of english language in the same texts'
    try:
        detect_language_sentiments_response = ai_language_client.detect_dominant_language(
            detect_dominant_language_details=oci.ai_language.models.DetectDominantLanguageDetails(
            text=text))
        s_data = detect_language_sentiments_response.data
        print(detect_language_sentiments_response.data)
    except oci.exceptions.ServiceError as e:
        print(e)
    
    print(s_data.languages[0].code)
    print(s_data.languages[0].score)
    print(s_data.languages[0].name)
    return s_data.languages[0].code, s_data.languages[0].score

ai_language_client = init_oracle_lang_service()
detect_language(ai_language_client)