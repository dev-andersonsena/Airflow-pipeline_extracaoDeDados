from datetime import datetime, timedelta #permite fazer operações matematicas
import os # permite interação com sistema operacional. 
import requests
import json


TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z" # formato padrão de formato de data = ano/mes/dia hora/minuto/segundo  
# .00Z timezone


end_time = (datetime.now() + timedelta(hours=3, seconds=30)).strftime(TIMESTAMP_FORMAT) # formata o tempo em string 
start_time = (datetime.now() + timedelta(days=-7, hours=3)).strftime(TIMESTAMP_FORMAT)
                      # timedelta -1  = esta extraindo um dia do dia de hoje. dia começa hj iniciando ontem. 

query = "data science"

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

# tweet_field veio no formato json, vamos selecionar os campos que serão extraidos no caso seria as colunas 
# especifica os campos que vai ser puxado. 

# user_field = campos do usuario, api diz como selecionar os campos dos usuarios. 


url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

response = requests.request("GET", url_raw)
# GET = Metodo de requisição de http 
# variavel response = resposta 

json_response = response.json()
# extraindo .json e guardando na variavel. 

print(json.dumps(json_response, indent=4, sort_keys=True)) # parâmetros 
# dumps = vai jogar e extrair para fora e transforma numa string 
# identação ident = 4 
#sort_keys = ordenação das chaves 

while "next_token" in json_response.get("meta",{}):
    next_token = json_response['meta'] ['next_token']
    url = f"{url_raw}&next_token={next_token}"
    json_response = response.json()
    print(json.dumps(json_response, indent=4, sort_keys=True)) 