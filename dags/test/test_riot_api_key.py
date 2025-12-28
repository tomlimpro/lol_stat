from datetime import datetime
from airflow.sdk import dag, task, Variable
import requests
import os
import logging

@dag(
    dag_id = 'test_riot_api_key',
    start_date = datetime(2025,1,1),
    schedule = None,
    catchup = False,
    tags = ['test', 'riot', 'API'],
    description = "Test de la validité de la clé API Riot Games"
)

def test_riot_api_key():
    @task
    def check_api_key_exists() -> dict:
        """
        Verifie que la clé API est bien configurée
        """
        try:
            api_key_env = os.getenv('RIOT_API_KEY')
            api_key_var = None
            try: 
                api_key_var = Variable.get('RIOT_API_KEY', default_var = None)
            except: 
                pass
            result = {
                'env_configured' : api_key_env is not None,
                'variable_configured' : api_key_var is not None,
                'key_length' : len(api_key_env) if api_key_env else 0,
                'key_prefix' : api_key_env[:10] + '...' if api_key_env and len(api_key_env) > 10 else 'N/A'

            }

            if result['env_configured']:
                logging.info("✅ Clé API trouvée dans les variables d'environnement !")
                logging.info(f"📏 Longueur: {result['key_length']} caractère.")
                logging.info(f"🔑 Préfix: {result['key_prefix']}")
            else:
                logging.error("❌ Clé API non trouvée dans l'environnement.")
                raise ValueError("RIOT API KEY non configurée")
            if result['variable_configured']:
                logging.info("✅ Clé API également disponible dans Airflow Variable")
            else:
                logging.warning("⚠️ Clé API non trouvée dans Airflow Variable.")
            return result
        except Exception as e:
            logging.error(f"❌ Erreur: {str(e)}")
            raise

    @task
    def test_api_basic_call() -> dict:
        try:
            api_key = os.getenv("RIOT_API_KEY") or Variable.get('RIOT_API_KEY')
            url = f"https://euw1.api.riotgames.com/lol/platform/v3/champion-rotations?api_key={api_key}"
            headers = {"X-Riot-Token" : api_key}
            logging.info(f"Test d'API : {url}")
            response = requests.get(url, headers = headers, timeout=10)
            result = {
                'status_code' : response.status_code,
                'success' : response.status_code == 200,
                'response_time_ms' : int(response.elapsed.total_seconds() * 1000)
            }

            if response.status_code == 200:
                data = response.json()
                result['free_champion_ids_count'] = len(data.get('freeChampionIds',[]))
                logging.info(f"✅ API fonctionnelle ! Temps de reponse : {result['response_time_ms']}")
                logging.info(f"Il y a {result['free_champion_ids_count']} champions gratuits cette semaine")
            elif response.status_code == 401:
                logging.error("❌ Clé API invalide ou expirée")
                result['error'] = 'Clé API invalide ou expirée'
            elif response.status_code == 403:
                logging.error("❌ Accès interdit")
                result['error'] = "Accès interdit"
            elif response.status_code == 429:
                logging.error("❌ Rate limit dépassé")
                result['error'] = "Rate limit dépassé"
            else: 
                logging.error(f"❌ Erreur api: {result[response.status_code]}")
                result['error'] = f"HTTP {response.status_code} : {response.text[:200]}"
            return result
        except requests.exceptions.Timeout:
            logging.error("❌ Timeout : API ne répond pas.")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Erreur reseau : {str(e)}")
            raise
        except Exception as e:
            logging.error(f"❌ Erreur : {str(e)}")
            raise

    key_check = check_api_key_exists()
    test_api = test_api_basic_call()
    key_check >> test_api
test_riot_api_key()