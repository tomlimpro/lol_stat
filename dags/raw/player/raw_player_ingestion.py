from datetime import datetime, timedelta
import requests
import logging
import os
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time

POSTGRES_CONN_ID = "postgres_default"

@dag(
    dag_id = 'raw_ingestion_riot_player',
    start_date = datetime(2025, 1, 1),
    schedule = None,
    catchup = False,
    tags = ['ingestion', 'player','postgres','API'],
    description = "Extraction et chargement bruts de l'API League-V4 des Master, Grandmaster et Challenger",
    params = {
        'queue_type' : 'RANKED_SOLO_5x5',
        'tiers' : ['MASTER', 'GRANDMASTER', 'CHALLENGER']
    }

)

def raw_ingestion_player():
    @task
    def extraction_player_entries(**context) -> list[dict]:
        try:
            api_key = os.getenv("RIOT_API_KEY")
            queue = context['params'].get('queue_type','RANKED_SOLO_5X5')
            tiers = context['params'].get('tiers', ['MASTER','GRANDMASTER','CHALLENGER'])   
            data = []
            headers = {"X-Riot-Token": api_key}
            
            for tier in tiers:
                logging.info("#########################################")
                logging.info(f"🪏 EXTRACTION : {tier} ...")
                logging.info("#########################################")
                page = 1
                while True:
                    url = f"https://euw1.api.riotgames.com/lol/league-exp/v4/entries/{queue}/{tier}/I"
                    try:
                        response = requests.get(url, 
                                                headers=headers, 
                                                params={"page" : page},
                                                timeout=30)
                        response.raise_for_status()
                        tier_data = response.json()
                        if not tier_data:
                            logging.info("tier=%s | page=%s | entries=%s | time=%.2fs", tier,page, len(tier_data), response.elapsed.total_seconds())
                            break
                        if tier_data:
                            data.extend(tier_data)
                            logging.info(f"✅ {tier} page {page} : {len(tier_data)} entrées récupérées.")
                            logging.info(f"Temps de réponse : {response.elapsed.total_seconds():.2f}s")
                            page += 1
                            time.sleep(1.2)
                        else:
                            logging.warning(f"⚠️ {tier} : Aucune entrée trouvée")
                    except requests.exceptions.HTTPError as e:
                        if e.response.status_code == 429:
                            logging.warning(f"{tier}: Rate limit atteint, pause de 2s ...")
                            time.sleep(2)
                            response = requests.get(url, params={'page': 1}, timeout=30)
                            response.raise_for_status()
                            tier_data = response.json()
                            data.extend(tier_data)
                            logging.info(f"✅ {tier}: {len(tier_data)} entrées après retry")
                        else:
                            raise
            logging.info("")
            logging.info(f"✅ Extraction terminée pour {tier}!")   
            return data     
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logging.error("❌ Clé API invalide.")
            elif e.response.status_code == 403:
                logging.error("❌ Accès refusé.")
            elif e.response.status_code == 429:
                logging.error("❌ Rate limite dépassé.")
            else:
                logging.error(f"❌ Erreur HTTP: {e.response.status_code}")
            raise
        except requests.exceptions.Timeout:
            logging.error("❌ Timeout")
            raise
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Erreur réseau : {str(e)}")
            raise
        except Exception as e:
            logging.error(f"❌ Erreur inattendue lors de l'extraction : {str(e)}")
            raise

    @task
    def load_player_entries(league_data: list[dict]) -> dict:
        if not league_data:
            logging.warning("⚠️ Aucune donnée.")
            return{
                'inserted': 0,
                'skipped' : 0,
                'total' : 0
            }
        try: 
            pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            extraction_date = datetime.now().date()
            inserted_count = 0
            logging.info("💾 DEBUT DU CHARGEMENT :")
            logging.info(f"Date d'extraction : {extraction_date}")
            logging.info(f"Nombre d'entrées à insérer : {len(league_data)}")
            insert_query = """
                INSERT INTO raw.player_entries_raw(league_id, queue_type, tier, rank, puuid, league_points, wins, losses, veteran, inactive, fresh_blood, hot_streak, extraction_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            batch_size = 100
            for i in range(0, len(league_data), batch_size):
                batch = league_data[i:i + batch_size]
                for entry in batch:
                    cursor.execute(insert_query, (
                        entry.get('league_id', ''),
                        entry.get('queue_type',''),
                        entry.get('tier', ''),
                        entry.get('rank',''),
                        entry.get('puuid',''),
                        entry.get('league_points', 0),
                        entry.get('wins', 0),
                        entry.get('losses', 0),
                        entry.get('veteran',False),
                        entry.get('inactive', False),
                        entry.get('fresh_blood', False),
                        entry.get('hotStreak',False),
                        extraction_date
                    ))
                    inserted_count += 1
                conn.commit()
                logging.info(f"📦 Batch {i//batch_size+1}: {len(batch)} entrées insérées")
            logging.info(f"✅ Chargement terminé : {inserted_count} entrées insérées")
            cursor.close()
            conn.close()
            return{
                'inserted': inserted_count,
                'skipped': 0,
                'total': len(league_data),
                'extraction_date': str(extraction_date)
            }
        except Exception as e:
            if 'conn' in locals():
                conn.rollback()
                logging.error(f"❌ Erreur lors du chargement : {str(e)}")
                raise
        finally:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
    @task
    def validate_data(load_stats: dict) -> dict:
        try:
            pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
            extraction_date = load_stats['extraction_date']
            expected_count = load_stats['inserted']
            logging.info("✅ VALIDATION DES DONNEES")
            # Verifier le nombre d'enregistrement
            count_query = """
                SELECT COUNT(*)
                FROM RAW.PLAYER_ENTRIES_RAW
                WHERE extraction_date = %s
            """
            actual_count = pg_hook.get_first(count_query, parameters =(extraction_date,))[0]
            logging.info(f"Attendu : {expected_count} | Trouvé : {actual_count}")
            results = {
                'expected_count' : expected_count,
                'actual_count' : actual_count
            }
            return results
        except Exception as e:
            logging.error(f"Erreur de la validation : {str(e)}")
            raise
    extracted_data = extraction_player_entries()
    loaded_data = load_player_entries(extracted_data)
    validation_data = validate_data(loaded_data)
raw_ingestion_player()