from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging

@dag(
    dag_id = 'test_postgres_connection',
    start_date = datetime(2025,1,1),
    schedule = None ,
    catchup = False,
    tags = ['test', 'postgres', 'connection'],
    description = "Test de la connexion PostgreSQL"
)

def test_postgres_connection():
    @task
    def test_connection() -> dict:
        """
        Test de la connexion PostgreSQL et verifie la table bronze
        """
        try : 
            pg_hook = PostgresHook(postgres_conn_id = 'postgres_default')

            # Test 1 Connexion basique
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            logging.info(f"✅ PostgreSQL version: {version}")

            # Test 2 Verifier que le schéma raw existe
            cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = 'raw';
            """)
            schema_exists = cursor.fetchone()
            if schema_exists:
                logging.info("✅ Schema 'Raw' existe !")
            else: 
                logging.warning("❌ Schema 'Raw' n'existe pas")
            
            # Test 3 Vérifier que la table existe
            cursor.execute("""
            SELECT EXISTS(
                        SELECT FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = 'raw'
                        AND TABLE_NAME = 'player_entries_raw'
                        );
            """)
            table_exists = cursor.fetchone()[0]
            if table_exists: 
                cursor.execute("SELECT COUNT(*) FROM raw.player_entries_raw;")
                count = cursor.fetchone()[0]
                logging.info(f"✅ Table raw.player_entries_raw trouvée avec {count} enregistrements.")
            else:
                logging.warning("❌ la table raw.player_entries_raw n'existe pas.")
            cursor.close()
            conn.close()
            
            return{
                'status': 'success',
                'version': version,
                'schema_exists': bool(schema_exists),
                'table_exists': bool(table_exists)
            }
        except Exception as e:
            logging.error(f"❌ Erreur de connexion: {str(e)}")
            raise

    
    result = test_connection()
    result
            
test_postgres_connection()