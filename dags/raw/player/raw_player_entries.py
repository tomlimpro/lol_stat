from datetime import datetime, timedelta
import requests
import logging
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook