import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Charger les variables d'environnement
env_path = '../.env' if os.path.exists('../.env') else '.env'
load_dotenv(env_path)

DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_NAME = os.getenv('POSTGRES_DB')
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', 5432)

# Connexion à la base de données
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
cur = conn.cursor()

# Création de la table si elle n'existe pas
cur.execute("""
CREATE TABLE IF NOT EXISTS distribution_purchases (
    transaction_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    purchase_amount NUMERIC,
    purchase_date DATE,
    product_category VARCHAR(255),
    product_rating NUMERIC,
    return_date DATE
)
""")
conn.commit()

# Charger le fichier CSV
df = pd.read_csv('input_data/distribution_purchases.csv')

# Remplacer les NaN par None pour les valeurs manquantes
df = df.where(pd.notnull(df), None)

# Insertion idempotente (upsert)
for _, row in df.iterrows():
    cur.execute(
        """
        INSERT INTO distribution_purchases (
            transaction_id, customer_id, purchase_amount, purchase_date,
            product_category, product_rating, return_date
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            purchase_amount = EXCLUDED.purchase_amount,
            purchase_date = EXCLUDED.purchase_date,
            product_category = EXCLUDED.product_category,
            product_rating = EXCLUDED.product_rating,
            return_date = EXCLUDED.return_date
        """,
        (
            row['transaction_id'],
            row['customer_id'],
            row['purchase_amount'],
            row['purchase_date'],
            row['product_category'],
            row['product_rating'],
            row['return_date']
        )
    )
conn.commit()

cur.close()
conn.close()
print("Table créée et données importées de façon idempotente !") 