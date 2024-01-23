import pandas as pd
import mysql.connector
from datetime import datetime

# Établir la connexion à la base de données
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='associationrules'
)

# Définir la date de référence
date_reference = datetime(2023, 10, 1)  # La date de référence souhaitée

# Requête pour obtenir les données de transactions
query_transactions = "SELECT client, date_transaction FROM Transactionstest20"
df = pd.read_sql_query(query_transactions, conn)

# Convertir les dates de transaction au format datetime
df['date_transaction'] = pd.to_datetime(df['date_transaction'])

# Insérer la date de référence après la colonne 'date_transaction'
df.insert(loc=2, column='date_reference', value=date_reference)

# Calculer la dernière date de transaction par client
derniere_date_transaction = df.groupby('client')['date_transaction'].max().reset_index()

# Calculer la fréquence des transactions par client
frequence_par_client = df.groupby('client').size().reset_index(name='frequence')

# Calculer la récence pour chaque transaction
df['recence'] = (df['date_reference'] - df['date_transaction']).dt.days

# Obtenir la récence minimale par client
recence_par_client = df.groupby('client')['recence'].min().reset_index()

# Fusionner les données pour obtenir récence et la dernière date de transaction par client
recence_par_client = recence_par_client.merge(derniere_date_transaction, on='client', how='left')

# Fusionner pour obtenir récence et fréquence par client
recence_frequence_par_client = recence_par_client.merge(frequence_par_client, on='client', how='left')

# Requête pour obtenir d'autres données de transactions
query_monetaire = "SELECT client, qte, cattc, margedpr FROM Transactionstest16"
transactions = pd.read_sql_query(query_monetaire, conn)

# Calculer la valeur monétaire par client
result = transactions.groupby('client').apply(lambda x: (x['qte'] * x['cattc'] - x['margedpr']).sum()).reset_index(name='monetaire')

# Fusionner pour obtenir récence, fréquence et valeur monétaire par client
recence_frequence_monetaire = recence_frequence_par_client.merge(result, on='client', how='left')

# Ajouter la date de référence à côté de la dernière date de transaction pour chaque client
recence_frequence_monetaire.insert(loc=3, column='date_reference', value=date_reference)

# Afficher le DataFrame résultant
print(recence_frequence_monetaire)

# Sauvegarder le résultat dans un fichier CSV
recence_frequence_monetaire.to_csv('resultat_RFM1.csv', index=False)

# Fermer la connexion à la base de données
conn.close()
