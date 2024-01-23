import pandas as pd
import mysql.connector
from datetime import datetime

# �tablir la connexion � la base de donn�es
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='associationrules'
)

# D�finir la date de r�f�rence
date_reference = datetime(2023, 10, 1)  # La date de r�f�rence souhait�e

# Requ�te pour obtenir les donn�es de transactions
query_transactions = "SELECT client, date_transaction FROM Transactionstest20"
df = pd.read_sql_query(query_transactions, conn)

# Convertir les dates de transaction au format datetime
df['date_transaction'] = pd.to_datetime(df['date_transaction'])

# Ins�rer la date de r�f�rence apr�s la colonne 'date_transaction'
df.insert(loc=2, column='date_reference', value=date_reference)

# Calculer la derni�re date de transaction par client
derniere_date_transaction = df.groupby('client')['date_transaction'].max().reset_index()

# Calculer la fr�quence des transactions par client
frequence_par_client = df.groupby('client').size().reset_index(name='frequence')

# Calculer la r�cence pour chaque transaction
df['recence'] = (df['date_reference'] - df['date_transaction']).dt.days

# Obtenir la r�cence minimale par client
recence_par_client = df.groupby('client')['recence'].min().reset_index()

# Fusionner les donn�es pour obtenir r�cence et la derni�re date de transaction par client
recence_par_client = recence_par_client.merge(derniere_date_transaction, on='client', how='left')

# Fusionner pour obtenir r�cence et fr�quence par client
recence_frequence_par_client = recence_par_client.merge(frequence_par_client, on='client', how='left')

# Requ�te pour obtenir d'autres donn�es de transactions
query_monetaire = "SELECT client, qte, cattc, margedpr FROM Transactionstest16"
transactions = pd.read_sql_query(query_monetaire, conn)

# Calculer la valeur mon�taire par client
result = transactions.groupby('client').apply(lambda x: (x['qte'] * x['cattc'] - x['margedpr']).sum()).reset_index(name='monetaire')

# Fusionner pour obtenir r�cence, fr�quence et valeur mon�taire par client
recence_frequence_monetaire = recence_frequence_par_client.merge(result, on='client', how='left')

# Ajouter la date de r�f�rence � c�t� de la derni�re date de transaction pour chaque client
recence_frequence_monetaire.insert(loc=3, column='date_reference', value=date_reference)

# Afficher le DataFrame r�sultant
print(recence_frequence_monetaire)

# Sauvegarder le r�sultat dans un fichier CSV
recence_frequence_monetaire.to_csv('resultat_RFM1.csv', index=False)

# Fermer la connexion � la base de donn�es
conn.close()
