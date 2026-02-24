import sys
sys.path.append('.')
from api.ingestion import process_csv
risultato = process_csv("data/raw/test.csv")
print("RISULTATO:", risultato)