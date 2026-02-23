import os
import time
from openai import OpenAI
from services.database import supabase
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Configurazione
BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 2
MODEL = "text-embedding-ada-002"

def get_articles_without_embedding(limit=BATCH_SIZE):
    try:
        response = supabase.table("articles") \
            .select("id, titolo, occhiello, sottotitolo, testo_completo, macrosettori, dominant_topic") \
            .is_("embedding", "null") \
            .limit(limit) \
            .execute()
        return response.data
    except Exception as e:
        print(f"Errore nel recupero articoli: {e}")
        return []

def update_embedding(article_id, embedding_vector):
    try:
        supabase.table("articles") \
            .update({"embedding": embedding_vector}) \
            .eq("id", article_id) \
            .execute()
        return True
    except Exception as e:
        print(f"Errore aggiornamento ID {article_id}: {e}")
        return False

def generate_embedding(text):
    if not text or len(text.strip()) == 0:
        text = "nessun contenuto"
    try:
        # Nuova sintassi per openai>=1.0.0
        response = client.embeddings.create(
            model=MODEL,
            input=text[:8000]
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"Errore generazione embedding: {e}")
        return None

def main():
    print("🔍 Avvio generazione embedding per articoli senza embedding...")
    total_processed = 0
    while True:
        articles = get_articles_without_embedding(limit=BATCH_SIZE)
        if not articles:
            print("✅ Nessun altro articolo da processare.")
            break

        print(f"📄 Processati {len(articles)} articoli (totale finora: {total_processed})")
        for art in articles:
            text_parts = [
                art.get('titolo', ''),
                art.get('occhiello', ''),
                art.get('sottotitolo', ''),
                art.get('testo_completo', ''),
                art.get('macrosettori', ''),
                art.get('dominant_topic', '')
            ]
            full_text = " ".join([p for p in text_parts if p])
            if not full_text.strip():
                full_text = "articolo senza testo"

            embedding = generate_embedding(full_text)
            if embedding:
                if update_embedding(art['id'], embedding):
                    print(f"  ✅ ID {art['id']} aggiornato")
                else:
                    print(f"  ❌ ID {art['id']} fallito")
            else:
                print(f"  ❌ ID {art['id']} embedding non generato")

            time.sleep(0.5)

        total_processed += len(articles)
        print(f"⏳ Pausa di {SLEEP_BETWEEN_BATCHES} secondi...")
        time.sleep(SLEEP_BETWEEN_BATCHES)

    print(f"\n🏁 Fatto! Totale articoli processati: {total_processed}")

if __name__ == "__main__":
    main()