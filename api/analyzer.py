import os
from openai import OpenAI
from services.database import supabase
import json
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def run_retroactive_analysis():
    # 1. Prendi tutti gli articoli che non hanno ancora un 'tone'
    res = supabase.table("articles").select("*").is_("tone", "null").execute()
    articles = res.data or []
    
    print(f"Trovati {len(articles)} articoli da analizzare...")

    for art in articles:
        titolo = art.get('titolo', 'N/D')
        print(f"Analizzo: {titolo[:50]}...")
        
        testo = art.get('testo_completo', '')
        prompt = f"""
        Analizza questo articolo e restituisci SOLO un JSON con:
        "tone": (Positivo, Neutro, Negativo),
        "dominant_topic": (una parola, es: Energia, AI, Fisco),
        "reputational_risk": (Basso, Medio, Alto)
        
        Testo: {testo[:1500]}
        """
        
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                response_format={ "type": "json_object" }
            )
            analysis = json.loads(response.choices[0].message.content)
            
            # 2. Aggiorna il database
            art_id = art.get("id")
            if art_id:
                supabase.table("articles").update(analysis).eq("id", art_id).execute()
        except Exception as e:
            print(f"Errore su articolo {art.get('id', 'unknown')}: {e}")

if __name__ == "__main__":
    run_retroactive_analysis()