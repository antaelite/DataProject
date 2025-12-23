import pandas as pd 
import os

def accidents_cleaning (input_path, output_path):
    """
    Nettoie les données d'accidents.
    """
    print(f"--- Traitement Accidents : {input_path} ---")

    if not os.path.exists(input_path):
        print(f"SKIPPING: Fichier introuvable {input_path}. Lancer l'ingestion d'abord.")
        return
    
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    print("Lecture du fichier brut...")
    df = pd.read_csv(input_path)
    if "dep" in df.columns:
        df = df[df["dep"].astype(str) == "69"]
    else:
        print("La colonne 'dep' n'existe pas dans ce dataset ! Vérifie le nom exact.")

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["long"] = pd.to_numeric(df["long"], errors="coerce")
    df = df.dropna(subset=["lat", "long"])
    df = df[(df["lat"] != 0) & (df["long"] != 0)]

    df = df.drop_duplicates()

    keep_columns = []
    cols_to_keep = ["Num_Acc", "grav", "date", "an", "mois", "jour", "hrmn", "com", "lat", "long"]
    keep_columns = [col for col in df.columns if col in cols_to_keep]

    df = df[keep_columns]

    output_path = os.path.join(output_dir, "accidents_clean_lyon.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f" Fichier nettoyé enregistré dans : {output_path}")
    print(f"Nombre de lignes finales : {len(df)}")


def clean_velov_data(input_path, output_path):
    """
    Nettoie les données Vélo'v.
    """
    print(f"--- Traitement Vélo'v : {input_path} ---")
    
    if not os.path.exists(input_path):
        print(f"ERREUR : Fichier introuvable {input_path}. Lancer l'ingestion d'abord.")
        return

    df = pd.read_csv(input_path, sep=",") # Vérifie si c'est , ou ;

    # 1. Types
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lng'] = pd.to_numeric(df['lng'], errors='coerce')
    df['available_bikes'] = pd.to_numeric(df['available_bikes'], errors='coerce').fillna(0).astype(int)
    
    # 2. Filtres
    df = df.dropna(subset=['lat', 'lng'])
    
    if 'status' in df.columns:
        df = df[df['status'] == 'OPEN']

    # 3. Dates
    if 'last_update' in df.columns:
        df['last_update'] = pd.to_datetime(df['last_update'], errors='coerce')

    # 4. Sauvegarde
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"-> Sauvegardé : {output_path} ({len(df)} stations)")



if __name__ == "__main__":
    print("Démarrage du script de Wrangling...")

    # Définir BASE_DIR si non fourni (utile pour exécution en local)
    BASE_DIR = globals().get("BASE_DIR") or os.environ.get("BASE_DIR") or os.getcwd()

    # 1. Définir les chemins d'entrée (Landing)
    accidents_raw = os.path.join(BASE_DIR, "landing", "accidentsVelo.csv")
    velov_raw = os.path.join(BASE_DIR, "landing", "velov_raw.csv")

    # 2. Définir les chemins de sortie (Staging)
    accidents_clean_path = os.path.join(BASE_DIR, "staging", "accidents_clean_lyon.csv")
    velov_clean_path = os.path.join(BASE_DIR, "staging", "velov_clean.csv")

    # 3. Exécuter les fonctions séquentiellement
    # On gère les exceptions pour qu'une erreur sur l'un ne bloque pas forcément l'autre
    try:
        accidents_cleaning(accidents_raw, accidents_clean_path)
    except Exception as e:
        print(f"Erreur sur Accidents: {e}")

    try:
        clean_velov_data(velov_raw, velov_clean_path)
    except Exception as e:
        print(f"Erreur sur Vélo'v: {e}")

    print("Fin du Wrangling.")
