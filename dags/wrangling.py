import pandas as pd 

def accidents_cleaning ():
    input_path = "data/landing/accidentsVelo.csv"
    output_dir = "data/staging"
    os.makedirs(output_dir, exist_ok=True)

    # Charger les données
    print("📥 Lecture du fichier brut...")
    df = pd.read_csv(input_path)

    # Vérifier les colonnes disponibles
    print("Colonnes disponibles :", df.columns.tolist())

    # 1️⃣ Garder uniquement le département du Rhône (69)
    if "dep" in df.columns:
        df = df[df["dep"].astype(str) == "69"]
    else:
        print("⚠️ La colonne 'dep' n'existe pas dans ce dataset ! Vérifie le nom exact.")

    # 2️⃣ Supprimer les lignes sans coordonnées
    if {"lat", "long"}.issubset(df.columns):
        df = df.dropna(subset=["lat", "long"])
    elif {"latitude", "longitude"}.issubset(df.columns):
        df = df.dropna(subset=["latitude", "longitude"])

    # 3️⃣ Supprimer les doublons
    df = df.drop_duplicates()

    # 4️⃣ Sélectionner les colonnes utiles
    cols_to_keep = []
    for col in df.columns:
        if any(k in col.lower() for k in ["date", "heure", "lat", "lon", "dep", "atm", "lum", "grav", "surf", "veh"]):
            cols_to_keep.append(col)
    df = df[cols_to_keep]

    # 5️⃣ Nettoyer les valeurs manquantes ou incohérentes
    df = df.fillna({"atm": "inconnu", "lum": "inconnu", "grav": "non_precise", "surf": "inconnu"})

    # 6️⃣ Enregistrer le fichier propre dans la staging zone
    output_path = os.path.join(output_dir, "accidents_clean.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"✅ Fichier nettoyé enregistré dans : {output_path}")
    print(f"Nombre de lignes finales : {len(df)}")

