import pandas as pd 
import os
def accidents_cleaning ():
    input_path = "../data/landing/accidentsVelo.csv"
    output_dir = "../data/staging"
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

if __name__ == "__main__":
    accidents_cleaning()

