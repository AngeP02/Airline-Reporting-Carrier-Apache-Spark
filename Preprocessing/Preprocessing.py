import pandas as pd
import os

# Percorso della cartella dei dati
data_folder = "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/"

# Lista dei file CSV
file_paths = [
    os.path.join(data_folder, f"On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_{i}.csv")
    for i in range(1, 13)
]

# Elenco delle colonne da rimuovere
colonne_da_rimuovere = [
    "CancellationCode", "FirstDepTime", "TotalAddGTime", "LongestAddGTime", "DivReachedDest",
    "DivActualElapsedTime", "DivArrDelay", "DivDistance", "Div1Airport", "Div1AirportID",
    "Div1AirportSeqID", "Div1WheelsOn", "Div1TotalGTime", "Div1LongestGTime", "Div1WheelsOff",
    "Div1TailNum", "Div2Airport", "Div2AirportID", "Div2AirportSeqID", "Div2WheelsOn",
    "Div2TotalGTime", "Div2LongestGTime", "Div2WheelsOff", "Div2TailNum", "Div3Airport",
    "Div3AirportID", "Div3AirportSeqID", "Div3WheelsOn", "Div3TotalGTime", "Div3LongestGTime",
    "Div3WheelsOff", "Div3TailNum", "Div4Airport", "Div4AirportID", "Div4AirportSeqID",
    "Div4WheelsOn", "Div4TotalGTime", "Div4LongestGTime", "Div4WheelsOff", "Div4TailNum",
    "Div5Airport", "Div5AirportID", "Div5AirportSeqID", "Div5WheelsOn", "Div5TotalGTime",
    "Div5LongestGTime", "Div5WheelsOff", "Div5TailNum", "_c109"
]

# Cartella di output per i file puliti
output_folder = "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/"

# Assicura che la cartella di output esista
os.makedirs(output_folder, exist_ok=True)

# Carica, pulisce e salva i file
for i, file_path in enumerate(file_paths, start=1):
    print(f"Processing {file_path}...")

    df = pd.read_csv(file_path, low_memory=False)  # low_memory evita warning su colonne con tipi misti
    df_clean = df.drop(columns=[col for col in colonne_da_rimuovere if col in df.columns], errors='ignore')
    df_clean = df_clean.drop_duplicates()
    df_clean = df_clean.dropna(how='all')

    # Salva il DataFrame pulito in CSV
    output_path = os.path.join(output_folder, f"On_Time_Reporting_Carrier_{i}_clean.csv")
    df_clean.to_csv(output_path, index=False)

    print(f"Saved cleaned file: {output_path}")

# Mostra i primi 5 record del primo file pulito
print(df_clean.head())
