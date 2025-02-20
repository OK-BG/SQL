import pandas as pd
import psycopg2
import psycopg2.extras
import sqlite3
import numpy as np

# -------------------------- #

db_path = "insee.db"
conn = sqlite3.connect(db_path)
cur = conn.cursor()


# ------------------ TABLES REGION ET DEPARTEMENT ------------------------------ #

def detect_sql_type(series):
    """Retourne le type SQL correspondant a une colonne pandas."""
    if pd.api.types.is_integer_dtype(series):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(series):
        return "REAL"
    else:
        return "TEXT"

def generate_create_table_sql(file_path, table_name, primary_key):
    """GenÃ¨re et execute une requÃªte CREATE TABLE et retourne les colonnes valides pour l'insertion."""
    df = pd.read_csv(file_path, sep=',')
    df.columns = df.columns.str.lower()

    columns_definitions = []
    valid_columns = []

    for column in df.columns:
        sql_type = detect_sql_type(df[column])
        column_def = f'"{column}" {sql_type}'
        if column == primary_key and sql_type == "INTEGER":
            column_def += " PRIMARY KEY"
        columns_definitions.append(column_def)
        valid_columns.append(column)

    columns_sql = ",\n    ".join(columns_definitions)

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_sql}\n);"
    cur.execute(create_table_sql)
    conn.commit()

    print(f"Table {table_name} creee avec succes.")

    return df[valid_columns], valid_columns  # Retourne uniquement les colonnes filtrees

# Correspondance fichiers -> noms de tables SQL + cle primaire
files = {
    "departement2020.csv": ("departement", "dep"),
    "region2020.csv": ("region", "reg"),
}

# Creation des tables et insertion des donnees
for file_path, (table, primary_key) in files.items():
    df_filtered, valid_columns = generate_create_table_sql(file_path, table, primary_key)

    if df_filtered is not None and not df_filtered.empty:
        # Generation de la requÃªte d'insertion avec les colonnes valides
        columns_sql = ", ".join([f'"{col}"' for col in valid_columns])
        values_placeholder = ", ".join(["?"] * len(valid_columns))
        insert_query = f"INSERT INTO {table} ({columns_sql}) VALUES ({values_placeholder})"

        # Execution des insertions
        cur.executemany(insert_query, df_filtered.itertuples(index=False, name=None))

        conn.commit()
        print(f"{len(df_filtered)} enregistrement(s) insere(s) dans la table {table}.")



# ------------------ TABLES INDICATEUR ET POPULATION ------------------------------ #

# -- Feuille indicateur --

file_path = "DD-indic-reg-dep_2008_2019_2022.xls"
xls = pd.ExcelFile(file_path)

economie = pd.read_excel(xls, sheet_name="Economie")
reg_economie = economie.iloc[5:23, :]
dep_economie = economie.iloc[33:135, :]

social = pd.read_excel(xls, sheet_name="Social")
reg_social = social.iloc[5:23, :]
dep_social = social.iloc[31:133, :]


# -- Feuille population --

file_path = "Evolution_population_2012-2023.xlsx"
xls = pd.ExcelFile(file_path)

dep_population = pd.read_excel(xls, sheet_name="DEP", skiprows=3)
reg_population = pd.read_excel(xls, sheet_name="REG", skiprows=3)

dep_population = dep_population.iloc[:102, :]
reg_population = reg_population.iloc[:19, :]

# ---------------------------------- #

# -- Renommage des colonnes des tableaux

reg_economie.columns = ["Id", "Id_Geo", "Nom", "Taux_Activite_2019", "Taux_Activite_2017", "Taux_Emplois_2014", 
                               "Taux_Emplois_2009", "Part_Diplomes_2017", "Part_Jeunes_Diplomes_2014", "Part_Jeunes_Diplomes_2009", "Transport_Voiture_2014", 
                               "Transport_Voiture_2009", "Transport_en_Commun_2014", "Transport_en_Commun_2009", "Transport_Autre_Moyen_2014", "Transport_Autre_Moyen_2009", 
                               "Poids_Economie_Emploi_2015", "Recherche_Developpement_2014", "Recherche_Developpement_2010"]

dep_economie.columns = ["Id", "Id_Geo", "Nom", "Taux_Activite_2019", "Taux_Activite_2017", 
                           "Taux_Emplois_2014", "Taux_Emplois_2009", "Part_Diplomes_2017", 
                           "Part_Jeunes_Diplomes_2014", "Part_Jeunes_Diplomes_2009", "Transport_Voiture_2014", 
                           "Transport_Voiture_2009", "Transport_en_Commun_2014", "Transport_en_Commun_2009", 
                           "Transport_Autre_Moyen_2014", "Transport_Autre_Moyen_2009", 
                           "Poids_Economie_Emploi_2015", "Recherche_Developpement_2014", "Recherche_Developpement_2010"]



reg_social.columns = ["Id_Geo", "Nom", "Esperence_de_vie_Homme_2022", "Esperence_de_vie_Homme_2019", "Esperence_de_vie_Homme_2015", "Esperence_de_vie_Homme_2010", 
                             "Esperence_de_vie_Femme_2022", "Esperence_de_vie_Femme_2019", "Esperence_de_vie_Femme_2015", "Esperence_de_vie_Femme_2010", "Disparite_2020", 
                             "Disparite_2014", "Pauvrete_2018", "Pauvrete_2014", "Jeunes_non_Inseres_2017", "Jeunes_non_Inseres_2014", 
                             "Jeunes_non_Inseres_2009", "Eloignement_7mn_Sante_2021", "Eloignement_7mn_Sante_2016", "Zone_Inondee_2013", "Zone_Inondee_2008"]

dep_social.columns = ["Id_Geo", "Nom", "Esperence_de_vie_Homme_2022", "Esperence_de_vie_Homme_2019", "Esperence_de_vie_Homme_2015", "Esperence_de_vie_Homme_2010", 
                                  "Esperence_de_vie_Femme_2022", "Esperence_de_vie_Femme_2019", "Esperence_de_vie_Femme_2015", "Esperence_de_vie_Femme_2010", "Disparite_2020", 
                                  "Disparite_2014", "Pauvrete_2018", "Pauvrete_2014", "Jeunes_non_Inseres_2017", "Jeunes_non_Inseres_2014", 
                                  "Jeunes_non_Inseres_2009", "Eloignement_7mn_Sante_2021", "Eloignement_7mn_Sante_2016", "Zone_Inondee_2013", "Zone_Inondee_2008"]


dep_population.columns = ["Id_Geo", "Nom", "Population_2012", "Population_2017", "Population_2018", "Estimation_2015", 
                  "Estimation_2020", "Estimation_2023", "Variation_annuelle_totale_2020-2023", "Variation_naturel_2020-2023",
                  "Variation_migration_2020-2023", "Variation_annuelle_totale_2015-2020", "Variation_naturel_2015-2020",
                  "Variation_migration_2015-2020", "Variation_annuelle_totale_2010-2015", "Variation_naturel_2010-2015",
                  "Variation_migration_2010-2015"]


reg_population.columns = ["Id_Geo", "Nom", "Population_2012", "Population_2017", "Estimation_2020", "Estimation_2023",
                  "Variation_annuelle_totale_2012-2017", "Variation_naturel_2012-2017", "Variation_migration_2012-2017",
                  "Variation_annuelle_totale_2020-2023", "Variation_naturel_2020-2023", "Variation_migration_2020-2023"]


# ---------------------------------- #

dep_social = dep_social.copy()
reg_social = reg_social.copy()
dep_economie = dep_economie.copy()
reg_economie = reg_economie.copy()
dep_population = dep_population.copy()
reg_population = reg_population.copy()

dep_social.loc[:, "Type_Geo"] = "Departement"
reg_social.loc[:, "Type_Geo"] = "Region"
dep_economie.loc[:, "Type_Geo"] = "Departement"
reg_economie.loc[:, "Type_Geo"] = "Region"

dep_variation = pd.concat([dep_population.iloc[:, :2], dep_population.iloc[:, 8:]], axis=1)
dep_population = dep_population.iloc[:, :8] 

reg_variation = pd.concat([reg_population.iloc[:, :2], reg_population.iloc[:, 8:]], axis=1)
reg_population = reg_population.iloc[:, :6] 


dep_population.loc[:, "Type_Geo"] = "Departement"
dep_variation.loc[:, "Type_Geo"] = "Departement"
reg_population.loc[:, "Type_Geo"] = "Region"
reg_variation.loc[:, "Type_Geo"] = "Region"

# ---------------------------------- #

# Transformation et assemblage des dataframes

def assemblage_dataframe(dataframe):
    return dataframe.melt(
        id_vars=["Id_Geo", "Nom", "Type_Geo"],
        var_name="Indicateur",
        value_name="Valeur"
    )

# Concaténer deux DataFrames
def concat_dataframe(df_1, df_2):
    return pd.concat([df_1, df_2], ignore_index=True)

# Remplacer les valeurs manquantes par None
def remove_null(dataframe):
    dataframe = dataframe.replace(["N/A - résultat non disponible", "nd"], np.nan)
    return dataframe


# Transformation des DataFrames sociaux et économiques
dep_long = assemblage_dataframe(dep_social)
reg_long = assemblage_dataframe(reg_social)
socials_df = concat_dataframe(dep_long, reg_long)
socials_df = remove_null(socials_df)

dep_long = assemblage_dataframe(dep_economie)
reg_long = assemblage_dataframe(reg_economie)
economies_df = concat_dataframe(dep_long, reg_long)
economies_df = remove_null(economies_df)

dep_long = assemblage_dataframe(dep_population)
reg_long = assemblage_dataframe(reg_population)
population_df = concat_dataframe(dep_long, reg_long)
population_df = remove_null(population_df)

dep_long = assemblage_dataframe(dep_variation)
reg_long = assemblage_dataframe(reg_variation)
variation_df = concat_dataframe(dep_long, reg_long)
variation_df = remove_null(variation_df)

# ---- Creation des tables et insertion ----

def create_table(table_name, cur):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Region CHAR(5),
        Departement CHAR(5),
        Indicateur VARCHAR(50),
        Annee INT NOT NULL,
        Valeur REAL,
        FOREIGN KEY (Region) REFERENCES region(reg),
        FOREIGN KEY (Departement) REFERENCES departement(dep)
    );
""")


def insert_elements(dataframe, table_name, cur):
    def process_row(row):
        indicateur = row.Indicateur
        for i in range(len(indicateur)):
            if indicateur[i] == '2':
                annee = indicateur[i:]
                indicateur = indicateur[:i-1]
                return (row.Id_Geo, row.Type_Geo, indicateur, annee, row.Valeur)
        return None

    data = [result for row in dataframe.itertuples(index=False) if (result := process_row(row)) is not None]

    if data:
        for result in data:
            if result[1] == "Region":  # result[1] correspond à Type_Geo
                command = f'INSERT INTO {table_name} (Region, Departement, Indicateur, Annee, Valeur) VALUES (?,NULL,?,?,?)'
            else:
                command = f'INSERT INTO {table_name} (Region, Departement, Indicateur, Annee, Valeur) VALUES (NULL,?,?,?,?)'
            
            if result[2] == "Population":
                values = (result[0], result[2], result[3], result[4]*1000) # (Dep/Reg, Indicateur, Annee, Valeur)
            else:
                values = (result[0], result[2], result[3], result[4])  
            cur.execute(command, values)


# Dictionnaire des tables
df_table = [
    (socials_df, "indice_social"),
    (economies_df, "indice_economie"),
    (population_df, "population"),
    (variation_df, "variation")
]

for dataframe, table in df_table:
    create_table(table, cur)
    insert_elements(dataframe, table, cur)

# ----------------------- #

# Fermeture de la session

conn.commit()
conn.close() 

print("Données insérées avec succès dans SQLite.")
print(f"Base de données créée : {db_path}")