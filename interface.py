#########################################################################################################################################
########################################    PROJET INSEE               ##################################################################
########################################    Autheur : Tom Bourachot    ##################################################################
########################################              Marion Turgault  ##################################################################
########################################    Date : 19/02/2024          ##################################################################
#########################################################################################################################################

### IMPORTATION DES LIBRAIRIES
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# Définir le chemin de la base de données SQLite
db_path = "insee.db"

# Créer une connexion SQLAlchemy
engine = create_engine(f"sqlite:///{db_path}")

# Créer une session
Session = sessionmaker(bind=engine)
session = Session()

    
    
### FONCTION D'EXECUTION DES REQUETES
def execute_query(session, query, params=None):
    try:
        if params:
            result = session.execute(query, params)
        else:
            result = session.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        print(f"Erreur lors de l'execution de la requete : {e}")
        return pd.DataFrame()
    

### FONCTION POUR ELIMINER LES ACCENTS D'UN MOT
def accent(str):
    tableAccent = {
    'àâãäåáǎăąā': 'a',
    'çćčĉċ': 'c',
    'èéêëěėęē': 'e',
    'îïíīįì': 'i',
    'ôöòóõøōő': 'o',
    'ùûüúūųů': 'u',
    'ÿýŷ': 'y',
    'ñńņň': 'n',
    'šśşŝ': 's',
    'žźż': 'z',
    'ðđ': 'd',
    'ł': 'l',
    'ß': 'ss'
}

    motSansAccents = ''
    for i in str.lower():
        for k, v in tableAccent.items():
            if i in k: 
                i = v
                break
        motSansAccents += i
    return motSansAccents
    
### FONCTIONS POUR CHAQUE CHOIX DU MENU PRINCIPAL
def afficher_regions(session):
    query = text("SELECT libelle AS nom_region, reg AS code_region FROM region ORDER BY reg")
    df = execute_query(session, query)
    return df


def afficher_departements(session, region_choisie):
    query = text("SELECT libelle AS nom_departement, dep AS code_departement FROM Departement WHERE reg = :region ORDER BY dep")
    df = execute_query(session, query, {'region':region_choisie})
    return df


def afficher_annees(session, departement_choisi) :
    query = text(f"""
        SELECT DISTINCT Annee 
        FROM population 
        WHERE {departement_choisi} = Departement 
        ORDER BY Annee
    """)
    df = execute_query(session, query, {'departement': departement_choisi})

    if df.empty:
        print("Donnees non disponibles")
        return
    print(f"\nAnnees disponibles pour le département {departement_choisi} :\n {df['Annee'].tolist()}")


def afficher_population(session, departement, annee):
    query = text(f"""
        SELECT * 
        FROM population 
        WHERE Departement = {departement} AND Annee = {annee}
    """)
    df = execute_query(session, query, {'departement': departement, 'annee': annee})
    if df.empty:
        print("Donnees non disponibles")
    else:
        print(df)


def afficher_donnees_theme(session, departement, annee, theme):
    if theme == 'social':
        table = 'indice_social'  
    else :
        table = 'indice_economie'
    
    query = text(f"""
        SELECT indicateur, valeur 
        FROM {table} 
        WHERE Departement = {departement} AND Annee = {annee}
    """)
    df = execute_query(session, query, {'departement': departement, 'annee': annee})

    if df.empty:
        print("Aucun indicateur trouvé pour ce département et cette année.")
        return

    print(f"\nIndicateurs pour le département {departement} en {annee} :\n")
    for _, row in df.iterrows():
        print(f"{row['Indicateur']} : {row['Valeur']}%.")
    
    
    
### LES FONCTIONS D'AFFICHAGE
def menu():
    print("-------------------------------------------------------")
    print("\nMenu :")
    print("1 - Afficher la liste des regions")
    print("2 - Choisir une region et afficher la liste de ses departements")
    print("3 - Choisir un departement et afficher les annees disponibles")
    print("4 - Afficher les informations de population pour une annee")
    print("5 - Afficher les donnees sociales ou economiques")
    print("0 - Quit")
    print("-------------------------------------------------------")
    
    

def menu_principal():
    print("Bienvenue")
    # Connection a la base de donnee
    """session = connect_to_db("mturgault", "Azerty@123!")
    if not session:
        print("Impossible de se connecter a la base de donnee")
        return None"""
    
    #initialisation des variables
    region_choisie = None
    departement_choisi = None
    annee_choisie = None
    
    while True:
        menu()
        choix=input("Veuillez saisir votre choix (de 0 a 5): ")
        while choix not in ["0", "1", "2", "3", "4", "5"]:
            choix=input("Veuillez saisir votre choix (de 0 a 5): ")
        choix = int(choix)

        if choix == 0:
            break
        
        elif choix == 1:
            print("Voici la liste des regions :\n" , afficher_regions(session))
        
        elif choix == 2:
            while True:
                region = input("Souhaitez vous afficher la liste des regions (o/n) ? ")
                if region == 'o' :
                    print(afficher_regions(session))
                    break
                if region == 'n' :
                    break

            region_choisie = input("Entrez le code de la region : ")
            print("Voici la liste des departements associes a la region ", region_choisie, " : \n", afficher_departements(session, region_choisie))
        
        elif choix == 3:
            if not region_choisie:
                print("Veuillez d'abord choisir une region  (choix n°2).")
                continue
            while True:
                departement = input("Souhaitez vous afficher la liste des departements (o/n) ? ")                
                if departement == 'o' :
                    print("Liste des departements de la region ", region_choisie, " : \n", afficher_departements(session, region_choisie))
                    break
                if departement == 'n' :
                    break

            departement_choisi = input("Entrez le code du departement : ")
            print("Les annees disponibles sont : ")
            afficher_annees(session, departement_choisi)
            
        elif choix == 4:
            if not region_choisie:
                print("Veuillez d'abord choisir une region  (choix n°2).")
                continue
            if not departement_choisi:
                print("Veuillez d'abord choisir un departement (choix n°3).")
                continue

            while True:
                annee = input("Souhaitez vous afficher la liste des annees disponibles (o/n) ? ")
                if annee == 'o' :
                    afficher_annees(session, departement_choisi)
                    break
                if annee == "n":
                    break

            annee_choisie = input("Entrez l'annee: ")
            afficher_population(session, departement_choisi, annee_choisie)
        
        elif choix == 5:
            if not departement_choisi:
                print("Veuillez d'abord choisir un departement (choix n°3).")
                continue
            if not annee_choisie:
                print("Veuillez d'abord choisir une annee (choix n°4).")
                continue
            theme = input("Choisissez le theme (social/economique): ")
            while theme not in ["social", "economique"]:
                theme = input("Veuillez choisir un theme valide (social/economique): ")
            themeNorm = accent(theme)
            afficher_donnees_theme(session, departement_choisi, annee_choisie, themeNorm)
        
    
    # Fermeture de la session
    session.commit()
    session.close()
    print("Au revoir!")


### EXECUTION DU PROGRAMME
menu_principal()
