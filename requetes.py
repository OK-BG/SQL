########################################################################################################################################
########################################    PROJET INSEE               ##################################################################
########################################    Autheur : Tom Bourachot    ##################################################################
########################################              Marion Turgault  ##################################################################
########################################    Date : 18/02/2024          ##################################################################
#########################################################################################################################################

### IMPORTATION DES LIBRAIRIES
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# Fonction pour se connecter a la base de donnee avec sqlAlchemy
def connect_to_db(username, password):
    print('Connexion a la base de donnees...')
    try:
        engine = create_engine(f'postgresql://{username}:{password}@pgsql/{username}')
        Session = sessionmaker(bind=engine)
        session = Session()
        print("Connecte a la base de donnees")
        return session
    except Exception as e:
        print("Connexion impossible a la base de donnees: " + str(e))
        return None

# Fonction pour executer les requetes
def execute_query(session, query, description):
    print(description)
    try:
        result = session.execute(query)  # execution de la requete
        df = pd.DataFrame(result.fetchall(), columns=result.keys()) # stockage des resultats dans un dataframe pour une meilleure manipulation des donnees
        print(df.to_string(index=False)) # Affichage du dataframe
        print("\n")
        return df
    except Exception as e:  # gestion des erreurs avec exept
        print(f"Erreur lors de l'execution de la requÃªte : {e}")  
        return pd.DataFrame()

### CONNEXION A LA BASE DE DONNEES
USERNAME="mturgault"
PASSWORD="Azerty@123!" 
session = connect_to_db(USERNAME, PASSWORD)



################    INTERROGATION DE LA BASE DE DONNEES    #################################
#Si une connection a la base de donnee a ete trouve, on lance les requÃªtes
if session: 

    ### REQUETE 1 
    query1 = text("""
        SELECT D.libelle AS nom_departement
        FROM departement D
        JOIN indice_social S ON D.dep = S.Departement
        AND LOWER(S.Indicateur) = LOWER('Pauvrete')
        AND S.Valeur >= 15 
        AND S.Valeur <= 20
        AND S.Annee = 2018
        ORDER BY S.Valeur DESC;
    """)
    df1 = execute_query(session, query1, "REQUETE 1 : Departements ou le taux de pauvrete en 2018 etait compris entre 15% et 20 % (par ordre decroissant) : ")
    
    


    ### REQUETE 2 
    query2 = text("""
        SELECT D.libelle AS nom_departement, E2.Valeur AS taux_activite_2017
        FROM departement D

        JOIN indice_economie E1 ON D.reg = E1.Region
            AND E1.Annee = 2014
            AND LOWER(E1.Indicateur) = LOWER('Recherche_Developpement')

        JOIN indice_economie E2 ON D.dep = E2.Departement
            AND E2.Annee = 2017
            AND LOWER(E2.Indicateur) = LOWER('Taux_Activite')

        WHERE E1.Valeur > 2;
    """)
    df2 = execute_query(session, query2, "REQUETE 2 : Departements dont la region avait un effort de recherche et developpement strictement superieur a 2 % en 2014 : ")
    

    
    ### REQUETE 3 
    query3 = text("""
        SELECT D.libelle AS nom_departement, (SH.Valeur - SF.Valeur) AS difference_esperance_vie
        FROM departement D
        JOIN (SELECT Region, Valeur
            FROM indice_social
            WHERE Region NOTNULL
            AND Annee = 2018 AND Indicateur = 'Pauvrete' AND Valeur NOTNULL
            ORDER BY Valeur DESC LIMIT 1) AS MaxPauvrete 
            ON D.reg = MaxPauvrete.Region

        JOIN indice_social SH ON D.dep = SH.Departement
            AND SH.Annee = 2019
            AND LOWER(SH.Indicateur) = LOWER('Esperence_de_vie_Homme')

        JOIN indice_social SF ON D.dep = SF.Departement
            AND SF.Annee = 2019
            AND LOWER(SF.Indicateur) = LOWER('Esperence_de_vie_Femme');

    """)
    df3 = execute_query(session, query3, "REQUETE 3 : Difference entre l'esperance de vie des hommes et des femmes en 2019 pour tous les departements de la region ayant le plus grand taux de pauvrete en 2018 : ")
    

   

    ### REQUETE 4
    query4 = text("""
        SELECT D.libelle AS nom_departement, P.Valeur AS estimation_population_2020
        FROM departement D
        JOIN indice_social S ON D.dep = S.Departement
            AND S.Annee = 2020
            AND S.Indicateur = 'Disparite'

        JOIN population P ON D.dep = P.Departement
            AND P.Annee = 2020

        WHERE S.Valeur >= 3.5;
    """)
    df4 = execute_query(session, query4, "REQUETE 4 : Estimation de population en 2020 de tous les departements ou la disparite de niveau de vie >=3,5 (en 2020 aussi) : ")
    
    

    ### REQUETE 5 
    query5 = text("""
        SELECT R.libelle AS nom_region,
        S_hommes_2019.Valeur AS esperance_vie_hommes_2019,
        S_femmes_2019.Valeur AS esperance_vie_femmes_2019,
        S_hommes_2022.Valeur AS esperance_vie_hommes_2022,
        S_femmes_2022.Valeur AS esperance_vie_femmes_2022

        FROM region R

        JOIN indice_economie E ON R.reg = E.Region
            AND E.Annee = 2019
            AND LOWER(E.Indicateur) = LOWER('Taux_Activite')

        JOIN indice_social S ON R.reg = S.Region
            AND S.Annee = 2021
            AND LOWER(S.Indicateur) = LOWER('Eloignement_7mn_Sante')

        JOIN indice_social S_hommes_2019 ON R.reg = S_hommes_2019.Region
            AND S_hommes_2019.Annee = 2019
            AND LOWER(S_hommes_2019.Indicateur) = LOWER('Esperence_de_vie_Homme')

        JOIN indice_social S_femmes_2019 ON R.reg = S_femmes_2019.Region
            AND S_femmes_2019.Annee = 2019
            AND LOWER(S_femmes_2019.Indicateur) = LOWER('Esperence_de_vie_Femme')

        JOIN indice_social S_hommes_2022 ON R.reg = S_hommes_2022.Region
            AND S_hommes_2022.Annee = 2022
            AND LOWER(S_hommes_2022.Indicateur) = LOWER('Esperence_de_vie_Homme')

        JOIN indice_social S_femmes_2022 ON R.reg = S_femmes_2022.Region
            AND S_femmes_2022.Annee = 2022
            AND LOWER(S_femmes_2022.Indicateur) = LOWER('Esperence_de_vie_Femme')

        WHERE E.Valeur > 75 AND S.Valeur < 5;

    """)
    df5 = execute_query(session, query5, "REQUETE 5 : Esperance de vie des femmes et des hommes en 2019 et en 2022 dans les regions ou le taux d'activite etait >75% en 2019 et ou la Part de la population eloignee de plus de 7 mn des services de sante de proximite etait de moins de 5% en 2021 : ")
    


################   FERMER LA CONNEXION    #################################
    session.close()
    print("La connexion PostgreSQL est fermee")
else:
    print("Impossible de se connecter a la base de donnees.")