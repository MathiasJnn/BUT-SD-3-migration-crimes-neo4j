#MIGRATION SQLITE VERS NEO4J : STRATÉGIE DE GRAPHÉISATION
#==========================================================
#Ce script transforme une base relationnelle classique en un modèle de graphe pour faciliter l'analyse des liens entre services de police/gendarmerie et types d'infractions.

#Démarche technique :
#SÉCURISATION DU SCHÉMA : Création de contraintes d'unicité (CREATE CONSTRAINT) pour garantir l'intégrité des nœuds et accélérer les futures recherches.

#EXTRACTION & INSERTION DES ENTITÉS : Parcours des tables SQLite (DEPARTEMENT, INFRACTION, UNITE) pour créer les nœuds correspondants dans Neo4j via la clause MERGE.

#LIAISON GÉOGRAPHIQUE : Création immédiate de la relation :SITUE_DANS connectant chaque unité à son département.

#MIGRATION DES FAITS PAR BATCHS :
#Transformation de la table de faits ENREGISTRER en relations :A_ENREGISTRE.
#Optimisation Cypher : Utilisation de UNWIND pour traiter les données par paquets de 1000 lignes, réduisant drastiquement le temps d'exécution.

pip install neo4j

mport sqlite3
from neo4j import GraphDatabase
import os

# --- CONFIGURATION ---
sqlite_path = r"C:\Users\XXXX\XXXX\XXXX\XXXX\XXXX\SAE Migration NoSql\sae_crimes.db"
uri = "bolt://localhost:7687"
user = "neo4j"
password = "XXXXXXXX"
database_name = "modelegraphe"

class Migrator:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def migrate(self, sqlite_conn):
        with self.driver.session(database=database_name) as session:
            cursor = sqlite_conn.cursor()

            # 1. Création des contraintes d'unicité
            print("Création des contraintes...")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (d:Departement) REQUIRE d.code IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (i:Infraction) REQUIRE i.code IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:Unite) REQUIRE u.id_sql IS UNIQUE")

            # 2. Import des Départements
            print("Import des Départements...")
            cursor.execute("SELECT Code_Dept, Nom_Dept FROM DEPARTEMENT")
            for code, nom in cursor.fetchall():
                session.run("MERGE (d:Departement {code: $code}) SET d.nom = $nom", code=code, nom=nom)

            # 3. Import des Infractions
            print("Import des Infractions...")
            cursor.execute("SELECT Code_Index, Libelle_Index FROM INFRACTION")
            for code, libelle in cursor.fetchall():
                session.run("MERGE (i:Infraction {code: $code}) SET i.libelle = $libelle", code=code, libelle=libelle)

            # 4. Import des Unités + Relation SITUE_DANS
            print("Import des Unités et localisations...")
            cursor.execute("SELECT Id_Unite, Nom_Unite, Service, Perimetre, Code_Dept FROM UNITE")
            for row in cursor.fetchall():
                session.run("""
                    MERGE (u:Unite {id_sql: $id})
                    SET u.nom = $nom, u.service = $service, u.perimetre = $perimetre
                    WITH u
                    MATCH (d:Departement {code: $dept_code})
                    MERGE (u)-[:SITUE_DANS]->(d)
                """, id=row[0], nom=row[1], service=row[2], perimetre=row[3], dept_code=row[4])

            # 5. Import des Faits (RELATIONS)
            print("Migration des faits (Relations)... Cela peut prendre 1 ou 2 minutes.")
            cursor.execute("SELECT Id_Unite, Code_Index, Annee, Nombre_Faits FROM ENREGISTRER WHERE Nombre_Faits > 0")
            
            # Requête préparée pour le batching
            cypher_faits = """
                UNWIND $rows AS row
                MATCH (u:Unite {id_sql: row.u_id})
                MATCH (i:Infraction {code: row.i_code})
                MERGE (u)-[r:A_ENREGISTRE {annee: row.annee}]->(i)
                SET r.quantite = row.val
            """

            batch = []
            count = 0
            for row in cursor.fetchall():
                batch.append({'u_id': row[0], 'i_code': row[1], 'annee': row[2], 'val': row[3]})
                if len(batch) >= 1000:
                    session.run(cypher_faits, rows=batch)
                    count += len(batch)
                    batch = []
                    if count % 10000 == 0: print(f"  > {count} relations créées...")

            # --- TRAITEMENT DU DERNIER BATCH (entre 1 et 999 lignes restantes) ---
            if batch:
                session.run(cypher_faits, rows=batch)
                count += len(batch)
                print(f"  > {count} relations créées au total.")

# --- EXÉCUTION ---
sql_conn = sqlite3.connect(sqlite_path)
migrator = Migrator(uri, user, password)

try:
    migrator.migrate(sql_conn)
    print("\n✅ MIGRATION RÉUSSIE !")
except Exception as e:
    print(f"\n❌ ERREUR : {e}")
finally:
    migrator.close()
    sql_conn.close()
