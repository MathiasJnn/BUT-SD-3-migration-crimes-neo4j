"""
AUDIT ET VALIDATION DES DONNÉES (BILAN DE SANTÉ)
===================================================

Ce script vérifie la cohérence et l'intégrité des données avant ou après la migration. Il agit comme un contrôle qualité pour s'assurer que la base relationnelle est complète et sans erreurs.

Étapes du diagnostic :
VÉRIFICATION STRUCTURELLE : Contrôle de la présence effective des 4 tables piliers (DEPARTEMENT, UNITE, INFRACTION, ENREGISTRER).

AUDIT VOLUMÉTRIQUE : Comptage exhaustif des lignes par table pour détecter d'éventuels manques lors de l'importation.

CONTRÔLE D'UNICITÉ : Détection de doublons critiques sur le couple (Nom_Unite, Code_Dept) afin d'éviter les redondances dans le futur graphe.

ANALYSE DE COHÉRENCE (ORPHELINS) : Identification des unités qui ne possèdent aucune statistique liée (vérification de l'intégrité référentielle).

SYNTHÈSE TEMPORELLE : Inventaire des couples Année / Service pour valider la couverture des données importées.
"""

import sqlite3
import os

# --- CONFIGURATION ---
dossier_source = r"C:\Users\XXXX\XXXX\XXXX\XXXX\XXXX\SAE Migration NoSql"
db_path = os.path.join(dossier_source, "sae_crimes.db")

# --- CONNEXION ---
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("=== BILAN DE SANTÉ DE LA BASE DE DONNÉES ===\n")

# 1. Vérification de l'existence des tables
tables = ["DEPARTEMENT", "UNITE", "INFRACTION", "ENREGISTRER"]
for table in tables:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    exists = cursor.fetchone()
    print(f"Table {table:12} : {'OK' if exists else 'MANQUANTE'}")

print("\n" + "-"*50)

# 2. Nombre d'enregistrements par table
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"Table {table:12} : {count:,} enregistrements")

print("\n" + "-"*50)

# 3. Vérification des doublons dans UNITE (Nom_Unite + Code_Dept)
cursor.execute("""
SELECT Nom_Unite, Code_Dept, COUNT(*) as cnt
FROM UNITE
GROUP BY Nom_Unite, Code_Dept
HAVING cnt > 1
""")
doublons = cursor.fetchall()
if doublons:
    print("⚠️ Doublons détectés dans UNITE (Nom_Unite + Code_Dept) :")
    for nom, dept, cnt in doublons:
        print(f"  {nom} - {dept} : {cnt} fois")
else:
    print("✅ Pas de doublons dans UNITE (Nom_Unite + Code_Dept)")

print("\n" + "-"*50)

# 4. Vérification des unités sans statistiques
cursor.execute("""
SELECT COUNT(*) FROM UNITE 
WHERE Id_Unite NOT IN (SELECT DISTINCT Id_Unite FROM ENREGISTRER)
""")
orphelins = cursor.fetchone()[0]
print(f"Unités sans données : {orphelins} (doit être proche de 0)")

print("\n" + "-"*50)

# 5. Volume total de faits enregistrés
cursor.execute("SELECT SUM(Nombre_Faits) FROM ENREGISTRER")
total = cursor.fetchone()[0] or 0
print(f"✅ Volume total de faits enregistrés : {total:,}")

print("\n" + "-"*50)

# 6. Liste des années et services présents
cursor.execute("""
SELECT DISTINCT E.Annee, U.Service
FROM ENREGISTRER E
JOIN UNITE U ON E.Id_Unite = U.Id_Unite
ORDER BY E.Annee, U.Service
""")
combinaisons = cursor.fetchall()
print("Années et services présents :")
for annee, service in combinaisons:
    print(f"  {annee} - {service}")

# --- FIN ---
conn.close()
print("\n=== FIN DU BILAN ===")
