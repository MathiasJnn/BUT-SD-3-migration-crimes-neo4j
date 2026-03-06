"""
SÉCURITÉ ET MIGRATION DE DONNÉES - IMPORTATION SQLITE
=====================================================

CE SCRIPT AUTOMATISE L'IMPORTATION DE DONNÉES DE CRIMINALITÉ (FORMAT CSV) 
DANS UNE BASE DE DONNÉES RELATIONNELLE SQLITE (DB Browser).

Fonctionnalités principales :
----------------------------
1. DÉTECTION AUTOMATIQUE : Identifie le service (Gendarmerie/Police) et l'année 
   directement depuis le contenu des fichiers.
2. ADAPTATION STRUCTURELLE : Gère les différences de colonnes entre les fichiers 
   PN (Périmètre inclus) et GN (Standard).
3. NETTOYAGE ROBUSTE : Nettoie les caractères non numériques et gère plusieurs 
   types d'encodages (UTF-8-BOM et CP1252) pour éviter les erreurs de lecture.
4. INTÉGRITÉ RELATIONNELLE : Remplit les tables DEPARTEMENT, UNITE, INFRACTION 
   et assure la liaison dans la table de faits ENREGISTRER.

Prérequis :
-----------
- Bibliothèques standards : os, sqlite3, csv, re
- Structure de base de données déjà existante ou prête à l'emploi (20 feuilles au format csv de l'Excel original)
"""

import csv
import sqlite3
import os
import re

# --- CONFIGURATION ---
dossier_source = r"C:\Users\XXXX\XXXX\XXXX\XXXX\XXXXX\SAE Migration NoSql"
db_path = os.path.join(dossier_source, "sae_crimes.db")

def clean_int(text):
    """Nettoie les nombres (enlève espaces, espaces insécables, etc.)"""
    if not text or text.strip() == "": return 0
    clean = re.sub(r'[^\d]', '', text)
    return int(clean) if clean else 0

def charger_donnees():
    # 1. Vérification du dossier
    if not os.path.exists(dossier_source):
        print(f"ERREUR : Le dossier est introuvable : {dossier_source}")
        return

    # 2. Connexion à la base
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    print(f"Connecté à la base : {db_path}")

    # 3. Liste des fichiers CSV
    fichiers = [f for f in os.listdir(dossier_source) if f.lower().endswith('.csv')]
    print(f"Nombre de fichiers CSV trouvés : {len(fichiers)}")

    for nom_fichier in fichiers:
        chemin_complet = os.path.join(dossier_source, nom_fichier)
        print(f"\nTraitement de : {nom_fichier}")

        try:
            # On teste l'encodage utf-8-sig (pour les fichiers Excel/CSV avec BOM)
            with open(chemin_complet, mode='r', encoding='utf-8-sig') as f:
                reader = list(csv.reader(f))
        except:
            # Si ça rate, on teste l'encodage Windows classique
            with open(chemin_complet, mode='r', encoding='cp1252') as f:
                reader = list(csv.reader(f))

        if not reader: continue

        # --- DÉTECTION SERVICE ET ANNÉE ---
        header_cell = reader[0][0]
        year_match = re.search(r'\d{4}', header_cell)
        if not year_match:
            print(f"  > Année non trouvée dans {nom_fichier}. Sauté.")
            continue
        
        annee = int(year_match.group())
        service = "GN" if "gendarmerie" in header_cell.lower() else "PN"
        print(f"  > Service : {service} | Année : {annee}")

        # --- RÉGLAGE DES LIGNES (PN a une ligne 'Périmètre' en plus) ---
        if service == "PN":
            idx_unite = 2
            idx_data_start = 3
            idx_perimetre = 1
        else:
            idx_unite = 1
            idx_data_start = 2
            idx_perimetre = None

        # --- MAPPING DES UNITÉS (Colonnes) ---
        mapping_colonnes = {}
        for col_idx in range(2, len(reader[0])):
            dept_code = reader[0][col_idx].strip()
            unite_nom = reader[idx_unite][col_idx].strip()
            if not unite_nom or unite_nom == "": continue

            perimetre = reader[idx_perimetre][col_idx].strip() if idx_perimetre else "N/A"
            
            # Insertions SQL
            cursor.execute("INSERT OR IGNORE INTO DEPARTEMENT (Code_Dept, Nom_Dept) VALUES (?, ?)", 
                           (dept_code, f"Département {dept_code}"))
            
            cursor.execute("INSERT OR IGNORE INTO UNITE (Nom_Unite, Service, Perimetre, Code_Dept) VALUES (?, ?, ?, ?)", 
                           (unite_nom, service, perimetre, dept_code))
            
            cursor.execute("SELECT Id_Unite FROM UNITE WHERE Nom_Unite = ? AND Code_Dept = ?", (unite_nom, dept_code))
            mapping_colonnes[col_idx] = cursor.fetchone()[0]

        # --- INSERTION DES STATISTIQUES ---
        count_faits = 0
        for row in reader[idx_data_start:]:
            if not row or len(row) < 2 or not row[0].isdigit(): continue
            
            code_index = int(row[0])
            libelle = row[1]
            
            cursor.execute("INSERT OR IGNORE INTO INFRACTION (Code_Index, Libelle_Index) VALUES (?, ?)", (code_index, libelle))
            
            for col_idx, id_unite in mapping_colonnes.items():
                if col_idx < len(row):
                    nb = clean_int(row[col_idx])
                    cursor.execute("INSERT OR REPLACE INTO ENREGISTRER (Id_Unite, Code_Index, Annee, Nombre_Faits) VALUES (?, ?, ?, ?)", 
                                   (id_unite, code_index, annee, nb))
                    count_faits += 1
        
        conn.commit()
        print(f"  > {count_faits} statistiques insérées.")

    print("\n--- TOUS LES FICHIERS ONT ÉTÉ TRAITÉS ---")
    conn.close()

# Exécution
charger_donnees()
