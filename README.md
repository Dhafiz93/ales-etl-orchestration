# sales-etl-orchestration
🚀 Plateforme de Data Engineering & Orchestration ETL (Apache Airflow)
Ce projet met en place une architecture de données unifiée, automatisée et conteneurisée, combinant l'orchestration de flux de travail en temps réel et en batch à l'aide d'Apache Airflow.

📌 À propos du projet
L'objectif est de répondre à un défi stratégique : collecter, transformer et centraliser des données hétérogènes issues de sources multiples pour alimenter un tableau de bord décisionnel unifié.   
Apache_Airflow_Presentation (2).pptx

Le projet intègre deux pipelines ETL principaux :

Pipeline Crypto (Temps Réel) : Récupération automatique des données du Top 50 des cryptomonnaies via l'API CoinGecko toutes les 2 minutes (soit environ 720 exécutions par jour).   
Apache_Airflow_Presentation (2).pptx

Pipeline Vente (Batch) : Traitement quotidien à 6h00 de 14 fichiers CSV (commandes, stocks, catalogue) pour alimenter une base de données relationnelle.   
Apache_Airflow_Presentation (2).pptx

🛠️ Stack Technique
Orchestration : Apache Airflow (DAGs, Schedulers, Workers)   
Apache_Airflow_Presentation (2).pptx

Langage & Traitement : Python, Pandas, SQLAlchemy   
Apache_Airflow_Presentation (2).pptx

Stockage : MySQL (cryptodb) & SQLite (vente)   
Apache_Airflow_Presentation (2).pptx

Visualisation : Streamlit (Dashboard interactif multi-domaines avec rafraîchissement automatique)   
Apache_Airflow_Presentation (2).pptx

DevOps : Docker (Solution conteneurisée et portable)   
Apache_Airflow_Presentation (2).pptx

📂 Structure des Livrables
dags/ : Contient les scripts d'orchestration (etl_crypto.py et etl_csv.py).   
Apache_Airflow_Presentation (2).pptx

databases/ : Configuration des bases de données MySQL et SQLite.   
Apache_Airflow_Presentation (2).pptx

dashboard/ : Application Streamlit unifiée pour le suivi des KPIs.   
Apache_Airflow_Presentation (2).pptx

Dockerfile & docker-compose.yml : Configuration de l'environnement d'exécution.   
Apache_Airflow_Presentation (2).pptx

💡 Compétences Démontrées
Data Engineering : Conception de pipelines ETL robustes et scalables.   
Apache_Airflow_Presentation (2).pptx

Orchestration : Gestion des dépendances, plannings et automatisation avec Apache Airflow.   
Apache_Airflow_Presentation (2).pptx

Développement : Manipulation de données avec Python et Pandas, interaction avec des bases SQL.   
Apache_Airflow_Presentation (2).pptx

Visualisation & DevOps : Création de tableaux de bord interactifs (Streamlit) et conteneurisation (Docker).   
Apache_Airflow_Presentation (2).pptx
