# Projet d'ETL via l'API d'enregistrement des tremblements de terre - Version Luigi

![Earthquake](images/earthquake.png)

Ce projet est approximativement le même que le projet [earthquake_etl_airflow](https://github.com/Aubin65/earthquake_etl_airflow) mais avec l'utilisation d'Apache Luigi comme outil d'orchestration dans le but de tester ses fonctionnalités et les comparer à celles d'Airflow.

La partie visualisation ne sera pas développée dans ce repo car elle n'apporte pas de plus value au comparatif.

## Structure du projet

Le projet est structuré suivant les répertoires suivants : 
* **images :**
    * Répertoire contenant les images à afficher dans le fichier README.md
* **target :**
    * Répertoire contenant le script d'import de la classe MongoTarget nécessaire à la lecture et au chargement des données
* **workflows :**
    * Répertoire contenant les scripts de travail sur la donnée
* **trigger :**
    * Répertoire contenant le dossier de lancement des différents scripts
* **tests :**
    * Répertoire contenant les tests unitaires et d'intégration du projet
