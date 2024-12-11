# Projet d'ETL via l'API d'enregistrement des tremblements de terre - Version Luigi

![Earthquake](images/earthquake.png)

Ce projet est approximativement le même que le projet [earthquake_etl_airflow](https://github.com/Aubin65/earthquake_etl_airflow) mais avec l'utilisation d'Apache Luigi comme outil d'orchestration dans le but de tester ses fonctionnalités et les comparer à celles d'Airflow.

La partie visualisation ne sera pas développée dans ce repo car elle n'apporte pas de plus value au comparatif.

## Structure du projet

Le projet est structuré suivant les répertoires suivants : 
* **images :**
    * Répertoire contenant les images à afficher dans le fichier README.md
* **target :**
    * Répertoire contenant les scripts d'import des classes MongoTarget et Buffer nécessaires à la lecture et au chargement des données
* **workflows :**
    * Répertoire contenant les scripts de travail sur la donnée
* **trigger :**
    * Répertoire contenant le dossier de lancement des différents scripts
* **tests :**
    * Répertoire contenant les tests unitaires et d'intégration du projet

## Commentaires quant à l'utilisation de Luigi par rapport à Airflow

L'utilisation de Luigi paraît assez simple avec une structure par classes et des méthodes que l'on retrouve souvent. 

On a :
* Une méthode ```output``` qui va définir la destination de la donnée
* Une méthode ```run``` qui va définir les transformations et plus généralement les actions de la tâche
* Une méthode ```requires``` qui va contenir la classe correspondant à la tâche précédante

Pour la méthode ```output```, on utilise des *Targets* prédéfinies dans le dossier [target](https://github.com/Aubin65/earthquake_etl_luigi/tree/main/target).
Comme décrit dans le partie Structure du projet, ce dossier contient deux types de target :
* La target **MongoTarget** qui permet l'accès à une base de données MongoDB. Elle est basée sur la librairie [pymongo](https://pymongo.readthedocs.io/en/stable/index.html). On y a ajouté une méthode *bulk_insert* pour ajouter une liste de documents dans la base de données.
* La target **Buffer** qui permet un stockage temporaire d'une liste et permet de transiter entre les différentes étapes d'extraction, de transformation et de stockage de la données. 