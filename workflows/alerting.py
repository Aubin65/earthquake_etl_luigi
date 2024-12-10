"""
Ce fichier est utilisé pour mettre en place l'alerte par mail grâce à Apache Luigi
"""

# Import des librairies nécessaires
import luigi
from ..target.mongotarget import MongoTarget  # noqa
from ..target.buffer import Buffer
from dotenv import load_dotenv
import os
import pymongo  # noqa
import requests  # noqa


class ExtractClothestEarthquakes(luigi.Task):
    """Tâche d'extraction de la donnée"""

    def output(self):
        """Chemin de chargement temporaire de la donnée"""
        return Buffer()

    def run(self):
        """Chargement des données dans la base de données MongoDB dont les références sont répertoriées dans le fichier.env"""

        # Chargement des variables d'environnement
        load_dotenv()

        # Récupération du client
        client = os.getenv("MONGO_CLIENT")

        # Récupération de la bdd
        db_name = os.getenv("MONGO_DATABASE")
        db = client[db_name]

        # Récupération de la collection
        collection_name = os.getenv("MONGO_COLLECTION")
        collection = db[collection_name]

        # Récupération de la distance minimale
        dist_min = int(os.getenv("MIN_DIST"))

        # Création de la variable de stockage temporaire
        buffer = self.output()

        # Création de la requête
        query = {"distance_from_us_km": {"$lte": dist_min}}
        projection = {"_id": 0}

        # Exécution de la requête
        cursor = collection.find(query, projection)

        # Stockage dans le buffer
        for record in cursor:
            buffer.put(record)


class TransformEarthquake(luigi.task):
    """Tâche de tranformation de la donnée"""

    def requires(self):
        """Définition de l'antécédant"""
        pass

    def output(self):
        """Chemin de chargement temporaire de la donnée"""
        pass

    def run(self):
        pass


class LoadEarthquake(luigi.task):
    """Tâche de chargement de la donnée"""

    def requires(self):
        """Définition de l'antécédant"""
        pass

    def output(self):
        pass

    def run(self):
        pass
