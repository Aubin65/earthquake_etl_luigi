"""
Ce fichier est utilisé pour mettre en place l'ETL grâce à Apache Luigi
"""

import luigi
from ..target.mongotarget import MongoTarget
import pymongo
import requests  # noqa


class ExtractEarthquake(luigi.Task):
    """Tâche d'extraction de la donnée"""

    def output(self):
        """Chemin de chargement temporaire de la donnée"""
        return luigi.LocalTarget("chemin")

    def run(self):
        pass


class TransformEarthquake(luigi.task):
    """Tâche de tranformation de la donnée"""

    def requires(self):
        """Définition de l'antécédant"""
        return ExtractEarthquake()

    def output(self):
        """Chemin de chargement temporaire de la donnée"""
        return luigi.LocalTarget("chemin")

    def run(self):
        pass


class LoadEarthquake(luigi.task):
    """Tâche de chargement de la donnée"""

    def requires(self):
        """Définition de l'antécédant"""
        return TransformEarthquake()

    def output(self):
        return MongoTarget(
            mongo_client=pymongo.MongoClient("mongodb://localhost:27017/"),
            index="earthquake_db",
            collection="earthquakes",
        )

    def run(self):
        pass
