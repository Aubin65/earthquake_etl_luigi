"""
Ce fichier est utilisé pour mettre en place l'ETL grâce à Apache Luigi
"""

# Import des librairies nécessaires
import luigi
from ..target.mongotarget import MongoTarget
from ..target.buffer import Buffer
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import pymongo
import requests
import pendulum
from geopy.distance import geodesic


class ExtractEarthquakes(luigi.Task):
    """Tâche d'extraction de la donnée"""

    def output(self):
        """Chemin de chargement temporaire de la donnée"""
        return Buffer()

    def run(self):
        """Extraction des données de l'API du gouvernement américain depuis la date la plus récente de la base de données MongoDB"""

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

        # Récupération de la dernière date d'extraction
        res = collection.aggregate([{"$group": {"_id": None, "maxDate": {"$max": "$date"}}}])
        result = next(res, None)
        starttime = result["maxDate"] if result else pendulum.now("UTC").add(days=-1).strftime("%Y-%m-%dT%H:%M:%S")

        # Initialisation de la requête
        request = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={starttime}"

        # Exécution de la requête
        raw_data = requests.get(request, timeout=15).json()

        # Fermeture du client
        client.close()

        # Création de la variable de stockage temporaire
        buffer = self.output()

        # Si on a de nouveaux enregistrements, on stocke le résultat de la requête dans notre Buffer
        if buffer.is_empty():
            raise ValueError("Validation échouée : aucun nouvel enregistrement n'a été détecté")
        else:
            return buffer.put(raw_data)


class TransformEarthquakes(luigi.task):
    """Tâche de tranformation de la donnée"""

    def requires(self):
        """Définition de l'antécédant"""
        return ExtractEarthquakes()

    def output(self):
        """Chemin de chargement temporaire de la donnée"""
        return Buffer()

    def run(self):
        """Etapes de transformation de la donnée"""

        # Chargement de la position actuelle depuis le fichier .env
        load_dotenv()
        own_position = (os.getenv("LAT_OWN_LOC"), os.getenv("LONG_OWN_LOC"))

        # Récupération des données brutes
        raw_data = self.input().data

        # Initialisation de la nouvelle variable de stockage temporaire
        buffer = self.output()

        for feature in raw_data["features"]:

            point = (feature["geometry"]["coordinates"][1], feature["geometry"]["coordinates"][0])

            # Création du dictionnaire temporaire
            buffer.put(
                {
                    "mag": feature["properties"]["mag"],
                    "place": feature["properties"]["place"],
                    "date": datetime.fromtimestamp(feature["properties"]["time"] / 1000, timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    ),
                    # "time": feature["properties"]["time"],
                    "type": feature["properties"]["type"],
                    "nst": feature["properties"]["nst"],
                    "dmin": feature["properties"]["dmin"],
                    "sig": feature["properties"]["sig"],
                    "magType": feature["properties"]["magType"],
                    "geometryType": feature["geometry"]["type"],
                    # "coordinates": feature["geometry"]["coordinates"],
                    "longitude": point[1],
                    "latitude": point[0],
                    "depth": feature["geometry"]["coordinates"][2],
                    "distance_from_us_km": round(geodesic(point, own_position).kilometers, 2),
                }
            )


class LoadEarthquakes(luigi.task):
    """Tâche de chargement de la donnée"""

    def requires(self):
        """Définition de l'antécédant"""
        return TransformEarthquakes()

    def output(self):

        client = os.getenv("MONGO_CLIENT")
        db = os.getenv("MONGO_DATABASE")
        collection = os.getenv("MONGO_COLLECTION")

        return MongoTarget(mongo_client=pymongo.MongoClient(client), index=db, collection=collection)

    def run(self):

        for record in self.input().data:

            self.output().write(record)
