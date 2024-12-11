"""
Ce fichier est utilisé pour mettre en place l'ETL grâce à Apache Luigi
"""

# Import des librairies nécessaires
import luigi
from target.mongotarget import MongoCollectionTarget
from target.buffer import Buffer
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from pymongo import MongoClient
import requests
import pendulum
from geopy.distance import geodesic

# Ajout du chemin vers client.cfg
luigi.configuration.LuigiConfigParser.add_config_path("../config/client.cfg")


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
        client = MongoClient(os.getenv("MONGO_CLIENT"))

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
        if len(raw_data["features"]) > 0:
            return buffer.put(raw_data)
        else:
            raise ValueError("Validation échouée : aucun nouvel enregistrement n'a été détecté")


class TransformEarthquakes(luigi.Task):
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
        raw_data = self.input().get_data()

        # Initialisation de la nouvelle variable de stockage temporaire
        buffer = self.output()

        # Itération sur chaque enregistrement
        for feature in raw_data["features"]:

            # Récupération des doordonnées du point pour pouvoir faire un calcul de distance
            point = (feature["geometry"]["coordinates"][1], feature["geometry"]["coordinates"][0])

            # Ajout du dictionnaire au buffer
            buffer.put(
                {
                    "mag": feature["properties"]["mag"],
                    "place": feature["properties"]["place"],
                    "date": datetime.fromtimestamp(feature["properties"]["time"] / 1000, timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    ),
                    "type": feature["properties"]["type"],
                    "nst": feature["properties"]["nst"],
                    "dmin": feature["properties"]["dmin"],
                    "sig": feature["properties"]["sig"],
                    "magType": feature["properties"]["magType"],
                    "geometryType": feature["geometry"]["type"],
                    "longitude": point[1],
                    "latitude": point[0],
                    "depth": feature["geometry"]["coordinates"][2],
                    "distance_from_us_km": round(geodesic(point, own_position).kilometers, 2),
                }
            )


class LoadEarthquakes(luigi.Task):
    """Tâche de chargement de la donnée"""

    def requires(self):
        """Définition de l'antécédant"""
        return TransformEarthquakes()

    def output(self):
        """Check de la sortie de l'ETL"""

        # Rechargement des variables d'environnement
        load_dotenv()

        # Récupération des variables d'environnement
        mongo_client = MongoClient(os.getenv("MONGO_CLIENT"))
        db = os.getenv("MONGO_DATABASE")
        collection = os.getenv("MONGO_COLLECTION")

        # Retourne un objet de l'instance MongoCollectionTarget
        return MongoCollectionTarget(mongo_client=mongo_client, index=db, collection=collection)

    def run(self):
        """Fonction de chargement de l'ETL à partir des données transformées précédemment"""

        # On applique la méthode bulk_insert définie dans la classe MongoCollectionTarget
        self.output().bulk_insert(self.input().get_data())


if __name__ == "__main__":
    luigi.run()
