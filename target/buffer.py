"""
Script d'import de la target en mémoire utilisée pour transiter entre différentes tâches
"""

from luigi.target import Target


class Buffer(Target):
    """Target pour une liste temporaire"""

    def __init__(self):
        """Initialisation de la liste de dictionnaires"""
        self.data = []

    def get_data(self):
        """Renvoie les données du buffer"""
        return self.data

    def exists(self):
        """Test sur la présence d'enregistrements dans le buffer"""
        return self.data != []

    def put(self, dictionnary):
        """Chargement de données dans le buffer"""
        self.data.append(dictionnary)
