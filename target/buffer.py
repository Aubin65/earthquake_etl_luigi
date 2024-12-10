"""
Script d'import de la target en mémoire utilisée pour transiter entre différentes tâches
"""

from luigi.target import Target


class Buffer(Target):
    """Target for a temporary resource"""

    def __init__(self):
        """Initialisation"""
        self.data = {}

    def is_empty(self):
        return self.data == {}

    def put(self, key, value):
        self[key] = value

    def get(self, key):

        if self.data[key]:
            return self.data[key]
        else:
            return None
