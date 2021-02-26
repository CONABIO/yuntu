from abc import ABC
from abc import abstractmethod

MODELS = [
    'recording',
]

Models = namedtuple('Models', MODELS)

    db_config = {
        'provider': 'irekua_REST',
        'config': {
            'recordings_url': None,
            'page_size': 1,
            'target_attr': 'results',
            'auth': None
        }
    }

class RESTManager(ABC):
    """Managet to fetch information on the fly from irekua REST api"""

    def __init__(self, provider, config):
        self.provider = provider
        self.recordings_url = config["recordings_url"]
        self.page_size = config["page_size"]
        self.auth = config["auth"]
        self.build_models()

    def select(self, query, limit=None, offset=None, model="recording"):
        """Query entries from database."""
        if not isinstance(query, dict):
            raise ValueError("Parameter 'query' should be a dictionary")

        model_class = self.get_model_class(model)

        return model_class.select(query=query, limit=limit, offset=offset)

    def get_model_class(self, model):
        """Return model class if exists or raise error."""
        model_dict = self.models._asdict()

        if model not in model_dict:
            options = model_dict.keys()
            options_str = ', '.join(options)
            message = (
                f'The model {model} is not available in this REST database. '
                f'Admisible options: {options_str}')
            raise NotImplementedError(message)
        return model_dict[model]

    @abstractmethod
    def build_models(self):
        """Construct all database entities."""
        recording = self.build_recording_model()
        annotation = self.build_annotation_model()

        models = {
            'recording': recording,
        }

        return Models(**models)

    @abstractmethod
    def build_recording_model(self):
        """Build REST recording model"""
