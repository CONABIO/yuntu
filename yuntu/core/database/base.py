"""Base definition for yuntu databases."""

from pony.orm import Database, Required, Optional, PrimaryKey, Set, Json

YuntuDb = Database()


class Datastore(YuntuDb.Entity):
    """Basic datastore entity for yuntu."""

    id = PrimaryKey(int, auto=True)
    parser = Required(str)
    recordings = Set(lambda: Recording)
    metadata = Required(Json)


class Recording(YuntuDb.Entity):
    """Basic recording entity for yuntu."""

    id = PrimaryKey(int, auto=True)
    datastore = Optional(Datastore)
    path = Required(str)
    hash = Required(str)
    timeexp = Required(float)
    spectrum = Required(str)
    media_info = Required(Json)
    annotations = Set(lambda: Annotation)
    metadata = Required(Json)

    def before_insert(self):
        if self.spectrum not in ["audible", "ultrasonic"]:
            raise ValueError("Invalid spectrum. Options are: 'audible', \
                             'ultrasonic'")
        if self.spectrum == "audible":
            if self.media_info["samplerate"] > 50000:
                raise ValueError("Not an audible recording.")
        elif self.media_info["samplerate"] <= 50000:
            raise ValueError("Not an ultrasonic recording.")


class Annotation(YuntuDb.Entity):
    """Basic annotation entity for yuntu."""

    id = PrimaryKey(int, auto=True)
    recording = Required(Recording)
    label = Required(Json)
    metadata = Required(Json)
