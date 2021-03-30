# Rediseño


0. yuntu.config
1. yuntu.core (Mainly base classes)
    1. yuntu.core.media
        1. yuntu.core.base
        2. yuntu.core.media.loaders
        3. yuntu.core.media.writers
        4. yuntu.core.media.info_extractors
        5. yuntu.core.media.storage
            1. filesystem
            2. s3
            3. scp
            4. remote HTTP (read only)
    2. yuntu.core.axis
    3. yuntu.core.geometries
    4. yuntu.core.annotations
        1. yuntu.core.annotated_object
        2. yuntu.core.annotations
        3. yuntu.core.labels
        4. yuntu.core.serializers
    5. yuntu.core.plotters
    6. yuntu.core.processors
        1. yuntu.core.processors.base
        2. yuntu.core.processors.models
    7. yuntu.core.grids/atlas

2. yuntu.audio
    1. yuntu.audio.audio
    2. yuntu.audio.spectrogram
    3. yuntu.audio.loaders
    4. yuntu.audio.plotters
    5. yuntu.audio.processors
    6. yuntu.audio.indices

3. yuntu.collections
    1. yuntu.collections.database
    2. yuntu.collections.parsers
    3. yuntu.collections.dataframe

4. yuntu.pipelines
    1. yuntu.pipelines.base
    2. yuntu.pipelines.places
    3. yuntu.pipelines.transitions

5. yuntu.soundscapes
    1. yuntu.soundscapes.hashers
    2. yuntu.soundscapes.pipelines
    3. yuntu.soundscapes.processors
    4. yuntu.soundscapes.cubes

6. yuntu.extra


## Yuntu core

Pocas dependencias:

1. numpy
2. shapely
3. matplotlib [opcional?]


## Media

1: Pruebas + documentación
2: Quitar librosa
3: Meter más métodos de generación de representaciones espectrales

## Pipelines
