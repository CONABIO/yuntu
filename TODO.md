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
        3. yuntu.core.processors.probes
        4. yuntu.core.processors.indices
    7. yuntu.core.grids/atlas

2. yuntu.audio
    1. yuntu.audio.audio
    2. yuntu.audio.spectrogram
    3. yuntu.audio.loaders
    4. yuntu.audio.plotters
    5. yuntu.audio.processors
    6. yuntu.audio.indices

3. yuntu.database
    1. yuntu.database.base
    2. yuntu.database.backends
        1. yuntu.database.backends.pony
        2. yuntu.database.backends.irekua
    3. yuntu.database.store
        1. yuntu.database.store.base
        2. yuntu.database.store.postgresql
        3. yuntu.database.store.mongodb
        4. yuntu.database.store.audiomoth

4. yuntu.collections
    1. yuntu.collections.base
    2. yuntu.collections.audio
    3. yuntu.collections.parsers
    4. yuntu.collections.dataframes
        1. yuntu.collections.dataframes.hashers
        2. yuntu.collections.dataframes.audio
    6. yuntu.collections.io

4. yuntu.pipelines
    1. yuntu.pipelines.base
    2. yuntu.pipelines.places
    3. yuntu.pipelines.transitions
    4. yuntu.pipelines.methods
        1. yuntu.pipelines.methods.dask
        2. yuntu.pipelines.methods.graphs
        3. yuntu.pipelines.methods.algebra

5. yuntu.soundscapes
    1. yuntu.soundscapes.hashers
    2. yuntu.soundscapes.pipelines
    3. yuntu.soundscapes.processors
    4. yuntu.soundscapes.cubes
    5. yuntu.soundscapes.plotters
    6. yuntu.soundscapes.axis
    7. yuntu.soundscapes.atlas
    8. yuntu.soundscapes.dataframes
    9. yuntu.soundscapes.analyisis


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

1: Modularizar el código y aislar los componentes de manipulación de grafos, álgebra de procesos y manejo de paralelismo con dask
2: Documentar y flexibilizar el manejo de lugares y transiciones
3: Mejorar el plot de pipelines
4: Producir pruebas para las distintas formas de construcción, operación, cómputo y modificación de pipelines
