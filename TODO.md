# Rediseño

0. yuntu.config
1. yuntu.core (Mainly base classes)
    0. yuntu.core.utils
        1. yuntu.core.plugins
        2. yuntu.core.cache
    1. yuntu.core.media
        1. yuntu.core.base
        2. yuntu.core.media.loaders
        3. yuntu.core.media.writers
        4. yuntu.core.media.info_extractors
        5. yuntu.core.media.storage
            1. filesystem
            2. s3
            3. scp
            4. remote HTTP (read only) 2. yuntu.core.axis
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
        0. yuntu.database.backends.base
        1. yuntu.database.backends.pony
        2. yuntu.database.backends.rest
        3. yuntu.database.backends.django
    3. yuntu.database.store
        1. yuntu.database.store.base
        2. yuntu.database.store.postgresql
        3. yuntu.database.store.mongodb
        4. yuntu.database.store.audiomoth
    4. yuntu.database.structures
        1. yuntu.database.irekua

4. yuntu.collections
    1. yuntu.collections.base
    2. yuntu.collections.audio
    3. yuntu.collections.parsers
    4. yuntu.collections.dataframes
        1. yuntu.collections.dataframes.hashers
        2. yuntu.collections.dataframes.audio
    5. yuntu.collections.writers

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
    9. yuntu.soundscapes.analysis

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

1. Nodos (Pipeline)
    0. Define la interfaz abstracta de los Nodos
    1. Guardar referencia al pipeline contenedor
    2. Mantener su identificador y su llave dentro del pipeline
    3. Métodos de consulta de información del nodo en el pipeline
    4. Asignación de valores
    5. Métodos para anexar a pipelines
    6. Métodos para validar los valores que puede alojar
    7. Graficación del contexto del nodo en el pipeline
    8. Computo de valores (en el pipeline)

2. MetaPipeline (Sin dask)
    -1. Define la interfaz abstracta de los Pipelines
    0. Almacenamiento de transitions y places.
    1. Métodos de acceso a transitions y places.
    2. Administración de nombres de nodos.
    3. Métodos para añadir y quitar transitions y places.
    4. Consultar la estructura la vecindad de un nodo.
    5. Acceso a atributos de nodos.
    6. Construcción de grafo de networkx
    7. Computo de propiedades globales de los nodos en el pipeline
    8. Graficación del pipeline
    9. Operaciones de pipeline
    10. Generación de copias

3. Pipeline (Dask)
    1. Construir gráfica de dask
    2. Inicializar los directorios de persistencia
    3. Computar, leer, o escribir el contenido de un nodo
    4. Computa el (el subgrafo del) pipeline con el cliente dado (escribe y
       lee)
    5. Operaciones de pipelines

4. Places(Nodo)
    1. Define la interfaz abstracta de los Places
    2. Sus atributos determinan si se va a persistir o no
    3. Métodos de consulta de información del nodo.
    4. Métodos de consulta de información del nodo en su pipeline.
    5. Lectura y escritura de valores en cualquier lugar.
    6. Actualización de ubicación del nodo en el pipeline
    7. Computar el valor del place
    8. Generación de copias

5. Transition(Nodo)
    1. Define la interfaz abstracta de las Transitions.
    2. Almacena un callable, la estructura y tipos de los inputs y outputs.
    3. Valida los outputs, los inputs y la signatura declarada.
    4. Sus atributos determinan si se va a persistir o no
    5. Métodos para asignación de valores.
    6. Actualización de ubicación de la transición en el pipeline.
    7. Consulta de información del nodo
    8. Consulta de los inputs y outputs
    9. Cirujía de outputs e inputs.
    10. Computar el resultado de la transition.
    11. Generación de copias

Requisitos:
Pipelines:
    + Cada nodo debe de tener un nombre (no necesariamente único) y un
    identificador único.

Observaciones:
    1. Los pipelines son los responsables de registrar y serializar los pasos
       de procesamiento.
    2. Los nodos de un pipeline deberán de tener un método de `to_dict` o
       `dumps`.
    3. Un método de reconstrucción `Pipeline.loads(nodo.dumps())`
    4. Yuntu deberá de ofrecer las siguientes funcionalidades:
        1. Poder cortar un arreglo con una geometría
        2. Poder generar una máscara binaria con una geometria
        3. Poder calcular agregaciones de los valores de un arreglo sobre una
           geometría
        4. Resamplear los datos sobre los distintos ejes
        5. Mantener la referencia las coordenadas que corresponden a cada bin
           del arreglo.
    5. Debemos de encontrar una abstracción de geometría que permita extender a
       3 dimensiones. Seguir usando shapely cuando se pueda. Una manera de
       asignar coordenadas a ejes.
    6. Yuntu deberá de poder traducir fácilmente anotaciones de irekua en
       anotaciones de yuntu y viceversa.
