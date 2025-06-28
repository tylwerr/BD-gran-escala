import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import redis
import logging
from ..utils import utils
from .. import settings

# Configuración del log para ver la salida de Beam
logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)



# --- Clase para el cliente de Redis (para gestionar la conexión en Beam) ---
# Es importante que el cliente de Redis se cree por trabajador (o por proceso)
# para evitar problemas de serialización y gestión de conexiones.
class RedisClientFactory:
    """
    Una fábrica simple para crear y gestionar la conexión a Redis por trabajador.
    """
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db
        self._redis_client = None

    def get_client(self):
        """
        Retorna una instancia del cliente Redis. Crea una si no existe.
        """
        if self._redis_client is None:
            logging.info(f"Creando nueva conexión a Redis en {self.host}:{self.port}/{self.db}...")
            self._redis_client = redis.StrictRedis(host=self.host, port=self.port, db=self.db, decode_responses=True)
            # Opcional: Probar la conexión
            try:
                self._redis_client.ping()
                logging.info("Conexión a Redis exitosa.")
            except redis.exceptions.ConnectionError as e:
                logging.error(f"Error al conectar a Redis: {e}")
                raise
        return self._redis_client

# Instancia global de la fábrica de clientes Redis.
# Esta instancia será serializada y enviada a los trabajadores,
# y cada trabajador creará su propia conexión real a través de get_client().
redis_factory = RedisClientFactory(settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_DB)

# --- Función para escribir datos individuales en Redis ---
def write_to_redis(element):
    """
    Escribe un elemento de datos en Redis.
    Este método se ejecutará en cada trabajador de Beam.
    """
    r = redis_factory.get_client() # Obtener la conexión a Redis para este trabajador

    # Los elementos de la PCollection serán diccionarios en este ejemplo
    key = element.get('id')
    value = element.get('value')
    
    if key and value:
        try:
            # Ejemplo 1: Guardar como un string simple
            r.set(f"simple_key:{key}", value)
            logging.info(f"Redis: SET simple_key:{key} = {value}")

            # Ejemplo 2: Guardar como un hash (para datos más estructurados)
            # Asumimos que `element` puede tener otros campos para el hash
            if 'details' in element:
                hash_key = f"hash_data:{key}"
                r.hmset(hash_key, element['details'])
                logging.info(f"Redis: HMSET {hash_key} = {element['details']}")
        except redis.exceptions.RedisError as e:
            logging.error(f"Error al escribir en Redis para la clave {key}: {e}")
    else:
        logging.warning(f"Elemento sin clave o valor: {element}. No se escribió en Redis.")

# --- Definición de las opciones del pipeline ---
def get_pipeline_options():
    """
    Configura las opciones del pipeline de Apache Beam.
    """
    options = PipelineOptions()
    # Usar el runner DirectRunner para ejecución local
    options.view_as(StandardOptions).runner = 'DirectRunner'
    return options

# --- Función principal del pipeline ---
def run_pipeline():
    """
    Define y ejecuta el pipeline de Apache Beam para escribir datos en Redis.
    """
    # Datos de ejemplo a escribir. Cada elemento es un diccionario.
    data_to_write = [
        {'id': 'user:101', 'value': 'Alice', 'details': {'email': 'alice@example.com', 'age': 30}},
        {'id': 'product:A', 'value': 'Laptop', 'details': {'price': 1200, 'category': 'Electronics'}},
        {'id': 'user:102', 'value': 'Bob', 'details': {'email': 'bob@example.com', 'city': 'New York'}},
        {'id': 'product:B', 'value': 'Mouse', 'details': {'price': 25, 'category': 'Accessories'}},
        {'id': 'event:001', 'value': 'RaceStart', 'details': {'race_id': 'RCE202501', 'timestamp': '2025-06-05T10:00:00'}}
    ]

    logging.info("Iniciando pipeline de Apache Beam para escribir en Redis...")

    with beam.Pipeline(options=get_pipeline_options()) as pipeline:
        # Crea un PCollection a partir de los datos de ejemplo
        elements_to_write = pipeline | 'Create Elements' >> beam.Create(data_to_write)

        # Aplica la función write_to_redis a cada elemento de la PCollection
        # Esta es la transformación clave para la escritura personalizada.
        elements_to_write | 'Write to Redis' >> beam.Map(write_to_redis)
    
    logging.info("Pipeline de Apache Beam completado. Los datos deberían estar en tu instancia de Redis.")
    logging.info("Puedes verificar los datos usando `redis-cli` o una herramienta de GUI para Redis.")


if __name__ == '__main__':
    run_pipeline()
