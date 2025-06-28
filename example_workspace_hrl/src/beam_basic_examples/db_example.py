import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import psycopg
from psycopg import sql # Para construir consultas SQL de forma segura
import logging
import os

# Configuración del log para ver la salida de Beam
logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# --- Configuración de PostgreSQL ---
PG_HOST = os.getenv('PG_HOST', 'ws-postgresql')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'my_database')
PG_USER = os.getenv('PG_USER', 'my_user')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'password123') # ¡Cambia esto en producción!
PG_TABLE_NAME = 'telemetry_data' # Nombre de la tabla donde se escribirán los datos

# --- Clase para la fábrica de conexiones de PostgreSQL ---
# Es crucial gestionar las conexiones a la base de datos por trabajador/proceso
# para evitar problemas de serialización y asegurar que cada trabajador tenga su propia conexión.
class PostgreSQLClientFactory:
    """
    Una fábrica simple para crear y gestionar la conexión a PostgreSQL por trabajador.
    """
    def __init__(self, host, port, dbname, user, password):
        self.conn_params = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password
        }
        self._conn = None

    def get_connection(self):
        """
        Retorna una instancia de conexión a PostgreSQL. Crea una si no existe o si se desconectó.
        """
        if self._conn is None or self._conn.closed:
            logging.info(f"Creando nueva conexión a PostgreSQL en {self.conn_params['host']}:{self.conn_params['port']}/{self.conn_params['dbname']}...")
            try:
                self._conn = psycopg.connect(**self.conn_params)
                self._conn.autocommit = False # Usar transacciones explícitas
                logging.info("Conexión a PostgreSQL exitosa.")
            except psycopg.Error as e:
                logging.error(f"Error al conectar a PostgreSQL: {e}")
                raise
        return self._conn

# Instancia global de la fábrica de clientes PostgreSQL.
# Esta instancia se serializará y enviará a los trabajadores de Beam.
# Cada trabajador usará `get_connection()` para obtener su propia conexión.
pg_factory = PostgreSQLClientFactory(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)

# --- Función para preparar la base de datos (crear tabla si no existe) ---
# Esta función se ejecuta ANTES de que el pipeline comience, no dentro de Beam.
def setup_database():
    """
    Se asegura de que la base de datos y la tabla de destino existan en PostgreSQL.
    """
    try:
        conn = psycopg.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
        )
        conn.autocommit = True # Necesario para CREATE DATABASE/TABLE
        cur = conn.cursor()

        # Crear la base de datos si no existe (puede requerir conexión a 'postgres' o 'template1')
        try:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(PG_DB)))
            logging.info(f"Base de datos '{PG_DB}' creada exitosamente.")
        except psycopg.errors.DuplicateDatabase:
            logging.info(f"Base de datos '{PG_DB}' ya existe.")

        conn.close() # Cerrar conexión y reabrir a la DB recién creada/existente

        # Reabrir conexión a la base de datos específica para crear la tabla
        conn = pg_factory.get_connection()
        conn.autocommit = True # Necesario para CREATE TABLE (aunque la factory pone False)
                              # La pondremos True aquí para el DDL, luego se usará con False en el Map

        cur = conn.cursor()

        # Crear la tabla si no existe
        # Asumimos que los datos del diccionario tendrán 'id', 'value', 'email', 'age', 'price', 'category', etc.
        # Ajusta este esquema de tabla según los datos reales que pasarás.
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                id VARCHAR(255) PRIMARY KEY,
                value TEXT,
                email VARCHAR(255),
                age INTEGER,
                price NUMERIC(10, 2),
                category VARCHAR(255),
                race_id VARCHAR(255),
                timestamp VARCHAR(255)
            );
        """).format(sql.Identifier(PG_TABLE_NAME))
        
        cur.execute(create_table_query)
        logging.info(f"Tabla '{PG_TABLE_NAME}' asegurada/creada en '{PG_DB}'.")
        
        cur.close()
        conn.close() # Cerrar la conexión usada para setup
    except psycopg.Error as e:
        logging.error(f"Error al configurar la base de datos: {e}")
        raise

# --- Función para escribir datos individuales en PostgreSQL ---
def write_to_postgres(element):
    
    """
    Escribe un elemento de datos (diccionario) en la tabla de PostgreSQL.
    Este método se ejecutará en cada trabajador de Beam.
    """
    pg_factory = PostgreSQLClientFactory(PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)
    conn = pg_factory.get_connection() # Obtener la conexión a PostgreSQL para este trabajador
    cur = conn.cursor() # Obtener un cursor para ejecutar la consulta
    
    # Extraer datos del diccionario de entrada.
    # Usamos .get() con valor por defecto None para manejar campos que podrían no existir en todos los dicts.
    data_id = element.get('id')
    value = element.get('value')
    # Extraer campos de 'details' si existen
    details = element.get('details', {})
    email = details.get('email')
    age = details.get('age')
    price = details.get('price')
    category = details.get('category')
    race_id = details.get('race_id')
    timestamp = details.get('timestamp')
    
    if not data_id:
        logging.warning(f"Elemento sin 'id'. No se escribió en PostgreSQL: {element}")
        return

    try:
        # Construir la consulta INSERT de forma segura usando psycopg2.sql
        # Solo insertamos los campos que realmente tenemos valores, para evitar errores de null
        # en columnas NOT NULL si un campo no está presente.
        
        # Mapeo de campos del dict a columnas de la tabla SQL
        columns_map = {
            'id': data_id,
            'value': value,
            'email': email,
            'age': age,
            'price': price,
            'category': category,
            'race_id': race_id,
            'timestamp': timestamp
        }

        # Filtra columnas con valores None o vacíos si no quieres insertarlos
        # Depende de la definición de tu tabla si acepta NULLs o no.
        # Aquí, filtramos None, pero si tu columna es NOT NULL y el valor puede ser None, esto fallaría.
        # Para simplificar, insertaremos todos los campos que tenemos.
        
        insert_columns = []
        insert_values = []
        
        # Iterar sobre las columnas_map para construir la consulta dinámicamente
        for col_name, col_value in columns_map.items():
            insert_columns.append(sql.Identifier(col_name))
            insert_values.append(sql.Literal(col_value)) # psycopg maneja el escapado de valores

        # Construir la consulta INSERT final
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {}") \
                    .format(
                        sql.Identifier(PG_TABLE_NAME),
                        sql.SQL(', ').join(insert_columns),
                        sql.SQL(', ').join(insert_values),
                        # Para ON CONFLICT, actualizamos todos los campos excepto la clave primaria
                        sql.SQL(', ').join([
                            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                            for col in columns_map.keys() if col != 'id'
                        ])
                    )

        cur.execute(query)
        conn.commit() # ¡Importante! Confirmar la transacción
        logging.info(f"PostgreSQL: Insertado/Actualizado elemento con ID: {data_id}")

    except psycopg.Error as e:
        conn.rollback() # Revertir la transacción en caso de error
        logging.error(f"Error al escribir en PostgreSQL para ID {data_id}: {e}")
    finally:
        cur.close() # Siempre cierra el cursor

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
    Define y ejecuta el pipeline de Apache Beam para escribir datos en PostgreSQL.
    """
    # Asegurarse de que la base de datos y la tabla existan antes de ejecutar el pipeline
    logging.info("Asegurando la configuración de la base de datos de PostgreSQL...")
    setup_database()
    logging.info("Configuración de la base de datos completada. Iniciando pipeline...")

    # Datos de ejemplo a escribir. Cada elemento es un diccionario.
    # ¡Asegúrate de que los campos coincidan con la tabla que creaste!
    data_to_write = [
        {'id': 'user:101', 'value': 'Alice', 'details': {'email': 'alice@example.com', 'age': 30}},
        {'id': 'product:A', 'value': 'Laptop', 'details': {'price': 1200.00, 'category': 'Electronics'}},
        {'id': 'user:102', 'value': 'Bob', 'details': {'email': 'bob@example.com', 'age': 25}},
        {'id': 'product:B', 'value': 'Mouse', 'details': {'price': 25.50, 'category': 'Accessories'}},
        {'id': 'event:001', 'value': 'RaceStart', 'details': {'race_id': 'RCE202501', 'timestamp': '2025-06-05T10:00:00'}}
    ]

    with beam.Pipeline(options=get_pipeline_options()) as pipeline:
        # Crea un PCollection a partir de los datos de ejemplo
        elements_to_write = pipeline | 'Create Elements' >> beam.Create(data_to_write)

        # Aplica la función write_to_postgres a cada elemento de la PCollection
        elements_to_write | 'Write to PostgreSQL' >> beam.Map(write_to_postgres)
    
    logging.info("Pipeline de Apache Beam completado. Los datos deberían estar en tu tabla de PostgreSQL.")


if __name__ == '__main__':
    run_pipeline()
