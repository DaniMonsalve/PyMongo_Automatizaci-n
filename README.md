# PyMongo_Automation
Exportación y Procesamiento de Identificadores desde MongoDB.
Este programa en Python realiza la exportación de identificadores desde una base de datos MongoDB, limpia y procesa los datos, y luego los guarda en archivos Excel divididos en particiones.
Requisitos
Python 3.x
Pandas
Pymongo
Instalación
Clona este repositorio o descarga los archivos.

Instala las dependencias necesarias:

bash
Copiar código
pip install pandas pymongo
Uso
Preparar el archivo de identificadores:

Crea un archivo llamado Identificadores.txt en el mismo directorio que el script. Este archivo debe contener los ID de los identificadores que deseas buscar, cada uno en una línea separada.
Configurar la conexión a MongoDB:

Modifica la línea de conexión en el script para incluir tu URI de MongoDB:
python
Copiar código
client = pymongo.MongoClient("mongodb+srv://<usuario>:<contraseña>@<cluster>/<base de datos>?retryWrites=true&w=majority")
Ejecutar el script:

Ejecuta el script principal:
bash
Copiar código
python script.py
Descripción de las funciones
exportar_Identificadores()
Lee los identificadores desde un archivo Identificadores.txt.
Conecta con una base de datos MongoDB y realiza una consulta para obtener los documentos correspondientes a los identificadores.
Exporta los resultados a un archivo resultado.json.
limpiar_json_resultado(file_path='resultado.json')
Lee y procesa el archivo resultado.json.
Extrae los campos relevantes y devuelve un DataFrame de pandas con los datos limpios.
realizar_agrupaciones(df)
Agrupa los datos del DataFrame según varios criterios.
Devuelve un DataFrame con las agrupaciones realizadas.
particionar_y_guardar(df, limite_filas_por_archivo=1000000)
Divide el DataFrame en partes más pequeñas según combinaciones de codRamoCont y codModalidadCont.
Guarda cada parte en un archivo Excel dentro de un directorio resultados.
Ejemplo de ejecución
Exportar Identificadores desde MongoDB:

La función exportar_Identificadores() realiza la consulta y guarda los resultados en resultado.json.
Limpiar y agrupar los datos:

La función limpiar_json_resultado() procesa el JSON exportado y limpia los datos.
La función realizar_agrupaciones() agrupa los datos limpios.
Particionar y guardar los datos:

La función particionar_y_guardar() divide los datos en partes y guarda cada parte en un archivo Excel.
Resultados
Los archivos Excel resultantes se guardarán en un directorio resultados en el directorio actual de trabajo.
Cada archivo se nombrará según la combinación de codRamoCont y codModalidadCont, seguido del número de parte correspondiente.
Notas
Asegúrate de que tu archivo Identificadores.txt y la configuración de la conexión a MongoDB estén correctamente configurados antes de ejecutar el script.
La función particionar_y_guardar() divide los archivos Excel en partes con un límite de filas especificado (por defecto, 1,000,000 filas por archivo).
