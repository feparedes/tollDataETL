Este es un proyecto realizado como parte de un trabajo de un curso de Coursera de IBM en relación al Big Data. En el trabajamos con Apache Airflow para 
definir un flujo de tareas a realizar en un proceso ETL en relación a varios dataset de información acerca de vehículos y peajes.

Lo primero que hemos realizado es crear el grafo acíclico dirigido (DAG) con Python. En él, definimos los argumentos y las tareas a realizar, que son 6:

1. unzip_data: debemos extraer los datos que se encuentran en un archivo TAR e insertarlos en su carpeta correspondiente. Previo a esto hemos descargado 
el archivo mediante wget.

2. extract_data_from_csv: extraemos las columnas RowID, Timestamp, Anonymized Vehicle Number, Vehicle type del archivo CSV.

3. extract_data_from_tsv: extraemos las columnas Number of axles, Tollplaza id, Tollplaza code del archivo TSV.

4. extract_data_from_fixed_width: extraemos los atributos types of payments code y vehicle code del archivo TXT

5. consolidate_data: unificamos todos estos archivos en uno solo separados por coma. Su formato es CSV.

6. transform_data: hacemos una pequeña transformación de los datos pasándolo todo a mayúsculas.

Después definimos la prioridad de las tareas y copiamos este DAG en el directorio dags de Airflow.
