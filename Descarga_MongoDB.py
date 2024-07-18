import pandas as pd
import json
import os
import pymongo
import math
import pymongo
import json

def exportar_Identificadores():
    # Leer el archivo .txt con los ID objeto de búsqueda y a exportar desde MongoDB
    with open('Identificadores.txt', 'r') as file:
        num_Identificador_odl = [line.strip() for line in file]

    # Conectarse a la base de datos de MongoDB
    client = pymongo.MongoClient("mongodb+srv:")
    db = client.IdentificadorDB
    coleccion = db.Identificadores

    # Definir y realizar la consulta
    query = {"cabecera.datosIdentificativos.idIdentificadorODL": {"$in": num_Identificador_odl}}
    cursor = coleccion.find(query)
    Identificadores_encontrados = coleccion.count_documents(query)
    print(f"Número total de documentos encontrados en Mongo: {Identificadores_encontrados}")
    print("\nRealizando descarga:")

    # Escribir los resultados en un archivo JSON directamente a medida que se leen
    file_path = 'resultado.json'
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write('[')
        first_document = True
        for doc in cursor:
            if not first_document:
                file.write(',')
            else:
                first_document = False
            json.dump(doc, file, default=str)
        file.write(']')

    # Cerrar la conexión al cliente
    client.close()
    del num_Identificador_odl  # Liberar memoria

    if Identificadores_encontrados > 0:
        print("\nExportación completada correctamente")
    else:
        print("\nNo se encontraron documentos para exportar")


def limpiar_json_resultado(file_path='resultado.json'):
    if os.path.exists(file_path):
        records = []

        # Leer el archivo JSON completo
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

            # Procesar cada documento en el JSON
            for doc in data:
                cabecera = doc.get('cabecera', {})
                datos_identificativos = cabecera.get('datosIdentificativos', {})
                id_Identificador_odl = datos_identificativos.get('idIdentificadorODL')
                producto = cabecera.get('producto', {})
                cod_ramo = producto.get('codRamo')
                cod_modalidad = producto.get('codModalidad')
                fechas_y_estados = cabecera.get('fechasYEstados', {})
                estados = fechas_y_estados.get('estados', [])

                for estado in estados:
                    cod_tipo_estado_rec = estado.get('codTipoEstadoRec')
                    cod_tipo_gest_cont = estado.get('codTipoGestCont')
                    fec_liqui_rec = estado.get('fechas', {}).get('fecLiquiRec')
                    record = {
                        "idIdentificadorODL": id_Identificador_odl,
                        "codRamoCont": cod_ramo,
                        "codModalidadCont": cod_modalidad,
                        "codTipoEstadoRec": cod_tipo_estado_rec,
                        "codTipoGestCont": cod_tipo_gest_cont,
                        "fecLiquiRec": fec_liqui_rec
                    }
                    records.append(record)

        df = pd.DataFrame(records)
        #print('\n',df.head(4))
        print(f"\nNúmero total de registros identificados al leer el JSON exportado: {len(records)}\n")
        return df if not df.empty else None

    else:
        print(f"\nEl archivo {file_path} no existe.\n")
        return None


def realizar_agrupaciones(df):
    if df is not None:
        # Agrupaciones
        df['codTipoEstadoRec_codTipoGestCont'] = df['codTipoEstadoRec'] + ' ' + df['codTipoGestCont']
        df = df.groupby(['idIdentificadorODL', 'codRamoCont', 'codModalidadCont', 'fecLiquiRec']).agg({
            'codTipoEstadoRec_codTipoGestCont': ' & '.join
        }).reset_index()
        df.rename(columns={'codTipoEstadoRec_codTipoGestCont': 'codTipoEstadoRec & codTipoGestCont'}, inplace=True)

        return df

    else:
        return None


def particionar_y_guardar(df, limite_filas_por_archivo=1000000):
    if df is not None:
        # Particiones del DataFrame
        combinaciones = df[["codRamoCont", "codModalidadCont"]].drop_duplicates()

        # Crear directorio para resultados si no existe
        ruta_resultados = os.path.join(os.getcwd(), "resultados")
        os.makedirs(ruta_resultados, exist_ok=True)

        base = 0
        for index, combinacion in combinaciones.iterrows():
            ramo = combinacion["codRamoCont"]
            modalidad = combinacion["codModalidadCont"]
            df_filtrado = df[(df["codRamoCont"] == ramo) & (df["codModalidadCont"] == modalidad)]
            num_archivos = math.ceil(len(df_filtrado) / limite_filas_por_archivo)

            for i in range(num_archivos):
                inicio = i * limite_filas_por_archivo
                fin = min((i + 1) * limite_filas_por_archivo, len(df_filtrado))
                df_parte = df_filtrado.iloc[inicio:fin]
                nombre_archivo = f"ramo_{ramo}_modalidad_{modalidad}_parte{i+1}.xlsx"
                ruta_completa = os.path.join(ruta_resultados, nombre_archivo)
                df_parte.to_excel(ruta_completa, index=False, header=True)
                base += 1
                print(f"Archivo guardado: {nombre_archivo}")

        print("\nProceso completado.")
    else:
        print("\nNo se puede particionar y guardar porque el DataFrame es None.")

# Ejecutar el flujo de trabajo:
if __name__ == "__main__":
    # Exportar Identificadores desde MongoDB
    exportar_Identificadores()

    # Realizar las agrupaciones directamente después de limpiar el JSON
    df_agrupado=realizar_agrupaciones(limpiar_json_resultado())

    # Particionar y guardar los archivos según las combinaciones
    particionar_y_guardar(df_agrupado)


