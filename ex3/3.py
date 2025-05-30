import lithops
import boto3
import csv
import io

# Configuración global
BUCKET = 'david-sdlab'
INSULTS = ['tonto', 'bobo', 'puta', 'idiota', 'cabron']

def crear_cliente_s3():
    """Crear y devolver cliente boto3 para S3."""
    return boto3.client('s3')

def descargar_archivo(bucket, file_key, s3_client):
    """Descargar archivo desde S3 y devolver su contenido en texto."""
    obj = s3_client.get_object(Bucket=bucket, Key=file_key)
    return obj['Body'].read().decode('utf-8')

def censurar_texto(texto, insultos):
    """Censurar insultos en el texto y devolver texto censurado y número de insultos."""
    total_censurados = 0
    for insulto in insultos:
        count = texto.lower().count(insulto)
        total_censurados += count
        texto = texto.replace(insulto, '***')
    return texto, total_censurados

def procesar_csv(contenido_csv, insultos):
    """Procesar CSV censurando insultos y devolver CSV censurado y total de insultos censurados."""
    input_csv = io.StringIO(contenido_csv)
    output_csv = io.StringIO()

    reader = csv.reader(input_csv)
    writer = csv.writer(output_csv)

    total_censurados = 0
    for row in reader:
        censored_row = []
        for cell in row:
            censored_cell, count = censurar_texto(cell, insultos)
            total_censurados += count
            censored_row.append(censored_cell)
        writer.writerow(censored_row)

    return output_csv.getvalue(), total_censurados

def guardar_archivo(bucket, file_key, contenido, s3_client):
    """Guardar contenido en archivo dentro del bucket S3."""
    s3_client.put_object(Bucket=bucket, Key=file_key, Body=contenido.encode('utf-8'))

def censor_csv(file_key):
    """
    Función que Lithops ejecuta:
    Descarga archivo CSV, censura insultos, guarda archivo censurado y devuelve total censurado.
    """
    s3_client = crear_cliente_s3()
    contenido = descargar_archivo(BUCKET, file_key, s3_client)
    contenido_censurado, total_censurados = procesar_csv(contenido, INSULTS)
    censored_key = 'censored/' + file_key
    guardar_archivo(BUCKET, censored_key, contenido_censurado, s3_client)
    return total_censurados

def main():
    files_to_process = ['frases.csv']  # Lista de archivos a procesar

    fexec = lithops.FunctionExecutor()

    results = fexec.map(censor_csv, files_to_process)
    results_list = fexec.get_result(results)
    total_censored = sum(results_list)

    print(f'Total insults censored across all files: {total_censored}')

if __name__ == '__main__':
    main()