import os
from datetime import datetime

from conexion import conexion

# Configuración del tamaño de lote para inserción en base de datos
# Puede ajustarse según la capacidad de la base de datos
DEFAULT_BATCH_SIZE = 500
MAX_BATCH_SIZE = 500  # Límite máximo para evitar problemas de memoria


def log(texto):
    """Función mejorada para logging con mejor manejo de archivos."""
    try:
        with open("log.log", "a+", encoding="utf-8") as f:
            f.write(str(datetime.now()) + ": " + texto + "\n")
    except Exception as e:
        print(f"Error al escribir en log: {e}")


def buscar_archivo(archivo):
    """Función mejorada para buscar archivos con mejor manejo de errores."""
    paths = "/home/u2"
    try:
        for dirpath, dirname, filename in os.walk(paths):
            # Buscar archivos con extensiones .jpg y .png
            for extension in [".jpg", ".png"]:
                archivo_completo = archivo + extension
                if archivo_completo in filename:
                    ruta_completa = os.path.join(dirpath, archivo_completo)
                    log(f"Archivo encontrado: {ruta_completa}")
                    return ruta_completa
        return None
    except Exception as e:
        log(f"Error al buscar archivo {archivo}: {e}")
        return None


def buscar_archivo_con_variantes(referencia, consecutivo, codcolor, referencia_base):
    """Busca un archivo probando diferentes variantes de nombres."""
    # Primera variante: referencia + "_a"
    valor = buscar_archivo(f"{referencia}_a")
    if valor is not None:
        return valor

    # Segunda variante: referencia_base + consecutivo + codcolor
    valor = buscar_archivo(f"{referencia_base}{consecutivo}{codcolor}_a")
    if valor is not None:
        return valor

    # Tercera variante: referencia_base + consecutivo
    valor = buscar_archivo(f"{referencia_base}{consecutivo}_a")
    return valor


def insertar_referencias_en_lote(cursor, con, referencias_data):
    """Inserta múltiples referencias en un solo lote para mejorar el rendimiento."""
    if not referencias_data:
        return 0, 0

    sql_insert = """
    INSERT INTO catalogo_WpFotosCatalago (referenciaunica, ruta) 
    VALUES (:referencia, :ruta)
    """

    try:
        # Preparar los datos para inserción en lote
        datos_para_insertar = []
        for referencia, ruta in referencias_data:
            datos_para_insertar.append({"referencia": referencia, "ruta": ruta})

        # Ejecutar inserción en lote
        cursor.executemany(sql_insert, datos_para_insertar)
        con.commit()

        log(f"Insertadas {len(referencias_data)} referencias en lote")
        return len(referencias_data), 0

    except Exception as e:
        log(f"Error al insertar lote de referencias: {e}")
        con.rollback()
        return 0, len(referencias_data)


def obtener_referencias_pendientes(cursor):
    """Obtiene las referencias pendientes de la base de datos."""
    sql = """
    SELECT DISTINCT REFERENCIAUNICA, consecutivo, codcolor, referencia  
    FROM wp_referenciasdisponibles 
    WHERE referenciaunica NOT IN (
        SELECT referenciaunica FROM catalogo_wpfotoscatalago
    )
    """

    log(f"Ejecutando consulta: {sql}")
    cursor.execute(sql)
    return cursor.fetchall()


def procesar_referencias(cursor, con, data, batch_size=DEFAULT_BATCH_SIZE):
    """Procesa todas las referencias encontradas usando inserción en lote."""
    archivos_encontrados = 0
    archivos_no_encontrados = 0
    referencias_para_insertar = []

    log(f"Procesando {len(data)} referencias con tamaño de lote: {batch_size}")

    for i, d in enumerate(data):
        referencia = str(d[0])
        consecutivo = str(d[1])
        codcolor = str(d[2])
        referencia_base = str(d[3])

        # Buscar archivo con diferentes variantes
        valor = buscar_archivo_con_variantes(
            referencia, consecutivo, codcolor, referencia_base
        )

        if valor is not None:
            referencias_para_insertar.append((referencia, valor))
            log(f"Archivo encontrado: {referencia} -> {valor}")
        else:
            log(f"No se encontró archivo para la referencia {referencia}")
            archivos_no_encontrados += 1

        # Procesar lote cuando se alcance el tamaño límite o sea el último elemento
        if len(referencias_para_insertar) >= batch_size or i == len(data) - 1:
            if referencias_para_insertar:
                insertados, fallidos = insertar_referencias_en_lote(
                    cursor, con, referencias_para_insertar
                )
                archivos_encontrados += insertados
                archivos_no_encontrados += fallidos
                referencias_para_insertar = []  # Limpiar el lote

                log(f"Progreso: {i+1}/{len(data)} referencias procesadas")

    return archivos_encontrados, archivos_no_encontrados


def validar_batch_size(batch_size):
    """Valida y ajusta el tamaño del lote si es necesario."""
    if batch_size <= 0:
        log(
            f"Tamaño de lote inválido ({batch_size}), usando valor por defecto: {DEFAULT_BATCH_SIZE}"
        )
        return DEFAULT_BATCH_SIZE
    elif batch_size > MAX_BATCH_SIZE:
        log(f"Tamaño de lote muy grande ({batch_size}), limitando a: {MAX_BATCH_SIZE}")
        return MAX_BATCH_SIZE
    return batch_size


def lista_de_referencias(batch_size=DEFAULT_BATCH_SIZE):
    """Función principal mejorada con inserción en lote para mejor rendimiento."""
    con = None
    try:
        # Validar tamaño del lote
        batch_size = validar_batch_size(batch_size)

        log(f"Iniciando búsqueda de archivos con tamaño de lote: {batch_size}")
        con = conexion.con()

        if con is None:
            log("ERROR: No se pudo establecer conexión con la base de datos")
            return

        cursor = con.cursor()
        data = obtener_referencias_pendientes(cursor)
        log(f"Se encontraron {len(data)} referencias pendientes")

        if not data:
            log("No hay referencias pendientes para procesar")
            return

        # Procesar con inserción en lote
        archivos_encontrados, archivos_no_encontrados = procesar_referencias(
            cursor, con, data, batch_size
        )

        log(
            f"Procesamiento completado. Encontrados: {archivos_encontrados}, No encontrados: {archivos_no_encontrados}"
        )

    except Exception as e:
        if con:
            try:
                con.rollback()
            except Exception:
                pass
        log(f"ERROR en lista_de_referencias: {e}")
    finally:
        if con:
            try:
                con.close()
            except Exception:
                pass


if __name__ == "__main__":
    lista_de_referencias()
