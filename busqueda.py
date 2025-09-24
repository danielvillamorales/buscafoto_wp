import os
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

from conexion import conexion

# Configuración del tamaño de lote para inserción en base de datos
# Puede ajustarse según la capacidad de la base de datos
DEFAULT_BATCH_SIZE = 500
MAX_BATCH_SIZE = 500  # Límite máximo para evitar problemas de memoria

# Configuración de threading
DEFAULT_MAX_WORKERS = 4  # Número de hilos para búsqueda de archivos
MAX_WORKERS = 4  # Límite máximo de hilos


# Lock para logging thread-safe
_log_lock = threading.Lock()


def log(texto):
    """Función thread-safe para logging con mejor manejo de archivos."""
    try:
        with _log_lock:
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


def procesar_referencia_individual(datos_referencia):
    """Procesa una referencia individual - función para threading."""
    referencia = str(datos_referencia[0])
    consecutivo = str(datos_referencia[1])
    codcolor = str(datos_referencia[2])
    referencia_base = str(datos_referencia[3])

    try:
        # Buscar archivo con diferentes variantes
        valor = buscar_archivo_con_variantes(
            referencia, consecutivo, codcolor, referencia_base
        )

        if valor is not None:
            log(f"Archivo encontrado: {referencia} -> {valor}")
            return referencia, valor, True
        else:
            log(f"No se encontró archivo para la referencia {referencia}")
            return referencia, None, False

    except Exception as e:
        log(f"Error al procesar referencia {referencia}: {e}")
        return referencia, None, False


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


def procesar_referencias_con_threading(
    cursor, con, data, batch_size=DEFAULT_BATCH_SIZE, max_workers=DEFAULT_MAX_WORKERS
):
    """Procesa todas las referencias usando threading para búsqueda paralela."""
    archivos_encontrados = 0
    archivos_no_encontrados = 0
    referencias_para_insertar = []

    log(
        f"Procesando {len(data)} referencias con {max_workers} hilos y tamaño de lote: {batch_size}"
    )

    # Procesar referencias en paralelo usando ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Enviar todas las tareas al pool de hilos
        future_to_data = {
            executor.submit(procesar_referencia_individual, d): d for d in data
        }

        # Procesar resultados conforme se completan
        for i, future in enumerate(as_completed(future_to_data)):
            try:
                referencia, valor, encontrado = future.result()

                if encontrado and valor is not None:
                    referencias_para_insertar.append((referencia, valor))
                else:
                    archivos_no_encontrados += 1

                # Procesar lote cuando se alcance el tamaño límite
                if len(referencias_para_insertar) >= batch_size:
                    if referencias_para_insertar:
                        insertados, fallidos = insertar_referencias_en_lote(
                            cursor, con, referencias_para_insertar
                        )
                        archivos_encontrados += insertados
                        archivos_no_encontrados += fallidos
                        referencias_para_insertar = []  # Limpiar el lote

                # Mostrar progreso cada 100 referencias procesadas
                if (i + 1) % 100 == 0:
                    log(f"Progreso: {i+1}/{len(data)} referencias procesadas")

            except Exception as e:
                log(f"Error al procesar referencia: {e}")
                archivos_no_encontrados += 1

    # Procesar el último lote si queda algo pendiente
    if referencias_para_insertar:
        insertados, fallidos = insertar_referencias_en_lote(
            cursor, con, referencias_para_insertar
        )
        archivos_encontrados += insertados
        archivos_no_encontrados += fallidos

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


def validar_max_workers(max_workers):
    """Valida y ajusta el número de hilos si es necesario."""
    if max_workers <= 0:
        log(
            f"Número de hilos inválido ({max_workers}), usando valor por defecto: {DEFAULT_MAX_WORKERS}"
        )
        return DEFAULT_MAX_WORKERS
    elif max_workers > MAX_WORKERS:
        log(f"Demasiados hilos ({max_workers}), limitando a: {MAX_WORKERS}")
        return MAX_WORKERS
    return max_workers


def lista_de_referencias(
    batch_size=DEFAULT_BATCH_SIZE, max_workers=DEFAULT_MAX_WORKERS
):
    """Función principal mejorada con threading y inserción en lote para mejor rendimiento."""
    con = None
    try:
        # Validar parámetros
        batch_size = validar_batch_size(batch_size)
        max_workers = validar_max_workers(max_workers)

        log(
            f"Iniciando búsqueda de archivos con {max_workers} hilos y tamaño de lote: {batch_size}"
        )
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

        # Procesar con threading e inserción en lote
        archivos_encontrados, archivos_no_encontrados = (
            procesar_referencias_con_threading(
                cursor, con, data, batch_size, max_workers
            )
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


def mostrar_estadisticas_rendimiento(
    inicio, fin, archivos_encontrados, archivos_no_encontrados
):
    """Muestra estadísticas de rendimiento del procesamiento."""
    tiempo_total = fin - inicio
    total_procesados = archivos_encontrados + archivos_no_encontrados

    if total_procesados > 0:
        archivos_por_segundo = total_procesados / tiempo_total.total_seconds()
        log(f"Rendimiento: {archivos_por_segundo:.2f} archivos/segundo")

    log(f"Tiempo total: {tiempo_total}")
    log(f"Archivos encontrados: {archivos_encontrados}")
    log(f"Archivos no encontrados: {archivos_no_encontrados}")


if __name__ == "__main__":
    import time

    inicio = time.time()

    try:
        lista_de_referencias()
    finally:
        fin = time.time()
        # Nota: Las estadísticas detalladas se muestran dentro de la función principal
