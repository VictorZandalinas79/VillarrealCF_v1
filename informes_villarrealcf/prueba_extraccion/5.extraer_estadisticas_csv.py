import pandas as pd
import os
import glob
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def identificar_archivos_postpartido(carpeta_partido):
    """
    Identifica los 2 archivos CSV que contienen ID EQUIPO e ID PARTIDO
    """
    archivos_postpartido = glob.glob(os.path.join(carpeta_partido, 'postpartido*.csv'))
    archivos_validos = []
    
    for archivo in archivos_postpartido:
        try:
            # Leer solo la primera fila para verificar columnas
            df_test = pd.read_csv(archivo, sep=';', nrows=1)
            columnas = df_test.columns.str.strip().str.upper()
            
            if 'ID EQUIPO' in columnas and 'ID PARTIDO' in columnas:
                archivos_validos.append(archivo)
                logger.info(f"Archivo válido encontrado: {archivo}")
        except Exception as e:
            logger.warning(f"Error al leer {archivo}: {e}")
    
    return archivos_validos

def leer_csv_con_encoding(archivo):
    """
    Intenta leer el CSV con diferentes encodings
    """
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(archivo, sep=';', encoding=encoding)
            logger.info(f"Archivo {archivo} leído exitosamente con encoding {encoding}")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error(f"Error al leer {archivo} con encoding {encoding}: {e}")
            continue
    
    raise Exception(f"No se pudo leer el archivo {archivo} con ningún encoding")

def limpiar_columnas(df):
    """
    Limpia los nombres de las columnas eliminando espacios extra
    """
    df.columns = df.columns.str.strip()
    return df

def separar_datos_equipo_jugador(df):
    """
    Separa los datos en estadísticas de equipo y jugador
    """
    # Limpiar columnas
    df = limpiar_columnas(df)
    
    # Identificar la columna de nombre del jugador (puede tener variaciones)
    columnas_jugador = [col for col in df.columns if 'NOMBRE' in col.upper() and 'JUGADOR' in col.upper()]
    if not columnas_jugador:
        # Buscar otras variaciones comunes
        columnas_jugador = [col for col in df.columns if any(keyword in col.upper() for keyword in ['PLAYER', 'NOMBRE', 'NAME'])]
    
    if not columnas_jugador:
        logger.warning("No se encontró columna de nombre del jugador")
        return pd.DataFrame(), df
    
    columna_jugador = columnas_jugador[0]
    logger.info(f"Usando columna de jugador: {columna_jugador}")
    
    # Datos de equipo: filas donde el nombre del jugador está vacío
    mask_equipo = df[columna_jugador].isna() | (df[columna_jugador].astype(str).str.strip() == '') | (df[columna_jugador].astype(str).str.strip() == 'nan')
    datos_equipo = df[mask_equipo].copy()
    
    # Eliminar columnas completamente vacías en datos de equipo
    datos_equipo = datos_equipo.dropna(axis=1, how='all')
    
    # Datos de jugador: filas donde todas las columnas están completas
    datos_jugador = df[~mask_equipo].copy()
    datos_jugador = datos_jugador.dropna()
    
    logger.info(f"Filas de equipo: {len(datos_equipo)}, Filas de jugador: {len(datos_jugador)}")
    
    return datos_equipo, datos_jugador

def extraer_jornada_partido(nombre_carpeta):
    """
    Extrae jornada y partido del nombre de la carpeta
    Ejemplo: j1_athleticclub1-1getafecf -> jornada='j1', partido='athleticclub1-1getafecf'
    """
    try:
        if '_' in nombre_carpeta:
            partes = nombre_carpeta.split('_', 1)
            jornada = partes[0].strip()
            partido = partes[1].strip()
        else:
            jornada = ''
            partido = nombre_carpeta
        
        logger.info(f"Extraído - Jornada: '{jornada}', Partido: '{partido}'")
        return jornada, partido
    except Exception as e:
        logger.warning(f"Error al extraer jornada y partido de '{nombre_carpeta}': {e}")
        return '', nombre_carpeta

def cargar_datos_existentes(archivo_parquet):
    """
    Carga datos existentes desde archivo parquet si existe
    """
    if os.path.exists(archivo_parquet):
        try:
            return pd.read_parquet(archivo_parquet)
        except Exception as e:
            logger.warning(f"Error al leer {archivo_parquet}: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

def combinar_y_deduplicar(df_nuevo, df_existente):
    """
    Combina datos nuevos con existentes y elimina duplicados
    """
    if df_existente.empty:
        return df_nuevo
    
    if df_nuevo.empty:
        return df_existente
    
    # Asegurar que ambos DataFrames tengan las mismas columnas
    columnas_comunes = set(df_nuevo.columns) & set(df_existente.columns)
    if not columnas_comunes:
        logger.warning("No hay columnas comunes entre datos nuevos y existentes")
        return df_nuevo
    
    # Filtrar solo columnas comunes
    df_nuevo_filtrado = df_nuevo[list(columnas_comunes)]
    df_existente_filtrado = df_existente[list(columnas_comunes)]
    
    # Combinar
    df_combinado = pd.concat([df_existente_filtrado, df_nuevo_filtrado], ignore_index=True)
    
    # Eliminar duplicados basándose en todas las columnas
    df_sin_duplicados = df_combinado.drop_duplicates()
    
    nuevas_filas = len(df_sin_duplicados) - len(df_existente_filtrado)
    logger.info(f"Filas nuevas añadidas: {nuevas_filas}")
    
    return df_sin_duplicados

def procesar_datos_vcf():
    """
    Función principal para procesar todos los datos
    """
    ruta_base = "VCF_Mediacoach_Data/Temporada_24_25/La_Liga/Partidos"
    
    if not os.path.exists(ruta_base):
        logger.error(f"La ruta {ruta_base} no existe")
        return
    
    # Crear carpeta data si no existe
    os.makedirs("data", exist_ok=True)
    logger.info("Carpeta 'data' verificada/creada")
    
    # Inicializar DataFrames para acumular datos
    todos_datos_equipo = pd.DataFrame()
    todos_datos_jugador = pd.DataFrame()
    
    # Cargar datos existentes
    datos_equipo_existentes = cargar_datos_existentes("data/estadisticas_equipo.parquet")
    datos_jugador_existentes = cargar_datos_existentes("data/estadisticas_jugador.parquet")
    
    # Buscar todas las carpetas de partidos
    carpetas_partidos = [d for d in os.listdir(ruta_base) if os.path.isdir(os.path.join(ruta_base, d))]
    logger.info(f"Encontradas {len(carpetas_partidos)} carpetas de partidos")
    
    partidos_procesados = 0
    
    for carpeta in carpetas_partidos:
        ruta_carpeta = os.path.join(ruta_base, carpeta)
        logger.info(f"Procesando carpeta: {carpeta}")
        
        # Extraer jornada y partido del nombre de la carpeta
        jornada, partido = extraer_jornada_partido(carpeta)
        
        # Identificar archivos válidos
        archivos_validos = identificar_archivos_postpartido(ruta_carpeta)
        
        if len(archivos_validos) < 2:
            logger.warning(f"Solo se encontraron {len(archivos_validos)} archivos válidos en {carpeta}")
        
        # Procesar cada archivo válido
        for archivo in archivos_validos:
            try:
                # Leer CSV
                df = leer_csv_con_encoding(archivo)
                
                # Separar datos de equipo y jugador
                datos_equipo, datos_jugador = separar_datos_equipo_jugador(df)
                
                # Añadir columnas de jornada y partido
                if not datos_equipo.empty:
                    datos_equipo['jornada'] = jornada
                    datos_equipo['partido'] = partido
                    
                    if todos_datos_equipo.empty:
                        todos_datos_equipo = datos_equipo
                    else:
                        todos_datos_equipo = pd.concat([todos_datos_equipo, datos_equipo], ignore_index=True)
                
                if not datos_jugador.empty:
                    datos_jugador['jornada'] = jornada
                    datos_jugador['partido'] = partido
                    
                    if todos_datos_jugador.empty:
                        todos_datos_jugador = datos_jugador
                    else:
                        todos_datos_jugador = pd.concat([todos_datos_jugador, datos_jugador], ignore_index=True)
                
                partidos_procesados += 1
                logger.info(f"Archivo procesado exitosamente: {archivo}")
                
            except Exception as e:
                logger.error(f"Error al procesar {archivo}: {e}")
    
    # Combinar con datos existentes y eliminar duplicados
    logger.info("Combinando con datos existentes y eliminando duplicados...")
    
    datos_equipo_finales = combinar_y_deduplicar(todos_datos_equipo, datos_equipo_existentes)
    datos_jugador_finales = combinar_y_deduplicar(todos_datos_jugador, datos_jugador_existentes)
    
    # Guardar en archivos parquet
    try:
        if not datos_equipo_finales.empty:
            datos_equipo_finales.to_parquet("data/estadisticas_equipo.parquet", index=False)
            logger.info(f"Guardado data/estadisticas_equipo.parquet con {len(datos_equipo_finales)} filas")
        
        if not datos_jugador_finales.empty:
            datos_jugador_finales.to_parquet("data/estadisticas_jugador.parquet", index=False)
            logger.info(f"Guardado data/estadisticas_jugador.parquet con {len(datos_jugador_finales)} filas")
        
        logger.info(f"Procesamiento completado. {partidos_procesados} archivos procesados.")
        
    except Exception as e:
        logger.error(f"Error al guardar archivos parquet: {e}")

if __name__ == "__main__":
    try:
        procesar_datos_vcf()
        print("✅ Procesamiento completado exitosamente")
    except Exception as e:
        logger.error(f"Error en el procesamiento: {e}")
        print("❌ Error en el procesamiento. Consulta los logs para más detalles.")