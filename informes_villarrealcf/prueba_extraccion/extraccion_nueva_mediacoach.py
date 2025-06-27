import requests
import subprocess
import json
import zipfile
import csv
import os
import io
import mimetypes
from datetime import datetime
from collections import defaultdict
import logging
import re

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mediacoach_download_por_partido.log'),
        logging.StreamHandler()
    ]
)

def detectar_tipo_archivo(content, content_type=None):
    """Detecta el tipo de archivo basándose en magic numbers y content-type"""
    if not content:
        return 'unknown'
    
    # Magic numbers para detectar tipos de archivo
    if content.startswith(b'%PDF'):
        return 'pdf'
    elif content.startswith(b'PK\x03\x04'):  # ZIP-based files (XLSX, DOCX, etc.)
        try:
            with zipfile.ZipFile(io.BytesIO(content), 'r') as zip_file:
                if 'xl/workbook.xml' in zip_file.namelist():
                    return 'xlsx'
                elif 'word/document.xml' in zip_file.namelist():
                    return 'docx'
        except:
            pass
        return 'zip'
    elif content.startswith(b'<?xml') or content.startswith(b'<'):
        return 'xml'
    else:
        # Intentar detectar CSV por contenido
        try:
            content_str = content.decode('utf-8', errors='ignore')[:1000]
            if (';' in content_str or ',' in content_str) and '\n' in content_str:
                lines = content_str.split('\n')[:5]
                if len(lines) > 1:
                    sep_counts = []
                    for line in lines:
                        if line.strip():
                            sep_counts.append(line.count(';') + line.count(','))
                    if len(set(sep_counts)) <= 2 and max(sep_counts) > 0:
                        return 'csv'
        except:
            pass
    
    return 'bin'

def analizar_contenido_xml(content):
    """Analiza el contenido del XML para categorizarlo"""
    try:
        content_str = content.decode('utf-8', errors='ignore')[:2000]
        
        # Buscar patrones específicos
        if 'ALL_INSTANCES' in content_str and 'instance' in content_str:
            return 'eventos_partido'
        elif 'IdGame' in content_str and 'start' in content_str and 'end' in content_str:
            return 'eventos_partido'
        elif 'beyond' in content_str.lower() or 'stats' in content_str.lower():
            return 'beyond_stats'
        elif 'maxima' in content_str.lower() or 'exigencia' in content_str.lower():
            return 'maxima_exigencia'
        else:
            return 'general'
    except:
        return 'general'

def analizar_contenido_csv(content):
    """Analiza el contenido del CSV para categorizarlo"""
    try:
        content_str = content.decode('utf-8', errors='ignore')[:1000]
        
        # Buscar patrones en headers
        if 'equipo' in content_str.lower() or 'team' in content_str.lower():
            return 'equipos'
        elif 'jugador' in content_str.lower() or 'player' in content_str.lower():
            return 'jugadores'
        else:
            # Analizar estructura para determinar tipo
            lines = content_str.split('\n')[:3]
            if len(lines) > 0:
                header = lines[0].lower()
                if any(word in header for word in ['nombre', 'name', 'player', 'jugador']):
                    return 'jugadores'
                elif any(word in header for word in ['equipo', 'team', 'club']):
                    return 'equipos'
            return 'general'
    except:
        return 'general'

def analizar_contenido_xlsx(content, posicion_original=None):
    """Analiza el contenido del XLSX para categorizarlo"""
    try:
        # Usar posición original como hint si está disponible
        if posicion_original is not None:
            if posicion_original in [0, 1]:  # Posiciones típicas de rendimiento
                return 'rendimiento'
            elif posicion_original in [11, 12]:  # Posiciones típicas de máxima exigencia
                return 'maxima_exigencia'
        
        # Si no podemos determinar por posición, usar análisis básico
        return 'general'
    except:
        return 'general'

def descargar_y_categorizar_archivo(file_url, match_id, posicion, timeout=30):
    """Descarga un archivo y lo categoriza según su contenido"""
    try:
        logging.info(f"Descargando archivo {posicion}: {file_url}")
        response = requests.get(file_url, timeout=timeout)
        
        if response.status_code == 200:
            content = response.content
            content_type = response.headers.get('Content-Type', '')
            tipo_archivo = detectar_tipo_archivo(content, content_type)
            
            # Categorizar según tipo y contenido
            categoria = 'general'
            if tipo_archivo == 'xml':
                categoria = analizar_contenido_xml(content)
            elif tipo_archivo == 'csv':
                categoria = analizar_contenido_csv(content)
            elif tipo_archivo == 'xlsx':
                categoria = analizar_contenido_xlsx(content, posicion)
            elif tipo_archivo == 'pdf':
                categoria = 'informe'  # Todos los PDFs los consideramos informes
            
            return {
                'success': True,
                'content': content,
                'tipo': tipo_archivo,
                'categoria': categoria,
                'size': len(content),
                'content_type': content_type,
                'posicion': posicion
            }
        else:
            logging.error(f"Error HTTP {response.status_code} para archivo en posición {posicion}")
            return {'success': False, 'error': f'HTTP {response.status_code}', 'posicion': posicion}
            
    except Exception as e:
        logging.error(f"Error descargando archivo en posición {posicion}: {e}")
        return {'success': False, 'error': str(e), 'posicion': posicion}

def procesar_partido(match_id, asset_data, temporada, competicion, ejecutar_curl_comando):
    """Procesa un partido completo y descarga todos sus archivos organizadamente"""
    
    logging.info(f"\n=== PROCESANDO PARTIDO {match_id} ===")
    
    if not asset_data or len(asset_data) == 0:
        logging.error(f"No hay asset_data para el partido {match_id}")
        return False
    
    # Crear carpeta específica para este partido
    carpeta_partido = f'./VCF_Mediacoach_Data/{temporada}/{competicion}/Partidos/Partido_{match_id}'
    os.makedirs(carpeta_partido, exist_ok=True)
    
    # Obtener información del partido si está disponible
    try:
        match_info = asset_data[0].get('metadata', {}) if len(asset_data) > 0 else {}
        jornada = match_info.get('matchDay', 'desconocida')
    except:
        jornada = 'desconocida'
    
    # Estructuras para organizar archivos por tipo
    archivos_descargados = {
        'xml_eventos': [],
        'pdf_informes': [],
        'xlsx_rendimiento': [],
        'xlsx_maxima_exigencia': [],
        'csv_equipos': [],
        'csv_jugadores': [],
        'otros': []
    }
    
    errores = []
    total_descargados = 0
    
    # Descargar todos los archivos disponibles
    for i, asset in enumerate(asset_data):
        if 'url' in asset and asset['url']:
            resultado = descargar_y_categorizar_archivo(asset['url'], match_id, i)
            
            if resultado['success']:
                # Categorizar el archivo según su tipo y contenido
                tipo = resultado['tipo']
                categoria = resultado['categoria']
                content = resultado['content']
                size = resultado['size']
                
                # Crear nombre de archivo descriptivo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                if tipo == 'xml' and categoria == 'eventos_partido':
                    nombre_archivo = f'eventos_partido_j{jornada}_{match_id}_{timestamp}.xml'
                    archivos_descargados['xml_eventos'].append(nombre_archivo)
                    
                elif tipo == 'pdf':
                    contador_pdf = len(archivos_descargados['pdf_informes']) + 1
                    nombre_archivo = f'informe_{contador_pdf}_j{jornada}_{match_id}_{timestamp}.pdf'
                    archivos_descargados['pdf_informes'].append(nombre_archivo)
                    
                elif tipo == 'xlsx' and categoria == 'rendimiento':
                    contador_xlsx = len(archivos_descargados['xlsx_rendimiento']) + 1
                    nombre_archivo = f'rendimiento_{contador_xlsx}_j{jornada}_{match_id}_{timestamp}.xlsx'
                    archivos_descargados['xlsx_rendimiento'].append(nombre_archivo)
                    
                elif tipo == 'xlsx' and categoria == 'maxima_exigencia':
                    contador_xlsx = len(archivos_descargados['xlsx_maxima_exigencia']) + 1
                    nombre_archivo = f'maxima_exigencia_{contador_xlsx}_j{jornada}_{match_id}_{timestamp}.xlsx'
                    archivos_descargados['xlsx_maxima_exigencia'].append(nombre_archivo)
                    
                elif tipo == 'csv' and categoria == 'equipos':
                    nombre_archivo = f'postpartido_equipos_j{jornada}_{match_id}_{timestamp}.csv'
                    archivos_descargados['csv_equipos'].append(nombre_archivo)
                    
                elif tipo == 'csv' and categoria == 'jugadores':
                    nombre_archivo = f'postpartido_jugadores_j{jornada}_{match_id}_{timestamp}.csv'
                    archivos_descargados['csv_jugadores'].append(nombre_archivo)
                    
                else:
                    # Archivos que no encajan en las categorías principales
                    nombre_archivo = f'otro_{tipo}_{categoria}_pos{i}_j{jornada}_{match_id}_{timestamp}.{tipo}'
                    archivos_descargados['otros'].append(nombre_archivo)
                
                # Guardar el archivo
                ruta_completa = os.path.join(carpeta_partido, nombre_archivo)
                try:
                    with open(ruta_completa, 'wb') as f:
                        f.write(content)
                    
                    total_descargados += 1
                    logging.info(f"  ✅ {nombre_archivo} ({size} bytes) - {tipo}/{categoria}")
                    
                except Exception as e:
                    errores.append(f"Error guardando {nombre_archivo}: {e}")
                    logging.error(f"  ❌ Error guardando {nombre_archivo}: {e}")
            else:
                errores.append(f"Posición {i}: {resultado['error']}")
                logging.error(f"  ❌ Posición {i}: {resultado['error']}")
    
    # Crear resumen del partido
    resumen = {
        'match_id': match_id,
        'jornada': jornada,
        'total_archivos_descargados': total_descargados,
        'archivos_por_tipo': {k: len(v) for k, v in archivos_descargados.items()},
        'archivos_descargados': archivos_descargados,
        'errores': errores,
        'timestamp': datetime.now().isoformat()
    }
    
    # Guardar resumen en JSON
    resumen_path = os.path.join(carpeta_partido, f'resumen_partido_{match_id}.json')
    try:
        with open(resumen_path, 'w', encoding='utf-8') as f:
            json.dump(resumen, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Error guardando resumen: {e}")
    
    # Log del resumen
    logging.info(f"RESUMEN PARTIDO {match_id} (Jornada {jornada}):")
    logging.info(f"  - XML eventos: {len(archivos_descargados['xml_eventos'])}")
    logging.info(f"  - PDF informes: {len(archivos_descargados['pdf_informes'])}")
    logging.info(f"  - XLSX rendimiento: {len(archivos_descargados['xlsx_rendimiento'])}")
    logging.info(f"  - XLSX máxima exigencia: {len(archivos_descargados['xlsx_maxima_exigencia'])}")
    logging.info(f"  - CSV equipos: {len(archivos_descargados['csv_equipos'])}")
    logging.info(f"  - CSV jugadores: {len(archivos_descargados['csv_jugadores'])}")
    logging.info(f"  - Otros: {len(archivos_descargados['otros'])}")
    
    if errores:
        logging.warning(f"  - Errores: {len(errores)}")
    
    return total_descargados > 0

def procesar_partidos(ids, temporada, competicion, archivo_ids, credenciales, api_url_base, ejecutar_curl_comando):
    """Procesa múltiples partidos"""
    
    logging.info(f"=== INICIANDO PROCESAMIENTO DE {len(ids)} PARTIDOS ===")
    
    estadisticas_globales = {
        'partidos_procesados': 0,
        'partidos_con_errores': 0,
        'total_archivos_descargados': 0,
        'archivos_por_tipo': defaultdict(int),
        'errores_globales': []
    }
    
    for i, match_id in enumerate(ids):
        logging.info(f"\n--- PARTIDO {i+1}/{len(ids)}: {match_id} ---")
        
        try:
            # Obtener asset data del partido
            asset_data = ejecutar_curl_comando(f"curl --location '{api_url_base}/Assets/{match_id}' {credenciales}")
            
            if not asset_data:
                estadisticas_globales['partidos_con_errores'] += 1
                estadisticas_globales['errores_globales'].append(f"Partido {match_id}: No se pudo obtener asset_data")
                continue
            
            # Procesar el partido
            exito = procesar_partido(match_id, asset_data, temporada, competicion, ejecutar_curl_comando)
            
            if exito:
                estadisticas_globales['partidos_procesados'] += 1
                # Guardar ID como procesado
                guardar_id_en_csv(match_id, temporada, competicion, archivo_ids)
            else:
                estadisticas_globales['partidos_con_errores'] += 1
                estadisticas_globales['errores_globales'].append(f"Partido {match_id}: Error en procesamiento")
            
        except Exception as e:
            estadisticas_globales['partidos_con_errores'] += 1
            estadisticas_globales['errores_globales'].append(f"Partido {match_id}: Excepción - {str(e)}")
            logging.error(f"Error procesando partido {match_id}: {e}")
    
    # Resumen final
    logging.info(f"\n=== RESUMEN FINAL ===")
    logging.info(f"Partidos procesados exitosamente: {estadisticas_globales['partidos_procesados']}")
    logging.info(f"Partidos con errores: {estadisticas_globales['partidos_con_errores']}")
    
    if estadisticas_globales['errores_globales']:
        logging.warning(f"\nErrores encontrados:")
        for error in estadisticas_globales['errores_globales'][:10]:
            logging.warning(f"  - {error}")
        if len(estadisticas_globales['errores_globales']) > 10:
            logging.warning(f"  ... y {len(estadisticas_globales['errores_globales'])-10} errores más")
    
    return estadisticas_globales['partidos_procesados']

def guardar_id_en_csv(id, temporada, competicion, archivo_ids):
    """Guarda ID en CSV"""
    try:
        nombre_archivo = f'./VCF_Mediacoach_Data/{temporada}/{competicion}/{archivo_ids}'
        os.makedirs(os.path.dirname(nombre_archivo), exist_ok=True)
        
        with open(nombre_archivo, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([id])
        logging.debug(f"ID {id} guardado en {archivo_ids}")
    except Exception as e:
        logging.error(f"Error guardando ID {id}: {e}")

def leer_ids_csv(temporada, competicion, archivo_ids):
    """Lee IDs desde CSV"""
    nombre_archivo = f'./VCF_Mediacoach_Data/{temporada}/{competicion}/{archivo_ids}'
    logging.info(f"Buscando archivo: {nombre_archivo}")
    
    if not os.path.exists(nombre_archivo):
        logging.info(f"Archivo '{nombre_archivo}' no encontrado. Se creará uno nuevo.")
        os.makedirs(os.path.dirname(nombre_archivo), exist_ok=True)
        return []
    
    try:
        with open(nombre_archivo, 'r', newline='') as file:
            reader = csv.reader(file)
            ids_existentes = [row[0] for row in reader if row]
        logging.info(f"Se encontraron {len(ids_existentes)} IDs ya procesados")
        return ids_existentes
    except Exception as e:
        logging.error(f"Error leyendo el archivo {nombre_archivo}: {e}")
        return []

def obtener_ids(max_match_day, season_id, temporada, competition_id, competicion, archivo_ids, 
               credenciales, api_url_base, ejecutar_curl_comando):
    """Obtiene IDs de partidos"""
    logging.info(f"Obteniendo IDs para {temporada} - {competicion}")
    
    ids_existente = leer_ids_csv(temporada, competicion, archivo_ids)
    
    try:
        matches = ejecutar_curl_comando(f"""
        curl --location '{api_url_base}/Championships/seasons/{season_id}/competitions/{competition_id}/matches' \\
        {credenciales}
        """)

        if not matches:
            logging.error("No se pudieron obtener los matches")
            return []
            
        ids_filtrados = [item['id'] for item in matches if int(item['matchDayNumber']) < int(max_match_day)]
        ids_nuevos = [id for id in ids_filtrados if id not in ids_existente]
        
        logging.info(f"Matches totales en temporada: {len(matches)}")
        logging.info(f"Matches antes de jornada {max_match_day}: {len(ids_filtrados)}")
        logging.info(f"Matches ya procesados: {len(ids_existente)}")
        logging.info(f"Matches nuevos a procesar: {len(ids_nuevos)}")
        
        return ids_nuevos
        
    except Exception as e:
        logging.error(f"Error obteniendo matches: {e}")
        return []

def main():
    """Función principal"""
    logging.info("=== DESCARGADOR MEDIACOACH - ORGANIZACIÓN POR PARTIDO ===")
    
    # Configuración de autenticación
    client_id = '58191b89-cee4-11ed-a09d-ee50c5eb4bb5'
    scope = 'b2bapiclub-api'
    grant_type = 'password'
    username = 'b2bvillarealcf@mediacoach.es'
    password = 'r728-FHj3RE!'
    token_url = 'https://id.mediacoach.es/connect/token'

    data = {
        'client_id': client_id,
        'scope': scope,
        'grant_type': grant_type,
        'username': username,
        'password': password
    }

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    # Obtener token
    logging.info("Obteniendo token de acceso...")
    response = requests.post(token_url, data=data, headers=headers)

    if response.status_code == 200:
        token_response = response.json()
        AccessToken = token_response.get('access_token', '')
        expires_in = token_response.get('expires_in', '')
        logging.info(f'Token obtenido exitosamente (expira en {expires_in}s)')
    else:
        logging.error(f'Error obteniendo token: {response.status_code}')
        return

    # Configuración de la API
    SubscriptionKey = '729f9154234d4ff3bb0a692c6a0510c4'
    api_url_base = "https://club-api.mediacoach.es"
    credenciales = f"--header 'Ocp-Apim-Subscription-Key: {SubscriptionKey}' --header 'Authorization: Bearer {AccessToken}'"

    def ejecutar_curl_comando(comando):
        process = subprocess.Popen(comando, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            logging.error("Error en el comando curl:")
            logging.error(stderr.decode())
            return None
        return json.loads(stdout)

    # Configuración de temporadas y competiciones
    temporadas = [{"nombre": "Temporada 24-25", "id": "3a134240-833f-41dd-c6b0-3d6b87479c15", "input": 0},
                  {"nombre": "Temporada 23-24", "id": "3a0bf8ee-7f55-aeb6-fd31-273f2d45aefa", "input": 1}]

    competiciones = [{"nombre": "La Liga", "id": "39df9ec8-be91-4be5-1925-4b670a4cbed9", "input": 0},
                     {"nombre": "La Liga 2", "id": "39df9ec8-becb-86ea-b5e8-600c1b47968d", "input": 1}]

    # Interfaz de usuario
    print("Lista de temporadas disponibles:")
    for temporada in temporadas:
        print(f'{temporada["nombre"]}, seleccione {temporada["input"]}')

    input_temporadas = int(input("Seleccione una temporada: "))
    while input_temporadas not in [0, 1]:
        print("Esa no es una opción válida, selecciona 0 o 1")
        input_temporadas = int(input("Seleccione una temporada: "))

    season_id = temporadas[input_temporadas]["id"]
    season_name = temporadas[input_temporadas]["nombre"]
    temporada = season_name.replace(" ", "_").replace("-", "_")
    print(f"Ha elegido la temporada {season_name}")

    print("Lista de competiciones disponibles:")
    for competicion in competiciones:
        print(f'{competicion["nombre"]}, seleccione {competicion["input"]}')

    input_competiciones = int(input("Seleccione una competición: "))
    while input_competiciones not in [0, 1]:
        print("Esa no es una opción válida, selecciona 0 o 1")
        input_competiciones = int(input("Seleccione una competición: "))

    competition_id = competiciones[input_competiciones]["id"]
    competition_name = competiciones[input_competiciones]["nombre"]
    competicion = competition_name.replace(" ", "_").replace("-", "_")
    print(f"Ha elegido la competición {competition_name}")

    archivo_ids = f'ids_procesados_{competicion}.csv'
    max_match_day = input("Ingrese la cantidad de días de partidos (Ej: 1, 2, 15, etc.): ")
    print(f"Ha elegido {max_match_day} días de partidos")

    # Obtener y procesar partidos
    ids = obtener_ids(max_match_day, season_id, temporada, competition_id, competicion, 
                     archivo_ids, credenciales, api_url_base, ejecutar_curl_comando)
    
    if ids:
        partidos_procesados = procesar_partidos(ids, temporada, competicion, archivo_ids, 
                                              credenciales, api_url_base, ejecutar_curl_comando)
        print(f"\n✅ Procesamiento completado: {partidos_procesados} partidos procesados exitosamente")
    else:
        print("No hay partidos nuevos para procesar.")

if __name__ == "__main__":
    main()