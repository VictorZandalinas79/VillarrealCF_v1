#!/usr/bin/env python3
"""
EXTRACTOR COMPLETO MEDIACOACH API → PARQUET (VERSIÓN FINAL)
===========================================================

Extrae todos los datos de MediaCoach API y convierte a parquet:
- XML: maximaexigenciarevisada, beyondstats  
- XLSX: rendimiento, maximaexigencia (fila 6 headers, columna E info partido)
- CSV: postpartidoequipos, postpartidojugador
- Proceso incremental con barra de progreso
- Un parquet consolidado por tipo de archivo

ESTRUCTURA DE SALIDA:
VCF_Mediacoach_Data/
├── parquets/
│   ├── rendimiento_consolidado.parquet
│   ├── rendimiento_alt_consolidado.parquet
│   ├── maximaexigencia_consolidado.parquet
│   ├── maximaexigencia_alt_consolidado.parquet
│   ├── beyondstats_consolidado.parquet
│   ├── maximaexigenciarevisada_consolidado.parquet
│   ├── postpartidoequipos_consolidado.parquet
│   └── postpartidojugador_consolidado.parquet
├── control/
│   └── partidos_procesados.csv
└── logs/
    └── extraccion.log
"""

import requests
import json
import pandas as pd
import xml.etree.ElementTree as ET
import os
import re
import io
import subprocess
from datetime import datetime
import time
import logging
from typing import List, Dict, Optional, Tuple
import warnings
from tqdm import tqdm
warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURACIÓN Y CREDENCIALES
# =============================================================================

class MediaCoachConfig:
    """Configuración y credenciales de MediaCoach"""
    
    def __init__(self):
        # ⚙️ CONFIGURACIÓN DE PRUEBA - CAMBIAR AQUÍ
        # 🧪 Para pruebas: self.limite_partidos_prueba = 10
        # 🚀 Para todo: self.limite_partidos_prueba = None
        self.limite_partidos_prueba = 1  # ← CAMBIAR ESTA LÍNEA
        
        # Credenciales API
        self.client_id = '58191b89-cee4-11ed-a09d-ee50c5eb4bb5'
        self.scope = 'b2bapiclub-api'
        self.grant_type = 'password'
        self.username = 'b2bvillarealcf@mediacoach.es'
        self.password = 'r728-FHj3RE!'
        self.subscription_key = '729f9154234d4ff3bb0a692c6a0510c4'
        
        # URLs
        self.token_url = 'https://id.mediacoach.es/connect/token'
        self.api_base_url = 'https://club-api.mediacoach.es'
        
        # Rutas locales
        self.base_path = './VCF_Mediacoach_Data'
        self.parquets_path = os.path.join(self.base_path, 'parquets')
        self.control_path = os.path.join(self.base_path, 'control')
        self.logs_path = os.path.join(self.base_path, 'logs')
        
        # Token
        self.access_token = None
        self.token_expires_at = None
        
        # Configuración de temporadas y competiciones
        self.temporadas = [
            {"nombre": "Temporada 24-25", "id": "3a134240-833f-41dd-c6b0-3d6b87479c15"},
            {"nombre": "Temporada 23-24", "id": "3a0bf8ee-7f55-aeb6-fd31-273f2d45aefa"}
        ]
        
        self.competiciones = [
            {"nombre": "La Liga", "id": "39df9ec8-be91-4be5-1925-4b670a4cbed9"},
            {"nombre": "La Liga 2", "id": "39df9ec8-becb-86ea-b5e8-600c1b47968d"}
        ]
        
        self._create_directories()
        self._setup_logging()
    
    def _create_directories(self):
        """Crea las carpetas necesarias"""
        for path in [self.parquets_path, self.control_path, self.logs_path]:
            os.makedirs(path, exist_ok=True)
    
    def _setup_logging(self):
        """Configura el logging"""
        log_file = os.path.join(self.logs_path, 'extraccion.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_access_token(self) -> bool:
        """Obtiene o renueva el access token"""
        if self.access_token and self.token_expires_at and time.time() < self.token_expires_at:
            return True
        
        self.logger.info("🔑 Obteniendo access token...")
        
        data = {
            'client_id': self.client_id,
            'scope': self.scope,
            'grant_type': self.grant_type,
            'username': self.username,
            'password': self.password
        }
        
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        
        try:
            response = requests.post(self.token_url, data=data, headers=headers)
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get('access_token')
                expires_in = token_data.get('expires_in', 3600)
                self.token_expires_at = time.time() + expires_in - 300
                
                self.logger.info("✅ Access token obtenido correctamente")
                return True
            else:
                self.logger.error(f"❌ Error obteniendo token: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Excepción obteniendo token: {e}")
            return False
    
    def ejecutar_curl_comando(self, comando: str, reintentos: int = 3) -> Optional[List]:
        """Ejecuta comando curl con manejo de rate limiting"""
        if not self.get_access_token():
            return None
        
        for intento in range(reintentos):
            try:
                process = subprocess.Popen(comando, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                stdout, stderr = process.communicate()
                
                if process.returncode != 0:
                    self.logger.debug(f"Error en comando curl: {stderr.decode()}")
                    return None
                
                result = json.loads(stdout.decode())
                
                # Verificar si hay error de cuota
                if isinstance(result, dict) and result.get('statusCode') == 403:
                    mensaje = result.get('message', '')
                    
                    if 'Out of call volume quota' in mensaje:
                        self._manejar_cuota_agotada(mensaje, intento, reintentos)
                        continue
                    else:
                        self.logger.error(f"❌ Error 403: {mensaje}")
                        return None
                
                return result
                
            except json.JSONDecodeError as e:
                self.logger.debug(f"Error decodificando JSON (intento {intento + 1}): {e}")
                if intento < reintentos - 1:
                    time.sleep(2 ** intento)  # Backoff exponencial
                    continue
                return None
            except Exception as e:
                self.logger.debug(f"Error ejecutando curl (intento {intento + 1}): {e}")
                if intento < reintentos - 1:
                    time.sleep(2 ** intento)
                    continue
                return None
        
        return None
    
    def _manejar_cuota_agotada(self, mensaje: str, intento: int, max_reintentos: int):
        """Maneja el error de cuota agotada"""
        # Extraer tiempo de espera del mensaje
        tiempo_match = re.search(r'(\d{1,2}):(\d{2}):(\d{2})', mensaje)
        
        if tiempo_match:
            horas = int(tiempo_match.group(1))
            minutos = int(tiempo_match.group(2))
            segundos = int(tiempo_match.group(3))
            tiempo_espera_segundos = horas * 3600 + minutos * 60 + segundos
            
            if tiempo_espera_segundos > 300:  # Si son más de 5 minutos
                self.logger.warning(f"🚫 CUOTA API AGOTADA")
                self.logger.warning(f"⏰ Se restablecerá en: {horas:02d}:{minutos:02d}:{segundos:02d}")
                self.logger.warning(f"💡 El progreso se guardará automáticamente")
                
                # Dar opción al usuario
                respuesta = input(f"\n🤔 ¿Esperar {horas}h {minutos}m para reanudar automáticamente? (s/n): ").lower().strip()
                
                if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
                    self._esperar_con_progreso(tiempo_espera_segundos + 60)  # +60s de margen
                else:
                    self.logger.info("⏸️  Extracción pausada. Reanudar ejecutando el script nuevamente")
                    raise KeyboardInterrupt("Cuota agotada - Pausado por usuario")
            else:
                # Espera corta
                self.logger.warning(f"⏳ Cuota agotada. Esperando {tiempo_espera_segundos + 10} segundos...")
                time.sleep(tiempo_espera_segundos + 10)
        else:
            # No se pudo extraer tiempo, espera por defecto
            tiempo_espera = min(300, 60 * (intento + 1))  # Máximo 5 min
            self.logger.warning(f"⏳ Cuota agotada. Esperando {tiempo_espera} segundos...")
            time.sleep(tiempo_espera)
    
    def _esperar_con_progreso(self, segundos_espera: int):
        """Espera con barra de progreso"""
        with tqdm(total=segundos_espera, desc="⏰ Esperando restablecimiento de cuota", unit="s") as pbar:
            for _ in range(segundos_espera):
                time.sleep(1)
                pbar.update(1)
                # Mostrar tiempo restante cada minuto
                if pbar.n % 60 == 0:
                    restante = segundos_espera - pbar.n
                    horas_r = restante // 3600
                    minutos_r = (restante % 3600) // 60
                    pbar.set_description(f"⏰ Cuota se restablece en {horas_r:02d}:{minutos_r:02d}")
        
        self.logger.info("✅ Cuota restablecida. Reanudando extracción...")
    
    def get_credenciales_curl(self) -> str:
        """Genera string de credenciales para curl"""
        if not self.get_access_token():
            return ""
        
        return f"--header 'Ocp-Apim-Subscription-Key: {self.subscription_key}' --header 'Authorization: Bearer {self.access_token}'"

# =============================================================================
# CONTROL DE PARTIDOS PROCESADOS
# =============================================================================

class PartidosControl:
    """Maneja el control de partidos ya procesados"""
    
    def __init__(self, config: MediaCoachConfig):
        self.config = config
        self.control_file = os.path.join(config.control_path, 'partidos_procesados.csv')
        self.procesados = self._cargar_procesados()
    
    def _cargar_procesados(self) -> set:
        """Carga la lista de partidos ya procesados"""
        if os.path.exists(self.control_file):
            try:
                df = pd.read_csv(self.control_file)
                return set(df['match_id'].astype(str))
            except Exception as e:
                self.config.logger.warning(f"⚠️  Error cargando control: {e}")
                return set()
        return set()
    
    def es_procesado(self, match_id: str) -> bool:
        """Verifica si un partido ya fue procesado"""
        return match_id in self.procesados
    
    def marcar_procesado(self, match_id: str, temporada: str, competicion: str):
        """Marca un partido como procesado"""
        self.procesados.add(match_id)
        
        try:
            if os.path.exists(self.control_file):
                df = pd.read_csv(self.control_file)
            else:
                df = pd.DataFrame(columns=['match_id', 'temporada', 'competicion', 'fecha_procesado'])
            
            nueva_fila = {
                'match_id': match_id,
                'temporada': temporada,
                'competicion': competicion,
                'fecha_procesado': datetime.now().isoformat()
            }
            
            df = pd.concat([df, pd.DataFrame([nueva_fila])], ignore_index=True)
            df.drop_duplicates(subset=['match_id'], keep='last', inplace=True)
            df.to_csv(self.control_file, index=False)
            
        except Exception as e:
            self.config.logger.error(f"❌ Error actualizando control: {e}")

# =============================================================================
# PROCESADORES DE ARCHIVOS
# =============================================================================

class ProcessorBase:
    """Clase base para procesadores de archivos"""
    
    def __init__(self, config: MediaCoachConfig):
        self.config = config
    
    def process(self, content: bytes, match_info: Dict) -> Optional[pd.DataFrame]:
        """Procesa el contenido del archivo y devuelve un DataFrame"""
        raise NotImplementedError

class RendimientoProcessor(ProcessorBase):
    """Procesador para archivos XLSX de rendimiento - ESTRUCTURA CONFIRMADA"""
    
    def process(self, content: bytes, match_info: Dict) -> Optional[pd.DataFrame]:
        # 🔍 DIAGNÓSTICO DETALLADO
        self.config.logger.info(f"🔍 DIAGNÓSTICO XLSX:")
        self.config.logger.info(f"   📏 Tamaño: {len(content) if content else 0} bytes")
        
        if content:
            self.config.logger.info(f"   🔤 Primeros 20 bytes: {content[:20]}")
            self.config.logger.info(f"   🔤 Últimos 10 bytes: {content[-10:]}")
            
            # Verificar si es realmente un archivo Excel
            magic_signatures = {
                b'PK\x03\x04': 'ZIP/XLSX válido',
                b'\xd0\xcf\x11\xe0': 'Excel antiguo (.xls)',
                b'<?xml': 'XML puro',
                b'<html': 'HTML (posible error)',
                b'<!DOCTYPE': 'HTML DOCTYPE',
                b'{"error"': 'JSON error',
                b'{"statusCode"': 'API error'
            }
            
            signature_found = "Desconocida"
            for magic, desc in magic_signatures.items():
                if content.startswith(magic):
                    signature_found = desc
                    break
            
            self.config.logger.info(f"   🔍 Tipo detectado: {signature_found}")
            
            # Si es HTML o JSON error, mostrar contenido
            if any(content.startswith(x) for x in [b'<html', b'<!DOCTYPE', b'{"error"', b'{"statusCode"']):
                try:
                    contenido_texto = content.decode('utf-8', errors='ignore')[:500]
                    self.config.logger.warning(f"   📄 Contenido (primeros 500 chars): {contenido_texto}")
                except:
                    pass
        
        # Validaciones previas con más detalle
        if not content:
            self.config.logger.warning(f"❌ XLSX: Contenido vacío")
            return None
            
        if len(content) < 100:  # Reducir umbral para ver archivos pequeños
            self.config.logger.warning(f"❌ XLSX: Archivo muy pequeño - {len(content)} bytes")
            return None
        
        # Verificar que sea realmente un archivo Excel válido
        if not (content.startswith(b'PK') or content.startswith(b'\xd0\xcf')):
            self.config.logger.warning(f"❌ XLSX: No es un archivo Excel válido")
            return None
        
        # Intentar diferentes engines y configuraciones para leer Excel
        estrategias_lectura = [
            ('openpyxl', {'data_only': True, 'read_only': True}),
            ('openpyxl', {'data_only': True}),
            ('openpyxl', {'read_only': True}),
            ('openpyxl', {}),
            ('calamine', {}),
            ('pyxlsb', {}),
        ]
        
        df_raw = None
        estrategia_exitosa = None
        
        for engine, kwargs in estrategias_lectura:
            try:
                self.config.logger.info(f"   🔧 Probando {engine} con {kwargs}")
                
                # Combinar argumentos
                read_args = {'header': None, 'engine': engine}
                read_args.update(kwargs)
                
                df_raw = pd.read_excel(io.BytesIO(content), **read_args)
                estrategia_exitosa = f"{engine} {kwargs}"
                self.config.logger.info(f"   ✅ Estrategia exitosa: {estrategia_exitosa} - Dimensiones: {df_raw.shape}")
                break
                
            except ImportError as e:
                self.config.logger.warning(f"   ⚠️ Engine {engine} no disponible: {str(e)[:80]}")
                continue
            except Exception as e:
                self.config.logger.warning(f"   ❌ {engine} {kwargs} falló: {str(e)[:100]}")
                continue
        
        if df_raw is None or df_raw.empty:
            self.config.logger.warning(f"❌ XLSX: Todos los engines fallaron. Intentando estrategia de rescate...")
            
            # 🔧 ESTRATEGIA DE RESCATE: Intentar extraer XML directamente
            try:
                df_raw = self._extraer_xlsx_con_zipfile(content)
                if df_raw is not None:
                    estrategia_exitosa = "zipfile rescue"
                    self.config.logger.info(f"   ✅ Rescate exitoso con zipfile - Dimensiones: {df_raw.shape}")
            except Exception as e:
                self.config.logger.error(f"   ❌ Rescate con zipfile falló: {str(e)[:100]}")
        
        if df_raw is None or df_raw.empty:
            self.config.logger.error(f"❌ XLSX: IMPOSIBLE LEER ARCHIVO")
            self.config.logger.error(f"💡 Posibles soluciones:")
            self.config.logger.error(f"   1. pip install calamine")
            self.config.logger.error(f"   2. pip install pyxlsb")
            self.config.logger.error(f"   3. Actualizar openpyxl: pip install --upgrade openpyxl")
            return None
        
        try:
            # ESTRUCTURA CONFIRMADA: Extraer info del partido de COLUMNA E, FILA 2 (índice 1)
            partido_info = self._extraer_info_partido_columna_e(df_raw)
            self.config.logger.info(f"   📊 Info partido extraída: {list(partido_info.keys())}")
            
            # ESTRUCTURA CONFIRMADA: Headers en FILA 6 (índice 5)
            fila_headers = 5  # Fija, como confirmado en la imagen
            
            if len(df_raw) <= fila_headers:
                self.config.logger.warning(f"❌ XLSX: Archivo no tiene suficientes filas (tiene {len(df_raw)}, necesita > {fila_headers})")
                return None
            
            # Extraer headers de fila 6
            headers = self._limpiar_headers(df_raw.iloc[fila_headers].tolist())
            self.config.logger.info(f"   🏷️  Headers extraídos: {headers[:5]}... ({len(headers)} total)")
            
            # Extraer datos desde fila 7 en adelante
            df_datos = df_raw.iloc[fila_headers + 1:].copy()
            
            # Ajustar columnas
            if len(headers) != len(df_datos.columns):
                min_cols = min(len(headers), len(df_datos.columns))
                headers = headers[:min_cols]
                df_datos = df_datos.iloc[:, :min_cols]
                self.config.logger.info(f"   🔧 Ajustadas columnas a: {min_cols}")
            
            df_datos.columns = headers
            df_datos = df_datos.dropna(how='all')
            
            if df_datos.empty:
                self.config.logger.warning(f"❌ XLSX: Datos extraídos están vacíos después de limpieza")
                return None
            
            # Añadir metadatos del partido
            for key, value in partido_info.items():
                df_datos[f'partido_{key}'] = value
            
            # Añadir metadatos del match
            for key, value in match_info.items():
                df_datos[f'match_{key}'] = value
            
            self.config.logger.info(f"✅ XLSX procesado exitosamente: {len(df_datos)} registros, {len(df_datos.columns)} columnas")
            return df_datos
            
        except Exception as e:
            self.config.logger.error(f"❌ XLSX: Error procesando estructura - {str(e)[:200]}")
            import traceback
            self.config.logger.error(f"❌ XLSX: Traceback - {traceback.format_exc()[:500]}")
            return None
    
    def _buscar_fila_headers(self, df: pd.DataFrame) -> Optional[int]:
        """Busca dinámicamente la fila que contiene los headers (comienza con 'Id Jugador')"""
        try:
            # Buscar en las primeras 10 filas
            for fila_idx in range(min(10, len(df))):
                fila = df.iloc[fila_idx]
                
                # Convertir todos los valores a string y buscar "Id Jugador"
                valores_fila = [str(val).strip() for val in fila if pd.notna(val)]
                
                # Buscar patrones típicos de headers
                for valor in valores_fila:
                    if valor.lower() in ['id jugador', 'id_jugador', 'idjugador']:
                        self.config.logger.info(f"   🎯 Headers encontrados por 'Id Jugador' en fila {fila_idx + 1}")
                        return fila_idx
                    
                    # También buscar otros patrones de headers típicos
                    if any(pattern in valor.lower() for pattern in ['dorsal', 'nombre', 'apellido', 'alias']):
                        # Verificar que esta fila tiene múltiples campos típicos de headers
                        texto_fila = ' '.join(valores_fila).lower()
                        if sum(pattern in texto_fila for pattern in ['id', 'dorsal', 'nombre', 'apellido']) >= 3:
                            self.config.logger.info(f"   🎯 Headers encontrados por patrón múltiple en fila {fila_idx + 1}")
                            return fila_idx
            
            # Si no encuentra, buscar la primera fila que tenga al menos 5 columnas no vacías
            for fila_idx in range(min(10, len(df))):
                fila = df.iloc[fila_idx]
                valores_no_vacios = [val for val in fila if pd.notna(val) and str(val).strip()]
                
                if len(valores_no_vacios) >= 5:
                    # Verificar que no sean solo números (datos de jugadores)
                    numeros_count = sum(1 for val in valores_no_vacios[:5] if str(val).replace('.', '').replace('-', '').isdigit())
                    if numeros_count < 3:  # Si menos de 3 de los primeros 5 son números, probablemente son headers
                        self.config.logger.info(f"   🎯 Headers estimados por columnas no-numéricas en fila {fila_idx + 1}")
                        return fila_idx
            
            # Como último recurso, usar fila 4 (índice 3) como fallback
            if len(df) > 3:
                self.config.logger.warning(f"   ⚠️ Usando fila 4 como fallback para headers")
                return 3
                
        except Exception as e:
            self.config.logger.warning(f"Error buscando headers: {e}")
        
        return None
    
    def _extraer_info_partido_columna_e(self, df: pd.DataFrame) -> Dict:
        """Extrae información del partido desde la COLUMNA E, FILA 2 (estructura real confirmada)"""
        info = {}
        
        try:
            # Verificar que existe columna E (índice 4) y fila 2 (índice 1)
            if len(df) > 1 and len(df.columns) > 4:
                valor_partido = df.iloc[1, 4]  # Fila 2, Columna E
                
                if pd.notna(valor_partido) and isinstance(valor_partido, str):
                    texto = valor_partido.strip()
                    
                    # FORMATO REAL: "LALIGA EA SPORTS | Temporada 2024 - 2025 | J3 | Real Betis 2 - 1 Getafe CF (2024-09-18)"
                    if '|' in texto:
                        partes = [parte.strip() for parte in texto.split('|')]
                        
                        if len(partes) >= 4:
                            # Competición
                            info['competicion'] = partes[0]  # "LALIGA EA SPORTS"
                            
                            # Temporada
                            if 'Temporada' in partes[1]:
                                info['temporada_info'] = partes[1]  # "Temporada 2024 - 2025"
                                # Extraer años
                                temporada_match = re.search(r'(\d{4})\s*-\s*(\d{4})', partes[1])
                                if temporada_match:
                                    info['temporada_inicio'] = temporada_match.group(1)
                                    info['temporada_fin'] = temporada_match.group(2)
                            
                            # Jornada
                            jornada_match = re.search(r'(J\d+)', partes[2])
                            if jornada_match:
                                info['jornada'] = jornada_match.group(1)
                            
                            # Partido y fecha (último elemento)
                            if len(partes) > 3:
                                partido_parte = partes[-1]
                                
                                # Extraer fecha
                                fecha_match = re.search(r'\((\d{4}-\d{2}-\d{2})\)', partido_parte)
                                if fecha_match:
                                    info['fecha'] = fecha_match.group(1)
                                    partido_sin_fecha = re.sub(r'\s*\(\d{4}-\d{2}-\d{2}\)', '', partido_parte).strip()
                                else:
                                    partido_sin_fecha = partido_parte.strip()
                                
                                # Extraer equipos y resultado
                                # Formato: "Real Betis 2 - 1 Getafe CF"
                                resultado_match = re.search(r'^(.+?)\s+(\d+)\s*-\s*(\d+)\s+(.+)$', partido_sin_fecha)
                                if resultado_match:
                                    info['equipo_local'] = resultado_match.group(1).strip()
                                    info['goles_local'] = int(resultado_match.group(2))
                                    info['goles_visitante'] = int(resultado_match.group(3))
                                    info['equipo_visitante'] = resultado_match.group(4).strip()
                    
                    # Si no hay pipes, intentar formatos alternativos
                    elif texto:
                        info['partido_raw'] = texto
                        
                        # Buscar fecha en cualquier lugar
                        fecha_match = re.search(r'(\d{4}-\d{2}-\d{2})', texto)
                        if fecha_match:
                            info['fecha'] = fecha_match.group(1)
        
        except Exception as e:
            self.config.logger.debug(f"Error extrayendo info de partido: {e}")
        
        return info
    
    def _extraer_xlsx_con_zipfile(self, content: bytes) -> Optional[pd.DataFrame]:
        """Método de rescate: extraer datos XLSX usando zipfile directamente"""
        import zipfile
        import xml.etree.ElementTree as ET_xlsx
        
        try:
            with zipfile.ZipFile(io.BytesIO(content), 'r') as zip_ref:
                # Listar contenidos del archivo
                file_list = zip_ref.namelist()
                self.config.logger.info(f"   📁 Archivos en XLSX: {file_list[:10]}...")
                
                # Buscar el archivo de datos principal (worksheet)
                worksheet_files = [f for f in file_list if f.startswith('xl/worksheets/')]
                if not worksheet_files:
                    self.config.logger.warning(f"   ❌ No se encontraron worksheets")
                    return None
                
                # Usar la primera worksheet
                worksheet_file = worksheet_files[0]
                self.config.logger.info(f"   📊 Procesando worksheet: {worksheet_file}")
                
                # Leer el XML de la worksheet
                with zip_ref.open(worksheet_file) as ws_file:
                    ws_content = ws_file.read()
                    
                # Parsear XML
                root = ET_xlsx.fromstring(ws_content)
                
                # Extraer datos (esto es una implementación básica)
                # Los datos están en elementos <row> con <c> (cells)
                rows_data = []
                
                # Namespace de Excel
                ns = {'': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
                
                # Buscar filas
                for row_elem in root.findall('.//row', ns):
                    row_data = []
                    for cell_elem in row_elem.findall('.//c', ns):
                        # Buscar valor de la celda
                        value_elem = cell_elem.find('v', ns)
                        if value_elem is not None:
                            row_data.append(value_elem.text)
                        else:
                            row_data.append('')
                    
                    if row_data:  # Solo añadir filas no vacías
                        rows_data.append(row_data)
                
                if not rows_data:
                    self.config.logger.warning(f"   ❌ No se pudieron extraer datos del XML")
                    return None
                
                # Crear DataFrame básico
                max_cols = max(len(row) for row in rows_data) if rows_data else 0
                
                # Rellenar filas cortas con valores vacíos
                for row in rows_data:
                    while len(row) < max_cols:
                        row.append('')
                
                df = pd.DataFrame(rows_data)
                self.config.logger.info(f"   ✅ Datos extraídos con zipfile: {df.shape}")
                return df
                
        except Exception as e:
            self.config.logger.error(f"   ❌ Error en rescate zipfile: {str(e)[:150]}")
            return None
    
    def _limpiar_headers(self, headers: List) -> List[str]:
        """Limpia los nombres de headers"""
        headers_limpios = []
        for i, header in enumerate(headers):
            if pd.notna(header) and str(header).strip():
                nombre = str(header).strip()
                nombre = re.sub(r'[^\w\s\(\)\%\-]', '_', nombre)
                nombre = re.sub(r'\s+', '_', nombre)
                nombre = re.sub(r'_+', '_', nombre).strip('_')
                headers_limpios.append(nombre or f'columna_{i+1}')
            else:
                headers_limpios.append(f'columna_{i+1}')
        return headers_limpios

class CSVProcessor(ProcessorBase):
    """Procesador para archivos CSV (postpartidoequipos, postpartidojugador)"""
    
    def process(self, content: bytes, match_info: Dict) -> Optional[pd.DataFrame]:
        try:
            # Primero probar con punto y coma (delimitador de MediaCoach)
            try:
                df = pd.read_csv(io.BytesIO(content), sep=';')
                if df.shape[1] > 1:  # Si tiene múltiples columnas, el delimitador es correcto
                    self.config.logger.debug(f"✅ CSV leído con delimitador ';' - {df.shape}")
                else:
                    # Probar con coma
                    df = pd.read_csv(io.BytesIO(content), sep=',')
                    self.config.logger.debug(f"✅ CSV leído con delimitador ',' - {df.shape}")
            except:
                # Último recurso: detectar automáticamente
                df = pd.read_csv(io.BytesIO(content))
                self.config.logger.debug(f"✅ CSV leído con detección automática - {df.shape}")
            
            if df.empty:
                return None
            
            # Limpiar datos
            df = df.dropna(how='all')  # Eliminar filas completamente vacías
            
            # Añadir metadatos
            for key, value in match_info.items():
                df[f'match_{key}'] = value
            
            return df
            
        except Exception as e:
            self.config.logger.error(f"❌ Error procesando CSV: {e}")
            return None

class XMLProcessor(ProcessorBase):
    """Procesador para archivos XML (beyondstats, maximaexigenciarevisada)"""
    
    def process(self, content: bytes, match_info: Dict) -> Optional[pd.DataFrame]:
        try:
            root = ET.fromstring(content)
            instances = root.findall('.//instance')
            
            if not instances:
                return None
            
            datos = []
            
            for instance in instances:
                registro = self._procesar_instancia(instance)
                if registro:
                    # Añadir metadatos
                    for key, value in match_info.items():
                        registro[f'match_{key}'] = value
                    datos.append(registro)
            
            if not datos:
                return None
            
            return pd.DataFrame(datos)
            
        except Exception as e:
            self.config.logger.error(f"❌ Error procesando XML: {e}")
            return None
    
    def _procesar_instancia(self, instance) -> Optional[Dict]:
        """Procesa una instancia XML"""
        try:
            # Elementos básicos
            id_elem = instance.find('ID')
            start_elem = instance.find('start')
            end_elem = instance.find('end')
            code_elem = instance.find('code')
            
            if not all([elem is not None for elem in [id_elem, start_elem, end_elem, code_elem]]):
                return None
            
            # Valores básicos
            registro = {
                'id_instancia': id_elem.text,
                'inicio_segundos': float(start_elem.text),
                'fin_segundos': float(end_elem.text),
                'codigo_completo': code_elem.text,
            }
            
            registro['duracion_segundos'] = registro['fin_segundos'] - registro['inicio_segundos']
            
            # Extraer información de labels
            jugador, equipo, periodo = self._extraer_labels(instance)
            registro['jugador'] = jugador
            registro['equipo'] = equipo
            registro['periodo'] = periodo
            
            # Parsear código
            info_codigo = self._parsear_codigo(code_elem.text)
            registro.update(info_codigo)
            
            return registro
            
        except Exception as e:
            return None
    
    def _extraer_labels(self, instance) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Extrae información de los elementos label"""
        jugador = None
        equipo = None
        periodo = None
        
        labels = instance.findall('label')
        
        for label in labels:
            text_elem = label.find('text')
            group_elem = label.find('group')
            
            if text_elem is not None:
                text_value = text_elem.text
                
                if group_elem is not None:
                    group_value = group_elem.text
                    
                    if group_value == 'Equipo':
                        equipo = text_value
                    elif group_value.startswith('Jugadores '):
                        jugador = text_value
                        if equipo is None:
                            equipo = group_value.replace('Jugadores ', '')
                else:
                    if text_value in ['P1', 'P2']:
                        periodo = text_value
                    elif jugador is None:
                        jugador = text_value
        
        return jugador, equipo, periodo
    
    def _parsear_codigo(self, codigo: str) -> Dict:
        """Parsea el código para extraer información"""
        info = {'tipo_evento': codigo}
        
        if codigo == "Inicio":
            info['tipo_evento'] = "Inicio"
            return info
        
        # Ventanas de exigencia: "Ventana 1min y >21km/h"
        ventana_match = re.search(r'Ventana (\d+)min y >(\d+)km/h', codigo)
        if ventana_match:
            info['tipo_evento'] = "Ventana"
            info['duracion_ventana_min'] = int(ventana_match.group(1))
            info['velocidad_minima_kmh'] = int(ventana_match.group(2))
            return info
        
        # Alta intensidad: "Alta Intensidad (21km/h y 20m)"
        intensidad_match = re.search(r'Alta Intensidad \((\d+)km/h y (\d+)m\)', codigo)
        if intensidad_match:
            info['tipo_evento'] = "Alta Intensidad"
            info['velocidad_minima_kmh'] = int(intensidad_match.group(1))
            info['distancia_minima_m'] = int(intensidad_match.group(2))
            return info
        
        return info

# =============================================================================
# EXTRACTOR PRINCIPAL CON BARRA DE PROGRESO
# =============================================================================

class MediaCoachExtractor:
    """Extractor principal de MediaCoach con barra de progreso"""
    
    def __init__(self):
        self.config = MediaCoachConfig()
        self.control = PartidosControl(self.config)
        
        # Procesadores por tipo de archivo
        self.processors = {
            'rendimiento': RendimientoProcessor(self.config),
            'rendimiento_alt': RendimientoProcessor(self.config),
            'postpartidoequipos': CSVProcessor(self.config),
            'postpartidojugador': CSVProcessor(self.config),
            'maximaexigencia': RendimientoProcessor(self.config),
            'maximaexigencia_alt': RendimientoProcessor(self.config),
            'beyondstats': XMLProcessor(self.config),
            'maximaexigenciarevisada': XMLProcessor(self.config)
        }
        
        # DataFrames acumuladores
        self.dataframes = {tipo: [] for tipo in self.processors.keys()}
        
        # Estadísticas
        self.stats = {
            'temporadas_procesadas': 0,
            'competiciones_procesadas': 0,
            'partidos_procesados': 0,
            'partidos_nuevos': 0,
            'archivos_procesados': 0,
            'errores': 0,
            'errores_por_tipo': {tipo: 0 for tipo in self.processors.keys()}
        }
        
        # Para barra de progreso
        self.total_partidos = 0
        self.partidos_procesados_counter = 0
    
    def extraer_todo(self):
        """Ejecuta la extracción completa con barra de progreso y manejo de cuota"""
        self.config.logger.info("🚀 INICIANDO EXTRACCIÓN COMPLETA MEDIACOACH")
        inicio = time.time()
        
        try:
            # 1. Probar credenciales
            if not self.config.get_access_token():
                self.config.logger.error("❌ No se pudo obtener access token")
                return
            
            # 2. Contar total de partidos para barra de progreso
            print("📊 Contando partidos disponibles...")
            self._contar_total_partidos()
            
            if self.total_partidos == 0:
                self.config.logger.error("❌ No se encontraron partidos")
                return
            
            print(f"🎯 Total de partidos encontrados: {self.total_partidos}")
            print(f"🔄 Partidos ya procesados: {len(self.control.procesados)}")
            partidos_pendientes = self.total_partidos - len(self.control.procesados)
            print(f"📈 Partidos pendientes: {partidos_pendientes}")
            
            if partidos_pendientes == 0:
                print("✅ Todos los partidos ya están procesados")
                # Aún así, consolidar por si hay nuevos datos
                self._consolidar_parquets()
                return
            
            # 3. Procesar con barra de progreso
            with tqdm(total=self.total_partidos, desc="🚀 Procesando partidos", unit="partido") as pbar:
                pbar.update(len(self.control.procesados))  # Actualizar con ya procesados
                
                for temporada in self.config.temporadas:
                    for competicion in self.config.competiciones:
                        try:
                            self._procesar_temporada_competicion(temporada, competicion, pbar)
                        except KeyboardInterrupt:
                            self.config.logger.info("⏸️  Extracción pausada por cuota agotada o usuario")
                            break
                        except Exception as e:
                            self.config.logger.error(f"❌ Error procesando {temporada['nombre']} - {competicion['nombre']}: {e}")
                            self.stats['errores'] += 1
                            continue
                            
                    self.stats['temporadas_procesadas'] += 1
                    
                    # Si se pausó por cuota, salir del bucle de temporadas también
                    if self.stats['partidos_nuevos'] == 0 and partidos_pendientes > 0:
                        break
            
            # 4. Consolidar y guardar parquets
            print("\n🔗 Consolidando parquets...")
            self._consolidar_parquets()
            
            # 5. Reporte final
            duracion = time.time() - inicio
            self._generar_reporte_final(duracion)
            
        except KeyboardInterrupt:
            self.config.logger.info("⏸️  Extracción pausada")
            # Consolidar progreso antes de salir
            print("\n💾 Guardando progreso antes de salir...")
            self._consolidar_parquets()
            print("✅ Progreso guardado. Ejecutar nuevamente para continuar")
        except Exception as e:
            self.config.logger.error(f"❌ Error en extracción: {e}")
            import traceback
            self.config.logger.error(f"❌ Traceback: {traceback.format_exc()}")
            # Intentar guardar progreso incluso si hay error
            try:
                self._consolidar_parquets()
            except:
                pass
    
    def _contar_total_partidos(self):
        """Cuenta el total de partidos disponibles con manejo de rate limiting"""
        self.total_partidos = 0
        partidos_reales_encontrados = 0
        
        for temporada in self.config.temporadas:
            for competicion in self.config.competiciones:
                try:
                    # Delay para evitar agotar cuota durante conteo
                    time.sleep(1)
                    
                    credenciales = self.config.get_credenciales_curl()
                    comando = f"""curl --location '{self.config.api_base_url}/Championships/seasons/{temporada['id']}/competitions/{competicion['id']}/matches' {credenciales}"""
                    
                    matches = self.config.ejecutar_curl_comando(comando)
                    
                    if matches:
                        partidos_reales_encontrados += len(matches)
                        print(f"   📊 {competicion['nombre']} - {temporada['nombre']}: {len(matches)} partidos")
                
                except KeyboardInterrupt:
                    self.config.logger.info("⏸️  Conteo pausado por cuota agotada")
                    if partidos_reales_encontrados > 0:
                        print(f"📊 Conteo parcial: {partidos_reales_encontrados} partidos encontrados")
                    return
                except Exception:
                    continue
        
        # 🧪 APLICAR LÍMITE DE PRUEBA AL CONTADOR
        if self.config.limite_partidos_prueba:
            self.total_partidos = min(partidos_reales_encontrados, self.config.limite_partidos_prueba)
            print(f"\n🧪 MODO PRUEBA ACTIVADO:")
            print(f"   📊 Partidos reales encontrados: {partidos_reales_encontrados}")
            print(f"   🎯 Límite de prueba: {self.config.limite_partidos_prueba}")
            print(f"   ✅ Se procesarán máximo: {self.total_partidos} partidos")
        else:
            self.total_partidos = partidos_reales_encontrados
            print(f"\n🚀 MODO COMPLETO:")
            print(f"   📊 Total de partidos a procesar: {self.total_partidos}")
    
    def _procesar_temporada_competicion(self, temporada: Dict, competicion: Dict, pbar: tqdm):
        """Procesa una combinación temporada-competición con progreso y rate limiting"""
        self.config.logger.info(f"🏆 Procesando {competicion['nombre']} - {temporada['nombre']}")
        
        # Delay antes de llamada a la API para evitar rate limiting
        time.sleep(2)
        
        # Usar curl como en el notebook original
        credenciales = self.config.get_credenciales_curl()
        comando = f"""curl --location '{self.config.api_base_url}/Championships/seasons/{temporada['id']}/competitions/{competicion['id']}/matches' {credenciales}"""
        
        matches = self.config.ejecutar_curl_comando(comando)
        
        if not matches:
            self.config.logger.warning(f"⚠️  No se pudieron obtener partidos para {competicion['nombre']}")
            return
        
        # 🧪 APLICAR LÍMITE DE PRUEBA SI ESTÁ CONFIGURADO
        if self.config.limite_partidos_prueba:
            partidos_ya_procesados = len([m for m in matches if self.control.es_procesado(m['id'])])
            partidos_pendientes_aqui = len([m for m in matches if not self.control.es_procesado(m['id'])])
            
            if partidos_ya_procesados >= self.config.limite_partidos_prueba:
                self.config.logger.info(f"🧪 MODO PRUEBA: Ya se procesaron {partidos_ya_procesados} partidos (límite: {self.config.limite_partidos_prueba})")
                # Actualizar barra con partidos ya procesados
                for match in matches:
                    if self.control.es_procesado(match['id']):
                        pbar.update(1)
                return
            
            partidos_restantes = self.config.limite_partidos_prueba - partidos_ya_procesados
            if partidos_pendientes_aqui > partidos_restantes:
                self.config.logger.info(f"🧪 MODO PRUEBA: Limitando a {partidos_restantes} partidos más (límite total: {self.config.limite_partidos_prueba})")
                matches_pendientes = [m for m in matches if not self.control.es_procesado(m['id'])]
                matches_ya_procesados = [m for m in matches if self.control.es_procesado(m['id'])]
                matches = matches_ya_procesados + matches_pendientes[:partidos_restantes]
        
        partidos_nuevos = 0
        partidos_procesados_en_bucle = 0
        
        for i, match in enumerate(matches):
            match_id = match['id']
            
            # Actualizar descripción de la barra
            pbar.set_description(f"🏆 {competicion['nombre'][:10]} - Partido {match_id[:8]}")
            
            if not self.control.es_procesado(match_id):
                # 🧪 VERIFICAR LÍMITE DE PRUEBA DURANTE PROCESAMIENTO
                if self.config.limite_partidos_prueba and partidos_procesados_en_bucle >= self.config.limite_partidos_prueba:
                    self.config.logger.info(f"🧪 MODO PRUEBA: Alcanzado límite de {self.config.limite_partidos_prueba} partidos")
                    break
                
                try:
                    # Delay entre partidos para evitar agotar cuota
                    if i > 0:  # No delay en el primer partido
                        time.sleep(1)
                    
                    if self._procesar_partido(match, temporada, competicion):
                        self.control.marcar_procesado(match_id, temporada['nombre'], competicion['nombre'])
                        partidos_nuevos += 1
                        partidos_procesados_en_bucle += 1
                        self.stats['partidos_nuevos'] += 1
                        
                        # Actualizar progreso
                        pbar.update(1)
                        pbar.set_postfix({
                            'Nuevos': self.stats['partidos_nuevos'],
                            'Archivos': self.stats['archivos_procesados'],
                            'Errores': self.stats['errores']
                        })
                        
                        # Guardar progreso cada 10 partidos
                        if partidos_nuevos % 10 == 0:
                            self.config.logger.info(f"💾 Guardando progreso intermedio ({partidos_nuevos} partidos)")
                            self._consolidar_parquets()
                        
                except KeyboardInterrupt:
                    self.config.logger.info("⏸️  Extracción pausada por cuota agotada")
                    raise
                except Exception as e:
                    self.config.logger.warning(f"⚠️  Partido {match_id}: Error - {e}")
                    self.stats['errores'] += 1
                    continue
            else:
                # Partido ya procesado, solo actualizar barra
                pbar.update(1)
        
        if self.config.limite_partidos_prueba:
            self.config.logger.info(f"✅ {competicion['nombre']}: {partidos_nuevos} partidos nuevos procesados (MODO PRUEBA: límite {self.config.limite_partidos_prueba})")
        else:
            self.config.logger.info(f"✅ {competicion['nombre']}: {partidos_nuevos} partidos nuevos procesados")
        self.stats['competiciones_procesadas'] += 1
    
    def _procesar_partido(self, match: Dict, temporada: Dict, competicion: Dict) -> bool:
        """Procesa un partido individual con rate limiting"""
        match_id = match['id']
        
        try:
            # Delay antes de obtener assets
            time.sleep(1)
            
            # Obtener assets del partido usando curl
            credenciales = self.config.get_credenciales_curl()
            comando = f"curl --location '{self.config.api_base_url}/Assets/{match_id}' {credenciales}"
            
            assets = self.config.ejecutar_curl_comando(comando)
            
            if not assets:
                return False
            
            # Información del match
            match_info = {
                'id': match_id,
                'temporada': temporada['nombre'],
                'competicion': competicion['nombre'],
                'match_day': match.get('matchDayNumber'),
                'fecha_match': match.get('date')
            }
            
            # Mapeo de índices a tipos de archivo (basado en el notebook que funciona)
            asset_mapping = [
                (0, 'rendimiento'),                    # xlsx
                (1, 'rendimiento_alt'),               # xlsx
                (5, 'postpartidoequipos'),            # csv
                (9, 'postpartidojugador'),            # csv
                (11, 'maximaexigencia'),              # xlsx
                (12, 'maximaexigencia_alt'),          # xlsx
                (13, 'beyondstats'),                  # xml
                (14, 'maximaexigenciarevisada')       # xml
            ]
            
            archivos_procesados = 0
            
            for asset_idx, tipo_archivo in asset_mapping:
                try:
                    if asset_idx < len(assets) and assets[asset_idx] and assets[asset_idx].get('url'):
                        url = assets[asset_idx]['url']
                        
                        # Delay entre descargas para no sobrecargar el servidor
                        if archivos_procesados > 0:
                            time.sleep(0.5)
                        
                        # Descargar archivo con timeout y headers
                        try:
                            file_response = requests.get(url, timeout=30, headers={'User-Agent': 'MediaCoach-Extractor/1.0'})
                            if file_response.status_code == 200:
                                content = file_response.content
                                
                                df = self.processors[tipo_archivo].process(content, match_info)
                                if df is not None and not df.empty:
                                    self.dataframes[tipo_archivo].append(df)
                                    archivos_procesados += 1
                                    self.stats['archivos_procesados'] += 1
                
                        except requests.exceptions.Timeout:
                            self.config.logger.warning(f"⚠️  Timeout descargando {tipo_archivo}")
                        except requests.exceptions.RequestException as e:
                            self.config.logger.warning(f"⚠️  Error descarga {tipo_archivo}: {e}")
                
                except Exception as e:
                    self.stats['errores'] += 1
                    self.stats['errores_por_tipo'][tipo_archivo] += 1
                    continue
            
            if archivos_procesados > 0:
                self.stats['partidos_procesados'] += 1
                return True
            else:
                return False
            
        except Exception as e:
            self.config.logger.error(f"❌ Error procesando partido {match_id}: {e}")
            self.stats['errores'] += 1
            return False
    
    def _consolidar_parquets(self):
        """Consolida todos los DataFrames y guarda los parquets"""
        archivos_creados = 0
        
        # Barra de progreso para consolidación
        with tqdm(total=len(self.processors), desc="💾 Consolidando parquets", unit="tipo") as pbar:
            
            for tipo, dfs in self.dataframes.items():
                pbar.set_description(f"💾 Consolidando {tipo}")
                
                if not dfs:
                    self.config.logger.info(f"📭 No hay datos nuevos para {tipo}")
                    pbar.update(1)
                    continue
                
                try:
                    # Cargar parquet existente si existe
                    parquet_file = os.path.join(self.config.parquets_path, f'{tipo}_consolidado.parquet')
                    
                    if os.path.exists(parquet_file):
                        try:
                            df_existente = pd.read_parquet(parquet_file)
                            self.config.logger.info(f"📁 {tipo}: Cargando {len(df_existente)} registros existentes")
                            dfs.insert(0, df_existente)
                        except Exception as e:
                            self.config.logger.warning(f"⚠️  Error cargando parquet existente {tipo}: {e}")
                    
                    # Consolidar todos los DataFrames
                    df_consolidado = pd.concat(dfs, ignore_index=True, sort=False)
                    
                    # Eliminar duplicados
                    columnas_id = [col for col in df_consolidado.columns if 'match_id' in col or 'id_instancia' in col]
                    if columnas_id:
                        antes = len(df_consolidado)
                        df_consolidado = df_consolidado.drop_duplicates(subset=columnas_id, keep='last')
                        duplicados = antes - len(df_consolidado)
                        if duplicados > 0:
                            self.config.logger.info(f"🔄 {tipo}: Eliminados {duplicados} duplicados")
                    
                    # Guardar parquet
                    df_consolidado.to_parquet(parquet_file, index=False)
                    
                    tamaño_mb = os.path.getsize(parquet_file) / 1024 / 1024
                    self.config.logger.info(f"💾 {tipo}: {len(df_consolidado):,} registros - {tamaño_mb:.2f} MB")
                    archivos_creados += 1
                    
                    pbar.set_postfix({'Creados': archivos_creados, 'Registros': f"{len(df_consolidado):,}"})
                    
                except Exception as e:
                    self.config.logger.error(f"❌ Error consolidando {tipo}: {e}")
                
                pbar.update(1)
        
        if archivos_creados == 0:
            self.config.logger.warning("⚠️  No se crearon archivos parquet nuevos")
        else:
            self.config.logger.info(f"✅ {archivos_creados} archivos parquet procesados")
    
    def _generar_reporte_final(self, duracion: float):
        """Genera el reporte final de la extracción"""
        print("\n" + "="*80)
        print("📋 REPORTE FINAL DE EXTRACCIÓN MEDIACOACH")
        print("="*80)
        
        print(f"⏱️  Duración total: {duracion/60:.1f} minutos")
        print(f"📅 Temporadas procesadas: {self.stats['temporadas_procesadas']}")
        print(f"🏆 Competiciones procesadas: {self.stats['competiciones_procesadas']}")
        print(f"⚽ Partidos procesados: {self.stats['partidos_procesados']}")
        print(f"🆕 Partidos nuevos: {self.stats['partidos_nuevos']}")
        print(f"📊 Archivos procesados: {self.stats['archivos_procesados']}")
        print(f"❌ Errores totales: {self.stats['errores']}")
        
        # Errores por tipo
        if self.stats['errores'] > 0:
            print(f"\n❌ ERRORES POR TIPO:")
            for tipo, errores in self.stats['errores_por_tipo'].items():
                if errores > 0:
                    print(f"   🔴 {tipo}: {errores} errores")
        
        # Tamaños de parquets
        print(f"\n📁 ARCHIVOS PARQUET GENERADOS:")
        total_size = 0
        
        if os.path.exists(self.config.parquets_path):
            for archivo in sorted(os.listdir(self.config.parquets_path)):
                if archivo.endswith('.parquet'):
                    ruta = os.path.join(self.config.parquets_path, archivo)
                    tamaño = os.path.getsize(ruta) / 1024 / 1024
                    total_size += tamaño
                    
                    # Contar registros
                    try:
                        df_temp = pd.read_parquet(ruta)
                        registros = len(df_temp)
                        print(f"   📊 {archivo}: {registros:,} registros - {tamaño:.2f} MB")
                    except:
                        print(f"   📊 {archivo}: {tamaño:.2f} MB")
        
        print(f"\n💾 Tamaño total: {total_size:.2f} MB")
        
        # Porcentaje de completitud
        if self.total_partidos > 0:
            porcentaje_completitud = (len(self.control.procesados) / self.total_partidos) * 100
            print(f"📈 Completitud: {porcentaje_completitud:.1f}% ({len(self.control.procesados)}/{self.total_partidos} partidos)")
            
            if self.config.limite_partidos_prueba:
                print(f"\n🧪 MODO PRUEBA:")
                print(f"   ✅ Límite actual: {self.config.limite_partidos_prueba} partidos")
                print(f"   📝 Para procesar TODOS: Cambiar línea 27 a None")
                print(f"   🔄 Para más pruebas: Cambiar línea 27 a 20, 50, etc.")
                if porcentaje_completitud >= 100:
                    print(f"   🎯 Prueba completada exitosamente!")
            else:
                if porcentaje_completitud < 100:
                    partidos_restantes = self.total_partidos - len(self.control.procesados)
                    print(f"📋 Pendientes: {partidos_restantes} partidos")
                    print(f"💡 Ejecutar nuevamente para continuar (respeta límites API)")
        
        # Información sobre cuota API
        print(f"\n🚫 INFORMACIÓN DE CUOTA API:")
        print(f"   ⏱️  Delays aplicados para conservar cuota")
        print(f"   💾 Progreso guardado automáticamente cada 10 partidos")
        print(f"   🔄 Reanudar ejecutando: python3 extractor_mediacoach_final.py")
        
        if self.config.limite_partidos_prueba:
            print(f"\n🧪 PRÓXIMOS PASOS:")
            print(f"   1. ✅ Verificar que los {self.config.limite_partidos_prueba} parquets se generaron correctamente")
            print(f"   2. 📊 Analizar los datos de prueba")
            print(f"   3. 🚀 Cambiar línea 27 a None para procesar todo")
            print(f"   4. 🔄 Ejecutar nuevamente para extracción completa")
        
        print("\n🎉 ¡EXTRACCIÓN COMPLETADA!")
        print(f"📂 Archivos generados en: {self.config.parquets_path}")

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def main():
    """Función principal"""
    print("⚽ EXTRACTOR COMPLETO MEDIACOACH API → PARQUET (VERSIÓN FINAL)")
    print("="*70)
    
    # Mostrar modo actual
    config_temp = MediaCoachConfig()
    if config_temp.limite_partidos_prueba:
        print("🧪 MODO PRUEBA ACTIVADO")
        print(f"   🎯 Límite: {config_temp.limite_partidos_prueba} partidos")
        print(f"   📝 Para cambiar: Línea 27 → self.limite_partidos_prueba = None")
        print("="*70)
    else:
        print("🚀 MODO COMPLETO ACTIVADO")
        print("   📊 Procesará TODOS los partidos disponibles")
        print("   📝 Para pruebas: Línea 27 → self.limite_partidos_prueba = 10")
        print("="*70)
    
    print("📊 DATOS EXTRAÍDOS:")
    print("   🏃 XLSX Rendimiento: Fila 6 headers, Columna E info partido")
    print("   📈 XLSX Máxima Exigencia: Datos físicos de jugadores")
    print("   📋 CSV Post-partido: Equipos y jugadores (delimitador ;)")
    print("   🔬 XML Beyond Stats: Estadísticas avanzadas")
    print("   ⚡ XML Máxima Exigencia Revisada: Ventanas de velocidad")
    print("="*70)
    print("🔧 CARACTERÍSTICAS:")
    print("   ✅ Proceso incremental (solo nuevos)")
    print("   📊 Barra de progreso en tiempo real")
    print("   🗂️  8 tipos de archivos por partido")
    print("   💾 Un parquet consolidado por tipo")
    print("   🛡️  Resiliente a errores")
    print("   🚫 MANEJO INTELIGENTE DE CUOTA API:")
    print("      • Detecta automáticamente cuota agotada")
    print("      • Guarda progreso antes de pausar")
    print("      • Opción de espera automática o manual")
    print("      • Delays entre llamadas para conservar cuota")
    print("="*70)
    print("📂 TEMPORADAS: 24-25 y 23-24")
    print("🏆 COMPETICIONES: La Liga y La Liga 2")
    print("="*70)
    print("⚠️  IMPORTANTE:")
    print("   🚫 La API tiene límite de cuota diario")
    print("   💾 El progreso se guarda automáticamente")
    print("   🔄 Se puede reanudar ejecutando nuevamente")
    print("   ⏱️  Delays automáticos para conservar cuota")
    if config_temp.limite_partidos_prueba:
        print(f"   🧪 MODO PRUEBA: Solo {config_temp.limite_partidos_prueba} partidos")
        print("   📝 Para procesar todo: Cambiar línea 27 a None")
    print("="*70)
    
    respuesta = input("\n🚀 ¿Iniciar extracción? (s/n): ").lower().strip()
    
    if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
        extractor = MediaCoachExtractor()
        extractor.extraer_todo()
    else:
        print("❌ Extracción cancelada")

if __name__ == "__main__":
    main()