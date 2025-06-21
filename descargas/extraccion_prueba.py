#!/usr/bin/env python3
"""
MediaCoach Data Extractor
Script mejorado para extraer datos de MediaCoach API y convertir a formato Parquet
"""

import requests
import json
import os
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
import argparse
from typing import List, Dict, Optional
import time
from io import BytesIO

class MediaCoachExtractor:
    def __init__(self):
        # Credenciales MediaCoach
        self.client_id = '58191b89-cee4-11ed-a09d-ee50c5eb4bb5'
        self.username = 'b2bvillarealcf@mediacoach.es'
        self.password = 'r728-FHj3RE!'
        self.subscription_key = '729f9154234d4ff3bb0a692c6a0510c4'
        self.api_url_base = "https://club-api.mediacoach.es"
        
        # Headers para autenticación
        self.headers = {}
        self.access_token = None
        
        # Configuración de archivos
        self.file_config = [
            {'index': 0, 'folder': 'Rendimiento', 'extension': 'xlsx', 'name': 'rendimiento_1'},
            {'index': 1, 'folder': 'Rendimiento', 'extension': 'xlsx', 'name': 'rendimiento_2'},
            {'index': 5, 'folder': 'PostPartidoEquipos', 'extension': 'csv', 'name': 'equipos'},
            {'index': 9, 'folder': 'PostPartidoJugador', 'extension': 'csv', 'name': 'jugadores'},
            {'index': 11, 'folder': 'MaximaExigencia', 'extension': 'xlsx', 'name': 'maxima_exigencia_1'},
            {'index': 12, 'folder': 'MaximaExigencia', 'extension': 'xlsx', 'name': 'maxima_exigencia_2'},
            {'index': 13, 'folder': 'BeyondStats', 'extension': 'xml', 'name': 'beyond_stats'},
            {'index': 14, 'folder': 'MaximaExigenciaRevisada', 'extension': 'xml', 'name': 'maxima_exigencia_revisada'}
        ]

    def authenticate(self) -> bool:
        """Autenticar con MediaCoach API"""
        print("🔐 Autenticando con MediaCoach API...")
        
        token_url = 'https://id.mediacoach.es/connect/token'
        data = {
            'client_id': self.client_id,
            'scope': 'b2bapiclub-api',
            'grant_type': 'password',
            'username': self.username,
            'password': self.password
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        
        try:
            response = requests.post(token_url, data=data, headers=headers)
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get('access_token')
                
                self.headers = {
                    'Ocp-Apim-Subscription-Key': self.subscription_key,
                    'Authorization': f'Bearer {self.access_token}'
                }
                print("✅ Autenticación exitosa")
                return True
            else:
                print(f"❌ Error en autenticación: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Error en autenticación: {e}")
            return False

    def get_seasons(self) -> List[Dict]:
        """Obtener todas las temporadas disponibles"""
        print("📅 Obteniendo temporadas disponibles...")
        
        url = f"{self.api_url_base}/Championships/seasons"
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                seasons = response.json()
                print(f"✅ Encontradas {len(seasons)} temporadas")
                return seasons
            else:
                print(f"❌ Error obteniendo temporadas: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Error obteniendo temporadas: {e}")
            return []

    def get_competitions(self, season_id: str) -> List[Dict]:
        """Obtener competiciones para una temporada"""
        url = f"{self.api_url_base}/Championships/seasons/{season_id}/competitions"
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ Error obteniendo competiciones: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Error obteniendo competiciones: {e}")
            return []

    def get_matches(self, season_id: str, competition_id: str, max_matches: int) -> List[str]:
        """Obtener IDs de partidos para una competición"""
        url = f"{self.api_url_base}/Championships/seasons/{season_id}/competitions/{competition_id}/matches"
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                matches = response.json()
                # Filtrar solo partidos completados y limitar cantidad
                completed_matches = [m['id'] for m in matches if m.get('status') == 'Completed']
                return completed_matches[:max_matches]
            else:
                print(f"❌ Error obteniendo partidos: {response.status_code}")
                return []
        except Exception as e:
            print(f"❌ Error obteniendo partidos: {e}")
            return []

    def xml_to_dict(self, xml_content: bytes) -> Dict:
        """Convertir XML a diccionario para después guardar como Parquet"""
        try:
            root = ET.fromstring(xml_content)
            
            def elem_to_dict(elem):
                result = {}
                for child in elem:
                    if len(child) == 0:
                        result[child.tag] = child.text
                    else:
                        result[child.tag] = elem_to_dict(child)
                return result
            
            return {root.tag: elem_to_dict(root)}
        except Exception as e:
            print(f"⚠️ Error parseando XML: {e}")
            return {}

    def download_and_convert_file(self, file_url: str, file_config: Dict, 
                                match_id: str, output_dir: Path) -> bool:
        """Descargar archivo y convertir a Parquet"""
        try:
            response = requests.get(file_url)
            if response.status_code != 200:
                return False
            
            # Crear directorio si no existe
            folder_path = output_dir / file_config['folder']
            folder_path.mkdir(parents=True, exist_ok=True)
            
            # Nombre del archivo final en Parquet
            filename = f"{file_config['name']}_{match_id}.parquet"
            file_path = folder_path / filename
            
            # Convertir según el tipo de archivo
            if file_config['extension'] in ['xlsx', 'csv']:
                if file_config['extension'] == 'xlsx':
                    df = pd.read_excel(BytesIO(response.content))
                else:  # csv
                    df = pd.read_csv(BytesIO(response.content))
                
                # Añadir metadatos del partido
                df['match_id'] = match_id
                df['file_type'] = file_config['name']
                
                # Guardar como Parquet
                df.to_parquet(file_path, engine='pyarrow', index=False)
                
            elif file_config['extension'] == 'xml':
                # Convertir XML a diccionario y luego a DataFrame
                xml_dict = self.xml_to_dict(response.content)
                
                # Flatten el diccionario para crear DataFrame
                flattened_data = self.flatten_dict(xml_dict)
                df = pd.DataFrame([flattened_data])
                
                # Añadir metadatos
                df['match_id'] = match_id
                df['file_type'] = file_config['name']
                
                # Guardar como Parquet
                df.to_parquet(file_path, engine='pyarrow', index=False)
            
            return True
            
        except Exception as e:
            print(f"⚠️ Error procesando archivo {file_config['name']}: {e}")
            return False

    def flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Aplanar diccionario anidado"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def download_match_files(self, match_id: str, output_dir: Path) -> int:
        """Descargar todos los archivos de un partido"""
        url = f"{self.api_url_base}/Assets/{match_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                return 0
            
            asset_data = response.json()
            files_downloaded = 0
            
            for config in self.file_config:
                try:
                    file_url = asset_data[config['index']]['url']
                    if file_url and self.download_and_convert_file(file_url, config, match_id, output_dir):
                        files_downloaded += 1
                except (IndexError, KeyError):
                    print(f"⚠️ Archivo {config['name']} no disponible para partido {match_id}")
                    continue
            
            return files_downloaded
            
        except Exception as e:
            print(f"❌ Error descargando archivos del partido {match_id}: {e}")
            return 0

    def extract_data(self, matches_per_competition: int, output_dir: str = "mediacoach_data"):
        """Extraer datos de todas las competiciones"""
        if not self.authenticate():
            return
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Obtener todas las temporadas
        seasons = self.get_seasons()
        if not seasons:
            print("❌ No se pudieron obtener las temporadas")
            return
        
        total_files = 0
        total_matches = 0
        
        for season in seasons:
            season_name = season.get('name', season.get('id'))
            print(f"\n🏆 Procesando temporada: {season_name}")
            
            # Crear directorio para la temporada
            season_dir = output_path / season_name.replace(' ', '_').replace('/', '_')
            
            # Obtener competiciones de la temporada
            competitions = self.get_competitions(season['id'])
            
            for competition in competitions:
                comp_name = competition.get('name', competition.get('id'))
                print(f"\n⚽ Procesando competición: {comp_name}")
                
                # Crear directorio para la competición
                comp_dir = season_dir / comp_name.replace(' ', '_').replace('/', '_')
                
                # Obtener partidos
                matches = self.get_matches(season['id'], competition['id'], matches_per_competition)
                
                if not matches:
                    print(f"⚠️ No hay partidos disponibles para {comp_name}")
                    continue
                
                print(f"📊 Descargando {len(matches)} partidos...")
                
                for i, match_id in enumerate(matches, 1):
                    print(f"   Partido {i}/{len(matches)}: {match_id}")
                    files_count = self.download_match_files(match_id, comp_dir)
                    total_files += files_count
                    
                    if files_count > 0:
                        total_matches += 1
                    
                    # Pequeña pausa para no sobrecargar la API
                    time.sleep(0.5)
        
        print(f"\n✅ Extracción completada:")
        print(f"   📁 Total de archivos descargados: {total_files}")
        print(f"   ⚽ Total de partidos procesados: {total_matches}")
        print(f"   📂 Datos guardados en: {output_path.absolute()}")

def main():
    parser = argparse.ArgumentParser(description='Extraer datos de MediaCoach API')
    parser.add_argument('--partidos', '-p', type=int, default=1,
                       help='Número de partidos por competición (por defecto: 1)')
    parser.add_argument('--output', '-o', type=str, default='mediacoach_data',
                       help='Directorio de salida (por defecto: mediacoach_data)')
    
    args = parser.parse_args()
    
    print("🚀 MediaCoach Data Extractor")
    print("=" * 50)
    print(f"📊 Partidos por competición: {args.partidos}")
    print(f"📂 Directorio de salida: {args.output}")
    print("=" * 50)
    
    extractor = MediaCoachExtractor()
    extractor.extract_data(args.partidos, args.output)

if __name__ == "__main__":
    main()