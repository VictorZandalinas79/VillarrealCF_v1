import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import numpy as np
import os
from difflib import SequenceMatcher
import warnings
warnings.filterwarnings('ignore')

# 🔥 CONFIGURACIÓN GLOBAL AGRESIVA (COPIADA DEL PRIMER SCRIPT)
plt.rcParams.update({
    'figure.autolayout': False,
    'figure.constrained_layout.use': False,
    'figure.subplot.left': 0,
    'figure.subplot.right': 1,
    'figure.subplot.top': 1,
    'figure.subplot.bottom': 0,
    'figure.subplot.wspace': 0,
    'figure.subplot.hspace': 0,
    'axes.xmargin': 0,
    'axes.ymargin': 0,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0
})

try:
    from mplsoccer import Pitch
except ImportError:
    print("Instalando mplsoccer...")
    import subprocess
    subprocess.check_call(["pip", "install", "mplsoccer"])
    from mplsoccer import Pitch

class ReporteTactico4CamposHorizontalesMejorado:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        """
        Inicializa la clase para generar reportes tácticos con 4 campos horizontales
        CON COORDENADAS FIJAS (SIN AUTO-MOVIMIENTO)
        """
        self.data_path = data_path
        self.df = None
        self.load_data()
        self.clean_team_names()
        
        # 🏟️ COORDENADAS FIJAS PARA CAMPOS HORIZONTALES (Pitch 0-120 x 0-80)
        self.coordenadas_posiciones = {
            'PORTERO': (10, 40),
            'LATERAL_DERECHO': (50, 13),
            'CENTRAL_DERECHO': (25, 20),
            'CENTRAL_IZQUIERDO': (25, 60),
            'LATERAL_IZQUIERDO': (50, 70),
            'MC_POSICIONAL': (30, 50),
            'MC_BOX_TO_BOX': (70, 20),
            'MC_ORGANIZADOR': (65, 65),
            'BANDA_DERECHA': (90, 13),
            'BANDA_IZQUIERDA': (90, 70),
            'MEDIAPUNTA': (75, 40),
            'DELANTERO_CENTRO': (105, 45),
            'SEGUNDO_DELANTERO': (105, 25),
        }
        
        # Mapeo de demarcaciones a posiciones
        self.demarcacion_to_posicion = {
            'Portero': 'PORTERO',
            'Defensa - Lateral Izquierdo': 'LATERAL_IZQUIERDO',
            'Defensa - Central Izquierdo': 'CENTRAL_IZQUIERDO',
            'Defensa - Central Derecho': 'CENTRAL_DERECHO', 
            'Defensa - Lateral Derecho': 'LATERAL_DERECHO',
            'Centrocampista - MC Posicional': 'MC_POSICIONAL',
            'Centrocampista - MC Box to Box': 'MC_BOX_TO_BOX',
            'Centrocampista - MC Organizador': 'MC_ORGANIZADOR',
            'Centrocampista de ataque - Banda Derecha': 'BANDA_DERECHA',
            'Centrocampista de ataque - Banda Izquierda': 'BANDA_IZQUIERDA',
            'Centrocampista de ataque - Mediapunta': 'MEDIAPUNTA',
            'Delantero - Delantero Centro': 'DELANTERO_CENTRO',
            'Delantero - Segundo Delantero': 'SEGUNDO_DELANTERO',
            'Sin Posición': 'MC_BOX_TO_BOX'
        }
        
        # ✅ MÉTRICAS COMPLETAS
        self.metricas_tabla = [
            'Distancia Total',
            'Distancia Total / min',
            'Distancia Total 14-21 km / h',
            'Distancia Total 14-21 km / h / min',
            'Distancia Total >21 km / h', 
            'Distancia Total >21 km / h / min',
            'Distancia Total >24 km / h',
            'Distancia Total >24 km / h / min',
            'Velocidad Máxima Total'
        ]
        
        # Colores por equipo
        self.team_colors = {
            'Athletic Club': {'primary': '#EE2E24', 'secondary': '#FFFFFF', 'text': 'white'},
            'Atlético de Madrid': {'primary': '#CB3524', 'secondary': '#FFFFFF', 'text': 'white'},
            'CA Osasuna': {'primary': '#D2001C', 'secondary': '#001A4B', 'text': 'white'},
            'CD Leganés': {'primary': '#004C9F', 'secondary': '#FFFFFF', 'text': 'white'},
            'Deportivo Alavés': {'primary': '#1F4788', 'secondary': '#FFFFFF', 'text': 'white'},
            'FC Barcelona': {'primary': '#004D98', 'secondary': '#A50044', 'text': 'white'},
            'Getafe CF': {'primary': '#005CA9', 'secondary': '#FFFFFF', 'text': 'white'},
            'Girona FC': {'primary': '#CC0000', 'secondary': '#FFFFFF', 'text': 'white'},
            'RC Celta': {'primary': '#87CEEB', 'secondary': '#FFFFFF', 'text': 'black'},
            'RCD Espanyol': {'primary': '#004C9F', 'secondary': '#FFFFFF', 'text': 'white'},
            'RCD Mallorca': {'primary': '#CC0000', 'secondary': '#FFFF00', 'text': 'white'},
            'Rayo Vallecano': {'primary': '#CC0000', 'secondary': '#FFFFFF', 'text': 'white'},
            'Real Betis': {'primary': '#00954C', 'secondary': '#FFFFFF', 'text': 'white'},
            'Real Madrid': {'primary': '#FFFFFF', 'secondary': '#FFD700', 'text': 'black'},
            'Real Sociedad': {'primary': '#004C9F', 'secondary': '#FFFFFF', 'text': 'white'},
            'Real Valladolid CF': {'primary': '#663399', 'secondary': '#FFFFFF', 'text': 'white'},
            'Sevilla FC': {'primary': '#D2001C', 'secondary': '#FFFFFF', 'text': 'white'},
            'UD Las Palmas': {'primary': '#FFFF00', 'secondary': '#004C9F', 'text': 'black'},
            'Valencia CF': {'primary': '#FF7F00', 'secondary': '#000000', 'text': 'white'},
            'Villarreal CF': {'primary': '#FFD700', 'secondary': '#004C9F', 'text': 'black'},
        }
        self.default_team_colors = {'primary': '#2c3e50', 'secondary': '#FFFFFF', 'text': 'white'}
        
    def load_data(self):
        """Carga los datos del archivo parquet"""
        try:
            self.df = pd.read_parquet(self.data_path)
            print(f"✅ Datos cargados: {self.df.shape[0]} filas, {self.df.shape[1]} columnas")
        except Exception as e:
            print(f"❌ Error al cargar datos: {e}")
            
    def similarity(self, a, b):
        """Calcula la similitud entre dos strings"""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    def clean_team_names(self):
        """Limpia y agrupa nombres de equipos similares"""
        if self.df is None:
            return
        
        # Limpiar nombres de equipos
        unique_teams = self.df['Equipo'].unique()
        team_mapping = {}
        processed_teams = set()
        
        for team in unique_teams:
            if team in processed_teams:
                continue
                
            similar_teams = [team]
            for other_team in unique_teams:
                if other_team != team and other_team not in processed_teams:
                    if self.similarity(team, other_team) > 0.7:
                        similar_teams.append(other_team)
            
            canonical_name = max(similar_teams, key=len)
            for similar_team in similar_teams:
                team_mapping[similar_team] = canonical_name
                processed_teams.add(similar_team)
        
        self.df['Equipo'] = self.df['Equipo'].map(team_mapping)
        
        # Normalizar jornadas
        def normalize_jornada(jornada):
            if isinstance(jornada, str) and jornada.startswith(('J', 'j')):
                try:
                    return int(jornada[1:])
                except ValueError:
                    return jornada
            return jornada
        
        self.df['Jornada'] = self.df['Jornada'].apply(normalize_jornada)
        print(f"✅ Limpieza completada. Equipos únicos: {len(self.df['Equipo'].unique())}")
        
    def get_available_teams(self):
        """Retorna equipos disponibles"""
        if self.df is None:
            return []
        return sorted(self.df['Equipo'].unique())
    
    def get_available_jornadas(self):
        """Retorna jornadas disponibles"""
        if self.df is None:
            return []
        return sorted(self.df['Jornada'].unique())
    
    def determinar_local_visitante(self, partido, equipo):
        """Determina si un partido es local o visitante para un equipo"""
        if '-' not in partido:
            return 'desconocido'
        
        partes = partido.split('-')
        if len(partes) != 2:
            return 'desconocido'
        
        equipo_local_partido = partes[0].strip()
        equipo_visitante_partido = partes[1].strip()
        
        sim_local = self.similarity(equipo, equipo_local_partido)
        sim_visitante = self.similarity(equipo, equipo_visitante_partido)
        
        if sim_local > sim_visitante:
            return 'local'
        elif sim_visitante > sim_local:
            return 'visitante'
        else:
            return 'desconocido'

    def extraer_rival(self, partido, equipo):
        """Extrae el nombre del equipo rival del partido"""
        if '-' not in partido:
            return 'Rival'
        
        partes = partido.split('-')
        if len(partes) != 2:
            return 'Rival'
        
        equipo_local = partes[0].strip()
        equipo_visitante = partes[1].strip()
        
        sim_local = self.similarity(equipo, equipo_local)
        sim_visitante = self.similarity(equipo, equipo_visitante)
        
        if sim_local > sim_visitante:
            return equipo_visitante
        else:
            return equipo_local
    
    def get_last_5_jornadas(self, equipo, jornada_referencia):
        """Obtiene las últimas 5 jornadas incluyendo la de referencia"""
        jornadas_disponibles = self.get_available_jornadas()
        
        # Normalizar jornada de referencia
        if isinstance(jornada_referencia, str) and jornada_referencia.startswith('J'):
            try:
                jornada_referencia = int(jornada_referencia[1:])
            except ValueError:
                pass
        elif isinstance(jornada_referencia, str) and jornada_referencia.startswith('j'):
            try:
                jornada_referencia = int(jornada_referencia[1:])
            except ValueError:
                pass
        
        # Filtrar jornadas menores o iguales a la de referencia
        jornadas_validas = [j for j in jornadas_disponibles if j <= jornada_referencia]
        
        # Tomar las últimas 5
        if len(jornadas_validas) >= 5:
            return sorted(jornadas_validas)[-5:]
        else:
            return sorted(jornadas_validas)

    def get_posible_11(self, equipo, jornada):
        """Obtiene el posible 11 inicial basado en minutos jugados en las últimas 5 jornadas"""
        
        # ✅ POSICIONES BÁSICAS OBLIGATORIAS (9 posiciones fijas)
        posiciones_basicas = [
            'PORTERO',
            'LATERAL_DERECHO', 
            'LATERAL_IZQUIERDO',
            'MC_POSICIONAL',
            'MC_BOX_TO_BOX',
            'MC_ORGANIZADOR',
            'BANDA_DERECHA',
            'BANDA_IZQUIERDA'
        ]
        
        # ✅ POSICIONES FLEXIBLES (pueden ser 1 o 2 jugadores)
        posiciones_flexibles = {
            'centrales': ['CENTRAL_DERECHO', 'CENTRAL_IZQUIERDO'],
            'delanteros': ['DELANTERO_CENTRO', 'SEGUNDO_DELANTERO']
        }

        # Obtener las últimas 5 jornadas
        jornadas_analizar = self.get_last_5_jornadas(equipo, jornada)
        print(f"🔄 Analizando jornadas: {jornadas_analizar}")
        
        # Filtrar datos del equipo en esas jornadas
        filtered_df = self.df[
            (self.df['Equipo'] == equipo) & 
            (self.df['Jornada'].isin(jornadas_analizar))
        ].copy()
        
        if filtered_df.empty:
            print(f"❌ No hay datos para {equipo} en las jornadas {jornadas_analizar}")
            return None
        
        # Rellenar demarcaciones vacías
        filtered_df = self.fill_missing_demarcaciones(filtered_df)
        
        # Verificar si Alias está vacío y usar Nombre en su lugar
        if 'Nombre' in filtered_df.columns:
            mask_empty_alias = filtered_df['Alias'].isna() | (filtered_df['Alias'] == '') | (filtered_df['Alias'].str.strip() == '')
            filtered_df.loc[mask_empty_alias, 'Alias'] = filtered_df.loc[mask_empty_alias, 'Nombre']
        
        # Agrupar por jugador y calcular estadísticas
        print(f"🔄 Calculando estadísticas acumuladas...")
        
        player_stats = {}
        
        for _, row in filtered_df.iterrows():
            jugador_id = row['Id Jugador']
            jugador_alias = row['Alias']
            demarcacion = row.get('Demarcacion', 'Sin Posición')
            
            if jugador_id not in player_stats:
                player_stats[jugador_id] = {
                    'Alias': jugador_alias,
                    'Dorsal': row.get('Dorsal', 'N/A'),
                    'Demarcacion': demarcacion,
                    'minutos_total': 0,
                    'partidos': 0,
                    'stats': {}
                }
            
            # Acumular minutos
            minutos = row.get('Minutos jugados', 0)
            player_stats[jugador_id]['minutos_total'] += minutos
            player_stats[jugador_id]['partidos'] += 1
            
            # Acumular estadísticas para todas las métricas disponibles
            for metric in self.metricas_tabla:
                if metric in row and pd.notna(row[metric]):
                    if metric not in player_stats[jugador_id]['stats']:
                        player_stats[jugador_id]['stats'][metric] = []
                    player_stats[jugador_id]['stats'][metric].append(row[metric])
        
        # Calcular promedios/sumas/máximos según la métrica
        for jugador_id in player_stats:
            for metric in self.metricas_tabla:
                if metric in player_stats[jugador_id]['stats']:
                    values = player_stats[jugador_id]['stats'][metric]
                    
                    if 'Velocidad Máxima' in metric:  # Velocidad máxima: promedio
                        player_stats[jugador_id]['stats'][metric] = np.mean(values)
                    elif '/min' in metric or '/ min' in metric:  # Métricas por minuto: promedio
                        player_stats[jugador_id]['stats'][metric] = np.mean(values)
                    else:  # Distancias totales: suma
                        player_stats[jugador_id]['stats'][metric] = np.sum(values)
                else:
                    player_stats[jugador_id]['stats'][metric] = 0
        
        # Agrupar jugadores por posición
        jugadores_por_posicion = {}
        for jugador_id, data in player_stats.items():
            demarcacion = data['Demarcacion']
            posicion = self.demarcacion_to_posicion.get(demarcacion, 'MC_BOX_TO_BOX')
            
            if posicion not in jugadores_por_posicion:
                jugadores_por_posicion[posicion] = []
            
            jugadores_por_posicion[posicion].append({
                'Id': jugador_id,
                'Alias': data['Alias'],
                'Dorsal': data['Dorsal'],
                'Demarcacion': demarcacion,
                'minutos_total': data['minutos_total'],
                'stats': data['stats']
            })
        
        # ✅ NUEVA LÓGICA: Seleccionar jugadores con flexibilidad para centrales y delanteros
        posible_11 = {}
        jugadores_seleccionados = set()  # Para evitar duplicados
        
        # 1. Llenar posiciones básicas obligatorias (8 jugadores)
        for posicion in posiciones_basicas:
            if posicion in jugadores_por_posicion:
                # Filtrar jugadores no seleccionados
                jugadores_disponibles = [j for j in jugadores_por_posicion[posicion] 
                                    if j['Id'] not in jugadores_seleccionados]
                
                if jugadores_disponibles:
                    # Ordenar por minutos y tomar el mejor
                    mejor_jugador = max(jugadores_disponibles, key=lambda x: x['minutos_total'])
                    posible_11[posicion] = mejor_jugador
                    jugadores_seleccionados.add(mejor_jugador['Id'])
                    print(f"✅ {posicion}: {mejor_jugador['Alias']} ({mejor_jugador['minutos_total']} min)")
        
        # 2. Manejar centrales (pueden ser 1 o 2)
        centrales_incluidos = 0
        for posicion_central in posiciones_flexibles['centrales']:
            if posicion_central in jugadores_por_posicion and centrales_incluidos < 2:
                # Filtrar jugadores no seleccionados
                jugadores_disponibles = [j for j in jugadores_por_posicion[posicion_central] 
                                    if j['Id'] not in jugadores_seleccionados]
                
                if jugadores_disponibles:
                    mejor_jugador = max(jugadores_disponibles, key=lambda x: x['minutos_total'])
                    posible_11[posicion_central] = mejor_jugador
                    jugadores_seleccionados.add(mejor_jugador['Id'])
                    centrales_incluidos += 1
                    print(f"✅ {posicion_central}: {mejor_jugador['Alias']} ({mejor_jugador['minutos_total']} min)")
        
        # 3. Manejar delanteros (pueden ser 1 o 2)
        delanteros_incluidos = 0
        for posicion_delantero in posiciones_flexibles['delanteros']:
            if posicion_delantero in jugadores_por_posicion and delanteros_incluidos < 2:
                # Filtrar jugadores no seleccionados
                jugadores_disponibles = [j for j in jugadores_por_posicion[posicion_delantero] 
                                    if j['Id'] not in jugadores_seleccionados]
                
                if jugadores_disponibles:
                    mejor_jugador = max(jugadores_disponibles, key=lambda x: x['minutos_total'])
                    posible_11[posicion_delantero] = mejor_jugador
                    jugadores_seleccionados.add(mejor_jugador['Id'])
                    delanteros_incluidos += 1
                    print(f"✅ {posicion_delantero}: {mejor_jugador['Alias']} ({mejor_jugador['minutos_total']} min)")
        
        # 4. Completar hasta 11 si faltan jugadores
        total_jugadores = len(posible_11)
        
        if total_jugadores < 11:
            # Buscar los mejores jugadores restantes
            todos_los_jugadores = []
            for posicion, jugadores_list in jugadores_por_posicion.items():
                for jugador in jugadores_list:
                    if jugador['Id'] not in jugadores_seleccionados:
                        todos_los_jugadores.append((posicion, jugador))
            
            # Ordenar por minutos y completar
            todos_los_jugadores.sort(key=lambda x: x[1]['minutos_total'], reverse=True)
            
            posiciones_faltantes = 11 - total_jugadores
            for i in range(min(posiciones_faltantes, len(todos_los_jugadores))):
                posicion, jugador = todos_los_jugadores[i]
                
                # Buscar un nombre único para la posición
                posicion_nombre = posicion
                contador = 2
                while posicion_nombre in posible_11:
                    posicion_nombre = f"{posicion}_{contador}"
                    contador += 1
                
                posible_11[posicion_nombre] = jugador
                jugadores_seleccionados.add(jugador['Id'])
                print(f"✅ {posicion_nombre}: {jugador['Alias']} (completado - {jugador['minutos_total']} min)")
        
        # 5. Si tenemos más de 11, quitar los de menos minutos
        elif total_jugadores > 11:
            # Ordenar por minutos y mantener solo los 11 mejores
            jugadores_ordenados = sorted(posible_11.items(), 
                                    key=lambda x: x[1]['minutos_total'], 
                                    reverse=True)
            
            posible_11 = dict(jugadores_ordenados[:11])
            print(f"⚠️ Reducido a 11 jugadores (eliminados {total_jugadores - 11})")
        
        print(f"✅ Posible 11 generado con exactamente {len(posible_11)} jugadores")
        print(f"   - Centrales: {sum(1 for pos in posible_11.keys() if 'CENTRAL' in pos)}")
        print(f"   - Delanteros: {sum(1 for pos in posible_11.keys() if 'DELANTERO' in pos)}")
        
        return posible_11
    
    def get_available_jornadas(self, equipo=None):
        """Retorna las jornadas disponibles para un equipo específico o todas"""
        if self.df is None:
            return []
        
        if equipo:
            filtered_df = self.df[self.df['Equipo'] == equipo]
            return sorted(filtered_df['Jornada'].unique())
        else:
            return sorted(self.df['Jornada'].unique())

    def fill_missing_demarcaciones(self, df):
        """Rellena demarcaciones vacías con histórico del jugador"""
        df_work = df.copy()
        
        mask_empty = df_work['Demarcacion'].isna() | (df_work['Demarcacion'] == '') | (df_work['Demarcacion'].str.strip() == '')
        
        for idx in df_work[mask_empty].index:
            jugador_id = df_work.loc[idx, 'Id Jugador']
            
            jugador_demarcaciones = self.df[
                (self.df['Id Jugador'] == jugador_id) & 
                (self.df['Demarcacion'].notna()) & 
                (self.df['Demarcacion'] != '') &
                (self.df['Demarcacion'].str.strip() != '')
            ]['Demarcacion']
            
            if len(jugador_demarcaciones) > 0:
                demarcacion_frecuente = jugador_demarcaciones.value_counts().index[0]
                df_work.loc[idx, 'Demarcacion'] = demarcacion_frecuente
            else:
                df_work.loc[idx, 'Demarcacion'] = 'Sin Posición'
        
        return df_work
    
    def parsear_partido_completo(self, partido, equipo):
        """Parsea un partido completo manteniendo el orden original"""
        if '-' not in partido:
            return equipo, 'Rival', 'N', 'N', 'desconocido'
        
        partes = partido.split('-')
        if len(partes) != 2:
            return equipo, 'Rival', 'N', 'N', 'desconocido'
        
        parte_local = partes[0].strip()
        parte_visitante = partes[1].strip()
        
        import re
        
        # Para la parte local
        match_local = re.match(r'(.+?)(\d+)$', parte_local)
        if match_local:
            equipo_local_raw = match_local.group(1)
            goles_local = match_local.group(2)
        else:
            equipo_local_raw = parte_local
            goles_local = 'N'
        
        # Para la parte visitante
        match_visitante = re.match(r'(\d+)(.+)$', parte_visitante)
        if match_visitante:
            goles_visitante = match_visitante.group(1)
            equipo_visitante_raw = match_visitante.group(2)
        else:
            goles_visitante = 'N'
            equipo_visitante_raw = parte_visitante
        
        equipo_local_limpio = self.limpiar_nombre_equipo(equipo_local_raw)
        equipo_visitante_limpio = self.limpiar_nombre_equipo(equipo_visitante_raw)
        
        sim_local = self.similarity(equipo, equipo_local_limpio)
        sim_visitante = self.similarity(equipo, equipo_visitante_limpio)
        
        if sim_local > sim_visitante:
            return equipo_local_limpio, equipo_visitante_limpio, goles_local, goles_visitante, 'local'
        else:
            return equipo_local_limpio, equipo_visitante_limpio, goles_local, goles_visitante, 'visitante'

    def limpiar_nombre_equipo(self, nombre_raw):
        """Limpia nombres de equipos"""
        equipos_conocidos = {
            'sevillafc': 'Sevilla FC',
            'getafecf': 'Getafe CF', 
            'gironafc': 'Girona FC',
            'villarrealcf': 'Villarreal CF',
            'realmadrid': 'Real Madrid',
            'fcbarcelona': 'FC Barcelona',
            'athleticclub': 'Athletic Club',
            'atleticodemadrid': 'Atlético de Madrid',
            'realbetis': 'Real Betis',
            'realsociedad': 'Real Sociedad',
            'valenciacf': 'Valencia CF',
            'rcelta': 'RC Celta',
            'caosasuna': 'CA Osasuna',
            'rayovallecano': 'Rayo Vallecano',
            'udlaspalmas': 'UD Las Palmas',
            'rcdespanyol': 'RCD Espanyol',
            'deportivoalaves': 'Deportivo Alavés',
            'cdleganes': 'CD Leganés',
            'realvalladolidcf': 'Real Valladolid CF',
            'rcdmallorca': 'RCD Mallorca'
        }
        
        nombre_lower = nombre_raw.lower().strip()
        
        if nombre_lower in equipos_conocidos:
            return equipos_conocidos[nombre_lower]
        
        for key, value in equipos_conocidos.items():
            if key in nombre_lower or nombre_lower in key:
                return value
        
        return nombre_raw.replace('fc', ' FC').replace('cf', ' CF').title()
    
    def get_ultimos_4_partidos(self, equipo, jornada_maxima, tipo_partido_filter=None, min_minutos=60):
        """Obtiene los últimos 4 partidos del equipo"""
        if self.df is None:
            return []
        
        if isinstance(jornada_maxima, str) and jornada_maxima.startswith(('J', 'j')):
            try:
                jornada_maxima = int(jornada_maxima[1:])
            except ValueError:
                pass
        
        tipo_display = tipo_partido_filter.upper() if tipo_partido_filter else "TODOS"
        print(f"🔍 Buscando últimos 4 partidos {tipo_display} para {equipo} hasta jornada {jornada_maxima}")
        
        filtrado = self.df[
            (self.df['Equipo'] == equipo) & 
            (self.df['Jornada'] <= jornada_maxima)
        ].copy()
        
        if len(filtrado) == 0:
            return []
        
        partidos_info = filtrado[['Partido', 'Jornada']].drop_duplicates()
        
        if tipo_partido_filter:
            partidos_filtrados = []
            for _, partido_info in partidos_info.iterrows():
                partido = partido_info['Partido']
                tipo = self.determinar_local_visitante(partido, equipo)
                if tipo == tipo_partido_filter:
                    partidos_filtrados.append(partido_info)
            
            if partidos_filtrados:
                partidos_info = pd.DataFrame(partidos_filtrados)
            else:
                print(f"❌ No hay partidos {tipo_partido_filter.upper()} para {equipo}")
                return []
        
        partidos_info = partidos_info.sort_values('Jornada', ascending=False)
        
        ultimos_partidos = partidos_info.head(4)
        
        resultados = []
        for _, partido_info in ultimos_partidos.iterrows():
            partido = partido_info['Partido']
            jornada = partido_info['Jornada']
            
            if 'Nombre' in filtrado.columns:
                mask_empty_alias = filtrado['Alias'].isna() | (filtrado['Alias'] == '') | (filtrado['Alias'].str.strip() == '')
                filtrado.loc[mask_empty_alias, 'Alias'] = filtrado.loc[mask_empty_alias, 'Nombre']
            
            datos_partido = filtrado[filtrado['Partido'] == partido].copy()
            
            if 'Minutos jugados' in datos_partido.columns:
                datos_partido = datos_partido[datos_partido['Minutos jugados'] >= min_minutos]
            
            if len(datos_partido) > 0:
                tipo_partido = self.determinar_local_visitante(partido, equipo)
                rival = self.extraer_rival(partido, equipo)
                
                resultados.append({
                    'partido': partido,
                    'jornada': jornada,
                    'tipo': tipo_partido,
                    'rival': rival,
                    'datos': datos_partido
                })
        
        print(f"🎯 Total partidos {tipo_display} seleccionados: {len(resultados)}")
        return resultados

    def calcular_dimensiones_tabla(self, jugadores_list, scale=0.9):
        """Calcula dimensiones simples y compactas"""
        if not jugadores_list:
            return 0, 0
        
        num_players = len(jugadores_list)
        num_metrics = len(self.metricas_tabla)
        
        # Dimensiones BASE más pequeñas
        base_metric_width = 3.5 * scale
        base_player_width = 2.0 * scale
        base_row_height = 0.8 * scale
        
        # 🔧 ANCHO MANUAL - USA EL MISMO VALOR QUE ARRIBA
        player_col_width = 5.0 * scale     # ← EL MISMO VALOR que en crear_tabla_posicion
        
        if num_players > 3:
            player_col_width *= 0.9
        
        table_width = base_metric_width + (num_players * player_col_width)
        table_height = (base_row_height * 1.5) + (base_row_height * 1.8) + (num_metrics * base_row_height)
        
        return table_width, table_height

    def get_team_colors(self, equipo):
        """Obtiene colores del equipo"""
        if equipo in self.team_colors:
            return self.team_colors[equipo]
        
        for team_name in self.team_colors.keys():
            if team_name.lower() in equipo.lower() or equipo.lower() in team_name.lower():
                return self.team_colors[team_name]
        
        return self.default_team_colors
    
    def load_team_logo(self, equipo):
        """Carga el escudo del equipo CON BÚSQUEDA INTELIGENTE"""
        import os
        import glob
        
        # 🏆 MAPEO DIRECTO DE EQUIPOS CONOCIDOS
        mapeo_escudos = {
            'Sevilla FC': 'sevillafc',
            'Villarreal CF': 'villarrealcf', 
            'Real Madrid': 'realmadrid',
            'FC Barcelona': 'fcbarcelona',
            'Athletic Club': 'athleticclub',
            'Atlético de Madrid': 'atleticodemadrid',
            'Real Betis': 'realbetis',
            'Valencia CF': 'valenciacf',
            'Real Sociedad': 'realsociedad',
            'Getafe CF': 'getafecf',
            'RC Celta': 'rcelta',
            'CA Osasuna': 'caosasuna',
            'Rayo Vallecano': 'rayovallecano',
            'UD Las Palmas': 'udlaspalmas',
            'RCD Espanyol': 'rcdespanyol',
            'Deportivo Alavés': 'deportivoalaves',
            'CD Leganés': 'cdleganes',
            'Real Valladolid CF': 'realvalladolidcf',
            'RCD Mallorca': 'rcdmallorca',
            'Girona FC': 'gironafc'
        }
        
        # Buscar mapeo directo primero
        if equipo in mapeo_escudos:
            logo_path = f"assets/escudos/{mapeo_escudos[equipo]}.png"
            if os.path.exists(logo_path):
                try:
                    print(f"✅ Escudo encontrado (mapeo directo): {logo_path}")
                    return plt.imread(logo_path)
                except Exception as e:
                    print(f"⚠️ Error al cargar {logo_path}: {e}")
        
        # Lista de posibles nombres a buscar
        possible_names = [
            equipo,
            equipo.replace(' ', '_'),
            equipo.replace(' ', ''),
            equipo.lower(),
            equipo.lower().replace(' ', '_'),
            equipo.lower().replace(' ', ''),
            equipo.upper(),
            equipo.upper().replace(' ', '_'),
            equipo.upper().replace(' ', ''),
            # Nombres específicos conocidos
            equipo.replace('FC ', '').replace('CF ', '').replace('CD ', '').replace('CA ', ''),
            equipo.replace('Real ', '').replace('RC ', '').replace('RCD ', '').replace('UD ', ''),
            equipo.replace('Atlético de ', 'Atletico').replace('Deportivo ', ''),
        ]
        
        # Buscar coincidencia exacta primero
        for name in possible_names:
            logo_path = f"assets/escudos/{name}.png"
            if os.path.exists(logo_path):
                try:
                    print(f"✅ Escudo encontrado: {logo_path}")
                    return plt.imread(logo_path)
                except Exception as e:
                    print(f"⚠️ Error al cargar {logo_path}: {e}")
                    continue
        
        # Si no encuentra, buscar por similitud en todos los archivos PNG
        try:
            escudos_disponibles = glob.glob("assets/escudos/*.png")
            equipo_limpio = equipo.lower().replace(' ', '').replace('fc', '').replace('cf', '')
            
            for escudo_path in escudos_disponibles:
                nombre_archivo = os.path.basename(escudo_path).lower().replace('.png', '')
                
                # Calcular similitud
                similitud = self.similarity(equipo_limpio, nombre_archivo)
                
                if similitud > 0.6:  # Si similitud > 60%
                    try:
                        print(f"✅ Escudo encontrado por similitud ({similitud:.2f}): {escudo_path}")
                        return plt.imread(escudo_path)
                    except Exception as e:
                        print(f"⚠️ Error al cargar {escudo_path}: {e}")
                        continue
            
            print(f"📁 Archivos disponibles en assets/escudos/:")
            for escudo in escudos_disponibles[:10]:  # Mostrar primeros 10
                print(f"   {os.path.basename(escudo)}")
            
        except Exception as e:
            print(f"⚠️ Error buscando escudos: {e}")
        
        print(f"❌ No se encontró escudo para: {equipo}")
        return None

    def load_background_image(self):
        """Carga la imagen de fondo"""
        background_path = "assets/fondo_informes.png"
        if os.path.exists(background_path):
            try:
                return plt.imread(background_path)
            except Exception:
                print(f"⚠️ No se pudo cargar la imagen de fondo: {background_path}")
                return None
        return None
    
    def crear_campo_sin_espacios(self, ax):
        """🔥 MÉTODO MEJORADO: Crea campo horizontal SIN ESPACIOS como el primer script"""
        print("🎯 Creando campo horizontal SIN espacios...")
        
        # Crear pitch sin padding
        pitch = Pitch(
            pitch_color='grass', 
            line_color='white', 
            stripe=True, 
            linewidth=2,
            pad_left=0, pad_right=0, pad_bottom=0, pad_top=0
        )
        
        # Dibujar en el ax proporcionado
        pitch.draw(ax=ax)
        
        # 🔥 CONFIGURACIÓN AGRESIVA PARA ELIMINAR ESPACIOS (COPIADA DEL PRIMER SCRIPT)
        ax.set_position(ax.get_position())
        ax.margins(0, 0)
        ax.set_xlim(0, 120)
        ax.set_ylim(0, 80)
        ax.autoscale(enable=False)
        ax.set_aspect('equal')
        ax.set_frame_on(False)
        
        return pitch
    
    def agrupar_jugadores_por_posicion(self, jugadores_df):
        """Agrupa jugadores por posición específica SIN BALANCEO (coordenadas fijas)"""
        jugadores_df = self.fill_missing_demarcaciones(jugadores_df)
        
        if 'Nombre' in jugadores_df.columns:
            mask_empty_alias = jugadores_df['Alias'].isna() | (jugadores_df['Alias'] == '') | (jugadores_df['Alias'].str.strip() == '')
            jugadores_df.loc[mask_empty_alias, 'Alias'] = jugadores_df.loc[mask_empty_alias, 'Nombre']
        
        jugadores_df_sorted = jugadores_df.sort_values('Minutos jugados', ascending=False)
        
        jugadores_por_posicion = {pos: [] for pos in self.coordenadas_posiciones.keys()}
        
        for _, jugador in jugadores_df_sorted.iterrows():
            demarcacion = jugador.get('Demarcacion', 'Sin Posición')
            posicion = self.demarcacion_to_posicion.get(demarcacion, 'MC_BOX_TO_BOX')
            
            if posicion in jugadores_por_posicion:
                jugadores_por_posicion[posicion].append(jugador.to_dict())
        
        # 🚫 BALANCEO ELIMINADO PARA COORDENADAS FIJAS
        # NO se llama a balancear_centrales() ni balancear_delanteros()
        print("📍 MODO COORDENADAS FIJAS: Sin balanceo de posiciones")
        
        return jugadores_por_posicion
    
    # 🚫 MÉTODOS DE BALANCEO ELIMINADOS PARA COORDENADAS FIJAS
    # Los jugadores permanecen exactamente en sus demarcaciones originales
    
    def crear_tabla_posicion(self, jugadores_list, x, y, ax, team_colors, posicion_name, scale=0.8, team_logo=None):
        """Crea una tabla moderna por posición CON DIMENSIONES COMPACTAS"""
        if not jugadores_list:
            return
        
        num_players = len(jugadores_list)
        num_metrics = len(self.metricas_tabla)
        
        # DIMENSIONES DINÁMICAS - se adaptan al texto
        # 🔧 ANCHO MANUAL - CAMBIA ESTOS VALORES A LO QUE QUIERAS
        metric_col_width = 6.0 * scale     # ← Columna de métricas
        player_col_width = 4.0 * scale     # ← Columnas de jugadores (¡AJUSTA AQUÍ!)
        
        # Dimensiones de filas
        header_height = 1.0 * scale
        names_height = 2.0 * scale  
        metric_row_height = 0.6 * scale
        
        # Dimensiones totales
        table_width = metric_col_width + (num_players * player_col_width)
        table_height = header_height + names_height + (num_metrics * metric_row_height)
        
        # Tamaños de fuente proporcionales
        fontsize_header = int(7 * scale)
        fontsize_metricas = int(6 * scale)
        fontsize_nombres = int(8 * scale)  
        fontsize_valores = int(6 * scale)
        
        # Métricas abreviadas
        metricas_cortas = []
        for metrica in self.metricas_tabla:
            metrica_corta = (metrica
                        .replace('Distancia Total 14-21 km / h / min', 'D14-21/m')
                        .replace('Distancia Total >21 km / h / min', 'D21+/m') 
                        .replace('Distancia Total >24 km / h / min', 'D24+/m')
                        .replace('Distancia Total 14-21 km / h', 'D14-21')
                        .replace('Distancia Total >21 km / h', 'D21+')
                        .replace('Distancia Total >24 km / h', 'D24+')
                        .replace('Distancia Total / min', 'Dist/m')
                        .replace('Distancia Total', 'Dist')
                        .replace('Velocidad Máxima Total', 'VMax'))
            metricas_cortas.append(metrica_corta)
        
        # Fondo principal
        main_rect = plt.Rectangle((x - table_width/2, y - table_height/2), 
                                table_width, table_height,
                                facecolor='#2c3e50', alpha=0.95, 
                                edgecolor='white', linewidth=1.5)
        ax.add_patch(main_rect)
        
        # Header
        header_rect = plt.Rectangle((x - table_width/2, y + table_height/2 - header_height), 
                                table_width, header_height,
                                facecolor=team_colors['primary'], alpha=0.8,
                                edgecolor='white', linewidth=1)
        ax.add_patch(header_rect)
        
        clean_position_name = posicion_name.replace('_', ' ').title()
        ax.text(x, y + table_height/2 - header_height/2, clean_position_name, 
                fontsize=fontsize_header, weight='bold', color=team_colors['text'],
                ha='center', va='center')
        
        # Fila de nombres
        names_y = y + table_height/2 - header_height - names_height/2
        
        names_rect = plt.Rectangle((x - table_width/2 + metric_col_width, names_y - names_height/2), 
                                num_players * player_col_width, names_height,
                                facecolor='#34495e', alpha=0.7, 
                                edgecolor='white', linewidth=0.5)
        ax.add_patch(names_rect)

        # Escudo
        if team_logo is not None:
            try:
                logo_x = x - table_width/2 + metric_col_width/2
                logo_y = names_y
                zoom_factor = min(metric_col_width / 120, names_height / 120) * 1
        
                imagebox = OffsetImage(team_logo, zoom=zoom_factor)
                ab = AnnotationBbox(imagebox, (logo_x, logo_y), frameon=False)
                ax.add_artist(ab)
            except Exception as e:
                print(f"⚠️ Error al añadir escudo: {e}")
        
        # Nombres y dorsales AJUSTADOS A COLUMNA
        for i, jugador in enumerate(jugadores_list):
            player_x = x - table_width/2 + metric_col_width + (i * player_col_width) + player_col_width/2
            player_name = jugador['Alias'] if pd.notna(jugador['Alias']) else 'N/A'
            dorsal = jugador.get('Dorsal', 'N/A')
            
            # 🔧 AJUSTAR NOMBRE A LA COLUMNA
            nombre_ajustado = self.ajustar_texto_columna(player_name, player_col_width, scale)
            
            # 🔧 AJUSTAR TAMAÑO DE FUENTE SEGÚN LONGITUD
            if len(player_name) <= 6:
                font_size_name = fontsize_nombres
            elif len(player_name) <= 10:
                font_size_name = int(fontsize_nombres * 0.85)
            else:
                font_size_name = int(fontsize_nombres * 0.7)
            
            ax.text(player_x, names_y + 0.8 * scale, nombre_ajustado, 
                    fontsize=font_size_name, weight='bold', color='white',
                    ha='center', va='center')
            
            ax.text(player_x, names_y - 0.8 * scale, str(dorsal), 
                    fontsize=int(7 * scale), weight='bold', color=team_colors['primary'],
                    ha='center', va='center')
        
        # Filas de métricas
        for i, metrica in enumerate(self.metricas_tabla):
            metric_y = names_y - names_height/2 - (i + 1) * metric_row_height + metric_row_height/2
            
            if i % 2 == 0:
                row_rect = plt.Rectangle((x - table_width/2, metric_y - metric_row_height/2), 
                                    table_width, metric_row_height,
                                    facecolor='#3c566e', alpha=0.3)
                ax.add_patch(row_rect)
            
            metric_bg = plt.Rectangle((x - table_width/2, metric_y - metric_row_height/2), 
                                    metric_col_width, metric_row_height,
                                    facecolor=team_colors['primary'], alpha=0.6,
                                    edgecolor='white', linewidth=0.3)
            ax.add_patch(metric_bg)
            
            metrica_corta = metricas_cortas[i]
            ax.text(x - table_width/2 + metric_col_width/2, metric_y, metrica_corta, 
                    fontsize=fontsize_metricas, weight='bold', color='white',
                    ha='center', va='center')
            
            # Valores
            for j, jugador in enumerate(jugadores_list):
                player_x = x - table_width/2 + metric_col_width + (j * player_col_width) + player_col_width/2
                
                if metrica in jugador:
                    valor = jugador[metrica]
                    if 'Velocidad' in metrica:
                        valor_format = f"{valor:.1f}"
                    elif '/min' in metrica or '/ min' in metrica:
                        valor_format = f"{valor:.1f}"
                    else:
                        valor_format = f"{valor:.0f}"
                else:
                    valor_format = "N/A"
                
                ax.text(player_x, metric_y, valor_format, 
                        fontsize=fontsize_valores, weight='bold', color='#FFD700',
                        ha='center', va='center')

    def ajustar_texto_columna(self, texto, ancho_columna, scale):
        """Ajusta el texto para que encaje perfectamente en la columna"""
        if not texto or pd.isna(texto):
            return "N/A"
        
        texto_str = str(texto).strip()
        
        # Cálculo más preciso del espacio disponible
        chars_disponibles = int(ancho_columna / (0.25 * scale))
        
        if len(texto_str) <= chars_disponibles:
            return texto_str
        
        # Si es muy largo, usar estrategias de truncado inteligente
        partes = texto_str.split(' ')
        
        if len(partes) > 1:
            # Para nombres con espacios: Primera letra + apellido
            primer_nombre = partes[0]
            ultimo_apellido = partes[-1]
            
            if len(primer_nombre) + len(ultimo_apellido) + 2 <= chars_disponibles:
                return f"{primer_nombre[0]}.{ultimo_apellido}"
            else:
                return f"{primer_nombre[0]}.{ultimo_apellido[:chars_disponibles-3]}"
        else:
            # Para una sola palabra: truncar con puntos
            return texto_str[:chars_disponibles-1] + "."
    
    def crear_titulo_elegante_con_escudos(self, ax, titulo_texto, equipo_local, equipo_visitante, 
                              team_colors, y_position=0.95):
        """Método COMPLETO con título y escudos abajo con imshow"""
        
        # 📍 TÍTULO EN LA PARTE DE ABAJO CON FONDO AJUSTADO AL TEXTO
        bbox = ax.get_position()
        fig_x = bbox.x0 + bbox.width/2
        fig_y = bbox.y0 + 0.005  # ← Abajo del campo

        # 🔧 CALCULAR ANCHO REAL DEL TEXTO
        import matplotlib.pyplot as plt
        temp_fig = plt.figure(figsize=(1, 1))
        temp_ax = temp_fig.add_subplot(111)

        # Crear texto temporal para medir dimensiones
        temp_text = temp_ax.text(0, 0, titulo_texto, fontsize=10, weight='bold')
        temp_fig.canvas.draw()

        # Obtener dimensiones reales del texto
        bbox_texto = temp_text.get_window_extent()
        ancho_texto_pixels = bbox_texto.width
        alto_texto_pixels = bbox_texto.height

        # Convertir a coordenadas de figura
        ancho_texto_fig = ancho_texto_pixels / ax.figure.dpi / ax.figure.get_size_inches()[0]
        alto_texto_fig = alto_texto_pixels / ax.figure.dpi / ax.figure.get_size_inches()[1]

        # Cerrar figura temporal
        plt.close(temp_fig)

        # 🔧 DIMENSIONES AJUSTADAS CON PADDING
        padding_horizontal = ancho_texto_fig * 0.2  # 20% de padding horizontal
        padding_vertical = alto_texto_fig * 0.3     # 30% de padding vertical

        titulo_width = ancho_texto_fig + padding_horizontal
        titulo_height = alto_texto_fig + padding_vertical

        # Fondo del título AJUSTADO
        fondo_rect = plt.Rectangle(
            (fig_x - titulo_width/2, fig_y - titulo_height/2), 
            titulo_width, titulo_height,
            facecolor=team_colors['primary'], 
            alpha=0.9,
            edgecolor='white', 
            linewidth=0.5,
            transform=ax.figure.transFigure,
            zorder=10
        )
        ax.figure.patches.append(fondo_rect)

        # Texto del título
        ax.figure.text(
            fig_x, fig_y, titulo_texto,
            fontsize=10, weight='bold', color=team_colors['text'],
            ha='center', va='center',
            transform=ax.figure.transFigure,
            zorder=12
        )
        
        # 🏆 ESCUDOS CON SOMBRA
        escudo_local = self.load_team_logo(equipo_local)
        escudo_visitante = self.load_team_logo(equipo_visitante)

        # Escudo LOCAL CON SOMBRA
        if escudo_local is not None:
            try:
                # 1. SOMBRA (desplazada hacia abajo-derecha)
                ax.imshow(escudo_local, 
                        extent=[2.5, 21.5, -0.5, 12.5],  # ← SOMBRA desplazada
                        aspect='auto', zorder=99, alpha=0.4)  # ← Semi-transparente y atrás
                
                # 2. ESCUDO PRINCIPAL (encima de la sombra)
                ax.imshow(escudo_local, 
                        extent=[1, 20, 1, 14],  # ← ESCUDO ORIGINAL
                        aspect='auto', zorder=100)  # ← Adelante
                
                print(f"✅ Escudo local CON SOMBRA añadido")
            except Exception as e:
                print(f"⚠️ Error escudo local: {e}")

        # Escudo VISITANTE CON SOMBRA
        if escudo_visitante is not None:
            try:
                # 1. SOMBRA (desplazada hacia abajo-izquierda)
                ax.imshow(escudo_visitante, 
                        extent=[98.5, 117.5, -0.5, 12.5],  # ← SOMBRA desplazada
                        aspect='auto', zorder=99, alpha=0.4)  # ← Semi-transparente y atrás
                
                # 2. ESCUDO PRINCIPAL (encima de la sombra)
                ax.imshow(escudo_visitante, 
                        extent=[100, 119, 1, 14],  # ← ESCUDO ORIGINAL
                        aspect='auto', zorder=100)  # ← Adelante
                
                print(f"✅ Escudo visitante CON SOMBRA añadido")
            except Exception as e:
                print(f"⚠️ Error escudo visitante: {e}")

    def guardar_sin_espacios(self, fig, filename):
        """🔥 MÉTODO ORIGINAL: Guarda sin espacios manteniendo landscape 16:9"""
        # Ajustar tamaño para 16:9 antes de guardar
        fig.set_size_inches(28.8, 16.2)
        
        fig.savefig(
            filename,
            dpi=300,
            bbox_inches='tight',
            pad_inches=0,
            facecolor='white',
            edgecolor='none',
            format='pdf' if filename.endswith('.pdf') else 'png',
            transparent=False,
            orientation='landscape'
        )
        print(f"✅ Archivo guardado SIN espacios formato 16:9: {filename}")

    def crear_4_partidos_campos_horizontales(self, equipo, jornada_maxima, tipo_partido_filter=None, figsize=(32, 18)):
        """🔥 MÉTODO CON COORDENADAS FIJAS: Crea 4 campos horizontales sin auto-movimiento"""
        
        tipo_display = tipo_partido_filter.upper() if tipo_partido_filter else "TODOS"
        print(f"\n🔄 Generando visualización 2x2 CON COORDENADAS FIJAS para {equipo}")
        
        # Obtener partidos
        partidos = self.get_ultimos_4_partidos(equipo, jornada_maxima, tipo_partido_filter)
        
        if len(partidos) == 0:
            print(f"❌ No hay partidos {tipo_display} para {equipo}")
            return None
        
        # 🔥 CREAR FIGURA CON DIMENSIONES MEJORADAS Y CONFIGURACIÓN AGRESIVA
        fig = plt.figure(figsize=figsize, constrained_layout=False)
        
        # 🔥 CONFIGURACIÓN AGRESIVA PARA ELIMINAR ESPACIOS (COPIADA DEL PRIMER SCRIPT)
        fig.subplots_adjust(left=0, right=1, top=0.93, bottom=0, wspace=0.0, hspace=0.0)
        fig.patch.set_visible(False)
        
        # Crear subplots con configuración agresiva
        axes = []
        subplot_positions = [
            [0.0, 0.5, 0.5, 0.5],   # Superior izquierda
            [0.5, 0.5, 0.5, 0.5],   # Superior derecha
            [0.0, 0.0, 0.5, 0.5],   # Inferior izquierda
            [0.5, 0.0, 0.5, 0.5]    # Inferior derecha
        ]
        
        for i, pos in enumerate(subplot_positions):
            ax = fig.add_axes(pos)
            ax.set_aspect('equal')
            axes.append(ax)
        
        # Cargar fondo CENTRADO Y OCUPANDO TODA LA FIGURA
        background_img = self.load_background_image()
        if background_img is not None:
            try:
                ax_background = fig.add_axes([0, 0, 1, 1], zorder=-1)
                ax_background.imshow(background_img, extent=[0, 1, 0, 1], aspect='auto', alpha=0.15, zorder=-1)
                ax_background.axis('off')
                ax_background.set_xticks([])
                ax_background.set_yticks([])
                for spine in ax_background.spines.values():
                    spine.set_visible(False)
                print("Fondo aplicado correctamente")
            except Exception as e:
                print(f"Error al aplicar fondo: {e}")
        
        # Obtener colores y escudo
        team_colors = self.get_team_colors(equipo)
        team_logo = self.load_team_logo(equipo)
        
        # Crear campos
        for i in range(4):
            ax = axes[i]
            
            if i < len(partidos):
                # Crear campo SIN espacios
                pitch = self.crear_campo_sin_espacios(ax)
                
                partido_info = partidos[i]
                print(f"🏟️ Campo {i+1}: J{partido_info['jornada']} - {partido_info['partido']}")
                
                # Agrupar jugadores
                jugadores_agrupados = self.agrupar_jugadores_por_posicion(partido_info['datos'])
                
                # Escala mejorada
                escala = 1.4
                
                # 🔥 USAR COORDENADAS FIJAS SIN AUTO-MOVIMIENTO
                print("📍 Usando coordenadas FIJAS para las tablas (sin auto-movimiento)")
                
                for posicion, jugadores in jugadores_agrupados.items():
                    if jugadores and posicion in self.coordenadas_posiciones:
                        # Usar coordenadas exactas definidas
                        x, y = self.coordenadas_posiciones[posicion]
                        
                        print(f"   {posicion}: ({x}, {y}) con {len(jugadores)} jugadores")
                        
                        # Crear tabla en las coordenadas exactas
                        self.crear_tabla_posicion(jugadores, x, y, ax, team_colors, posicion, escala, team_logo)
                
                # Parsear partido
                equipo_local, equipo_visitante, goles_local, goles_visitante, tipo_real = self.parsear_partido_completo(
                    partido_info['partido'], equipo)

                tipo_display_partido = "Local" if tipo_real == 'local' else "Visitante"
                titulo_partido = f"J{partido_info['jornada']} - {equipo_local} {goles_local} - {goles_visitante} {equipo_visitante} ({tipo_display_partido})"

                # Título con escudos
                self.crear_titulo_elegante_con_escudos(
                    ax, titulo_partido, equipo_local, equipo_visitante, 
                    team_colors
                )
                
            else:
                # Campo vacío
                ax.set_xlim(0, 120)
                ax.set_ylim(0, 80)
                ax.text(60, 40, f'Sin partido {tipo_display.lower()}\ndisponible', 
                       ha='center', va='center', fontsize=12, color='gray', style='italic')
                ax.set_facecolor('lightgray')
                ax.set_alpha(0.3)
        
        # Título general
        fig.suptitle(f'{equipo.upper()} - ÚLTIMOS 4 PARTIDOS {tipo_display} (hasta J{jornada_maxima})',
             fontsize=40, weight='bold', color='white', ha='center', va='center',
             y=1.0,  # ← MÁS ARRIBA (era 0.95 por defecto)
             bbox=dict(boxstyle="round,pad=0.03", facecolor='#1e3d59', alpha=0.95,
                      edgecolor='white', linewidth=1))
        
        print("✅ Visualización 2x2 CON COORDENADAS FIJAS creada correctamente")
        return fig

def main_4_campos_horizontales_coordenadas_fijas():
    """Función principal con coordenadas fijas"""
    try:
        report_gen = ReporteTactico4CamposHorizontalesMejorado()
        equipos = report_gen.get_available_teams()
        
        if not equipos:
            print("❌ No hay equipos disponibles")
            return
        
        print("\n🏟️ === REPORTE 4 CAMPOS HORIZONTALES - COORDENADAS FIJAS ===")
        for i, equipo in enumerate(equipos, 1):
            print(f"{i:2d}. {equipo}")
        
        while True:
            try:
                seleccion = input(f"\nSelecciona equipo (1-{len(equipos)}): ").strip()
                indice = int(seleccion) - 1
                if 0 <= indice < len(equipos):
                    equipo_seleccionado = equipos[indice]
                    break
                else:
                    print(f"❌ Número entre 1 y {len(equipos)}")
            except ValueError:
                print("❌ Ingresa un número válido")
        
        # Jornada
        jornadas = report_gen.get_available_jornadas()
        print(f"\nJornadas disponibles: {jornadas}")
        
        while True:
            try:
                jornada_input = input("Jornada máxima a considerar: ").strip()
                if jornada_input.startswith(('J', 'j')):
                    jornada = int(jornada_input[1:])
                else:
                    jornada = int(jornada_input)
                
                if jornada in jornadas:
                    break
                else:
                    print(f"❌ Jornada no disponible")
            except ValueError:
                print("❌ Formato de jornada inválido")
        
        # Tipo de partido
        print(f"\n🎯 Selecciona tipo de partidos:")
        print("1. SOLO LOCALES")
        print("2. SOLO VISITANTES") 
        print("3. TODOS")
        
        while True:
            try:
                tipo_seleccion = input("Selecciona opción (1-3): ").strip()
                if tipo_seleccion == "1":
                    tipo_partido_filter = "local"
                    break
                elif tipo_seleccion == "2":
                    tipo_partido_filter = "visitante"
                    break
                elif tipo_seleccion == "3":
                    tipo_partido_filter = None
                    break
                else:
                    print("❌ Selecciona 1, 2 o 3")
            except ValueError:
                print("❌ Ingresa 1, 2 o 3")
        
        # Generar
        fig = report_gen.crear_4_partidos_campos_horizontales(equipo_seleccionado, jornada, tipo_partido_filter)
        
        if fig:
            plt.show()
            
            # Guardar con método mejorado
            tipo_filename = f"_{tipo_partido_filter}" if tipo_partido_filter else "_todos"
            filename = f"reporte_4_campos_FIJAS_{equipo_seleccionado.replace(' ', '_')}_hasta_J{jornada}{tipo_filename}.pdf"
            report_gen.guardar_sin_espacios(fig, filename)
        else:
            print("❌ No se pudo generar el reporte")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def generar_4_campos_coordenadas_fijas(equipo, jornada_maxima, tipo_partido_filter=None, mostrar=True, guardar=True):
    """Función para uso directo con coordenadas fijas"""
    try:
        report_gen = ReporteTactico4CamposHorizontalesMejorado()
        fig = report_gen.crear_4_partidos_campos_horizontales(equipo, jornada_maxima, tipo_partido_filter)
        
        if fig:
            if mostrar:
                plt.show()
            if guardar:
                tipo_filename = f"_{tipo_partido_filter}" if tipo_partido_filter else "_todos"
                filename = f"reporte_4_campos_FIJAS_{equipo.replace(' ', '_')}_hasta_J{jornada_maxima}{tipo_filename}.pdf"
                report_gen.guardar_sin_espacios(fig, filename)
            return fig
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Funciones rápidas con coordenadas fijas
def rapido_horizontal_fijas(equipo, jornada=35, tipo_partido=None):
    """Genera 4 campos horizontales CON COORDENADAS FIJAS rápidamente"""
    tipo_display = tipo_partido.upper() if tipo_partido else "TODOS"
    print(f"🚀 Generación rápida horizontal COORDENADAS FIJAS: {equipo} hasta jornada {jornada} - Partidos {tipo_display}")
    return generar_4_campos_coordenadas_fijas(equipo, jornada, tipo_partido)

def sevilla_horizontal_fijas(tipo_partido=None):
    """Sevilla FC con campos horizontales COORDENADAS FIJAS"""
    return rapido_horizontal_fijas("Sevilla FC", 35, tipo_partido)

def sevilla_horizontal_local_fijas():
    """Sevilla FC solo locales horizontal COORDENADAS FIJAS"""
    return sevilla_horizontal_fijas("local")

def sevilla_horizontal_visitante_fijas():
    """Sevilla FC solo visitantes horizontal COORDENADAS FIJAS"""
    return sevilla_horizontal_fijas("visitante")

# Inicialización
print("🏟️ === REPORTE TÁCTICO 4 CAMPOS - COORDENADAS FIJAS ===")
try:
    report_gen = ReporteTactico4CamposHorizontalesMejorado()
    equipos = report_gen.get_available_teams()
    jornadas = report_gen.get_available_jornadas()
    print(f"✅ Sistema CON COORDENADAS FIJAS listo: {len(equipos)} equipos, {len(jornadas)} jornadas")
    print("📝 PARA USAR:")
    print("   → main_4_campos_horizontales_coordenadas_fijas() - INTERFAZ GUIADA")
    print("   → sevilla_horizontal_local_fijas() - DIRECTO: Sevilla solo locales")
    print("   → sevilla_horizontal_visitante_fijas() - DIRECTO: Sevilla solo visitantes")
    print("   → rapido_horizontal_fijas('Athletic Club', 20, 'local')")
    print("\n📍 COORDENADAS FIJAS IMPLEMENTADAS:")
    print("   • Sin detección ni resolución de colisiones")
    print("   • Tablas posicionadas exactamente en coordenadas definidas")
    print("   • Mayor control sobre el diseño del campo")
    print("   • Resultados más predecibles y consistentes")
    print("="*70)
    print("💡 TIP: Para Sevilla solo locales FIJAS: sevilla_horizontal_local_fijas()")
    print("="*70)
except Exception as e:
    print(f"❌ Error inicialización: {e}")

if __name__ == "__main__":
    main_4_campos_horizontales_coordenadas_fijas()