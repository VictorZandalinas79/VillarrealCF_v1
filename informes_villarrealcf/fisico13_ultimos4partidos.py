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
        CON ZOOM Y DIMENSIONES MEJORADAS
        """
        self.data_path = data_path
        self.df = None
        self.load_data()
        self.clean_team_names()
        
        # 🏟️ COORDENADAS PARA CAMPOS HORIZONTALES (Pitch 0-120 x 0-80)
        self.coordenadas_posiciones = {
            'PORTERO': (10, 40),
            'LATERAL_DERECHO': (45, 68),
            'CENTRAL_DERECHO': (20, 60),
            'CENTRAL_IZQUIERDO': (20, 20),
            'LATERAL_IZQUIERDO': (45, 12),
            'MC_POSICIONAL': (40, 45),
            'MC_BOX_TO_BOX': (65, 20),
            'MC_ORGANIZADOR': (55, 55),
            'BANDA_DERECHA': (85, 68),
            'BANDA_IZQUIERDA': (85, 12),
            'MEDIAPUNTA': (75, 40),
            'DELANTERO_CENTRO': (100, 45),
            'SEGUNDO_DELANTERO': (90, 65),
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

    def detectar_y_resolver_colisiones(self, tablas_info, campo_width=120, campo_height=80):
        """Detecta colisiones entre tablas y ajusta posiciones"""
        print("🔍 Detectando y resolviendo colisiones entre tablas...")
        
        ajustadas = []
        margen_seguridad = 2
        
        for i, (x, y, width, height, name, jugadores) in enumerate(tablas_info):
            x_ajustado, y_ajustado = x, y
            
            for x2, y2, w2, h2, name2, _ in ajustadas:
                if (abs(x_ajustado - x2) < (width + w2)/2 + margen_seguridad and 
                    abs(y_ajustado - y2) < (height + h2)/2 + margen_seguridad):
                    
                    if x_ajustado < x2:
                        x_ajustado = x2 - (width + w2)/2 - margen_seguridad
                    else:
                        x_ajustado = x2 + (width + w2)/2 + margen_seguridad
                    
                    if x_ajustado < width/2 or x_ajustado > campo_width - width/2:
                        x_ajustado = x
                        if y_ajustado < y2:
                            y_ajustado = y2 - (height + h2)/2 - margen_seguridad
                        else:
                            y_ajustado = y2 + (height + h2)/2 + margen_seguridad
            
            x_final = max(width/2 + 1, min(campo_width - width/2 - 1, x_ajustado))
            y_final = max(height/2 + 1, min(campo_height - height/2 - 1, y_ajustado))
            
            ajustadas.append((x_final, y_final, width, height, name, jugadores))
        
        return ajustadas

    def calcular_dimensiones_tabla(self, jugadores_list, scale=0.9):
        """Calcula las dimensiones de una tabla"""
        if not jugadores_list:
            return 0, 0
        
        num_players = len(jugadores_list)
        num_metrics = len(self.metricas_tabla)
        
        metric_col_width = 10 * scale
        player_col_width = 8 * scale
        table_width = metric_col_width + (num_players * player_col_width)
        
        header_height = 2 * scale
        names_height = 4.5 * scale
        metric_row_height = 1.5 * scale
        table_height = header_height + names_height + (num_metrics * metric_row_height)
        
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
    
    def agrupar_jugadores_por_posicion(self, jugadores_df):
        """Agrupa jugadores por posición específica"""
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
        
        self.balancear_centrales(jugadores_por_posicion)
        self.balancear_delanteros(jugadores_por_posicion)
        
        return jugadores_por_posicion
    
    def balancear_centrales(self, jugadores_por_posicion):
        """Balancea centrales"""
        centrales_derecho = jugadores_por_posicion['CENTRAL_DERECHO']
        centrales_izquierdo = jugadores_por_posicion['CENTRAL_IZQUIERDO']
        
        if len(centrales_izquierdo) == 0 and len(centrales_derecho) > 1:
            jugador_a_mover = centrales_derecho.pop()
            jugadores_por_posicion['CENTRAL_IZQUIERDO'].append(jugador_a_mover)
        elif len(centrales_derecho) == 0 and len(centrales_izquierdo) > 1:
            jugador_a_mover = centrales_izquierdo.pop()
            jugadores_por_posicion['CENTRAL_DERECHO'].append(jugador_a_mover)
    
    def balancear_delanteros(self, jugadores_por_posicion):
        """Balancea delanteros"""
        delanteros = jugadores_por_posicion['DELANTERO_CENTRO']
        
        if len(delanteros) > 1:
            mitad = len(delanteros) // 2 + len(delanteros) % 2
            primer_grupo = delanteros[:mitad]
            segundo_grupo = delanteros[mitad:]
            jugadores_por_posicion['DELANTERO_CENTRO'] = primer_grupo
            jugadores_por_posicion['SEGUNDO_DELANTERO'] = segundo_grupo
    
    def crear_tabla_posicion(self, jugadores_list, x, y, ax, team_colors, posicion_name, scale=0.8, team_logo=None):
        """Crea una tabla moderna por posición"""
        if not jugadores_list:
            return
        
        num_players = len(jugadores_list)
        num_metrics = len(self.metricas_tabla)
        
        metric_col_width = 10 * scale
        player_col_width = 8 * scale
        table_width = metric_col_width + (num_players * player_col_width)
        
        header_height = 2 * scale
        names_height = 4.5 * scale
        metric_row_height = 1.5 * scale
        table_height = header_height + names_height + (num_metrics * metric_row_height)
        
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
                fontsize=int(2.5 * scale), weight='bold', color=team_colors['text'],
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
                zoom_factor = min(metric_col_width / 120, names_height / 120) * 0.25
        
                imagebox = OffsetImage(team_logo, zoom=zoom_factor)
                ab = AnnotationBbox(imagebox, (logo_x, logo_y), frameon=False)
                ax.add_artist(ab)
            except Exception as e:
                print(f"⚠️ Error al añadir escudo: {e}")
        
        # Nombres y dorsales
        for i, jugador in enumerate(jugadores_list):
            player_x = x - table_width/2 + metric_col_width + (i * player_col_width) + player_col_width/2
            player_name = jugador['Alias'] if pd.notna(jugador['Alias']) else 'N/A'
            dorsal = jugador.get('Dorsal', 'N/A')
            
            ax.text(player_x, names_y + 0.8 * scale, player_name, 
                    fontsize=int(2.8 * scale), weight='bold', color='white',
                    ha='center', va='center')
            
            ax.text(player_x, names_y - 0.8 * scale, str(dorsal), 
                    fontsize=int(2.5 * scale), weight='bold', color=team_colors['primary'],
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
            ax.text(x - table_width/2 + metric_col_width/2, metric_y, metrica_corta, 
                    fontsize=int(2.5 * scale), weight='bold', color='white',
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
                        fontsize=int(2.8 * scale), weight='bold', color='#FFD700',
                        ha='center', va='center')

    def crear_titulo_elegante_con_escudos(self, ax, titulo_texto, equipo_local, equipo_visitante, 
                              team_colors, y_position=0.95):
        """Método COMPLETO con título y escudos abajo con imshow"""
        
        # 📍 TÍTULO EN LA PARTE DE ABAJO
        bbox = ax.get_position()
        fig_x = bbox.x0 + bbox.width/2
        fig_y = bbox.y0 + 0.005  # ← Abajo del campo
        
        titulo_width = bbox.width * 0.6
        titulo_height = 0.02
        
        # Fondo del título
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
            fontsize=8, weight='bold', color=team_colors['text'],
            ha='center', va='center',
            transform=ax.figure.transFigure,
            zorder=12
        )
        
        # 🏆 ESCUDOS TAMBIÉN ABAJO CON IMSHOW
        escudo_local = self.load_team_logo(equipo_local)
        escudo_visitante = self.load_team_logo(equipo_visitante)
        
        # Escudo LOCAL (abajo izquierda)
        if escudo_local is not None:
            try:
                ax.imshow(escudo_local, 
                        extent=[5, 20, 2, 12],  # ← ABAJO del campo (y negativo)
                        aspect='auto', zorder=100)
                print(f"✅ Escudo local añadido con imshow abajo")
            except Exception as e:
                print(f"⚠️ Error escudo local: {e}")
        
        # Escudo VISITANTE (abajo derecha)
        if escudo_visitante is not None:
            try:
                ax.imshow(escudo_visitante, 
                        extent=[100, 115, 2, 12],  # ← ABAJO del campo (y negativo)
                        aspect='auto', zorder=100)
                print(f"✅ Escudo visitante añadido con imshow abajo")
            except Exception as e:
                print(f"⚠️ Error escudo visitante: {e}")

    def guardar_sin_espacios(self, fig, filename):
        """🔥 MÉTODO COPIADO DEL PRIMER SCRIPT: Guarda sin espacios"""
        fig.savefig(
            filename,
            dpi=300,
            bbox_inches='tight',
            pad_inches=0,
            facecolor='white',
            edgecolor='none',
            format='pdf' if filename.endswith('.pdf') else 'png',
            transparent=False
        )
        print(f"✅ Archivo guardado SIN espacios: {filename}")

    def crear_4_partidos_campos_horizontales(self, equipo, jornada_maxima, tipo_partido_filter=None, figsize=(24, 16)):
        """🔥 MÉTODO MEJORADO: Crea 4 campos horizontales con mejor zoom y dimensiones"""
        
        tipo_display = tipo_partido_filter.upper() if tipo_partido_filter else "TODOS"
        print(f"\n🔄 Generando visualización 2x2 MEJORADA para {equipo}")
        
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
        
        # Cargar fondo
        background_img = self.load_background_image()
        if background_img is not None:
            fig.figimage(background_img, alpha=0.2, resize=True)
        
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
                escala = 1.0
                
                # Calcular dimensiones y resolver colisiones
                tablas_info = []
                for posicion, jugadores in jugadores_agrupados.items():
                    if jugadores and posicion in self.coordenadas_posiciones:
                        x, y = self.coordenadas_posiciones[posicion]
                        width, height = self.calcular_dimensiones_tabla(jugadores, escala)
                        tablas_info.append((x, y, width, height, posicion, jugadores))

                if tablas_info:
                    tablas_ajustadas = self.detectar_y_resolver_colisiones(tablas_info)
                    
                    for x_aj, y_aj, width, height, posicion, jugadores in tablas_ajustadas:
                        self.crear_tabla_posicion(jugadores, x_aj, y_aj, ax, team_colors, posicion, escala, team_logo)
                
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
             fontsize=14, weight='bold', color='white', ha='center', va='center',
             y=1.0,  # ← MÁS ARRIBA (era 0.95 por defecto)
             bbox=dict(boxstyle="round,pad=0.03", facecolor='#1e3d59', alpha=0.95,
                      edgecolor='white', linewidth=1))
        
        print("✅ Visualización 2x2 MEJORADA creada correctamente")
        return fig

def main_4_campos_horizontales_mejorado():
    """Función principal mejorada"""
    try:
        report_gen = ReporteTactico4CamposHorizontalesMejorado()
        equipos = report_gen.get_available_teams()
        
        if not equipos:
            print("❌ No hay equipos disponibles")
            return
        
        print("\n🏟️ === REPORTE 4 CAMPOS HORIZONTALES MEJORADO ===")
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
            filename = f"reporte_4_campos_MEJORADO_{equipo_seleccionado.replace(' ', '_')}_hasta_J{jornada}{tipo_filename}.pdf"
            report_gen.guardar_sin_espacios(fig, filename)
        else:
            print("❌ No se pudo generar el reporte")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def generar_4_campos_mejorado_personalizado(equipo, jornada_maxima, tipo_partido_filter=None, mostrar=True, guardar=True):
    """Función para uso directo mejorada"""
    try:
        report_gen = ReporteTactico4CamposHorizontalesMejorado()
        fig = report_gen.crear_4_partidos_campos_horizontales(equipo, jornada_maxima, tipo_partido_filter)
        
        if fig:
            if mostrar:
                plt.show()
            if guardar:
                tipo_filename = f"_{tipo_partido_filter}" if tipo_partido_filter else "_todos"
                filename = f"reporte_4_campos_MEJORADO_{equipo.replace(' ', '_')}_hasta_J{jornada_maxima}{tipo_filename}.pdf"
                report_gen.guardar_sin_espacios(fig, filename)
            return fig
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Funciones rápidas mejoradas
def rapido_horizontal_mejorado(equipo, jornada=35, tipo_partido=None):
    """Genera 4 campos horizontales MEJORADOS rápidamente"""
    tipo_display = tipo_partido.upper() if tipo_partido else "TODOS"
    print(f"🚀 Generación rápida horizontal MEJORADA: {equipo} hasta jornada {jornada} - Partidos {tipo_display}")
    return generar_4_campos_mejorado_personalizado(equipo, jornada, tipo_partido)

def sevilla_horizontal_mejorado(tipo_partido=None):
    """Sevilla FC con campos horizontales MEJORADOS"""
    return rapido_horizontal_mejorado("Sevilla FC", 35, tipo_partido)

def sevilla_horizontal_local_mejorado():
    """Sevilla FC solo locales horizontal MEJORADO"""
    return sevilla_horizontal_mejorado("local")

def sevilla_horizontal_visitante_mejorado():
    """Sevilla FC solo visitantes horizontal MEJORADO"""
    return sevilla_horizontal_mejorado("visitante")

# Inicialización
print("🏟️ === REPORTE TÁCTICO 4 CAMPOS HORIZONTALES MEJORADO ===")
try:
    report_gen = ReporteTactico4CamposHorizontalesMejorado()
    equipos = report_gen.get_available_teams()
    jornadas = report_gen.get_available_jornadas()
    print(f"✅ Sistema MEJORADO listo: {len(equipos)} equipos, {len(jornadas)} jornadas")
    print("📝 PARA USAR:")
    print("   → main_4_campos_horizontales_mejorado() - INTERFAZ GUIADA")
    print("   → sevilla_horizontal_local_mejorado() - DIRECTO: Sevilla solo locales")
    print("   → sevilla_horizontal_visitante_mejorado() - DIRECTO: Sevilla solo visitantes")
    print("   → rapido_horizontal_mejorado('Athletic Club', 20, 'local')")
    print("\n🔥 MEJORAS APLICADAS:")
    print("   • Configuración global agresiva copiada del primer script")
    print("   • Dimensiones optimizadas (24x16 como el primer script)")
    print("   • Método guardar_sin_espacios() implementado")
    print("   • Campos creados con crear_campo_sin_espacios()")
    print("   • Subplots posicionados manualmente sin espacios")
    print("   • Mejor zoom y calidad de PDF")
    print("="*70)
    print("💡 TIP: Para Sevilla solo locales MEJORADO: sevilla_horizontal_local_mejorado()")
    print("="*70)
except Exception as e:
    print(f"❌ Error inicialización: {e}")

if __name__ == "__main__":
    main_4_campos_horizontales_mejorado()