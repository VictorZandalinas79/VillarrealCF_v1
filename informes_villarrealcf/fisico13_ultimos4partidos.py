import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import numpy as np
import os
from difflib import SequenceMatcher
import warnings
warnings.filterwarnings('ignore')

# CONFIGURACIÓN GLOBAL PARA OPTIMIZAR ESPACIOS
plt.rcParams.update({
    'figure.autolayout': False,
    'figure.constrained_layout.use': False,
    'axes.xmargin': 0,
    'axes.ymargin': 0,
    'savefig.bbox': 'tight',
    'savefig.pad_inches': 0
})

try:
    from mplsoccer import Pitch  # Cambio a Pitch horizontal
except ImportError:
    print("Instalando mplsoccer...")
    import subprocess
    subprocess.check_call(["pip", "install", "mplsoccer"])
    from mplsoccer import Pitch

class ReporteTactico4CamposHorizontales:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        """
        Inicializa la clase para generar reportes tácticos con 4 campos horizontales
        """
        self.data_path = data_path
        self.df = None
        self.load_data()
        self.clean_team_names()
        
        # 🏟️ COORDENADAS PARA CAMPOS HORIZONTALES (Pitch 0-120 x 0-80)
        # Coordenadas específicas para cada posición en el campo HORIZONTAL
        self.coordenadas_posiciones = {
            # Portero
            'PORTERO': (10, 40),
            
            # Defensas - Línea defensiva
            'LATERAL_DERECHO': (45, 68),      # Lateral derecho (arriba)
            'CENTRAL_DERECHO': (20, 60),      # Central derecho 
            'CENTRAL_IZQUIERDO': (20, 20),    # Central izquierdo
            'LATERAL_IZQUIERDO': (45, 12),    # Lateral izquierdo (abajo)
            
            # Mediocampo - Distribuidos por el centro
            'MC_POSICIONAL': (40, 45),        # Mediocampo defensivo (centro)
            'MC_BOX_TO_BOX': (65, 20),        # Box to box 
            'MC_ORGANIZADOR': (55, 55),       # Organizador
            'BANDA_DERECHA': (85, 68),        # Banda derecha (abajo)
            'BANDA_IZQUIERDA': (85, 12),      # Banda izquierda (arriba)
            'MEDIAPUNTA': (75, 40),           # Mediapunta
            
            # Delanteros - Línea de ataque
            'DELANTERO_CENTRO': (100, 45),     # Delantero centro
            'SEGUNDO_DELANTERO': (90, 65),    # Segundo delantero
        }
        
        # Mapeo de demarcaciones a posiciones (adaptado del segundo script)
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
        
        # ✅ MÉTRICAS COMPLETAS SOLICITADAS
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
        
        # Dividir por el guión
        partes = partido.split('-')
        if len(partes) != 2:
            return 'desconocido'
        
        equipo_local_partido = partes[0].strip()
        equipo_visitante_partido = partes[1].strip()
        
        # Calcular similitud con ambas partes
        sim_local = self.similarity(equipo, equipo_local_partido)
        sim_visitante = self.similarity(equipo, equipo_visitante_partido)
        
        # Determinar cuál es más similar
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
        
        # Determinar cuál es el rival
        sim_local = self.similarity(equipo, equipo_local)
        sim_visitante = self.similarity(equipo, equipo_visitante)
        
        if sim_local > sim_visitante:
            return equipo_visitante
        else:
            return equipo_local
    
    def parsear_partido_completo(self, partido, equipo):
        """
        Parsea un partido completo manteniendo el orden original (Local - Visitante)
        y determina si el equipo seleccionado juega como local o visitante
        Ejemplo: 'athleticclub1-0valenciacf' → ('Athletic Club', 'Valencia CF', '1', '0', 'local')
        """
        if '-' not in partido:
            return equipo, 'Rival', 'N', 'N', 'desconocido'
        
        # Dividir por el guión central
        partes = partido.split('-')
        if len(partes) != 2:
            return equipo, 'Rival', 'N', 'N', 'desconocido'
        
        parte_local = partes[0].strip()    # athleticclub1
        parte_visitante = partes[1].strip() # 0valenciacf
        
        # Extraer resultado y equipos usando expresiones regulares
        import re
        
        # Para la parte local: extraer nombre del equipo y último dígito
        match_local = re.match(r'(.+?)(\d+)$', parte_local)
        if match_local:
            equipo_local_raw = match_local.group(1)
            goles_local = match_local.group(2)
        else:
            equipo_local_raw = parte_local
            goles_local = 'N'
        
        # Para la parte visitante: extraer primer dígito y nombre del equipo
        match_visitante = re.match(r'(\d+)(.+)$', parte_visitante)
        if match_visitante:
            goles_visitante = match_visitante.group(1)
            equipo_visitante_raw = match_visitante.group(2)
        else:
            goles_visitante = 'N'
            equipo_visitante_raw = parte_visitante
        
        # Limpiar nombres de equipos
        equipo_local_limpio = self.limpiar_nombre_equipo(equipo_local_raw)
        equipo_visitante_limpio = self.limpiar_nombre_equipo(equipo_visitante_raw)
        
        # MANTENER SIEMPRE EL ORDEN ORIGINAL: Local - Visitante
        # Solo determinar si el equipo seleccionado es local o visitante
        sim_local = self.similarity(equipo, equipo_local_limpio)
        sim_visitante = self.similarity(equipo, equipo_visitante_limpio)
        
        if sim_local > sim_visitante:
            # El equipo seleccionado es el LOCAL
            return equipo_local_limpio, equipo_visitante_limpio, goles_local, goles_visitante, 'local'
        else:
            # El equipo seleccionado es el VISITANTE
            return equipo_local_limpio, equipo_visitante_limpio, goles_local, goles_visitante, 'visitante'

    def limpiar_nombre_equipo(self, nombre_raw):
        """Limpia nombres de equipos eliminando caracteres no deseados"""
        
        # Diccionario de equipos conocidos para mapeo directo
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
        
        # Buscar coincidencia exacta
        if nombre_lower in equipos_conocidos:
            return equipos_conocidos[nombre_lower]  
        
        # Buscar coincidencia parcial
        for key, value in equipos_conocidos.items():
            if key in nombre_lower or nombre_lower in key:
                return value
        
        # Si no encuentra coincidencia, formatear manualmente
        return nombre_raw.replace('fc', ' FC').replace('cf', ' CF').title()
    
    def get_ultimos_4_partidos(self, equipo, jornada_maxima, tipo_partido_filter=None, min_minutos=60):
        """Obtiene los últimos 4 partidos del equipo hasta la jornada especificada, filtrados por tipo"""
        if self.df is None:
            return []
        
        # Normalizar jornada
        if isinstance(jornada_maxima, str) and jornada_maxima.startswith(('J', 'j')):
            try:
                jornada_maxima = int(jornada_maxima[1:])
            except ValueError:
                pass
        
        tipo_display = tipo_partido_filter.upper() if tipo_partido_filter else "TODOS"
        print(f"🔍 Buscando últimos 4 partidos {tipo_display} para {equipo} hasta jornada {jornada_maxima}")
        
        # Filtrar por equipo y jornadas hasta la máxima
        filtrado = self.df[
            (self.df['Equipo'] == equipo) & 
            (self.df['Jornada'] <= jornada_maxima)
        ].copy()
        
        if len(filtrado) == 0:
            return []
        
        # Obtener TODOS los partidos únicos con sus jornadas PRIMERO
        partidos_info = filtrado[['Partido', 'Jornada']].drop_duplicates()
        
        # FILTRAR POR TIPO DE PARTIDO SI SE ESPECIFICA
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
        
        print(f"📋 Partidos {tipo_display} encontrados:")
        for _, partido_info in partidos_info.iterrows():
            partido = partido_info['Partido']
            jornada = partido_info['Jornada']
            tipo = self.determinar_local_visitante(partido, equipo)
            rival = self.extraer_rival(partido, equipo)
            print(f"   J{jornada}: {partido} ({tipo}) vs {rival}")
        
        # Tomar los últimos 4 partidos
        ultimos_partidos = partidos_info.head(4)
        
        resultados = []
        for _, partido_info in ultimos_partidos.iterrows():
            partido = partido_info['Partido']
            jornada = partido_info['Jornada']
            
            # Verificar si Alias está vacío y usar Nombre
            if 'Nombre' in filtrado.columns:
                mask_empty_alias = filtrado['Alias'].isna() | (filtrado['Alias'] == '') | (filtrado['Alias'].str.strip() == '')
                filtrado.loc[mask_empty_alias, 'Alias'] = filtrado.loc[mask_empty_alias, 'Nombre']
            
            # Datos del partido específico
            datos_partido = filtrado[filtrado['Partido'] == partido].copy()
            
            # Filtrar jugadores con minutos suficientes DESPUÉS de seleccionar el partido
            if 'Minutos jugados' in datos_partido.columns:
                datos_partido = datos_partido[datos_partido['Minutos jugados'] >= min_minutos]
            
            if len(datos_partido) > 0:
                tipo_partido = self.determinar_local_visitante(partido, equipo)
                rival = self.extraer_rival(partido, equipo)
                
                print(f"✅ Partido J{jornada}: {len(datos_partido)} jugadores con {min_minutos}+ minutos")
                
                resultados.append({
                    'partido': partido,
                    'jornada': jornada,
                    'tipo': tipo_partido,
                    'rival': rival,
                    'datos': datos_partido
                })
            else:
                print(f"❌ Partido J{jornada}: Sin jugadores con {min_minutos}+ minutos")
        
        print(f"🎯 Total partidos {tipo_display} seleccionados: {len(resultados)}")
        return resultados

    def detectar_y_resolver_colisiones(self, tablas_info, campo_width=120, campo_height=80):
        """
        Detecta colisiones entre tablas y ajusta posiciones automáticamente
        tablas_info = [(x, y, width, height, posicion_name, jugadores), ...]
        Retorna: [(x_ajustado, y_ajustado, width, height, posicion_name, jugadores), ...]
        """
        print("🔍 Detectando y resolviendo colisiones entre tablas...")
        
        ajustadas = []
        margen_seguridad = 2  # Margen entre tablas
        
        for i, (x, y, width, height, name, jugadores) in enumerate(tablas_info):  # ← CAMBIADO: 6 elementos
            x_ajustado, y_ajustado = x, y
            colisiones_resueltas = 0
            
            # Verificar colisiones con tablas ya procesadas
            for x2, y2, w2, h2, name2, _ in ajustadas:  # ← CAMBIADO: 6 elementos
                # Detectar solapamiento
                if (abs(x_ajustado - x2) < (width + w2)/2 + margen_seguridad and 
                    abs(y_ajustado - y2) < (height + h2)/2 + margen_seguridad):
                    
                    print(f"⚠️  Colisión detectada: {name} vs {name2}")
                    
                    # Estrategia 1: Mover horizontalmente
                    if x_ajustado < x2:
                        x_ajustado = x2 - (width + w2)/2 - margen_seguridad
                        print(f"   → {name} movido a la izquierda")
                    else:
                        x_ajustado = x2 + (width + w2)/2 + margen_seguridad
                        print(f"   → {name} movido a la derecha")
                    
                    # Si sigue fuera de límites, probar movimiento vertical
                    if x_ajustado < width/2 or x_ajustado > campo_width - width/2:
                        x_ajustado = x  # Restaurar X original
                        if y_ajustado < y2:
                            y_ajustado = y2 - (height + h2)/2 - margen_seguridad
                            print(f"   → {name} movido hacia abajo")
                        else:
                            y_ajustado = y2 + (height + h2)/2 + margen_seguridad
                            print(f"   → {name} movido hacia arriba")
                    
                    colisiones_resueltas += 1
            
            # Mantener dentro de los límites del campo
            x_final = max(width/2 + 1, min(campo_width - width/2 - 1, x_ajustado))
            y_final = max(height/2 + 1, min(campo_height - height/2 - 1, y_ajustado))
            
            if x_final != x or y_final != y:
                print(f"   ✅ {name}: ({x:.1f},{y:.1f}) → ({x_final:.1f},{y_final:.1f})")
            
            ajustadas.append((x_final, y_final, width, height, name, jugadores))  # ← CAMBIADO: 6 elementos
        
        print(f"✅ Resolución de colisiones completada. {len(ajustadas)} tablas procesadas")
        return ajustadas

    def calcular_dimensiones_tabla(self, jugadores_list, scale=0.9):
        """Calcula las dimensiones que tendrá una tabla antes de crearla"""
        if not jugadores_list:
            return 0, 0
        
        num_players = len(jugadores_list)
        num_metrics = len(self.metricas_tabla)
        
        # Mismas fórmulas que en crear_tabla_posicion
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
        """Carga el escudo del equipo"""
        possible_names = [
            equipo,
            equipo.replace(' ', '_'),
            equipo.replace(' ', ''),
            equipo.lower(),
            equipo.lower().replace(' ', '_'),
            equipo.lower().replace(' ', '')
        ]
        
        for name in possible_names:
            logo_path = f"assets/escudos/{name}.png"
            if os.path.exists(logo_path):
                try:
                    return plt.imread(logo_path)
                except Exception:
                    continue
        
        print(f"⚠️ No se encontró escudo para: {equipo}")
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
        else:
            print(f"⚠️ No se encontró la imagen de fondo: {background_path}")
            return None
    
    def crear_campo_horizontal(self, ax):
        """Crea campo horizontal usando Pitch en el ax proporcionado"""
        pitch = Pitch(
            pitch_color='grass', 
            line_color='white', 
            stripe=True,
            linewidth=2,
            pad_left=0, pad_right=0, pad_bottom=0, pad_top=0
        )
        pitch.draw(ax=ax)
        return pitch
    
    def fill_missing_demarcaciones(self, df):
        """Rellena demarcaciones vacías con histórico del jugador"""
        df_work = df.copy()
        
        mask_empty = df_work['Demarcacion'].isna() | (df_work['Demarcacion'] == '') | (df_work['Demarcacion'].str.strip() == '')
        
        for idx in df_work[mask_empty].index:
            jugador_id = df_work.loc[idx, 'Id Jugador']
            
            # Buscar demarcaciones históricas
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
        """Agrupa jugadores por posición específica con lógica de balanceo"""
        # Rellenar demarcaciones vacías usando histórico
        jugadores_df = self.fill_missing_demarcaciones(jugadores_df)
        
        # Verificar si Alias está vacío y usar Nombre
        if 'Nombre' in jugadores_df.columns:
            mask_empty_alias = jugadores_df['Alias'].isna() | (jugadores_df['Alias'] == '') | (jugadores_df['Alias'].str.strip() == '')
            jugadores_df.loc[mask_empty_alias, 'Alias'] = jugadores_df.loc[mask_empty_alias, 'Nombre']
        
        # Ordenar por minutos jugados (descendente)
        jugadores_df_sorted = jugadores_df.sort_values('Minutos jugados', ascending=False)
        
        # Inicializar grupos
        jugadores_por_posicion = {pos: [] for pos in self.coordenadas_posiciones.keys()}
        
        # Agrupar jugadores por demarcación
        for _, jugador in jugadores_df_sorted.iterrows():
            demarcacion = jugador.get('Demarcacion', 'Sin Posición')
            posicion = self.demarcacion_to_posicion.get(demarcacion, 'MC_BOX_TO_BOX')
            
            if posicion in jugadores_por_posicion:
                jugadores_por_posicion[posicion].append(jugador.to_dict())
        
        # Lógica de balanceo (similar al segundo script)
        self.balancear_centrales(jugadores_por_posicion)
        self.balancear_delanteros(jugadores_por_posicion)
        
        return jugadores_por_posicion
    
    def balancear_centrales(self, jugadores_por_posicion):
        """Balancea centrales entre derecho e izquierdo"""
        centrales_derecho = jugadores_por_posicion['CENTRAL_DERECHO']
        centrales_izquierdo = jugadores_por_posicion['CENTRAL_IZQUIERDO']
        
        if len(centrales_izquierdo) == 0 and len(centrales_derecho) > 1:
            jugador_a_mover = centrales_derecho.pop()
            jugadores_por_posicion['CENTRAL_IZQUIERDO'].append(jugador_a_mover)
            print(f"   ✅ {jugador_a_mover['Alias']} movido a Central Izquierdo")
        
        elif len(centrales_derecho) == 0 and len(centrales_izquierdo) > 1:
            jugador_a_mover = centrales_izquierdo.pop()
            jugadores_por_posicion['CENTRAL_DERECHO'].append(jugador_a_mover)
            print(f"   ✅ {jugador_a_mover['Alias']} movido a Central Derecho")
    
    def balancear_delanteros(self, jugadores_por_posicion):
        """Balancea delanteros entre centro y segundo delantero"""
        delanteros = jugadores_por_posicion['DELANTERO_CENTRO']
        
        if len(delanteros) > 1:
            # Dividir en dos grupos
            mitad = len(delanteros) // 2 + len(delanteros) % 2
            
            primer_grupo = delanteros[:mitad]
            segundo_grupo = delanteros[mitad:]
            
            jugadores_por_posicion['DELANTERO_CENTRO'] = primer_grupo
            jugadores_por_posicion['SEGUNDO_DELANTERO'] = segundo_grupo
            
            print(f"   ✅ Divididos: {len(primer_grupo)} en Delantero Centro, {len(segundo_grupo)} en Segundo Delantero")
    
    def crear_tabla_posicion(self, jugadores_list, x, y, ax, team_colors, posicion_name, scale=0.8, team_logo=None):
        """Crea una tabla moderna por posición adaptada del segundo script"""
        if not jugadores_list:
            return
        
        num_players = len(jugadores_list)
        num_metrics = len(self.metricas_tabla)
        
        # Dimensiones de la tabla
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
        
        # Header con nombre de posición
        header_rect = plt.Rectangle((x - table_width/2, y + table_height/2 - header_height), 
                                  table_width, header_height,
                                  facecolor=team_colors['primary'], alpha=0.8,
                                  edgecolor='white', linewidth=1)
        ax.add_patch(header_rect)
        
        # Limpiar nombre de posición
        clean_position_name = posicion_name.replace('_', ' ').title()
        ax.text(x, y + table_height/2 - header_height/2, clean_position_name, 
                fontsize=int(8 * scale), weight='bold', color=team_colors['text'],
                ha='center', va='center')
        
        # Fila de nombres + dorsales
        names_y = y + table_height/2 - header_height - names_height/2
        
        names_rect = plt.Rectangle((x - table_width/2 + metric_col_width, names_y - names_height/2), 
                                num_players * player_col_width, names_height,
                                facecolor='#34495e', alpha=0.7, 
                                edgecolor='white', linewidth=0.5)
        ax.add_patch(names_rect)

        # 🏆 ESCUDO DEL EQUIPO en la celda de métricas (fila de nombres)
        if team_logo is not None:
            try:
                logo_x = x - table_width/2 + metric_col_width/2
                logo_y = names_y
                zoom_factor = min(metric_col_width / 120, names_height / 120) * 0.7
        
                imagebox = OffsetImage(team_logo, zoom=zoom_factor)
                ab = AnnotationBbox(imagebox, (logo_x, logo_y), frameon=False)
                ax.add_artist(ab)
                
                print(f"✅ Escudo añadido en {posicion_name}")
            except Exception as e:
                print(f"⚠️ Error al añadir escudo: {e}")
        
        # Agregar nombres y dorsales
        for i, jugador in enumerate(jugadores_list):
            player_x = x - table_width/2 + metric_col_width + (i * player_col_width) + player_col_width/2
            player_name = jugador['Alias'] if pd.notna(jugador['Alias']) else 'N/A'
            dorsal = jugador.get('Dorsal', 'N/A')
            
            # Nombre del jugador (arriba o centro)
            ax.text(player_x, names_y + 0.8 * scale, player_name, 
                    fontsize=int(6 * scale), weight='bold', color='white',
                    ha='center', va='center')
            
            # Dorsal (abajo o centro)
            ax.text(player_x, names_y - 0.8 * scale, str(dorsal), 
                    fontsize=int(10 * scale), weight='bold', color=team_colors['primary'],
                    ha='center', va='center')
        
        # Filas de métricas
        for i, metrica in enumerate(self.metricas_tabla):
            metric_y = names_y - names_height/2 - (i + 1) * metric_row_height + metric_row_height/2
            
            # Fondo alternado
            if i % 2 == 0:
                row_rect = plt.Rectangle((x - table_width/2, metric_y - metric_row_height/2), 
                                       table_width, metric_row_height,
                                       facecolor='#3c566e', alpha=0.3)
                ax.add_patch(row_rect)
            
            # Columna de métrica
            metric_bg = plt.Rectangle((x - table_width/2, metric_y - metric_row_height/2), 
                                    metric_col_width, metric_row_height,
                                    facecolor=team_colors['primary'], alpha=0.6,
                                    edgecolor='white', linewidth=0.3)
            ax.add_patch(metric_bg)
            
            # Nombre de métrica MÁS ABREVIADO
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
                    fontsize=int(6 * scale), weight='bold', color='white',
                    ha='center', va='center')
            
            # Valores para cada jugador
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
                        fontsize=int(6 * scale), weight='bold', color='#FFD700',
                        ha='center', va='center')

    def crear_titulo_elegante_con_escudos(self, ax, titulo_texto, equipo_local, equipo_visitante, 
                                  team_colors, y_position=0.95):
        """
        Crea un título elegante con fondo y escudos posicionados según local/visitante
        """
        # Obtener dimensiones del subplot
        bbox = ax.get_position()
        fig_width = ax.figure.get_figwidth()
        fig_height = ax.figure.get_figheight()
        
        # Calcular posición en coordenadas de figura
        fig_x = bbox.x0 + bbox.width/2  # Centro horizontal del subplot
        fig_y = bbox.y1 + 0.02          # Justo arriba del subplot
        
        # 🎨 CREAR FONDO ELEGANTE DEL TÍTULO
        # Rectángulo de fondo con gradiente simulado
        titulo_width = bbox.width * 0.9  # 90% del ancho del subplot
        titulo_height = 0.04             # Altura del fondo
        
        # Fondo principal
        fondo_rect = plt.Rectangle(
            (fig_x - titulo_width/2, fig_y - titulo_height/2), 
            titulo_width, titulo_height,
            facecolor=team_colors['primary'], 
            alpha=0.9,
            edgecolor='white', 
            linewidth=2,
            transform=ax.figure.transFigure,
            zorder=10
        )
        ax.figure.patches.append(fondo_rect)
        
        # Borde superior más claro para efecto de profundidad
        borde_top = plt.Rectangle(
            (fig_x - titulo_width/2, fig_y + titulo_height/2 - 0.003), 
            titulo_width, 0.003,
            facecolor='white', 
            alpha=0.4,
            transform=ax.figure.transFigure,
            zorder=11
        )
        ax.figure.patches.append(borde_top)
        
        # 📝 TEXTO DEL TÍTULO
        ax.figure.text(
            fig_x, fig_y, titulo_texto,
            fontsize=11, weight='bold', color=team_colors['text'],
            ha='center', va='center',
            transform=ax.figure.transFigure,
            zorder=12
        )
        
        # 🏆 ESCUDOS POSICIONADOS SEGÚN LOCAL/VISITANTE
        escudo_size = 0.025  # Tamaño de los escudos
        escudo_offset = titulo_width/2 + 0.03  # Distancia del centro al escudo
        
        # Cargar escudos
        escudo_local = self.load_team_logo(equipo_local)
        escudo_visitante = self.load_team_logo(equipo_visitante)
        
        print(f"   🏠 {equipo_local} (Local) ← | → {equipo_visitante} (Visitante)")
        
        # Posicionar escudo LOCAL (siempre a la izquierda)
        if escudo_local is not None:
            try:
                # Convertir coordenadas de figura a coordenadas del axes
                escudo_izq_x = fig_x - escudo_offset
                escudo_izq_y = fig_y
                
                # Convertir a coordenadas de datos del subplot
                point_data = ax.transData.inverted().transform(
                    ax.figure.transFigure.transform([escudo_izq_x, escudo_izq_y])
                )
                
                imagebox_izq = OffsetImage(escudo_local, zoom=0.05)
                ab_izq = AnnotationBbox(
                    imagebox_izq, point_data, 
                    frameon=False, xycoords='data'
                )
                ax.add_artist(ab_izq)
            except Exception as e:
                print(f"⚠️ Error al añadir escudo local: {e}")
        
        # Posicionar escudo VISITANTE (siempre a la derecha)
        if escudo_visitante is not None:
            try:
                # Convertir coordenadas de figura a coordenadas del axes
                escudo_der_x = fig_x + escudo_offset
                escudo_der_y = fig_y
                
                # Convertir a coordenadas de datos del subplot
                point_data = ax.transData.inverted().transform(
                    ax.figure.transFigure.transform([escudo_der_x, escudo_der_y])
                )
                
                imagebox_der = OffsetImage(escudo_visitante, zoom=0.05)
                ab_der = AnnotationBbox(
                    imagebox_der, point_data, 
                    frameon=False, xycoords='data'
                )
                ax.add_artist(ab_der)
            except Exception as e:
                print(f"⚠️ Error al añadir escudo visitante: {e}")

    def crear_4_partidos_campos_horizontales(self, equipo, jornada_maxima, tipo_partido_filter=None, figsize=(24, 18)):
        """Crea 4 campos horizontales en layout 2x2 con tablas posicionadas por demarcación"""
        
        tipo_display = tipo_partido_filter.upper() if tipo_partido_filter else "TODOS"
        print(f"\n🔄 Generando visualización 2x2 de 4 campos HORIZONTALES {tipo_display} para {equipo}")
        
        # Obtener últimos 4 partidos
        partidos = self.get_ultimos_4_partidos(equipo, jornada_maxima, tipo_partido_filter)
        
        if len(partidos) == 0:
            print(f"❌ No hay partidos {tipo_display} para {equipo}")
            return None
        
        print(f"📊 Se crearán {len(partidos)} campos horizontales en layout 2x2")
        
        # Crear figura SIN ESPACIOS con configuración agresiva
        fig = plt.figure(figsize=figsize, constrained_layout=False)
        fig.subplots_adjust(left=0, right=1, top=0.95, bottom=0, wspace=0, hspace=0)
        
        # Crear subplots manualmente con espaciado cero
        axes = []
        for i in range(2):
            row_axes = []
            for j in range(2):
                ax = fig.add_subplot(2, 2, i*2 + j + 1)
                row_axes.append(ax)
            axes.append(row_axes)
        axes = np.array(axes)
        
        # Cargar imagen de fondo
        background_img = self.load_background_image()
        if background_img is not None:
            fig.figimage(background_img, alpha=0.2, resize=True)
        
        # Aplanar axes para acceso fácil
        axes_flat = axes.flatten()
        
        # Obtener colores del equipo
        team_colors = self.get_team_colors(equipo)
        team_logo = self.load_team_logo(equipo)
        
        # Crear hasta 4 campos
        for i in range(4):
            ax = axes_flat[i]
            
            if i < len(partidos):
                # Crear campo horizontal
                pitch = self.crear_campo_horizontal(ax)
                
                # Obtener datos del partido
                partido_info = partidos[i]
                print(f"🏟️ Campo {i+1}: J{partido_info['jornada']} - {partido_info['partido']}")
                
                # Agrupar jugadores por posición
                jugadores_agrupados = self.agrupar_jugadores_por_posicion(partido_info['datos'])
                
                # Escala para las tablas (AUMENTADA)
                escala = 0.9  # ⬆️ AUMENTADO de 0.7 a 0.9 para tablas más grandes
                
                # 🔄 PASO 1: Calcular dimensiones de todas las tablas
                tablas_info = []
                for posicion, jugadores in jugadores_agrupados.items():
                    if jugadores and posicion in self.coordenadas_posiciones:
                        x, y = self.coordenadas_posiciones[posicion]
                        width, height = self.calcular_dimensiones_tabla(jugadores, escala)
                        tablas_info.append((x, y, width, height, posicion, jugadores))

                # 🔄 PASO 2: Resolver colisiones
                if tablas_info:
                    tablas_ajustadas = self.detectar_y_resolver_colisiones(tablas_info)
                    
                    # 🔄 PASO 3: Crear tablas en posiciones ajustadas
                    for x_aj, y_aj, width, height, posicion, jugadores in tablas_ajustadas:
                        self.crear_tabla_posicion(jugadores, x_aj, y_aj, ax, team_colors, posicion, escala, team_logo)
                else:
                    print("⚠️  No hay tablas para mostrar en este campo")
                
                # Parsear partido completo
                equipo_local, equipo_visitante, goles_local, goles_visitante, tipo_real = self.parsear_partido_completo(
                    partido_info['partido'], equipo)

                tipo_display_partido = "Local" if tipo_real == 'local' else "Visitante"

                # Formato elegante del título
                titulo_partido = f"J{partido_info['jornada']} - {equipo_local} {goles_local} - {goles_visitante} {equipo_visitante} ({tipo_display_partido})"

                # 🎨 CREAR TÍTULO ELEGANTE CON ESCUDOS
                self.crear_titulo_elegante_con_escudos(
                    ax, titulo_partido, equipo_local, equipo_visitante, 
                    team_colors
                )

                # Eliminar título por defecto de matplotlib
                ax.set_title('', fontsize=1)
                
            
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
                    fontsize=16, color='black', weight='bold', y=0.95)
        
        # CONFIGURACIÓN AGRESIVA PARA ELIMINAR TODOS LOS ESPACIOS
        fig.subplots_adjust(
            top=0.95,        # Mínimo margen arriba para título
            bottom=0.01,     # Mínimo margen abajo
            left=0.01,       # Mínimo margen izquierda
            right=0.99,      # Mínimo margen derecha
            hspace=0,        # CERO separación vertical
            wspace=0         # CERO separación horizontal
        )
        
        # Eliminar márgenes de cada subplot individualmente
        for i in range(4):
            ax = axes_flat[i]
            ax.set_position(ax.get_position())
            ax.margins(0, 0)
        
        print("✅ Visualización 2x2 de campos horizontales creada correctamente")
        return fig

def main_4_campos_horizontales():
    """Función principal para generar el reporte de 4 campos horizontales"""
    try:
        report_gen = ReporteTactico4CamposHorizontales()
        equipos = report_gen.get_available_teams()
        
        if not equipos:
            print("❌ No hay equipos disponibles")
            return
        
        print("\n🏟️ === REPORTE 4 CAMPOS HORIZONTALES ===")
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
        
        # Seleccionar jornada máxima
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
        
        # Seleccionar tipo de partido
        print(f"\n🎯 Selecciona tipo de partidos a mostrar:")
        print("1. SOLO LOCALES")
        print("2. SOLO VISITANTES") 
        print("3. TODOS (locales y visitantes mezclados)")
        
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
        
        # Generar visualización
        fig = report_gen.crear_4_partidos_campos_horizontales(equipo_seleccionado, jornada, tipo_partido_filter)
        
        if fig:
            plt.show()
            
            # Guardar
            tipo_filename = f"_{tipo_partido_filter}" if tipo_partido_filter else "_todos"
            filename = f"reporte_4_campos_horizontales_{equipo_seleccionado.replace(' ', '_')}_hasta_J{jornada}{tipo_filename}.pdf"
            fig.savefig(filename, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            print(f"✅ Guardado: {filename}")
        else:
            print("❌ No se pudo generar el reporte")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def generar_4_campos_horizontales_personalizado(equipo, jornada_maxima, tipo_partido_filter=None, mostrar=True, guardar=True):
    """Función para uso directo de 4 campos horizontales"""
    try:
        report_gen = ReporteTactico4CamposHorizontales()
        fig = report_gen.crear_4_partidos_campos_horizontales(equipo, jornada_maxima, tipo_partido_filter)
        
        if fig:
            if mostrar:
                plt.show()
            if guardar:
                tipo_filename = f"_{tipo_partido_filter}" if tipo_partido_filter else "_todos"
                filename = f"reporte_4_campos_horizontales_{equipo.replace(' ', '_')}_hasta_J{jornada_maxima}{tipo_filename}.pdf"
                fig.savefig(filename, dpi=300, bbox_inches='tight', 
                           facecolor='white', edgecolor='none')
                print(f"✅ Guardado: {filename}")
            return fig
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Funciones rápidas
def rapido_horizontal(equipo, jornada=35, tipo_partido=None):
    """Genera 4 campos horizontales rápidamente"""
    tipo_display = tipo_partido.upper() if tipo_partido else "TODOS"
    print(f"🚀 Generación rápida horizontal: {equipo} hasta jornada {jornada} - Partidos {tipo_display}")
    return generar_4_campos_horizontales_personalizado(equipo, jornada, tipo_partido)

def sevilla_horizontal(tipo_partido=None):
    """Sevilla FC con campos horizontales"""
    return rapido_horizontal("Sevilla FC", 35, tipo_partido)

def sevilla_horizontal_local():
    """Sevilla FC solo locales horizontal"""
    return sevilla_horizontal("local")

def sevilla_horizontal_visitante():
    """Sevilla FC solo visitantes horizontal"""
    return sevilla_horizontal("visitante")

# Inicialización
print("🏟️ === REPORTE TÁCTICO 4 CAMPOS HORIZONTALES INICIALIZADO ===")
try:
    report_gen = ReporteTactico4CamposHorizontales()
    equipos = report_gen.get_available_teams()
    jornadas = report_gen.get_available_jornadas()
    print(f"✅ Sistema listo: {len(equipos)} equipos, {len(jornadas)} jornadas")
    print("📝 PARA USAR:")
    print("   → main_4_campos_horizontales() - INTERFAZ GUIADA")
    print("   → sevilla_horizontal_local() - DIRECTO: Sevilla solo locales")
    print("   → sevilla_horizontal_visitante() - DIRECTO: Sevilla solo visitantes")
    print("   → rapido_horizontal('Athletic Club', 20, 'local')")
    print("\n🎯 CARACTERÍSTICAS:")
    print("   • 4 campos horizontales en layout 2x2")
    print("   • Tablas posicionadas por demarcación en el campo")
    print("   • 9 métricas completas por jugador")
    print("   • Filtro por tipo de partido (local/visitante)")
    print("   • Balanceo automático de centrales y delanteros")
    print("   • Escudos de ambos equipos en cada campo")
    print("="*70)
    print("💡 TIP: Para Sevilla solo locales horizontal: sevilla_horizontal_local()")
    print("="*70)
except Exception as e:
    print(f"❌ Error inicialización: {e}")

if __name__ == "__main__":
    main_4_campos_horizontales()