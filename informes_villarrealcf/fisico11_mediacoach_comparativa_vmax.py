import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import numpy as np
import os
from difflib import SequenceMatcher
import warnings
warnings.filterwarnings('ignore')

# CONFIGURACIÓN GLOBAL PARA ELIMINAR ESPACIOS
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

# Instalar mplsoccer si no está instalado
try:
    from mplsoccer import Pitch
except ImportError:
    print("Instalando mplsoccer...")
    import subprocess
    subprocess.check_call(["pip", "install", "mplsoccer"])
    from mplsoccer import Pitch

class CampoFutbolBarras:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        """
        Inicializa la clase para generar informes con gráficos de barras por demarcación
        """
        self.data_path = data_path
        self.df = None
        self.load_data()
        self.clean_team_names()
        
        # 🎨 COLORES ESPECÍFICOS POR EQUIPO
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

        # Colores por defecto para equipos no reconocidos
        self.default_team_colors = {'primary': '#2c3e50', 'secondary': '#FFFFFF', 'text': 'white'}

        # Mapeo exacto basado en las demarcaciones encontradas
        self.demarcacion_to_position = {
            # Portero (queda igual)
            'Portero': 'PORTERO',
            
            # Defensas - Posiciones específicas
            'Defensa - Central Derecho': 'CENTRAL_DERECHO',
            'Defensa - Lateral Derecho': 'LATERAL_DERECHO', 
            'Defensa - Central Izquierdo': 'CENTRAL_IZQUIERDO',
            'Defensa - Lateral Izquierdo': 'LATERAL_IZQUIERDO',
            
            # Mediocampo - Posiciones específicas
            'Centrocampista - MC Box to Box': 'MC_BOX_TO_BOX',
            'Centrocampista - MC Organizador': 'MC_ORGANIZADOR',
            'Centrocampista - MC Posicional': 'MC_POSICIONAL',
            'Centrocampista de ataque - Banda Derecha': 'BANDA_DERECHA',
            'Centrocampista de ataque - Banda Izquierda': 'BANDA_IZQUIERDA',
            'Centrocampista de ataque - Mediapunta': 'MC_BOX_TO_BOX',

            # Delanteros - Dos posiciones diferenciadas
            'Delantero - Delantero Centro': 'DELANTERO_CENTRO',
            'Delantero - Segundo Delantero': 'DELANTERO_CENTRO',
            
            # Jugadores sin posición definida
            'Sin Posición': 'MC_POSICIONAL',
        }
        
        # Coordenadas específicas para cada posición en el campo
        self.coordenadas_graficos = {
            # Villarreal (lado izquierdo)
            'villarreal': {
                'PORTERO': (10, 40),              # Portería
                'LATERAL_DERECHO': (25, 12),      # Lateral derecho (arriba)
                'CENTRAL_DERECHO': (20, 25),      # Central derecho (centro-arriba)
                'CENTRAL_IZQUIERDO': (20, 53),    # Central izquierdo (centro-abajo)
                'LATERAL_IZQUIERDO': (25, 68),    # Lateral izquierdo (abajo)
                'MC_POSICIONAL': (35, 40),        # Mediocampo defensivo (centro)
                'MC_BOX_TO_BOX': (62, 55),        # Box to box (centro-arriba)
                'MC_ORGANIZADOR': (50, 40),       # Organizador (centro-abajo)
                'BANDA_DERECHA': (70, 12),        # Banda derecha (extremo arriba)
                'BANDA_IZQUIERDA': (70, 68),      # Banda izquierda (extremo abajo)
                'DELANTERO_CENTRO': (85, 55),     # Delantero centro (arriba)
                'SEGUNDO_DELANTERO': (85, 25),    # Segundo delantero (abajo)
            },
            # Equipo rival (lado derecho - espejo)
            'rival': {
                'PORTERO': (110, 40),             # Portería
                'LATERAL_DERECHO': (100, 68),      # Lateral derecho (abajo - espejo)
                'CENTRAL_DERECHO': (105, 53),      # Central derecho (centro-abajo - espejo)
                'CENTRAL_IZQUIERDO': (105, 25),    # Central izquierdo (centro-arriba - espejo)
                'LATERAL_IZQUIERDO': (100, 12),    # Lateral izquierdo (arriba - espejo)
                'MC_POSICIONAL': (90, 40),        # Mediocampo defensivo (centro)
                'MC_BOX_TO_BOX': (60, 25),        # Box to box (centro-abajo - espejo)
                'MC_ORGANIZADOR': (70, 40),       # Organizador (centro-arriba - espejo)
                'BANDA_DERECHA': (45, 68),        # Banda derecha (extremo abajo - espejo)
                'BANDA_IZQUIERDA': (45, 12),      # Banda izquierda (extremo arriba - espejo)
                'DELANTERO_CENTRO': (38, 25),     # Delantero centro (abajo - espejo)
                'SEGUNDO_DELANTERO': (38, 53),    # Segundo delantero (arriba - espejo)
            }
        }
        
        # 🔥 MÉTRICAS PARA LOS GRÁFICOS DE BARRAS
        self.metricas_barras = [
            'Distancia Total 14-21 km / h',
            'Distancia Total >21 km / h', 
            'Distancia Total >24 km / h'
        ]
        
        # Métrica para el punto rojo
        self.metrica_punto_rojo = 'Velocidad Máxima Total'
        
        # 🔥 MÉTRICAS AMPLIADAS PARA RESUMEN DE EQUIPOS
        self.metricas_resumen_equipos = [
            'Distancia Total 14-21 km / h',
            'Distancia Total >21 km / h',
            'Distancia Total >24 km / h',
            'Velocidad Máxima Total'
        ]
        
        # Colores para las barras de métricas
        self.colores_barras = [
            '#45B7D1',  # Azul claro para 14-21 km/h
            '#4ECDC4',  # Turquesa para >21 km/h
            '#FF7F50',  # Naranja para >24 km/h
        ]
        
    def load_data(self):
        """Carga los datos del archivo parquet"""
        try:
            self.df = pd.read_parquet(self.data_path)
            print(f"✅ Datos cargados exitosamente: {self.df.shape[0]} filas, {self.df.shape[1]} columnas")
        except Exception as e:
            print(f"❌ Error al cargar los datos: {e}")
            
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
                
            # Buscar equipos similares
            similar_teams = [team]
            for other_team in unique_teams:
                if other_team != team and other_team not in processed_teams:
                    if self.similarity(team, other_team) > 0.7:
                        similar_teams.append(other_team)
            
            # Elegir el nombre más largo como representativo
            canonical_name = max(similar_teams, key=len)
            
            # Mapear todos los nombres similares al canónico
            for similar_team in similar_teams:
                team_mapping[similar_team] = canonical_name
                processed_teams.add(similar_team)
        
        # Aplicar el mapeo
        self.df['Equipo'] = self.df['Equipo'].map(team_mapping)
        
        # Normalizar jornadas
        def normalize_jornada(jornada):
            if isinstance(jornada, str) and jornada.startswith('J'):
                try:
                    return int(jornada[1:])
                except ValueError:
                    return jornada
            elif isinstance(jornada, str) and jornada.startswith('j'):
                try:
                    return int(jornada[1:])
                except ValueError:
                    return jornada
            return jornada
        
        self.df['Jornada'] = self.df['Jornada'].apply(normalize_jornada)
        print(f"✅ Limpieza completada. Equipos únicos: {len(self.df['Equipo'].unique())}")
        
    def get_available_teams(self):
        """Retorna la lista de equipos disponibles"""
        if self.df is None:
            return []
        return sorted(self.df['Equipo'].unique())
    
    def get_available_jornadas(self, equipo=None):
        """Retorna las jornadas disponibles"""
        if self.df is None:
            return []
        
        if equipo:
            filtered_df = self.df[self.df['Equipo'] == equipo]
            return sorted(filtered_df['Jornada'].unique())
        else:
            return sorted(self.df['Jornada'].unique())
    
    def fill_missing_demarcaciones(self, df):
        """Rellena demarcaciones vacías con la más frecuente para cada jugador"""
        print("🔄 Rellenando demarcaciones vacías...")
        
        # Crear copia para trabajar
        df_work = df.copy()
        
        # Identificar registros con demarcación vacía
        mask_empty = df_work['Demarcacion'].isna() | (df_work['Demarcacion'] == '') | (df_work['Demarcacion'].str.strip() == '')
        empty_count = mask_empty.sum()
        
        if empty_count > 0:
            print(f"📝 Encontrados {empty_count} registros con demarcación vacía")
            
            # Para cada jugador con demarcación vacía, buscar su demarcación más frecuente
            for idx in df_work[mask_empty].index:
                jugador_id = df_work.loc[idx, 'Id Jugador']
                jugador_alias = df_work.loc[idx, 'Alias']
                
                # Buscar todas las demarcaciones de este jugador (no vacías)
                jugador_demarcaciones = self.df[
                    (self.df['Id Jugador'] == jugador_id) & 
                    (self.df['Demarcacion'].notna()) & 
                    (self.df['Demarcacion'] != '') &
                    (self.df['Demarcacion'].str.strip() != '')
                ]['Demarcacion']
                
                if len(jugador_demarcaciones) > 0:
                    # Usar la demarcación más frecuente
                    demarcacion_mas_frecuente = jugador_demarcaciones.value_counts().index[0]
                    df_work.loc[idx, 'Demarcacion'] = demarcacion_mas_frecuente
                    print(f"   ✅ {jugador_alias}: {demarcacion_mas_frecuente} (histórico)")
                else:
                    # Si no hay datos históricos, asignar "Sin Posición"
                    df_work.loc[idx, 'Demarcacion'] = 'Sin Posición'
                    print(f"   ⚠️  {jugador_alias}: Sin posición histórica -> MC Posicional")
        
        return df_work
    
    def filter_and_accumulate_data(self, equipo, jornadas, min_avg_minutes=60):
        """Filtra por promedio de minutos y acumula datos por jugador"""
        if self.df is None:
            return None
        
        # Normalizar jornadas
        normalized_jornadas = []
        for jornada in jornadas:
            if isinstance(jornada, str) and jornada.startswith('J'):
                try:
                    normalized_jornadas.append(int(jornada[1:]))
                except ValueError:
                    normalized_jornadas.append(jornada)
            elif isinstance(jornada, str) and jornada.startswith('j'):
                try:
                    normalized_jornadas.append(int(jornada[1:]))
                except ValueError:
                    normalized_jornadas.append(jornada)
            else:
                normalized_jornadas.append(jornada)
        
        # Filtrar por equipo y jornadas
        filtered_df = self.df[
            (self.df['Equipo'] == equipo) & 
            (self.df['Jornada'].isin(normalized_jornadas))
        ].copy()
        
        # Rellenar demarcaciones vacías
        filtered_df = self.fill_missing_demarcaciones(filtered_df)
        
        # Verificar si Alias está vacío y usar Nombre en su lugar
        if 'Nombre' in filtered_df.columns:
            mask_empty_alias = filtered_df['Alias'].isna() | (filtered_df['Alias'] == '') | (filtered_df['Alias'].str.strip() == '')
            filtered_df.loc[mask_empty_alias, 'Alias'] = filtered_df.loc[mask_empty_alias, 'Nombre']
        
        if 'Minutos jugados' not in filtered_df.columns:
            print("⚠️  Columna 'Minutos jugados' no encontrada.")
            return None
        
        # Agrupar por jugador y calcular estadísticas acumuladas
        print(f"🔄 Procesando datos acumulados por jugador para {equipo}...")
        
        accumulated_data = []
        
        for jugador in filtered_df['Alias'].unique():
            jugador_data = filtered_df[filtered_df['Alias'] == jugador]
    
            # NUEVO: Filtrar solo partidos donde jugó 60+ minutos
            jugador_data_filtered = jugador_data[jugador_data['Minutos jugados'] >= min_avg_minutes]
            
            # Solo incluir jugadores que tengan al menos 1 partido con 60+ minutos
            if len(jugador_data_filtered) > 0:
                # Tomar datos básicos del jugador (usar el registro más reciente)
                latest_record = jugador_data.iloc[-1]
                
                # Crear registro acumulado
                accumulated_record = {
                    'Id Jugador': latest_record['Id Jugador'],
                    'Dorsal': latest_record['Dorsal'],
                    'Nombre': latest_record['Nombre'],
                    'Alias': latest_record['Alias'],
                    'Demarcacion': jugador_data_filtered['Demarcacion'].mode().iloc[0] if len(jugador_data_filtered['Demarcacion'].mode()) > 0 else latest_record['Demarcacion'],
                    'Equipo': latest_record['Equipo'],
                    
                    # Minutos: promedio SOLO de partidos 60+
                    'Minutos jugados': jugador_data_filtered['Minutos jugados'].mean(),
                    
                    # Distancias: suma total SOLO de partidos 60+
                    'Distancia Total': jugador_data_filtered['Distancia Total'].fillna(0).sum(),
                    'Distancia Total 14-21 km / h': jugador_data_filtered['Distancia Total 14-21 km / h'].fillna(0).sum(),
                    'Distancia Total >21 km / h': jugador_data_filtered['Distancia Total >21 km / h'].fillna(0).sum(),
                    'Distancia Total >24 km / h': jugador_data_filtered.get('Distancia Total >24 km / h', pd.Series([0])).fillna(0).sum(),
                    
                    # Distancias por minuto: promedio
                    'Distancia Total / min': jugador_data_filtered['Distancia Total / min'].mean(),
                    'Distancia Total 14-21 km / h / min': jugador_data_filtered.get('Distancia Total 14-21 km / h / min', pd.Series([0])).mean(),
                    'Distancia Total >21 km / h / min': jugador_data_filtered.get('Distancia Total >21 km / h / min', pd.Series([0])).mean(),
                    
                    # Velocidades: máximo
                    'Velocidad Máxima Total': jugador_data_filtered['Velocidad Máxima Total'].max(),
                    'Velocidad Máxima 1P': jugador_data_filtered.get('Velocidad Máxima 1P', pd.Series([0])).max(),
                    'Velocidad Máxima 2P': jugador_data_filtered.get('Velocidad Máxima 2P', pd.Series([0])).max(),
                }
                
                accumulated_data.append(accumulated_record)
        
        # Convertir a DataFrame
        if accumulated_data:
            result_df = pd.DataFrame(accumulated_data)
            print(f"✅ {len(result_df)} jugadores con promedio {min_avg_minutes}+ minutos")
            print(f"📊 Datos acumulados para {equipo}: {len(result_df)} jugadores únicos")
            return result_df
        else:
            print(f"❌ No hay jugadores con promedio {min_avg_minutes}+ minutos para {equipo}")
            return None
    
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
                except Exception as e:
                    print(f"⚠️  Error al cargar escudo {logo_path}: {e}")
                    continue
        
        print(f"⚠️  No se encontró el escudo para: {equipo}")
        return None

    def get_team_colors(self, equipo):
        """Obtiene los colores del equipo o devuelve colores por defecto"""
        # Buscar coincidencia exacta primero
        if equipo in self.team_colors:
            return self.team_colors[equipo]
        
        # Buscar coincidencia parcial (por si hay variaciones en el nombre)
        for team_name in self.team_colors.keys():
            if team_name.lower() in equipo.lower() or equipo.lower() in team_name.lower():
                return self.team_colors[team_name]
        
        # Si no encuentra nada, devolver colores por defecto
        print(f"⚠️  Equipo '{equipo}' no reconocido, usando colores por defecto")
        return self.default_team_colors
    
    def group_players_by_demarcation(self, filtered_df):
        """Agrupa jugadores por demarcación con combinaciones especiales"""
        # Verificar si Alias está vacío y usar Nombre en su lugar
        if 'Nombre' in filtered_df.columns:
            mask_empty_alias = filtered_df['Alias'].isna() | (filtered_df['Alias'] == '') | (filtered_df['Alias'].str.strip() == '')
            filtered_df.loc[mask_empty_alias, 'Alias'] = filtered_df.loc[mask_empty_alias, 'Nombre']
        
        # 🔥 AGRUPAMIENTOS COMBINADOS ESPECIALES
        grouped_by_demarcation = {}
        
        # Grupo 1: MEDIOCAMPISTAS OFENSIVOS (Box to Box + Mediapunta)
        mc_box_players = filtered_df[filtered_df['Demarcacion'] == 'Centrocampista - MC Box to Box']
        mediapunta_players = filtered_df[filtered_df['Demarcacion'] == 'Centrocampista de ataque - Mediapunta']
        
        if len(mc_box_players) > 0 or len(mediapunta_players) > 0:
            combined_mc = pd.concat([mc_box_players, mediapunta_players], ignore_index=True)
            combined_mc_sorted = combined_mc.sort_values('Distancia Total', ascending=False)
            grouped_by_demarcation['MC Box to Box'] = combined_mc_sorted.to_dict('records')
        
        # Grupo 2: DELANTEROS (Delantero Centro + Segundo Delantero)
        delantero_centro_players = filtered_df[filtered_df['Demarcacion'] == 'Delantero - Delantero Centro']
        segundo_delantero_players = filtered_df[filtered_df['Demarcacion'] == 'Delantero - Segundo Delantero']
        
        if len(delantero_centro_players) > 0 or len(segundo_delantero_players) > 0:
            combined_delanteros = pd.concat([delantero_centro_players, segundo_delantero_players], ignore_index=True)
            combined_delanteros_sorted = combined_delanteros.sort_values('Distancia Total', ascending=False)
            grouped_by_demarcation['Delantero Centro'] = combined_delanteros_sorted.to_dict('records')
        
        # Grupo 3: MC ORGANIZADOR (MC Organizador + Sin Posición)
        mc_organizador_players = filtered_df[filtered_df['Demarcacion'] == 'Centrocampista - MC Organizador']
        sin_posicion_players = filtered_df[filtered_df['Demarcacion'] == 'Sin Posición']

        if len(mc_organizador_players) > 0 or len(sin_posicion_players) > 0:
            combined_organizador = pd.concat([mc_organizador_players, sin_posicion_players], ignore_index=True)
            combined_organizador_sorted = combined_organizador.sort_values('Distancia Total', ascending=False)
            grouped_by_demarcation['MC Organizador'] = combined_organizador_sorted.to_dict('records')
        
        # Grupo 4: RESTO DE DEMARCACIONES (individuales)
        demarcaciones_combinadas = [
            'Centrocampista - MC Box to Box',
            'Centrocampista de ataque - Mediapunta', 
            'Delantero - Delantero Centro',
            'Delantero - Segundo Delantero',
            'Centrocampista - MC Organizador',
            'Sin Posición'
        ]
        
        for demarcacion in filtered_df['Demarcacion'].unique():
            if pd.notna(demarcacion) and demarcacion.strip() != '' and demarcacion not in demarcaciones_combinadas:
                players = filtered_df[filtered_df['Demarcacion'] == demarcacion]
                # Ordenar por Distancia Total (descendente)
                players_sorted = players.sort_values('Distancia Total', ascending=False)
                # Limpiar nombre de demarcación
                clean_name = demarcacion.replace('Defensa - ', '').replace('Centrocampista - ', '').replace('Centrocampista de ataque - ', '').replace('Delantero - ', '')
                grouped_by_demarcation[clean_name] = players_sorted.to_dict('records')
        
        return grouped_by_demarcation
    
    def create_campo_sin_espacios(self, figsize=(24, 16)):
        """Crea el campo que ocupe TODA la página sin espacios"""
        print("🎯 Creando campo SIN espacios...")
        
        # Crear pitch sin padding
        pitch = Pitch(
            pitch_color='grass', 
            line_color='white', 
            stripe=True, 
            linewidth=3,
            pad_left=0, pad_right=0, pad_bottom=0, pad_top=0
        )
        
        # Crear figura sin layouts automáticos
        fig, ax = pitch.draw(
            figsize=figsize,
            tight_layout=False,
            constrained_layout=False
        )
        
        # ✅ CONFIGURACIÓN AGRESIVA PARA ELIMINAR TODOS LOS ESPACIOS
        fig.subplots_adjust(left=0, right=1, top=1, bottom=0, wspace=0, hspace=0)
        ax.set_position([0, 0, 1, 1])
        ax.margins(0, 0)
        ax.set_xlim(0, 120)
        ax.set_ylim(0, 80)
        ax.autoscale(enable=False)
        ax.set_aspect('equal')
        fig.patch.set_visible(False)
        ax.set_frame_on(False)
        
        return fig, ax
    
    def create_position_graph(self, players_list, demarcacion, x, y, ax, team_colors, team_logo=None):
        """🔥 NUEVA FUNCIÓN: Crea un gráfico de barras para cada demarcación"""
        if not players_list or len(players_list) < 1:
            return
        
        print(f"🎯 Creando gráfico de barras para {demarcacion} con {len(players_list)} jugadores")
        
        # Dimensiones del gráfico
        graph_width = 18
        graph_height = 12
        
        # Fondo del gráfico
        graph_rect = plt.Rectangle((x - graph_width/2, y - graph_height/2), 
                                 graph_width, graph_height,
                                 facecolor='#2c3e50', alpha=0.95, 
                                 edgecolor='white', linewidth=2)
        ax.add_patch(graph_rect)
        
        # Título del gráfico
        if demarcacion == 'MC Box to Box':
            titulo_grafico = 'MC BOX TO BOX + MEDIAPUNTA'
        elif demarcacion == 'Delantero Centro':
            titulo_grafico = 'DELANTERO CENTRO + 2º DELANTERO'
        else:
            titulo_grafico = demarcacion.upper()
        
        # Ajustar tamaño de fuente según longitud
        if len(titulo_grafico) > 20:
            font_size = 7
        elif len(titulo_grafico) > 15:
            font_size = 8
        else:
            font_size = 9

        ax.text(x, y + graph_height/2 - 1.5, titulo_grafico, 
                fontsize=font_size, weight='bold', color='white',
                ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.3", facecolor=team_colors['primary'], alpha=0.8))
        
        # 🏆 AÑADIR ESCUDO en la esquina superior izquierda
        if team_logo is not None:
            try:
                logo_x = x - graph_width/2 + 1.5
                logo_y = y + graph_height/2 - 1.5
                zoom_factor = 0.03
                
                imagebox = OffsetImage(team_logo, zoom=zoom_factor)
                ab = AnnotationBbox(imagebox, (logo_x, logo_y), 
                                frameon=False, 
                                boxcoords='data')
                ax.add_artist(ab)
            except Exception as e:
                print(f"⚠️  Error al añadir escudo: {e}")
        
        # Preparar datos para el gráfico
        num_players = len(players_list)
        if num_players < 1:
            return
        
        # Crear posiciones X para los jugadores (ya ordenados por Distancia Total descendente)
        if num_players == 1:
            x_positions = [x]
        else:
            x_positions = np.linspace(x - graph_width/2 + 3, x + graph_width/2 - 3, num_players)
        
        # Calcular rangos para normalización Y de las barras
        y_base = y - graph_height/2 + 2
        y_max_bars = y + graph_height/2 - 5
        
        # 🔥 CALCULAR RANGO DE VMAX PARA NORMALIZACIÓN
        vmax_values = [player.get(self.metrica_punto_rojo, 0) for player in players_list]
        min_vmax = min(vmax_values) if vmax_values else 0
        max_vmax = max(vmax_values) if vmax_values else 1
        vmax_range = max_vmax - min_vmax if max_vmax > min_vmax else 1

        # Rango de alturas para puntos rojos
        punto_y_min = y_max_bars + 1
        punto_y_max = y_max_bars + 2
        
        # Recopilar todos los valores para normalización
        all_bar_values = []
        players_bar_data = []
        
        for player in players_list:
            player_values = []
            for metric in self.metricas_barras:
                value = player.get(metric, 0)
                player_values.append(value)
                all_bar_values.append(value)
            players_bar_data.append(player_values)
        
        # Encontrar valor máximo para normalización
        max_bar_value = max(all_bar_values) if all_bar_values else 1
        if max_bar_value == 0:
            max_bar_value = 1
        
        # Ancho de las barras
        bar_width = 1.2
        bar_spacing = 0.3
        total_bar_width = len(self.metricas_barras) * bar_width + (len(self.metricas_barras) - 1) * bar_spacing
        
        # Lista para almacenar posiciones de puntos rojos
        puntos_rojos_x = []
        puntos_rojos_y = []
        
        # 🔥 CREAR BARRAS PARA CADA JUGADOR
        for i, (x_pos, player, player_values) in enumerate(zip(x_positions, players_list, players_bar_data)):
            
            # Posición inicial para las barras de este jugador
            start_x = x_pos - total_bar_width/2
            
            # Crear las 3 barras para cada métrica
            for j, (metric, value, color) in enumerate(zip(self.metricas_barras, player_values, self.colores_barras)):
                bar_x = start_x + j * (bar_width + bar_spacing)
                
                # Altura normalizada de la barra
                bar_height = (value / max_bar_value) * (y_max_bars - y_base)
                
                # Crear la barra
                bar_rect = plt.Rectangle((bar_x, y_base), bar_width, bar_height,
                                       facecolor=color, alpha=0.8, 
                                       edgecolor='white', linewidth=1)
                ax.add_patch(bar_rect)
                
                # Valor encima de cada barra
                ax.text(bar_x + bar_width/2, y_base + bar_height + 0.2, f"{value:.0f}",
                        fontsize=4, color='white', weight='bold',
                        ha='center', va='bottom')
            
            # 🔴 PUNTO ROJO VARIABLE SEGÚN VMAX
            vmax_value = player.get(self.metrica_punto_rojo, 0)
            
            # Altura variable según Vmax
            if vmax_range > 0:
                vmax_normalized = (vmax_value - min_vmax) / vmax_range
            else:
                vmax_normalized = 0.5
            punto_y = punto_y_min + (vmax_normalized * (punto_y_max - punto_y_min))
            
            # Guardar posiciones para líneas discontinuas
            puntos_rojos_x.append(x_pos)
            puntos_rojos_y.append(punto_y)
            
            # Punto rojo
            circle = plt.Circle((x_pos, punto_y), 0.3, color='red', alpha=0.9)
            ax.add_patch(circle)
            
            # Línea roja discontinua conectando
            ax.plot([x_pos, x_pos], [y_max_bars + 0.5, punto_y - 0.3], 
                    color='red', linewidth=2, alpha=0.8, linestyle='--')
            
            # Valor de Vmax en blanco encima del punto
            ax.text(x_pos, punto_y + 0.6, f"{vmax_value:.1f}",
                    fontsize=6, color='white', weight='bold',
                    ha='center', va='bottom')
            
            # Nombre del jugador debajo de las barras (más arriba)
            player_name = player.get('Alias', 'N/A')
            dorsal = player.get('Dorsal', '')
            display_text = f"{dorsal}\n{player_name}"
            
            ax.text(x_pos, y_base - 0.6, display_text,  # Cambiado de -1.5 a -0.8
                    fontsize=7, color='white', weight='bold',
                    ha='center', va='top')
        
        # 🔗 LÍNEAS DISCONTINUAS CONECTANDO PUNTOS ROJOS
        if len(players_list) > 1:
            # Dibujar línea discontinua conectando todos los puntos rojos
            ax.plot(puntos_rojos_x, puntos_rojos_y, 
                    color='red', linewidth=2, alpha=0.6, linestyle='--')
    
    def create_global_legend(self, ax):
        """🔥 LEYENDA GLOBAL para todas las gráficas"""
        legend_x = 95  # Posición X en el campo
        legend_y = 78  # MÁS ARRIBA (era 75)

        # Fondo de la leyenda MÁS PEQUEÑO
        legend_bg = plt.Rectangle((legend_x - 0.5, legend_y - 2), 18, 4,  # Era (22, 6)
                                 facecolor='#2c3e50', alpha=0.95,
                                 edgecolor='white', linewidth=2)
        ax.add_patch(legend_bg)
        
        # Título de la leyenda
        ax.text(legend_x + 8.5, legend_y + 1, 'LEYENDA',
                fontsize=8, weight='bold', color='white',
                ha='center', va='center')
        
        # Colores y métricas de barras
        metric_labels = ['14-21 Km/h', '>21 Km/h', '>24 Km/h']
        
        for i, (label, color) in enumerate(zip(metric_labels, self.colores_barras)):
            rect_x = legend_x + (i * 5.5)
            rect_y = legend_y - 0.5
            
            # Rectángulo de color
            legend_rect = plt.Rectangle((rect_x, rect_y - 0.3), 1, 0.6,
                                      facecolor=color, alpha=0.8,
                                      edgecolor='white', linewidth=0.5)
            ax.add_patch(legend_rect)
            
            # Etiqueta
            ax.text(rect_x + 1.2, rect_y, label,
                    fontsize=5, color='white', weight='bold',
                    ha='left', va='center')
        
        # Punto rojo para Vmax
        circle_x = legend_x + 4
        circle_y = legend_y - 1.5
        circle_legend = plt.Circle((circle_x, circle_y), 0.25, color='red', alpha=0.9)
        ax.add_patch(circle_legend)
        
        # Línea discontinua ejemplo
        ax.plot([circle_x, circle_x], [circle_y - 0.4, circle_y - 0.8], 
                color='red', linewidth=2, linestyle='--', alpha=0.8)
        
        ax.text(circle_x + 1, circle_y, 'Velocidad Máxima',
                fontsize=5, color='white', weight='bold',
                ha='left', va='center')
    
    def create_team_summary_table(self, team_data, ax, x_pos, y_pos, team_name, team_colors, team_logo=None):
        """Crea una tabla de resumen del equipo con métricas específicas de barras"""
        
        # Calcular estadísticas del equipo
        summary_stats = {}
        
        for metric in self.metricas_resumen_equipos:
            if metric in team_data.columns:
                if 'Velocidad Máxima' in metric:
                    summary_stats[metric] = team_data[metric].max()
                else:
                    summary_stats[metric] = team_data[metric].mean()
        
        # Dimensiones de la tabla (2 FILAS)
        num_metrics = len(summary_stats)
        metric_col_width = 8  # Ancho por cada métrica
        table_width = num_metrics * metric_col_width
        row_height = 1.5  # Altura de cada fila
        table_height = row_height * 2  # 2 filas
        
        # 🎨 FONDO MODERNO
        main_rect = plt.Rectangle((x_pos - table_width/2, y_pos - table_height/2), 
                                table_width, table_height,
                                facecolor='#2c3e50', alpha=0.95, 
                                edgecolor='white', linewidth=2)
        ax.add_patch(main_rect)
        
        # Efecto de borde superior
        top_rect = plt.Rectangle((x_pos - table_width/2, y_pos + table_height/2 - 0.3), 
                                table_width, 0.3,
                                facecolor=team_colors['primary'], alpha=0.9,
                                edgecolor='none')
        ax.add_patch(top_rect)
        
        # 📍 FILA 1: NOMBRES DE MÉTRICAS
        metrics_y = y_pos + row_height/2  # Fila superior
        
        for i, (metric, value) in enumerate(summary_stats.items()):
            metric_x = x_pos - table_width/2 + (i * metric_col_width) + metric_col_width/2
            
            # Fondo para cada métrica en fila 1
            metric_rect = plt.Rectangle((metric_x - metric_col_width/2, metrics_y - row_height/2), 
                                    metric_col_width, row_height,
                                    facecolor=team_colors['primary'], alpha=0.6, 
                                    edgecolor='white', linewidth=0.5)
            ax.add_patch(metric_rect)
            
            # Nombre de la métrica (ABREVIADO)
            if '14-21' in metric:
                metric_short = '14-21 Km/h'
            elif '>21' in metric and '>24' not in metric:
                metric_short = '>21 Km/h'
            elif '>24' in metric:
                metric_short = '>24 Km/h'
            elif 'Velocidad Máxima' in metric:
                metric_short = 'Vmax'
            else:
                metric_short = metric.replace('Distancia Total ', '').replace('Distancia Total', 'Distancia')
                
            ax.text(metric_x, metrics_y, metric_short, 
                    fontsize=6, weight='bold', color='white',
                    ha='center', va='center')
        
        # 📍 FILA 2: VALORES DE MÉTRICAS
        values_y = y_pos - row_height/2  # Fila inferior
        
        for i, (metric, value) in enumerate(summary_stats.items()):
            metric_x = x_pos - table_width/2 + (i * metric_col_width) + metric_col_width/2
            
            # Fondo alternado para valores en fila 2
            if i % 2 == 0:
                value_rect = plt.Rectangle((metric_x - metric_col_width/2, values_y - row_height/2), 
                                        metric_col_width, row_height,
                                        facecolor='#3c566e', alpha=0.3, 
                                        edgecolor='none')
                ax.add_patch(value_rect)
            
            # Valor de la métrica
            if 'Velocidad' in metric:
                formatted_value = f"{value:.1f}"
            else:
                formatted_value = f"{value:.0f}"
            
            ax.text(metric_x, values_y, formatted_value, 
                    fontsize=9, weight='bold', color='#FFD700',  # Valores en dorado
                    ha='center', va='center')
        
        # 🔹 LÍNEA SEPARADORA entre filas
        ax.plot([x_pos - table_width/2, x_pos + table_width/2], 
                [y_pos, y_pos], 
                color='white', linewidth=1.5, alpha=0.8)
        
        # Líneas verticales separando columnas
        for i in range(1, num_metrics):
            line_x = x_pos - table_width/2 + (i * metric_col_width)
            ax.plot([line_x, line_x], 
                    [y_pos - table_height/2, y_pos + table_height/2], 
                    color='white', linewidth=0.5, alpha=0.6)
    
    def get_position_for_demarcation(self, demarcacion_display, team_side):
        """Obtiene la posición correcta para una demarcación específica basándose en el mapeo original"""
        
        # 🔥 MAPEO ESPECIAL PARA DEMARCACIONES COMBINADAS
        demarcacion_to_position_map = {
            # Combinadas
            'MC Box to Box': 'MC_BOX_TO_BOX',        # Para mediapunta + box to box
            'Delantero Centro': 'DELANTERO_CENTRO',  # Para delantero centro + segundo delantero
            
            # Individuales (nombres limpios)
            'Portero': 'PORTERO',
            'Central Derecho': 'CENTRAL_DERECHO',
            'Central Izquierdo': 'CENTRAL_IZQUIERDO', 
            'Lateral Derecho': 'LATERAL_DERECHO',
            'Lateral Izquierdo': 'LATERAL_IZQUIERDO',
            'MC Organizador': 'MC_ORGANIZADOR',
            'MC Posicional': 'MC_POSICIONAL',
            'Banda Derecha': 'BANDA_DERECHA',
            'Banda Izquierda': 'BANDA_IZQUIERDA',
        }
        
        # Obtener posición mapeada
        position = demarcacion_to_position_map.get(demarcacion_display, 'MC_BOX_TO_BOX')
        
        # Obtener coordenadas de esa posición
        if position in self.coordenadas_graficos[team_side]:
            return self.coordenadas_graficos[team_side][position]
        else:
            # Si no encuentra la posición, usar una por defecto
            return self.coordenadas_graficos[team_side]['MC_BOX_TO_BOX']
    
    def create_visualization(self, equipo_rival, jornadas, figsize=(24, 16)):
        """Crea la visualización completa con gráficos de barras por demarcación"""
        
        # Crear campo SIN espacios
        fig, ax = self.create_campo_sin_espacios(figsize)
        
        # Título superpuesto en el campo
        ax.text(60, 78, f'DATOS PROMEDIO - ÚLTIMAS {len(jornadas)} JORNADAS | MÍNIMO 60+ MIN', 
                fontsize=14, weight='bold', color='white', ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.8", facecolor='#1e3d59', alpha=0.95,
                         edgecolor='white', linewidth=2))
        
        # Obtener datos acumulados de ambos equipos
        villarreal_data = self.filter_and_accumulate_data('Villarreal CF', jornadas, min_avg_minutes=60)
        rival_data = self.filter_and_accumulate_data(equipo_rival, jornadas, min_avg_minutes=60)
        
        if villarreal_data is None or len(villarreal_data) == 0:
            print("❌ No hay jugadores de Villarreal CF con promedio 60+ minutos")
            return None
            
        if rival_data is None or len(rival_data) == 0:
            print(f"❌ No hay jugadores de {equipo_rival} con promedio 60+ minutos")
            return None
        
        # Cargar escudos
        villarreal_logo = self.load_team_logo('Villarreal CF')
        rival_logo = self.load_team_logo(equipo_rival)
        
        # Posicionar escudos dentro del campo
        if villarreal_logo is not None:
            imagebox = OffsetImage(villarreal_logo, zoom=0.08)
            ab = AnnotationBbox(imagebox, (5, 5), frameon=False)
            ax.add_artist(ab)
        
        if rival_logo is not None:
            imagebox = OffsetImage(rival_logo, zoom=0.08)
            ab = AnnotationBbox(imagebox, (115, 5), frameon=False)
            ax.add_artist(ab)
        
        # 🔥 LEYENDA GLOBAL
        self.create_global_legend(ax)
        
        # 🔥 AGRUPAR POR DEMARCACIÓN REAL (no por posición mappeada)
        print("🔄 Agrupando jugadores por demarcación...")
        villarreal_by_demarcation = self.group_players_by_demarcation(villarreal_data)
        rival_by_demarcation = self.group_players_by_demarcation(rival_data)

        # Obtener colores para cada equipo
        villarreal_colors = self.get_team_colors('Villarreal CF')
        rival_colors = self.get_team_colors(equipo_rival)

        # 🔥 CREAR GRÁFICOS EN LAS POSICIONES CORRECTAS DEL SCRIPT ORIGINAL
        print(f"🔄 Creando {len(villarreal_by_demarcation)} gráficos de barras para Villarreal CF")
        for demarcacion, players in villarreal_by_demarcation.items():
            x, y = self.get_position_for_demarcation(demarcacion, 'villarreal')
            self.create_position_graph(players, demarcacion, x, y, ax, 
                                     villarreal_colors, villarreal_logo)

        print(f"🔄 Creando {len(rival_by_demarcation)} gráficos de barras para {equipo_rival}")
        for demarcacion, players in rival_by_demarcation.items():
            x, y = self.get_position_for_demarcation(demarcacion, 'rival')
            self.create_position_graph(players, demarcacion, x, y, ax, 
                                     rival_colors, rival_logo)
        
        # Resúmenes de equipos con métricas de barras
        self.create_team_summary_table(villarreal_data, ax, 30, 2, 'Villarreal CF', 
                             villarreal_colors, villarreal_logo)
        self.create_team_summary_table(rival_data, ax, 90, 2, equipo_rival, 
                             rival_colors, rival_logo)
        
        return fig
    
    def guardar_sin_espacios(self, fig, filename):
        """Guarda el archivo sin ningún espacio en blanco"""
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

def seleccionar_equipo_jornadas_barras():
    """Permite al usuario seleccionar un equipo rival y jornadas"""
    try:
        report_generator = CampoFutbolBarras()
        equipos = report_generator.get_available_teams()
        
        # Filtrar Villarreal CF de la lista de oponentes
        equipos_rival = [eq for eq in equipos if 'Villarreal' not in eq]
        
        if len(equipos_rival) == 0:
            print("❌ No se encontraron equipos rivales en los datos.")
            return None, None
        
        print("\n=== SELECCIÓN DE EQUIPO RIVAL - GRÁFICOS DE BARRAS POR DEMARCACIÓN ===")
        for i, equipo in enumerate(equipos_rival, 1):
            print(f"{i:2d}. {equipo}")
        
        while True:
            try:
                seleccion = input(f"\nSelecciona un equipo rival (1-{len(equipos_rival)}): ").strip()
                indice = int(seleccion) - 1
                
                if 0 <= indice < len(equipos_rival):
                    equipo_seleccionado = equipos_rival[indice]
                    break
                else:
                    print(f"❌ Por favor, ingresa un número entre 1 y {len(equipos_rival)}")
            except ValueError:
                print("❌ Por favor, ingresa un número válido")
        
        # Obtener jornadas disponibles
        jornadas_disponibles = report_generator.get_available_jornadas()
        print(f"\nJornadas disponibles: {jornadas_disponibles}")
        
        # Preguntar cuántas jornadas incluir
        while True:
            try:
                num_jornadas = input(f"¿Cuántas jornadas incluir? (máximo {len(jornadas_disponibles)}): ").strip()
                num_jornadas = int(num_jornadas)
                
                if 1 <= num_jornadas <= len(jornadas_disponibles):
                    jornadas_seleccionadas = sorted(jornadas_disponibles)[-num_jornadas:]
                    break
                else:
                    print(f"❌ Por favor, ingresa un número entre 1 y {len(jornadas_disponibles)}")
            except ValueError:
                print("❌ Por favor, ingresa un número válido")
        
        return equipo_seleccionado, jornadas_seleccionadas
        
    except Exception as e:
        print(f"❌ Error en la selección: {e}")
        return None, None

def main_campo_futbol_barras():
    """Función principal para generar el informe con gráficos de barras por demarcación"""
    try:
        print("🏟️ === GENERADOR DE INFORMES CON GRÁFICOS DE BARRAS ===")
        
        # Selección interactiva
        equipo_rival, jornadas = seleccionar_equipo_jornadas_barras()
        
        if equipo_rival is None or jornadas is None:
            print("❌ No se pudo completar la selección.")
            return
        
        print(f"\n🔄 Generando reporte CON GRÁFICOS DE BARRAS para Villarreal CF vs {equipo_rival}")
        print(f"📅 Jornadas: {jornadas}")
        print(f"🔥 Características:")
        print(f"   • Mínimo 60 minutos (en lugar de 70)")
        print(f"   • Gráfico de barras por cada demarcación")
        print(f"   • Jugadores ordenados por Distancia Total (mayor a menor)")
        print(f"   • 3 barras por jugador: 14-21 Km/h, >21 Km/h, >24 Km/h")
        print(f"   • Punto rojo con Vmax variable en altura")
        print(f"   • Líneas discontinuas conectando puntos rojos")
        print(f"   • Leyenda global en esquina superior derecha")
        print(f"   • COMBINADAS: Mediapunta + Box to Box | Delantero Centro + 2º Delantero | MC Organizador + Sin Posición")
        
        # Crear el reporte
        report_generator = CampoFutbolBarras()
        fig = report_generator.create_visualization(equipo_rival, jornadas)
        
        if fig:
            # Mostrar en pantalla
            plt.show()
            
            # Guardar
            equipo_filename = equipo_rival.replace(' ', '_').replace('/', '_')
            output_path = f"reporte_BARRAS_Villarreal_vs_{equipo_filename}.pdf"
            
            report_generator.guardar_sin_espacios(fig, output_path)
            
        else:
            print("❌ No se pudo generar la visualización")
            
    except Exception as e:
        print(f"❌ Error en la ejecución: {e}")
        import traceback
        traceback.print_exc()

def generar_reporte_barras_personalizado(equipo_rival, jornadas, mostrar=True, guardar=True):
    """Función para generar un reporte personalizado con gráficos de barras"""
    try:
        report_generator = CampoFutbolBarras()
        fig = report_generator.create_visualization(equipo_rival, jornadas)
        
        if fig:
            if mostrar:
                plt.show()
            
            if guardar:
                equipo_filename = equipo_rival.replace(' ', '_').replace('/', '_')
                output_path = f"reporte_BARRAS_Villarreal_vs_{equipo_filename}.pdf"
                report_generator.guardar_sin_espacios(fig, output_path)
            
            return fig
        else:
            print("❌ No se pudo generar la visualización")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Inicialización
print("🏟️ === INICIALIZANDO GENERADOR CON GRÁFICOS DE BARRAS MEJORADO ===")
try:
    report_generator = CampoFutbolBarras()
    equipos = report_generator.get_available_teams()
    print(f"\n✅ Sistema CON GRÁFICOS DE BARRAS MEJORADO listo. Equipos disponibles: {len(equipos)}")
    
    if len(equipos) > 0:
        print("📝 Para generar un reporte CON GRÁFICOS DE BARRAS ejecuta: main_campo_futbol_barras()")
        print("📝 Para uso directo: generar_reporte_barras_personalizado('Equipo_Rival', [33,34,35])")
        print("\n🔥 CARACTERÍSTICAS MEJORADAS:")
        print("   • Mínimo 60 minutos (en lugar de 70)")
        print("   • GRÁFICO DE BARRAS por cada demarcación")
        print("   • Jugadores ordenados por Distancia Total (mayor → menor)")
        print("   • 3 barras por jugador: 14-21 Km/h, >21 Km/h, >24 Km/h")
        print("   • Puntos rojos con altura variable según Velocidad Máxima")
        print("   • Líneas discontinuas conectando puntos rojos de jugadores")
        print("   • Leyenda global única en esquina superior derecha")
        print("   • Nombres de jugadores más altos")
        print("   • DEMARCACIONES COMBINADAS:")
        print("     - MC Box to Box + Mediapunta = Mismo gráfico")
        print("     - Delantero Centro + Segundo Delantero = Mismo gráfico")
        print("     - MC Organizador + Sin Posición = Mismo gráfico")
    
except Exception as e:
    print(f"❌ Error al inicializar: {e}")

if __name__ == "__main__":
    main_campo_futbol_barras()