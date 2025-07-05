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

class Posible11Inicial:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        """
        Inicializa la clase para generar el posible 11 inicial
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

        # Mapeo de demarcaciones a posiciones específicas
        self.demarcacion_to_position = {
            # Portero
            'Portero': 'PORTERO',
            
            # Defensas
            'Defensa - Central Derecho': 'CENTRAL_DERECHO',
            'Defensa - Lateral Derecho': 'LATERAL_DERECHO', 
            'Defensa - Central Izquierdo': 'CENTRAL_IZQUIERDO',
            'Defensa - Lateral Izquierdo': 'LATERAL_IZQUIERDO',
            
            # Mediocampo
            'Centrocampista - MC Box to Box': 'MC_BOX_TO_BOX',
            'Centrocampista - MC Organizador': 'MC_ORGANIZADOR',
            'Centrocampista - MC Posicional': 'MC_POSICIONAL',
            'Centrocampista de ataque - Banda Derecha': 'BANDA_DERECHA',
            'Centrocampista de ataque - Banda Izquierda': 'BANDA_IZQUIERDA',
            'Centrocampista de ataque - Mediapunta': 'MEDIAPUNTA',

            # Delanteros
            'Delantero - Delantero Centro': 'DELANTERO_CENTRO',
            'Delantero - Segundo Delantero': 'SEGUNDO_DELANTERO',
            
            # Sin posición
            'Sin Posición': 'MC_POSICIONAL',
        }
        
        # Coordenadas para posicionar las tablas en el campo (formación 4-3-3)
        self.coordenadas_posiciones = {
            'PORTERO': (10, 40),
            'LATERAL_DERECHO': (40, 15),
            'CENTRAL_DERECHO': (25, 20),
            'CENTRAL_IZQUIERDO': (25, 60),
            'LATERAL_IZQUIERDO': (40, 65),
            'MC_POSICIONAL': (45, 40),
            'MC_BOX_TO_BOX': (65, 25),
            'MC_ORGANIZADOR': (45, 55),
            'BANDA_DERECHA': (90, 15),
            'BANDA_IZQUIERDA': (90, 65),
            'DELANTERO_CENTRO': (95, 40),
        }
        
        # Métricas a mostrar en las tablas (versiones abreviadas)
        self.metricas_mostrar = [
            'Dist. Total',
            'Dist./min',
            '14-21 km/h',
            '14-21/min',
            '>21 km/h',
            '>21/min',
            '>24 km/h', 
            '>24/min',
            'V.Max'
        ]
        
        # Mapeo de métricas abreviadas a nombres completos en el dataset
        self.metricas_mapping = {
            'Dist. Total': 'Distancia Total',
            'Dist./min': 'Distancia Total / min',
            '14-21 km/h': 'Distancia Total 14-21 km / h',
            '14-21/min': 'Distancia Total 14-21 km / h / min',
            '>21 km/h': 'Distancia Total >21 km / h',
            '>21/min': 'Distancia Total >21 km / h / min',
            '>24 km/h': 'Distancia Total >24 km / h',
            '>24/min': 'Distancia Total >24 km / h / min',
            'V.Max': 'Velocidad Máxima Total'
        }

    def check_collision(self, x1, y1, width1, height1, x2, y2, width2, height2, margin=2):
        """Verifica si dos rectángulos se solapan con un margen de separación"""
        return not (x1 + width1/2 + margin < x2 - width2/2 or 
                    x1 - width1/2 - margin > x2 + width2/2 or 
                    y1 + height1/2 + margin < y2 - height2/2 or 
                    y1 - height1/2 - margin > y2 + height2/2)

    def get_fixed_areas(self):
        """Define las áreas ocupadas por título y escudo (inamovibles)"""
        return [
            {'x': 60, 'y': 75, 'width': 40, 'height': 4, 'name': 'titulo_principal'},
            {'x': 60, 'y': 72, 'width': 35, 'height': 3, 'name': 'titulo_secundario'},
            {'x': 20, 'y': 70, 'width': 8, 'height': 8, 'name': 'escudo'},  # Ajustar según dónde pongas el escudo
            {'x': 60, 'y': 2, 'width': 50, 'height': 4, 'name': 'tabla_resumen'}  # Tabla resumen
        ]

    def reposition_tables(self, posible_11, table_width=16, table_height=20):
        """Reposiciona las tablas para evitar solapamientos"""
        fixed_areas = self.get_fixed_areas()
        positioned_tables = []
        new_positions = {}
        
        for posicion, player_data in posible_11.items():
            if posicion in self.coordenadas_posiciones:
                original_x, original_y = self.coordenadas_posiciones[posicion]
                
                # Buscar la mejor posición cerca de la original
                best_x, best_y = self.find_best_position(
                    original_x, original_y, table_width, table_height,
                    fixed_areas + positioned_tables
                )
                
                new_positions[posicion] = (best_x, best_y)
                positioned_tables.append({
                    'x': best_x, 'y': best_y, 
                    'width': table_width, 'height': table_height,
                    'name': posicion
                })
        
        return new_positions
    
    def find_best_position(self, original_x, original_y, width, height, occupied_areas):
        """Encuentra la mejor posición cerca del punto original"""
        max_distance = 15  # Máxima distancia de búsqueda
        step = 1  # Paso de búsqueda
        
        for distance in range(0, max_distance, step):
            # Buscar en círculos concéntricos alrededor del punto original
            for angle in range(0, 360, 30):  # Cada 30 grados
                x = original_x + distance * np.cos(np.radians(angle))
                y = original_y + distance * np.sin(np.radians(angle))
                
                # Verificar límites del campo
                if not (5 <= x <= 115 and 5 <= y <= 75):
                    continue
                
                # Verificar colisiones
                collision = False
                for area in occupied_areas:
                    if self.check_collision(x, y, width, height, 
                                        area['x'], area['y'], area['width'], area['height']):
                        collision = True
                        break
                
                if not collision:
                    return x, y
        
        # Si no encuentra posición, devolver la original
        return original_x, original_y

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

    def get_last_5_jornadas(self, equipo, jornada_referencia):
        """Obtiene las últimas 5 jornadas incluyendo la de referencia"""
        jornadas_disponibles = self.get_available_jornadas(equipo)
        
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

    def get_posible_11(self, equipo, jornada):
        """Obtiene el posible 11 inicial basado en minutos jugados en las últimas 5 jornadas"""
        
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
            
            # Acumular estadísticas
            for metric_short, metric_full in self.metricas_mapping.items():
                if metric_full in row and pd.notna(row[metric_full]):
                    if metric_short not in player_stats[jugador_id]['stats']:
                        player_stats[jugador_id]['stats'][metric_short] = []
                    player_stats[jugador_id]['stats'][metric_short].append(row[metric_full])
        
        # Calcular promedios/sumas/máximos según la métrica
        for jugador_id in player_stats:
            for metric_short in self.metricas_mostrar:
                if metric_short in player_stats[jugador_id]['stats']:
                    values = player_stats[jugador_id]['stats'][metric_short]
                    
                    if metric_short in ['V.Max']:  # Velocidad máxima: promedio
                        player_stats[jugador_id]['stats'][metric_short] = np.mean(values)
                    elif '/min' in metric_short:  # Métricas por minuto: promedio
                        player_stats[jugador_id]['stats'][metric_short] = np.mean(values)
                    else:  # Distancias totales: suma
                        player_stats[jugador_id]['stats'][metric_short] = np.sum(values)
                else:
                    player_stats[jugador_id]['stats'][metric_short] = 0
        
        # Seleccionar el mejor jugador por posición basándose en minutos jugados
        posible_11 = {}
        
        # Agrupar jugadores por posición
        jugadores_por_posicion = {}
        for jugador_id, data in player_stats.items():
            demarcacion = data['Demarcacion']
            posicion = self.demarcacion_to_position.get(demarcacion, 'MC_POSICIONAL')
            
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
        
        # Seleccionar el jugador con más minutos por posición
        posiciones_objetivo = [
            'PORTERO', 'LATERAL_DERECHO', 'CENTRAL_DERECHO', 'CENTRAL_IZQUIERDO', 
            'LATERAL_IZQUIERDO', 'MC_POSICIONAL', 'MC_BOX_TO_BOX', 'MC_ORGANIZADOR',
            'BANDA_DERECHA', 'BANDA_IZQUIERDA', 'DELANTERO_CENTRO'
        ]
        
        for posicion in posiciones_objetivo:
            if posicion in jugadores_por_posicion:
                # Ordenar por minutos jugados (descendente)
                jugadores_posicion = sorted(
                    jugadores_por_posicion[posicion], 
                    key=lambda x: x['minutos_total'], 
                    reverse=True
                )
                
                # Tomar el que más minutos ha jugado
                posible_11[posicion] = jugadores_posicion[0]
                print(f"✅ {posicion}: {jugadores_posicion[0]['Alias']} ({jugadores_posicion[0]['minutos_total']} min)")
        
        return posible_11

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
        
        # Buscar coincidencia parcial
        for team_name in self.team_colors.keys():
            if team_name.lower() in equipo.lower() or equipo.lower() in team_name.lower():
                return self.team_colors[team_name]
        
        # Si no encuentra nada, devolver colores por defecto
        print(f"⚠️  Equipo '{equipo}' no reconocido, usando colores por defecto")
        return self.default_team_colors

    def create_campo_sin_espacios(self, figsize=(20, 14)):
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

    def create_player_table(self, player_data, x, y, ax, team_colors, position_name, team_logo=None):
        """Crea una tabla individual para un jugador con sus estadísticas"""
        
        # Dimensiones de la tabla compacta
        table_width = 16
        header_height = 2.5
        name_height = 3.0
        metric_row_height = 1.8
        table_height = header_height + name_height + (len(self.metricas_mostrar) * metric_row_height)
        
        # 🎨 FONDO MODERNO
        main_rect = plt.Rectangle((x - table_width/2, y - table_height/2), 
                                table_width, table_height,
                                facecolor='#2c3e50', alpha=0.95, 
                                edgecolor='white', linewidth=2)
        ax.add_patch(main_rect)
        
        # Efecto de borde superior
        top_rect = plt.Rectangle((x - table_width/2, y + table_height/2 - 0.4), 
                                table_width, 0.4,
                                facecolor=team_colors['primary'], alpha=0.9,
                                edgecolor='none')
        ax.add_patch(top_rect)
        
        # 📍 FILA 1: POSICIÓN
        clean_position_name = position_name.replace('_', ' ').replace('Mc ', 'MC ')
        header_rect = plt.Rectangle((x - table_width/2, y + table_height/2 - header_height), 
                                table_width, header_height,
                                facecolor=team_colors['primary'], alpha=0.8,
                                edgecolor='white', linewidth=1)
        ax.add_patch(header_rect)

        ax.text(x, y + table_height/2 - header_height/2, clean_position_name, 
                fontsize=10, weight='bold', color=team_colors['text'],
                ha='center', va='center')
        
        # 📍 FILA 2: NOMBRE + DORSAL
        names_y = y + table_height/2 - header_height - name_height/2
        
        names_rect = plt.Rectangle((x - table_width/2, names_y - name_height/2), 
                                table_width, name_height,
                                facecolor='#34495e', alpha=0.7, 
                                edgecolor='white', linewidth=0.5)
        ax.add_patch(names_rect)
        
        # Dorsal (más grande y destacado)
        ax.text(x, names_y + 0.4, str(player_data['Dorsal']), 
                fontsize=18, weight='bold', color=team_colors['primary'],
                ha='center', va='center')
        
        # Nombre del jugador
        ax.text(x, names_y - 0.4, player_data['Alias'], 
                fontsize=12, weight='bold', color='white',
                ha='center', va='center')
        
        # 📍 FILAS 3+: MÉTRICAS Y VALORES
        for i, metric_short in enumerate(self.metricas_mostrar):
            metric_y = names_y - name_height/2 - (i + 1) * metric_row_height + metric_row_height/2
            
            # Fondo alternado para las filas de métricas
            if i % 2 == 0:
                row_rect = plt.Rectangle((x - table_width/2, metric_y - metric_row_height/2), 
                                    table_width, metric_row_height,
                                    facecolor='#3c566e', alpha=0.3, 
                                    edgecolor='none')
                ax.add_patch(row_rect)
            
            # Valor de la métrica
            value = player_data['stats'].get(metric_short, 0)
            
            if 'V.Max' in metric_short or '/min' in metric_short:
                formatted_value = f"{value:.1f}"
            else:
                formatted_value = f"{value:.0f}"
            
            # Métrica y valor en la misma fila
            ax.text(x - table_width/4, metric_y, metric_short, 
                    fontsize=12, weight='bold', color='white',
                    ha='center', va='center')
            
            ax.text(x + table_width/4, metric_y, formatted_value, 
                    fontsize=12, weight='bold', color='#FFD700',
                    ha='center', va='center')

    def create_team_summary_table(self, posible_11, ax, x_pos, y_pos, team_name, team_colors, team_logo=None):
        """Crea una tabla de resumen del equipo con promedios del posible 11"""
        
        # Calcular estadísticas promedio del posible 11
        summary_stats = {}
        
        for metric_short in self.metricas_mostrar:
            values = []
            for posicion, player in posible_11.items():
                if metric_short in player['stats']:
                    values.append(player['stats'][metric_short])
            
            if values:
                if metric_short in ['V.Max']:  # Velocidad máxima: máximo
                    summary_stats[metric_short] = max(values)
                else:  # Resto: promedio
                    summary_stats[metric_short] = np.mean(values)
            else:
                summary_stats[metric_short] = 0
        
        # Dimensiones de la tabla (2 FILAS)
        num_metrics = len(summary_stats)
        metric_col_width = 7
        table_width = num_metrics * metric_col_width
        row_height = 1.2
        table_height = row_height * 2
        
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
        metrics_y = y_pos + row_height/2
        
        for i, (metric_short, value) in enumerate(summary_stats.items()):
            metric_x = x_pos - table_width/2 + (i * metric_col_width) + metric_col_width/2
            
            # Fondo para cada métrica en fila 1
            metric_rect = plt.Rectangle((metric_x - metric_col_width/2, metrics_y - row_height/2), 
                                    metric_col_width, row_height,
                                    facecolor=team_colors['primary'], alpha=0.6, 
                                    edgecolor='white', linewidth=0.5)
            ax.add_patch(metric_rect)
            
            # Nombre de la métrica
            ax.text(metric_x, metrics_y, metric_short, 
                    fontsize=5, weight='bold', color='white',
                    ha='center', va='center')
        
        # 📍 FILA 2: VALORES DE MÉTRICAS
        values_y = y_pos - row_height/2
        
        for i, (metric_short, value) in enumerate(summary_stats.items()):
            metric_x = x_pos - table_width/2 + (i * metric_col_width) + metric_col_width/2
            
            # Fondo alternado para valores en fila 2
            if i % 2 == 0:
                value_rect = plt.Rectangle((metric_x - metric_col_width/2, values_y - row_height/2), 
                                        metric_col_width, row_height,
                                        facecolor='#3c566e', alpha=0.3, 
                                        edgecolor='none')
                ax.add_patch(value_rect)
            
            # Valor de la métrica
            if 'V.Max' in metric_short or '/min' in metric_short:
                formatted_value = f"{value:.1f}"
            else:
                formatted_value = f"{value:.0f}"
            
            ax.text(metric_x, values_y, formatted_value, 
                    fontsize=7, weight='bold', color='#FFD700',
                    ha='center', va='center')

    def create_visualization(self, equipo, jornada, figsize=(20, 14)):
        """Crea la visualización completa del posible 11 inicial"""
        
        # Obtener el posible 11
        posible_11 = self.get_posible_11(equipo, jornada)
        
        if not posible_11:
            print("❌ No se pudo generar el posible 11 inicial")
            return None
        
        # Crear campo SIN espacios
        fig, ax = self.create_campo_sin_espacios(figsize)
        
        # Obtener las jornadas analizadas para el título
        jornadas_analizadas = self.get_last_5_jornadas(equipo, jornada)
        
        # Título superpuesto en el campo
        # Título principal - moverlo arriba del campo
        ax.text(60, 85, f'POSIBLE 11 INICIAL - {equipo.upper()}',   # Era 75, ahora 85
                fontsize=16, weight='bold', color='white', ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.8", facecolor='#1e3d59', alpha=1.0,
                        edgecolor='white', linewidth=2))

        # Título secundario - también más arriba
        ax.text(60, 81, f'Basado en minutos jugados | Jornadas: {min(jornadas_analizadas)} - {max(jornadas_analizadas)}',  # Era 72, ahora 81
                fontsize=10, weight='bold', color='white', ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.5", facecolor='#34495e', alpha=0.98,
                        edgecolor='white', linewidth=1))
        
        # Cargar escudo
        team_logo = self.load_team_logo(equipo)
        
        # Posicionar escudo
        if team_logo is not None:
            imagebox = OffsetImage(team_logo, zoom=0.12)
            ab = AnnotationBbox(imagebox, (110, 70), frameon=False)  # Era (60, 5)
            ax.add_artist(ab)
        
        # Obtener colores del equipo
        team_colors = self.get_team_colors(equipo)
        
        # ✅ NUEVO: Calcular posiciones optimizadas para evitar solapamientos
        new_positions = self.reposition_tables(posible_11, table_width=16, table_height=20)
        
        # Crear tablas para cada jugador en las nuevas posiciones
        for posicion, player_data in posible_11.items():
            if posicion in new_positions:
                x, y = new_positions[posicion]
                self.create_player_table(player_data, x, y, ax, team_colors, posicion, team_logo)
        
        # Crear tabla resumen del equipo
        self.create_team_summary_table(posible_11, ax, 60, 2, equipo, team_colors, team_logo)
        
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

def seleccionar_equipo_jornada():
    """Permite al usuario seleccionar un equipo y jornada"""
    try:
        report_generator = Posible11Inicial()
        equipos = report_generator.get_available_teams()
        
        if len(equipos) == 0:
            print("❌ No se encontraron equipos en los datos.")
            return None, None
        
        print("\n=== SELECCIÓN DE EQUIPO - POSIBLE 11 INICIAL ===")
        for i, equipo in enumerate(equipos, 1):
            print(f"{i:2d}. {equipo}")
        
        while True:
            try:
                seleccion = input(f"\nSelecciona un equipo (1-{len(equipos)}): ").strip()
                indice = int(seleccion) - 1
                
                if 0 <= indice < len(equipos):
                    equipo_seleccionado = equipos[indice]
                    break
                else:
                    print(f"❌ Por favor, ingresa un número entre 1 y {len(equipos)}")
            except ValueError:
                print("❌ Por favor, ingresa un número válido")
        
        # Obtener jornadas disponibles para el equipo
        jornadas_disponibles = report_generator.get_available_jornadas(equipo_seleccionado)
        print(f"\nJornadas disponibles para {equipo_seleccionado}: {jornadas_disponibles}")
        
        # Seleccionar jornada de referencia
        while True:
            try:
                jornada_input = input(f"Selecciona la jornada de referencia (ej: {max(jornadas_disponibles)}): ").strip()
                
                # Intentar convertir a entero
                if jornada_input.startswith('J') or jornada_input.startswith('j'):
                    jornada_seleccionada = int(jornada_input[1:])
                else:
                    jornada_seleccionada = int(jornada_input)
                
                if jornada_seleccionada in jornadas_disponibles:
                    break
                else:
                    print(f"❌ Jornada {jornada_seleccionada} no disponible. Disponibles: {jornadas_disponibles}")
            except ValueError:
                print("❌ Por favor, ingresa una jornada válida")
        
        return equipo_seleccionado, jornada_seleccionada
        
    except Exception as e:
        print(f"❌ Error en la selección: {e}")
        return None, None

def main_posible_11():
    """Función principal para generar el posible 11 inicial"""
    try:
        print("⚽ === GENERADOR POSIBLE 11 INICIAL ===")
        
        # Selección interactiva
        equipo, jornada = seleccionar_equipo_jornada()
        
        if equipo is None or jornada is None:
            print("❌ No se pudo completar la selección.")
            return
        
        print(f"\n🔄 Generando posible 11 inicial para {equipo}")
        print(f"📅 Jornada de referencia: {jornada}")
        
        # Crear el reporte
        report_generator = Posible11Inicial()
        fig = report_generator.create_visualization(equipo, jornada)
        
        if fig:
            # Mostrar en pantalla
            plt.show()
            
            # Guardar
            equipo_filename = equipo.replace(' ', '_').replace('/', '_')
            output_path = f"posible_11_inicial_{equipo_filename}_J{jornada}.pdf"
            
            report_generator.guardar_sin_espacios(fig, output_path)
            
        else:
            print("❌ No se pudo generar la visualización")
            
    except Exception as e:
        print(f"❌ Error en la ejecución: {e}")
        import traceback
        traceback.print_exc()

def generar_posible_11_personalizado(equipo, jornada, mostrar=True, guardar=True):
    """Función para generar un posible 11 personalizado"""
    try:
        report_generator = Posible11Inicial()
        fig = report_generator.create_visualization(equipo, jornada)
        
        if fig:
            if mostrar:
                plt.show()
            
            if guardar:
                equipo_filename = equipo.replace(' ', '_').replace('/', '_')
                output_path = f"posible_11_inicial_{equipo_filename}_J{jornada}.pdf"
                report_generator.guardar_sin_espacios(fig, output_path)
            
            return fig
        else:
            print("❌ No se pudo generar la visualización")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Inicialización
print("⚽ === INICIALIZANDO GENERADOR POSIBLE 11 INICIAL ===")
try:
    report_generator = Posible11Inicial()
    equipos = report_generator.get_available_teams()
    print(f"\n✅ Sistema POSIBLE 11 INICIAL listo. Equipos disponibles: {len(equipos)}")
    
    if len(equipos) > 0:
        print("📝 Para generar un reporte ejecuta: main_posible_11()")
        print("📝 Para uso directo: generar_posible_11_personalizado('Equipo', jornada)")
        print("\n🔥 CARACTERÍSTICAS:")
        print("   • Selecciona al jugador con más minutos por posición")
        print("   • Analiza las últimas 5 jornadas")
        print("   • Métricas físicas completas por jugador")
        print("   • Formación 4-3-3 visual")
        print("   • Colores personalizados por equipo")
    
except Exception as e:
    print(f"❌ Error al inicializar: {e}")

if __name__ == "__main__":
    main_posible_11()