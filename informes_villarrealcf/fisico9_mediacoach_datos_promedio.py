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

class CampoFutbolAcumulado:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        """
        Inicializa la clase para generar informes con tablas completas
        """
        self.data_path = data_path
        self.df = None
        self.load_data()
        self.clean_team_names()
        
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
        }
        
        # Coordenadas específicas para cada posición en el campo
        self.coordenadas_tablas = {
            # Villarreal (lado izquierdo)
            'villarreal': {
                'PORTERO': (3, 40),              # Portería
                'LATERAL_DERECHO': (25, 10),      # Lateral derecho (arriba)
                'CENTRAL_DERECHO': (20, 25),      # Central derecho (centro-arriba)
                'CENTRAL_IZQUIERDO': (20, 53),    # Central izquierdo (centro-abajo)
                'LATERAL_IZQUIERDO': (25, 68),    # Lateral izquierdo (abajo)
                'MC_POSICIONAL': (50, 40),        # Mediocampo defensivo (centro)
                'MC_BOX_TO_BOX': (55, 55),        # Box to box (centro-arriba)
                'MC_ORGANIZADOR': (45, 40),       # Organizador (centro-abajo)
                'BANDA_DERECHA': (70, 10),        # Banda derecha (extremo arriba)
                'BANDA_IZQUIERDA': (70, 68),      # Banda izquierda (extremo abajo)
                'DELANTERO_CENTRO': (85, 55),     # Delantero centro (arriba)
                'SEGUNDO_DELANTERO': (85, 25),    # Segundo delantero (abajo)
            },
            # Equipo rival (lado derecho - espejo)
            'rival': {
                'PORTERO': (115, 40),             # Portería
                'LATERAL_DERECHO': (100, 68),      # Lateral derecho (abajo - espejo)
                'CENTRAL_DERECHO': (105, 53),      # Central derecho (centro-abajo - espejo)
                'CENTRAL_IZQUIERDO': (105, 25),    # Central izquierdo (centro-arriba - espejo)
                'LATERAL_IZQUIERDO': (100, 10),    # Lateral izquierdo (arriba - espejo)
                'MC_POSICIONAL': (70, 40),        # Mediocampo defensivo (centro)
                'MC_BOX_TO_BOX': (60, 25),        # Box to box (centro-abajo - espejo)
                'MC_ORGANIZADOR': (70, 40),       # Organizador (centro-arriba - espejo)
                'BANDA_DERECHA': (45, 68),        # Banda derecha (extremo abajo - espejo)
                'BANDA_IZQUIERDA': (45, 10),      # Banda izquierda (extremo arriba - espejo)
                'DELANTERO_CENTRO': (40, 25),     # Delantero centro (abajo - espejo)
                'SEGUNDO_DELANTERO': (40, 53),    # Segundo delantero (arriba - espejo)
            }
        }
        
        # Métricas principales para mostrar en las tablas
        self.metricas_principales = [
            'Distancia Total',
            'Distancia Total / min',
            'Distancia Total 14-21 km / h',
            'Distancia Total >21 km / h',
            'Velocidad Máxima Total'
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
                else:
                    # Si no hay datos históricos, asignar una demarcación por defecto
                    df_work.loc[idx, 'Demarcacion'] = 'Centrocampista - MC Box to Box'
        
        return df_work
    
    def get_player_position_history(self, jugador_id):
        """Obtiene el historial de posiciones de un jugador"""
        if self.df is None:
            return []
        
        player_positions = self.df[
            (self.df['Id Jugador'] == jugador_id) & 
            (self.df['Demarcacion'].notna()) & 
            (self.df['Demarcacion'] != '') &
            (self.df['Demarcacion'].str.strip() != '')
        ]['Demarcacion'].tolist()
        
        return player_positions
    
    def has_played_position(self, jugador_id, demarcacion):
        """Verifica si un jugador ha jugado en una demarcación específica"""
        history = self.get_player_position_history(jugador_id)
        return demarcacion in history
    
    def filter_and_accumulate_data(self, equipo, jornadas, min_avg_minutes=70):
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
            
            # Calcular promedio de minutos
            avg_minutes = jugador_data['Minutos jugados'].mean()
            
            # Solo incluir jugadores con promedio >= min_avg_minutes
            if avg_minutes >= min_avg_minutes:
                # Tomar datos básicos del jugador (usar el registro más reciente)
                latest_record = jugador_data.iloc[-1]
                
                # Crear registro acumulado
                accumulated_record = {
                    'Id Jugador': latest_record['Id Jugador'],
                    'Dorsal': latest_record['Dorsal'],
                    'Nombre': latest_record['Nombre'],
                    'Alias': latest_record['Alias'],
                    'Demarcacion': jugador_data['Demarcacion'].mode().iloc[0] if len(jugador_data['Demarcacion'].mode()) > 0 else latest_record['Demarcacion'],
                    'Equipo': latest_record['Equipo'],
                    
                    # Minutos: promedio
                    'Minutos jugados': avg_minutes,
                    
                    # Distancias: suma total
                    'Distancia Total': jugador_data['Distancia Total'].sum(),
                    'Distancia Total 14-21 km / h': jugador_data['Distancia Total 14-21 km / h'].sum(),
                    'Distancia Total >21 km / h': jugador_data['Distancia Total >21 km / h'].sum(),
                    
                    # Distancias por minuto: promedio
                    'Distancia Total / min': jugador_data['Distancia Total / min'].mean(),
                    'Distancia Total 14-21 km / h / min': jugador_data.get('Distancia Total 14-21 km / h / min', pd.Series([0])).mean(),
                    'Distancia Total >21 km / h / min': jugador_data.get('Distancia Total >21 km / h / min', pd.Series([0])).mean(),
                    
                    # Velocidades: máximo
                    'Velocidad Máxima Total': jugador_data['Velocidad Máxima Total'].max(),
                    'Velocidad Máxima 1P': jugador_data.get('Velocidad Máxima 1P', pd.Series([0])).max(),
                    'Velocidad Máxima 2P': jugador_data.get('Velocidad Máxima 2P', pd.Series([0])).max(),
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
    
    def group_players_by_specific_position(self, filtered_df):
        """Agrupa jugadores por posiciones específicas con lógica mejorada"""
        # Verificar si Alias está vacío y usar Nombre en su lugar
        if 'Nombre' in filtered_df.columns:
            mask_empty_alias = filtered_df['Alias'].isna() | (filtered_df['Alias'] == '') | (filtered_df['Alias'].str.strip() == '')
            filtered_df.loc[mask_empty_alias, 'Alias'] = filtered_df.loc[mask_empty_alias, 'Nombre']
        
        # Ordenar jugadores por minutos jugados (descendente)
        filtered_df_sorted = filtered_df.sort_values('Minutos jugados', ascending=False)
        
        grouped_players = {
            'PORTERO': [],
            'LATERAL_DERECHO': [],
            'CENTRAL_DERECHO': [],
            'CENTRAL_IZQUIERDO': [],
            'LATERAL_IZQUIERDO': [],
            'MC_POSICIONAL': [],
            'MC_BOX_TO_BOX': [],
            'MC_ORGANIZADOR': [],
            'BANDA_DERECHA': [],
            'BANDA_IZQUIERDA': [],
            'DELANTERO_CENTRO': [],
        }
        
        for _, player in filtered_df_sorted.iterrows():
            demarcacion = player.get('Demarcacion', 'Centrocampista - MC Box to Box')
            position = self.demarcacion_to_position.get(demarcacion, 'MC_BOX_TO_BOX')
            
            # Convertir Series a dict para facilitar el acceso
            player_dict = player.to_dict()
            
            # Agrupar por posiciones específicas
            if position in grouped_players:
                grouped_players[position].append(player_dict)
        
        # 🔥 NUEVA LÓGICA 2: Balancear centrales - SIEMPRE DOS TABLAS
        print("🔄 Balanceando centrales...")
        centrales_derecho = grouped_players['CENTRAL_DERECHO']
        centrales_izquierdo = grouped_players['CENTRAL_IZQUIERDO']
        total_centrales = len(centrales_derecho) + len(centrales_izquierdo)

        if total_centrales > 1:  # Si hay más de 1 central en total
            if len(centrales_izquierdo) == 0:  # No hay centrales izquierdos
                # Buscar si alguno ha jugado de central izquierdo
                moved = False
                for central in centrales_derecho[1:]:  # Dejar al menos 1 en derecho
                    if self.has_played_position(central['Id Jugador'], 'Defensa - Central Izquierdo'):
                        grouped_players['CENTRAL_IZQUIERDO'].append(central)
                        grouped_players['CENTRAL_DERECHO'].remove(central)
                        print(f"   ✅ {central['Alias']} movido a Central Izquierdo (experiencia previa)")
                        moved = True
                        break
                
                # Si no hay experiencia previa, mover el último
                if not moved and len(centrales_derecho) > 1:
                    jugador_a_mover = centrales_derecho.pop()
                    grouped_players['CENTRAL_IZQUIERDO'].append(jugador_a_mover)
                    print(f"   ✅ {jugador_a_mover['Alias']} movido a Central Izquierdo (balanceo)")
            
            elif len(centrales_derecho) == 0:  # No hay centrales derechos
                # Buscar si alguno ha jugado de central derecho
                moved = False
                for central in centrales_izquierdo[1:]:  # Dejar al menos 1 en izquierdo
                    if self.has_played_position(central['Id Jugador'], 'Defensa - Central Derecho'):
                        grouped_players['CENTRAL_DERECHO'].append(central)
                        grouped_players['CENTRAL_IZQUIERDO'].remove(central)
                        print(f"   ✅ {central['Alias']} movido a Central Derecho (experiencia previa)")
                        moved = True
                        break
                
                # Si no hay experiencia previa, mover el último
                if not moved and len(centrales_izquierdo) > 1:
                    jugador_a_mover = centrales_izquierdo.pop()
                    grouped_players['CENTRAL_DERECHO'].append(jugador_a_mover)
                    print(f"   ✅ {jugador_a_mover['Alias']} movido a Central Derecho (balanceo)")
        
        # 🔥 NUEVA LÓGICA 3: Dividir delanteros cuando hay más de 1 columna
        print("🔄 Dividiendo delanteros en dos tablas...")
        delanteros = grouped_players['DELANTERO_CENTRO']

        if len(delanteros) > 1:  # Si hay más de 1 delantero
            # Dividir en dos grupos
            mitad = len(delanteros) // 2 + len(delanteros) % 2  # Redondear hacia arriba
            
            # Primer grupo se queda en DELANTERO_CENTRO
            primer_grupo = delanteros[:mitad]
            # Segundo grupo va a SEGUNDO_DELANTERO
            segundo_grupo = delanteros[mitad:]
            
            # Actualizar los grupos
            grouped_players['DELANTERO_CENTRO'] = primer_grupo
            grouped_players['SEGUNDO_DELANTERO'] = segundo_grupo
            
            print(f"   ✅ Divididos: {len(primer_grupo)} en Delantero Centro, {len(segundo_grupo)} en Segundo Delantero")
        else:
            # Si solo hay 1 delantero, crear grupo vacío para segundo delantero
            grouped_players['SEGUNDO_DELANTERO'] = []

        # Limitar jugadores por posición (máximo 3 por posición para evitar tablas muy anchas)
        for posicion in grouped_players:
            grouped_players[posicion] = grouped_players[posicion][:3]
        
        return grouped_players
    
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
    

    def create_position_table(self, players_list, x, y, ax, team_color, position_name):
        """Crea una tabla moderna con demarcación, nombres+dorsales y métricas en filas"""
        if not players_list:
            return
        
        num_players = len(players_list)
        num_metrics = len(self.metricas_principales)
        
        # Dimensiones de la tabla (REDUCIDAS)
        metric_col_width = 8   # Ancho de la columna de métricas
        player_col_width = 6   # Ancho por columna de jugador
        table_width = metric_col_width + (num_players * player_col_width)
        
        header_height = 2      # Altura del header de demarcación
        names_height = 3       # Altura de la fila de nombres
        metric_row_height = 1.5  # Altura por fila de métrica
        table_height = header_height + names_height + (num_metrics * metric_row_height)
        
        # 🎨 NUEVO FONDO MODERNO - Gradiente simulado con múltiples rectángulos (MÁS COMPACTO)
        # Fondo principal con bordes redondeados simulados
        main_rect = plt.Rectangle((x - table_width/2, y - table_height/2), 
                                table_width, table_height,
                                facecolor='#2c3e50', alpha=0.95, 
                                edgecolor='white', linewidth=2)  # Reducido de 3 a 2
        ax.add_patch(main_rect)
        
        # Efecto de borde superior más claro (MÁS FINO)
        top_rect = plt.Rectangle((x - table_width/2, y + table_height/2 - 0.5), 
                                table_width, 0.5,  # Reducido de 1 a 0.5
                                facecolor=team_color, alpha=0.8, 
                                edgecolor='none')
        ax.add_patch(top_rect)
        
        # 📍 FILA 1: DEMARCACIÓN (TAMAÑO REDUCIDO)
        clean_position_name = position_name.replace('_', ' ').replace('Mc ', 'MC ').replace('Delantero Centro', 'DEL. CENTRO').replace('Segundo Delantero', '2º DELANTERO')
        ax.text(x, y + table_height/2 - header_height/2, clean_position_name, 
                fontsize=8, weight='bold', color='white',  # Reducido de 11 a 8
                ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.3", facecolor=team_color, alpha=0.9,  # Reducido padding
                        edgecolor='white', linewidth=1))
        
        # 📍 FILA 2: NOMBRES + DORSALES
        names_y = y + table_height/2 - header_height - names_height/2
        
        # Fondo especial para la fila de nombres (BORDE MÁS FINO)
        names_rect = plt.Rectangle((x - table_width/2 + metric_col_width, names_y - names_height/2), 
                                num_players * player_col_width, names_height,
                                facecolor='#34495e', alpha=0.7, 
                                edgecolor='white', linewidth=0.5)  # Reducido de 1 a 0.5
        ax.add_patch(names_rect)
        
        # Agregar nombres y dorsales
        for i, player in enumerate(players_list):
            player_x = x - table_width/2 + metric_col_width + (i * player_col_width) + player_col_width/2
            player_name = player['Alias'] if pd.notna(player['Alias']) else 'N/A'
            dorsal = player.get('Dorsal', 'N/A')
            
            # Dorsal SIN círculo - MÁS GRANDE Y EN NEGRITA
            # (Se elimina el círculo completamente)
            
            # Número del dorsal (MÁS GRANDE Y EN NEGRITA)
            ax.text(player_x, names_y + 0.6, str(dorsal), 
                    fontsize=12, weight='bold', color=team_color,  # Aumentado a 12 y color del equipo
                    ha='center', va='center')
            
            # Nombre del jugador debajo del dorsal
            ax.text(player_x, names_y - 0.6, player_name, 
                    fontsize=5, weight='bold', color='white',
                    ha='center', va='center')
        
        # 📍 FILAS 3+: MÉTRICAS Y VALORES
        for i, metric in enumerate(self.metricas_principales):
            metric_y = names_y - names_height/2 - (i + 1) * metric_row_height + metric_row_height/2
            
            # Fondo alternado para las filas de métricas
            if i % 2 == 0:
                row_rect = plt.Rectangle((x - table_width/2, metric_y - metric_row_height/2), 
                                    table_width, metric_row_height,
                                    facecolor='#3c566e', alpha=0.3, 
                                    edgecolor='none')
                ax.add_patch(row_rect)
            
            # Columna de métrica (nombre) con fondo destacado (BORDE MÁS FINO)
            metric_bg = plt.Rectangle((x - table_width/2, metric_y - metric_row_height/2), 
                                    metric_col_width, metric_row_height,
                                    facecolor=team_color, alpha=0.6, 
                                    edgecolor='white', linewidth=0.3)  # Reducido de 0.5 a 0.3
            ax.add_patch(metric_bg)
            
            # Nombre de la métrica (FUENTE MÁS PEQUEÑA)
            metric_name = metric.replace('Distancia Total ', 'Dist. ').replace('Velocidad Máxima Total', 'V.Max').replace('Distancia Total', 'Distancia')
            ax.text(x - table_width/2 + metric_col_width/2, metric_y, metric_name, 
                    fontsize=5, weight='bold', color='white',  # Reducido de 7 a 5
                    ha='center', va='center')
            
            # Valores para cada jugador
            for j, player in enumerate(players_list):
                player_x = x - table_width/2 + metric_col_width + (j * player_col_width) + player_col_width/2
                
                if metric in player:
                    value = player[metric]
                    if 'Velocidad' in metric:
                        formatted_value = f"{value:.1f}"
                    elif 'min' in metric:
                        formatted_value = f"{value:.0f}"
                    else:
                        formatted_value = f"{value:.0f}"
                else:
                    formatted_value = "N/A"
                
                # Destacar valores altos con color diferente
                text_color = '#FFD700' if j == 0 else 'white'  # Primer jugador en dorado
                
                ax.text(player_x, metric_y, formatted_value, 
                        fontsize=6, weight='bold', color=text_color,  # Reducido de 8 a 6
                        ha='center', va='center')
        
        # 🔹 LÍNEAS SEPARADORAS ELEGANTES (MÁS FINAS)
        # Línea horizontal debajo de nombres
        ax.plot([x - table_width/2 + metric_col_width, x + table_width/2], 
                [names_y - names_height/2, names_y - names_height/2], 
                color='white', linewidth=1.5, alpha=0.8)  # Reducido de 2 a 1.5
        
        # Línea vertical separando métricas de valores
        ax.plot([x - table_width/2 + metric_col_width, x - table_width/2 + metric_col_width], 
                [y - table_height/2, y + table_height/2], 
                color='white', linewidth=1.5, alpha=0.8)  # Reducido de 2 a 1.5
    
    def create_team_summary_table(self, team_data, ax, x_pos, y_pos, team_name, team_color, team_logo=None):
        """Crea una tabla de resumen del equipo con métricas y valores en línea horizontal"""
        
        # Calcular estadísticas del equipo
        summary_stats = {}
        
        for metric in self.metricas_principales:
            if metric in team_data.columns:
                if 'Velocidad Máxima' in metric:
                    summary_stats[metric] = team_data[metric].max()
                else:
                    summary_stats[metric] = team_data[metric].mean()
        
        # Dimensiones de la tabla (TODO EN HORIZONTAL)
        num_metrics = len(summary_stats)
        metric_pair_width = 8  # Ancho por cada par métrica+valor
        table_width = num_metrics * metric_pair_width
        table_height = 6  # Altura fija más pequeña
        header_height = 2  # Altura del header del equipo
        
        # 🎨 FONDO MODERNO - Mismo estilo que las tablas de jugadores
        main_rect = plt.Rectangle((x_pos - table_width/2, y_pos - table_height/2), 
                                table_width, table_height,
                                facecolor='#2c3e50', alpha=0.95, 
                                edgecolor='white', linewidth=2)
        ax.add_patch(main_rect)
        
        # Efecto de borde superior
        top_rect = plt.Rectangle((x_pos - table_width/2, y_pos + table_height/2 - 0.5), 
                                table_width, 0.5,
                                facecolor=team_color, alpha=0.8, 
                                edgecolor='none')
        ax.add_patch(top_rect)
        
        # 📍 HEADER: Nombre del equipo
        ax.text(x_pos, y_pos + table_height/2 - header_height/2, team_name, 
                fontsize=9, weight='bold', color='white',
                ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.3", facecolor=team_color, alpha=0.9,
                        edgecolor='white', linewidth=1))
        
        # 📍 MÉTRICAS Y VALORES EN LÍNEA HORIZONTAL
        start_x = x_pos - table_width/2 + metric_pair_width/2
        metrics_y = y_pos - 0.5  # Posición vertical para toda la línea
        
        for i, (metric, value) in enumerate(summary_stats.items()):
            metric_x = start_x + (i * metric_pair_width)
            
            # Fondo alternado para cada par métrica+valor
            if i % 2 == 0:
                pair_rect = plt.Rectangle((metric_x - metric_pair_width/2, metrics_y - 1.5), 
                                        metric_pair_width, 3,
                                        facecolor='#3c566e', alpha=0.3, 
                                        edgecolor='none')
                ax.add_patch(pair_rect)
            
            # Fondo destacado para la métrica
            metric_bg = plt.Rectangle((metric_x - metric_pair_width/2, metrics_y - 1.5), 
                                    metric_pair_width, 1.5,
                                    facecolor=team_color, alpha=0.6, 
                                    edgecolor='white', linewidth=0.3)
            ax.add_patch(metric_bg)
            
            # Nombre de la métrica (parte superior)
            metric_short = metric.replace('Distancia Total ', 'Dist. ').replace('Velocidad Máxima Total', 'V.Max').replace('Distancia Total', 'Distancia')
            ax.text(metric_x, metrics_y - 0.75, metric_short, 
                    fontsize=5, weight='bold', color='white',
                    ha='center', va='center')
            
            # Valor de la métrica (parte inferior) 
            if 'Velocidad' in metric:
                formatted_value = f"{value:.1f}"
            else:
                formatted_value = f"{value:.0f}"
            
            ax.text(metric_x, metrics_y + 0.75, formatted_value, 
                    fontsize=8, weight='bold', color='#FFD700',  # Valores en dorado
                    ha='center', va='center')
        
        # 🔹 LÍNEA SEPARADORA entre header y métricas
        ax.plot([x_pos - table_width/2, x_pos + table_width/2], 
                [y_pos + header_height/2, y_pos + header_height/2], 
                color='white', linewidth=1.5, alpha=0.8)
    
    def create_visualization(self, equipo_rival, jornadas, figsize=(24, 16)):
        """Crea la visualización completa con tablas por posición y datos acumulados"""
        
        # Crear campo SIN espacios
        fig, ax = self.create_campo_sin_espacios(figsize)
        
        # Título superpuesto en el campo
        ax.text(60, 78, f'DATOS ACUMULADOS - ÚLTIMAS {len(jornadas)} JORNADAS | PROMEDIO 70+ MIN', 
                fontsize=14, weight='bold', color='white', ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.8", facecolor='#1e3d59', alpha=0.95,
                         edgecolor='white', linewidth=2))
        
        # Obtener datos acumulados de ambos equipos
        villarreal_data = self.filter_and_accumulate_data('Villarreal CF', jornadas)
        rival_data = self.filter_and_accumulate_data(equipo_rival, jornadas)
        
        if villarreal_data is None or len(villarreal_data) == 0:
            print("❌ No hay jugadores de Villarreal CF con promedio 70+ minutos")
            return None
            
        if rival_data is None or len(rival_data) == 0:
            print(f"❌ No hay jugadores de {equipo_rival} con promedio 70+ minutos")
            return None
        
        # Cargar escudos
        villarreal_logo = self.load_team_logo('Villarreal CF')
        rival_logo = self.load_team_logo(equipo_rival)
        
        # Posicionar escudos dentro del campo
        if villarreal_logo is not None:
            imagebox = OffsetImage(villarreal_logo, zoom=0.12)
            ab = AnnotationBbox(imagebox, (5, 5), frameon=False)
            ax.add_artist(ab)
        
        if rival_logo is not None:
            imagebox = OffsetImage(rival_logo, zoom=0.12)
            ab = AnnotationBbox(imagebox, (115, 5), frameon=False)
            ax.add_artist(ab)
        
        # Agrupar jugadores por posiciones específicas CON NUEVA LÓGICA
        print("🔄 Aplicando lógica mejorada de distribución...")
        villarreal_grouped = self.group_players_by_specific_position(villarreal_data)
        rival_grouped = self.group_players_by_specific_position(rival_data)

        # Crear tablas para Villarreal
        for position, players in villarreal_grouped.items():
            if players and position in self.coordenadas_tablas['villarreal']:
                x, y = self.coordenadas_tablas['villarreal'][position]
                position_name = position.replace('_', ' ').title()
                self.create_position_table(players, x, y, ax, '#FFD700', position_name)

        # ✅ MANEJAR SEGUNDO_DELANTERO SI EXISTE
        if 'SEGUNDO_DELANTERO' in villarreal_grouped and villarreal_grouped['SEGUNDO_DELANTERO']:
            x, y = self.coordenadas_tablas['villarreal']['SEGUNDO_DELANTERO']
            self.create_position_table(villarreal_grouped['SEGUNDO_DELANTERO'], x, y, ax, '#FFD700', 'Segundo Delantero')

        # Crear tablas para equipo rival
        for position, players in rival_grouped.items():
            if players and position in self.coordenadas_tablas['rival']:
                x, y = self.coordenadas_tablas['rival'][position]
                position_name = position.replace('_', ' ').title()
                self.create_position_table(players, x, y, ax, '#cc3300', position_name)

        # ✅ MANEJAR SEGUNDO_DELANTERO SI EXISTE
        if 'SEGUNDO_DELANTERO' in rival_grouped and rival_grouped['SEGUNDO_DELANTERO']:
            x, y = self.coordenadas_tablas['rival']['SEGUNDO_DELANTERO']
            self.create_position_table(rival_grouped['SEGUNDO_DELANTERO'], x, y, ax, '#cc3300', 'Segundo Delantero')
        
        # Resúmenes de equipos con tablas modernas (SIN escudos pequeños)
        self.create_team_summary_table(villarreal_data, ax, 30, 1, 'Villarreal CF', 
                                     '#FFD700', villarreal_logo)
        self.create_team_summary_table(rival_data, ax, 90, 1, equipo_rival, 
                                     '#cc3300', rival_logo)
        
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

def seleccionar_equipo_jornadas_campo():
    """Permite al usuario seleccionar un equipo rival y jornadas"""
    try:
        report_generator = CampoFutbolAcumulado()
        equipos = report_generator.get_available_teams()
        
        # Filtrar Villarreal CF de la lista de oponentes
        equipos_rival = [eq for eq in equipos if 'Villarreal' not in eq]
        
        if len(equipos_rival) == 0:
            print("❌ No se encontraron equipos rivales en los datos.")
            return None, None
        
        print("\n=== SELECCIÓN DE EQUIPO RIVAL - POSICIONES MEJORADAS ===")
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

def main_campo_futbol():
    """Función principal para generar el informe con posiciones mejoradas"""
    try:
        print("🏟️ === GENERADOR DE INFORMES - POSICIONES MEJORADAS ===")
        
        # Selección interactiva
        equipo_rival, jornadas = seleccionar_equipo_jornadas_campo()
        
        if equipo_rival is None or jornadas is None:
            print("❌ No se pudo completar la selección.")
            return
        
        print(f"\n🔄 Generando reporte con POSICIONES MEJORADAS para Villarreal CF vs {equipo_rival}")
        print(f"📅 Jornadas: {jornadas}")
        
        # Crear el reporte
        report_generator = CampoFutbolAcumulado()
        fig = report_generator.create_visualization(equipo_rival, jornadas)
        
        if fig:
            # Mostrar en pantalla
            plt.show()
            
            # Guardar
            equipo_filename = equipo_rival.replace(' ', '_').replace('/', '_')
            output_path = f"reporte_MEJORADO_Villarreal_vs_{equipo_filename}.pdf"
            
            report_generator.guardar_sin_espacios(fig, output_path)
            
        else:
            print("❌ No se pudo generar la visualización")
            
    except Exception as e:
        print(f"❌ Error en la ejecución: {e}")
        import traceback
        traceback.print_exc()

def generar_reporte_campo_personalizado(equipo_rival, jornadas, mostrar=True, guardar=True):
    """Función para generar un reporte personalizado con posiciones mejoradas"""
    try:
        report_generator = CampoFutbolAcumulado()
        fig = report_generator.create_visualization(equipo_rival, jornadas)
        
        if fig:
            if mostrar:
                plt.show()
            
            if guardar:
                equipo_filename = equipo_rival.replace(' ', '_').replace('/', '_')
                output_path = f"reporte_MEJORADO_Villarreal_vs_{equipo_filename}.pdf"
                report_generator.guardar_sin_espacios(fig, output_path)
            
            return fig
        else:
            print("❌ No se pudo generar la visualización")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Inicialización
print("🏟️ === INICIALIZANDO GENERADOR DE POSICIONES MEJORADAS ===")
try:
    report_generator = CampoFutbolAcumulado()
    equipos = report_generator.get_available_teams()
    print(f"\n✅ Sistema de POSICIONES MEJORADAS listo. Equipos disponibles: {len(equipos)}")
    
    if len(equipos) > 0:
        print("📝 Para generar un reporte con POSICIONES MEJORADAS ejecuta: main_campo_futbol()")
        print("📝 Para uso directo: generar_reporte_campo_personalizado('Equipo_Rival', [33,34,35])")
        print("\n🔥 NUEVAS CARACTERÍSTICAS:")
        print("   • Mediapuntas también aparecen en MC Box to Box")
        print("   • Balanceo automático de centrales derecho/izquierdo")
        print("   • Distribución inteligente entre delantero centro y segundo delantero")
        print("   • Considera historial de posiciones para decisiones de reubicación")
    
except Exception as e:
    print(f"❌ Error al inicializar: {e}")

if __name__ == "__main__":
    main_campo_futbol()