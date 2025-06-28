import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import numpy as np
import os
from difflib import SequenceMatcher
import warnings
warnings.filterwarnings('ignore')

# Instalar mplsoccer si no está instalado
try:
    from mplsoccer import Pitch
except ImportError:
    print("Instalando mplsoccer...")
    import subprocess
    subprocess.check_call(["pip", "install", "mplsoccer"])
    from mplsoccer import Pitch

class CampoFutbolReportCompleto:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        """
        Inicializa la clase para generar informes modernos sobre campo de fútbol
        """
        self.data_path = data_path
        self.df = None
        self.load_data()
        self.clean_team_names()
        
        # Mapeo exacto basado en las demarcaciones encontradas
        self.demarcacion_to_position = {
            # Defensas
            'Defensa - Lateral Derecho': 'LATERAL_DERECHO',
            'Defensa - Central Derecho': 'CENTRAL_DERECHO', 
            'Defensa - Central Izquierdo': 'CENTRAL_IZQUIERDO',
            'Defensa - Lateral Izquierdo': 'LATERAL_IZQUIERDO',
            
            # Mediocampo
            'Centrocampista - MC Posicional': 'MC_POSICIONAL',
            'Centrocampista - MC Box to Box': 'MC_BOX_TO_BOX',
            'Centrocampista - MC Organizador': 'MC_BOX_TO_BOX',  # Se agrupa con Box to Box
            'Centrocampista de ataque - Mediapunta': 'MC_BOX_TO_BOX',  # Se agrupa con Box to Box
            'Centrocampista de ataque - Banda Izquierda': 'BANDA_IZQUIERDA',
            'Centrocampista de ataque - Banda Derecha': 'BANDA_DERECHA',
            
            # Delanteros
            'Delantero - Delantero Centro': 'DELANTERO_1',
            'Delantero - Segundo Delantero': 'DELANTERO_2',
            
            # Portero
            'Portero': 'PORTERO'
        }
        
        # Coordenadas mejoradas para evitar solapamientos (StatsBomb: 120x80)
        self.coordenadas_posiciones = {
            # Villarreal (lado izquierdo - ataca hacia la derecha)
            'villarreal': {
                'PORTERO': (8, 40),
                'LATERAL_DERECHO': (25, 70), 
                'CENTRAL_DERECHO': (25, 55),
                'CENTRAL_IZQUIERDO': (25, 25),
                'LATERAL_IZQUIERDO': (25, 10),
                'MC_POSICIONAL': (45, 40),        # Centro del campo
                'MC_BOX_TO_BOX': (60, 40),        # Más adelantado en el centro
                'BANDA_IZQUIERDA': (50, 10),      # Banda izquierda en zona media
                'BANDA_DERECHA': (50, 70),        # Banda derecha en zona media
                'DELANTERO_1': (85, 45),          # Campo rival
                'DELANTERO_2': (85, 35),          # Campo rival
            },
            # Equipo rival (lado derecho - ataca hacia la izquierda) - coordenadas espejo
            'rival': {
                'PORTERO': (112, 40),
                'LATERAL_DERECHO': (95, 10),      # Espejo del lateral izquierdo
                'CENTRAL_DERECHO': (95, 25),      # Espejo del central izquierdo
                'CENTRAL_IZQUIERDO': (95, 55),    # Espejo del central derecho
                'LATERAL_IZQUIERDO': (95, 70),    # Espejo del lateral derecho
                'MC_POSICIONAL': (75, 40),        # Centro del campo
                'MC_BOX_TO_BOX': (60, 40),        # Más adelantado en el centro
                'BANDA_IZQUIERDA': (70, 70),      # Banda izquierda en zona media (espejo)
                'BANDA_DERECHA': (70, 10),        # Banda derecha en zona media (espejo)
                'DELANTERO_1': (35, 35),          # Campo rival (espejo)
                'DELANTERO_2': (35, 45),          # Campo rival (espejo)
            }
        }
        
        # Colores para diferentes rangos de métricas
        self.metric_colors = {
            'excellent': '#00ff00',    # Verde brillante
            'good': '#7fff00',         # Verde lima
            'average': '#ffff00',      # Amarillo
            'below_average': '#ff7f00', # Naranja
            'poor': '#ff0000'          # Rojo
        }
        
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
    
    def filter_data_by_minutes(self, equipo, jornadas, min_minutes=70):
        """Filtra los datos por equipo, jornadas y minutos jugados"""
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
        
        # Filtrar por minutos jugados
        if 'Minutos jugados' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['Minutos jugados'] >= min_minutes]
            print(f"✅ Filtrado por {min_minutes}+ minutos jugados")
        else:
            print("⚠️  Columna 'Minutos jugados' no encontrada. Mostrando todos los jugadores.")
        
        print(f"📊 Datos filtrados: {len(filtered_df)} filas para {equipo}")
        return filtered_df
    
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
    
    def get_metric_color(self, value, metric_type, all_values):
        """Determina el color basado en el rendimiento de la métrica"""
        if len(all_values) == 0:
            return self.metric_colors['average']
        
        # Calcular percentiles
        percentiles = np.percentile(all_values, [20, 40, 60, 80])
        
        if value >= percentiles[3]:  # Top 20%
            return self.metric_colors['excellent']
        elif value >= percentiles[2]:  # 60-80%
            return self.metric_colors['good']
        elif value >= percentiles[1]:  # 40-60%
            return self.metric_colors['average']
        elif value >= percentiles[0]:  # 20-40%
            return self.metric_colors['below_average']
        else:  # Bottom 20%
            return self.metric_colors['poor']
    
    def assign_positions_to_players(self, filtered_df, team_side):
        """Asigna posiciones específicas a jugadores evitando solapamientos"""
        player_positions = {}
        
        # Verificar si Alias está vacío y usar Nombre en su lugar
        if 'Nombre' in filtered_df.columns:
            mask_empty_alias = filtered_df['Alias'].isna() | (filtered_df['Alias'] == '') | (filtered_df['Alias'].str.strip() == '')
            filtered_df.loc[mask_empty_alias, 'Alias'] = filtered_df.loc[mask_empty_alias, 'Nombre']
        
        # Ordenar jugadores por minutos jugados (descendente) para priorizar titulares
        filtered_df_sorted = filtered_df.sort_values('Minutos jugados', ascending=False)
        
        # Contadores para posiciones
        position_counts = {pos: 0 for pos in self.coordenadas_posiciones[team_side].keys()}
        max_players_per_position = 1  # Máximo 1 jugador por posición para evitar solapamientos
        
        for _, player in filtered_df_sorted.iterrows():
            player_name = player['Alias']
            demarcacion = player.get('Demarcacion', 'Centrocampista - MC Box to Box')
            
            # Obtener posición base
            base_position = self.demarcacion_to_position.get(demarcacion, 'MC_BOX_TO_BOX')
            
            # Verificar si la posición está disponible
            if position_counts[base_position] < max_players_per_position:
                player_positions[player_name] = base_position
                position_counts[base_position] += 1
            else:
                # Buscar posición alternativa
                alternative_found = False
                
                # Mapeo de posiciones alternativas
                alternatives = {
                    'MC_BOX_TO_BOX': ['MC_POSICIONAL', 'BANDA_IZQUIERDA', 'BANDA_DERECHA'],
                    'MC_POSICIONAL': ['MC_BOX_TO_BOX'],
                    'BANDA_IZQUIERDA': ['MC_BOX_TO_BOX', 'LATERAL_IZQUIERDO'],
                    'BANDA_DERECHA': ['MC_BOX_TO_BOX', 'LATERAL_DERECHO'],
                    'DELANTERO_1': ['DELANTERO_2'],
                    'DELANTERO_2': ['DELANTERO_1'],
                }
                
                if base_position in alternatives:
                    for alt_pos in alternatives[base_position]:
                        if position_counts[alt_pos] < max_players_per_position:
                            player_positions[player_name] = alt_pos
                            position_counts[alt_pos] += 1
                            alternative_found = True
                            break
                
                if not alternative_found:
                    # Si no hay alternativas, usar la posición original (se solaparán)
                    player_positions[player_name] = base_position
            
            # Limitar a 11 jugadores por equipo
            if len(player_positions) >= 11:
                break
        
        return player_positions
    
    def create_modern_player_card(self, player_data, x, y, ax, team_color='blue', all_team_data=None):
        """Crea una tarjeta moderna de estadísticas para un jugador"""
        # Información básica del jugador
        nombre = player_data['Alias'] if pd.notna(player_data['Alias']) else 'N/A'
        dorsal = player_data.get('Dorsal', 'N/A')
        
        # Métricas principales con colores dinámicos
        metrics = []
        
        # Calcular todos los valores del equipo para comparación
        if all_team_data is not None:
            dist_total_values = all_team_data['Distancia Total'].values if 'Distancia Total' in all_team_data else []
            dist_min_values = all_team_data['Distancia Total / min'].values if 'Distancia Total / min' in all_team_data else []
            dist_14_21_values = all_team_data['Distancia Total 14-21 km / h'].values if 'Distancia Total 14-21 km / h' in all_team_data else []
            dist_21_plus_values = all_team_data['Distancia Total >21 km / h'].values if 'Distancia Total >21 km / h' in all_team_data else []
            vmax_values = all_team_data['Velocidad Máxima Total'].values if 'Velocidad Máxima Total' in all_team_data else []
        else:
            dist_total_values = dist_min_values = dist_14_21_values = dist_21_plus_values = vmax_values = []
        
        # Crear métricas con colores
        if 'Distancia Total' in player_data:
            val = player_data['Distancia Total']
            color = self.get_metric_color(val, 'distance', dist_total_values)
            metrics.append((f"{val:.0f}m", color))
        
        if 'Distancia Total / min' in player_data:
            val = player_data['Distancia Total / min']
            color = self.get_metric_color(val, 'distance_min', dist_min_values)
            metrics.append((f"{val:.0f} m/min", color))
        
        if 'Distancia Total 14-21 km / h' in player_data:
            val = player_data['Distancia Total 14-21 km / h']
            color = self.get_metric_color(val, 'distance_14_21', dist_14_21_values)
            metrics.append((f"{val:.0f}m 14-21", color))
        
        if 'Distancia Total >21 km / h' in player_data:
            val = player_data['Distancia Total >21 km / h']
            color = self.get_metric_color(val, 'distance_21_plus', dist_21_plus_values)
            metrics.append((f"{val:.0f}m >21", color))
        
        if 'Velocidad Máxima Total' in player_data:
            val = player_data['Velocidad Máxima Total']
            color = self.get_metric_color(val, 'vmax', vmax_values)
            metrics.append((f"{val:.1f} km/h", color))
        
        # Crear la tarjeta moderna
        card_width = 8
        card_height = 6
        
        # Fondo de la tarjeta con gradiente
        rect = plt.Rectangle((x - card_width/2, y - card_height/2), card_width, card_height,
                           facecolor=team_color, alpha=0.9, edgecolor='white', linewidth=2,
                           transform=ax.transData)
        ax.add_patch(rect)
        
        # Dorsal grande en la esquina superior
        ax.text(x - card_width/2 + 1, y + card_height/2 - 1, f"#{dorsal}", 
                fontsize=16, weight='bold', color='white', 
                ha='left', va='top', transform=ax.transData)
        
        # Nombre del jugador en color diferente
        ax.text(x, y + card_height/2 - 2.5, nombre, 
                fontsize=8, weight='bold', color='#FFD700',  # Dorado para destacar
                ha='center', va='center', transform=ax.transData)
        
        # Métricas con colores dinámicos
        y_offset = 0.5
        for i, (metric_text, metric_color) in enumerate(metrics[:4]):  # Máximo 4 métricas
            ax.text(x, y - y_offset, metric_text, 
                    fontsize=6, weight='bold', color=metric_color,
                    ha='center', va='center', transform=ax.transData)
            y_offset += 1
    
    def create_modern_team_summary(self, team_data, ax, x_pos, y_pos, team_name, team_color, team_logo=None):
        """Crea un resumen moderno de estadísticas del equipo con escudo"""
        
        # Calcular estadísticas del equipo
        summary_stats = {}
        
        if 'Distancia Total' in team_data.columns:
            summary_stats['Dist. Total'] = team_data['Distancia Total'].mean()
        if 'Distancia Total / min' in team_data.columns:
            summary_stats['(m/min)'] = team_data['Distancia Total / min'].mean()
        if 'Distancia Total 14-21 km / h' in team_data.columns:
            summary_stats['14-21 km/h'] = team_data['Distancia Total 14-21 km / h'].mean()
        if 'Distancia Total >21 km / h' in team_data.columns:
            summary_stats['>21 km/h'] = team_data['Distancia Total >21 km / h'].mean()
        if 'Velocidad Máxima Total' in team_data.columns:
            summary_stats['Vmax'] = team_data['Velocidad Máxima Total'].max()
        
        # Crear tabla moderna
        table_width = 25
        table_height = 15
        
        # Fondo de la tabla
        rect = plt.Rectangle((x_pos - table_width/2, y_pos - table_height/2), 
                           table_width, table_height,
                           facecolor=team_color, alpha=0.95, 
                           edgecolor='white', linewidth=3,
                           transform=ax.transData)
        ax.add_patch(rect)
        
        # Escudo del equipo (más pequeño)
        if team_logo is not None:
            imagebox = OffsetImage(team_logo, zoom=0.08)  # Más pequeño
            ab = AnnotationBbox(imagebox, (x_pos - table_width/2 + 3, y_pos + table_height/2 - 3), 
                              frameon=False)
            ax.add_artist(ab)
        
        # Nombre del equipo
        ax.text(x_pos, y_pos + table_height/2 - 2, team_name, 
                fontsize=12, weight='bold', color='white',
                ha='center', va='top', transform=ax.transData)
        
        # Estadísticas en formato tabla
        y_offset = 3
        for stat_name, stat_value in list(summary_stats.items())[:6]:
            if 'Vmax' in stat_name:
                text = f"{stat_name}: {stat_value:.1f}"
            else:
                text = f"{stat_name}: {stat_value:.0f}"
            
            ax.text(x_pos, y_pos + table_height/2 - y_offset, text, 
                    fontsize=9, weight='bold', color='#FFD700',
                    ha='center', va='center', transform=ax.transData)
            y_offset += 2
    
    def create_visualization(self, equipo_rival, jornadas, figsize=(24, 16)):
        """Crea la visualización completa moderna en el campo de fútbol SIN MÁRGENES"""
        
        # Crear campo de fútbol
        pitch = Pitch(pitch_color='grass', line_color='white', stripe=True, linewidth=3)
        fig, ax = pitch.draw(figsize=figsize)
        
        # ✅ ELIMINAR TODOS LOS ESPACIOS Y MÁRGENES
        fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
        ax.set_position([0, 0, 1, 1])
        fig.patch.set_visible(False)
        ax.margins(0)
        
        # Título superpuesto en el campo (parte superior)
        ax.text(60, 78, f'DATOS PROMEDIO - ÚLTIMAS {len(jornadas)} JORNADAS | AL MENOS 70\' JUGADOS', 
                fontsize=16, weight='bold', color='white', ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.8", facecolor='#1e3d59', alpha=0.95,
                         edgecolor='white', linewidth=2), transform=ax.transData)
        
        # Obtener datos de ambos equipos
        villarreal_data = self.filter_data_by_minutes('Villarreal CF', jornadas)
        rival_data = self.filter_data_by_minutes(equipo_rival, jornadas)
        
        if villarreal_data is None or len(villarreal_data) == 0:
            print("❌ No hay datos suficientes para Villarreal CF")
            return None
            
        if rival_data is None or len(rival_data) == 0:
            print(f"❌ No hay datos suficientes para {equipo_rival}")
            return None
        
        # Cargar escudos
        villarreal_logo = self.load_team_logo('Villarreal CF')
        rival_logo = self.load_team_logo(equipo_rival)
        
        # Posicionar escudos según especificaciones (dentro del campo)
        # Villarreal: arriba a la izquierda
        if villarreal_logo is not None:
            imagebox = OffsetImage(villarreal_logo, zoom=0.12)
            ab = AnnotationBbox(imagebox, (15, 70), frameon=False)
            ax.add_artist(ab)
        
        # Rival: abajo a la derecha  
        if rival_logo is not None:
            imagebox = OffsetImage(rival_logo, zoom=0.12)
            ab = AnnotationBbox(imagebox, (105, 10), frameon=False)
            ax.add_artist(ab)
        
        # Asignar posiciones a jugadores
        villarreal_positions = self.assign_positions_to_players(villarreal_data, 'villarreal')
        rival_positions = self.assign_positions_to_players(rival_data, 'rival')
        
        # Colocar jugadores del Villarreal con tarjetas modernas
        for _, player in villarreal_data.iterrows():
            player_name = player['Alias']
            if player_name in villarreal_positions:
                position = villarreal_positions[player_name]
                if position in self.coordenadas_posiciones['villarreal']:
                    x, y = self.coordenadas_posiciones['villarreal'][position]
                    self.create_modern_player_card(player, x, y, ax, 
                                                 team_color='#FFD700',  # Amarillo Villarreal
                                                 all_team_data=villarreal_data)
        
        # Colocar jugadores del equipo rival con tarjetas modernas
        for _, player in rival_data.iterrows():
            player_name = player['Alias']
            if player_name in rival_positions:
                position = rival_positions[player_name]
                if position in self.coordenadas_posiciones['rival']:
                    x, y = self.coordenadas_posiciones['rival'][position]
                    self.create_modern_player_card(player, x, y, ax, 
                                                 team_color='#cc3300',  # Rojo rival
                                                 all_team_data=rival_data)
        
        # Resúmenes modernos de equipo con escudos (ajustados para campo completo)
        self.create_modern_team_summary(villarreal_data, ax, 30, 15, 'Villarreal CF', 
                                      '#FFD700', villarreal_logo)
        self.create_modern_team_summary(rival_data, ax, 90, 65, equipo_rival, 
                                      '#cc3300', rival_logo)
        
        # Leyenda moderna (posicionada dentro del campo)
        villarreal_patch = mpatches.Patch(color='#FFD700', label='Villarreal CF')
        rival_patch = mpatches.Patch(color='#cc3300', label=equipo_rival)
        legend = ax.legend(handles=[villarreal_patch, rival_patch], 
                          loc='center', bbox_to_anchor=(0.5, 0.05), 
                          frameon=True, fontsize=12, ncol=2,
                          fancybox=True, shadow=True)
        legend.get_frame().set_facecolor('white')
        legend.get_frame().set_alpha(0.9)
        
        return fig

# Funciones auxiliares (sin cambios)
def seleccionar_equipo_jornadas_campo():
    """Permite al usuario seleccionar un equipo rival y jornadas"""
    try:
        report_generator = CampoFutbolReportCompleto()
        equipos = report_generator.get_available_teams()
        
        # Filtrar Villarreal CF de la lista de oponentes
        equipos_rival = [eq for eq in equipos if 'Villarreal' not in eq]
        
        if len(equipos_rival) == 0:
            print("❌ No se encontraron equipos rivales en los datos.")
            return None, None
        
        print("\n=== SELECCIÓN DE EQUIPO RIVAL - CAMPO COMPLETO ===")
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
    """Función principal para generar el informe completo sin márgenes"""
    try:
        print("🏟️ === GENERADOR DE INFORMES - CAMPO COMPLETO SIN MÁRGENES ===")
        
        # Selección interactiva
        equipo_rival, jornadas = seleccionar_equipo_jornadas_campo()
        
        if equipo_rival is None or jornadas is None:
            print("❌ No se pudo completar la selección.")
            return
        
        print(f"\n🔄 Generando reporte de campo completo para Villarreal CF vs {equipo_rival}")
        print(f"📅 Jornadas: {jornadas}")
        
        # Crear el reporte
        report_generator = CampoFutbolReportCompleto()
        fig = report_generator.create_visualization(equipo_rival, jornadas)
        
        if fig:
            # Mostrar en pantalla
            plt.tight_layout(pad=0)
            plt.show()
            
            # Guardar como PDF SIN ESPACIOS
            equipo_filename = equipo_rival.replace(' ', '_').replace('/', '_')
            output_path = f"reporte_campo_completo_Villarreal_vs_{equipo_filename}.pdf"
            
            # ✅ GUARDAR SIN MÁRGENES NI ESPACIOS
            fig.savefig(output_path, 
                       bbox_inches='tight',    # Ajustar límites automáticamente
                       pad_inches=0,          # Sin padding adicional
                       facecolor='none',      # Sin color de fondo
                       edgecolor='none',      # Sin bordes
                       dpi=300,              # Alta resolución
                       transparent=False)     # Fondo no transparente para PDF
            
            print(f"✅ Reporte de campo completo guardado como: {output_path}")
        else:
            print("❌ No se pudo generar la visualización")
            
    except Exception as e:
        print(f"❌ Error en la ejecución: {e}")
        import traceback
        traceback.print_exc()

def generar_reporte_campo_personalizado(equipo_rival, jornadas, mostrar=True, guardar=True):
    """Función para generar un reporte personalizado sin márgenes"""
    try:
        report_generator = CampoFutbolReportCompleto()
        fig = report_generator.create_visualization(equipo_rival, jornadas)
        
        if fig:
            if mostrar:
                plt.tight_layout(pad=0)
                plt.show()
            
            if guardar:
                equipo_filename = equipo_rival.replace(' ', '_').replace('/', '_')
                output_path = f"reporte_campo_completo_Villarreal_vs_{equipo_filename}.pdf"
                
                # ✅ GUARDAR SIN MÁRGENES NI ESPACIOS
                fig.savefig(output_path, 
                           bbox_inches='tight', pad_inches=0, 
                           facecolor='none', edgecolor='none', 
                           dpi=300, transparent=False)
                
                print(f"✅ Reporte de campo completo guardado como: {output_path}")
            
            return fig
        else:
            print("❌ No se pudo generar la visualización")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Inicialización
print("🏟️ === INICIALIZANDO GENERADOR DE REPORTES DE CAMPO COMPLETO ===")
try:
    report_generator = CampoFutbolReportCompleto()
    equipos = report_generator.get_available_teams()
    print(f"\n✅ Sistema de campo completo listo. Equipos disponibles: {len(equipos)}")
    
    if len(equipos) > 0:
        print("📝 Para generar un reporte de campo completo ejecuta: main_campo_futbol()")
        print("📝 Para uso directo: generar_reporte_campo_personalizado('Equipo_Rival', [33,34,35])")
    
except Exception as e:
    print(f"❌ Error al inicializar: {e}")

if __name__ == "__main__":
    main_campo_futbol()