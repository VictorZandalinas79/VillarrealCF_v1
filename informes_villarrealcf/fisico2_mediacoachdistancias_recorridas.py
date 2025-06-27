import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from matplotlib import patheffects
import seaborn as sns
from PIL import Image, ImageDraw, ImageFont
import numpy as np
import os
from difflib import SequenceMatcher
import warnings
warnings.filterwarnings('ignore')

class DistanciasRecorridasReport:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        """
        Inicializa la clase para generar informes de distancias recorridas
        """
        self.data_path = data_path
        self.df = None
        self.load_data()
        self.clean_team_names()
        
    def load_data(self):
        """Carga los datos del archivo parquet"""
        try:
            self.df = pd.read_parquet(self.data_path)
            print(f"Datos cargados exitosamente: {self.df.shape[0]} filas, {self.df.shape[1]} columnas")
            print(f"Columnas disponibles: {list(self.df.columns)}")
        except Exception as e:
            print(f"Error al cargar los datos: {e}")
            
    def similarity(self, a, b):
        """Calcula la similitud entre dos strings"""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    def clean_team_names(self):
        """Limpia y agrupa nombres de equipos similares y normaliza jornadas"""
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
                    if self.similarity(team, other_team) > 0.7:  # 70% de similitud
                        similar_teams.append(other_team)
            
            # Elegir el nombre más largo como representativo
            canonical_name = max(similar_teams, key=len)
            
            # Mapear todos los nombres similares al canónico
            for similar_team in similar_teams:
                team_mapping[similar_team] = canonical_name
                processed_teams.add(similar_team)
        
        # Aplicar el mapeo
        self.df['Equipo'] = self.df['Equipo'].map(team_mapping)
        
        # Normalizar jornadas: convertir 'J1', 'J2', etc. a números 1, 2, etc.
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
        
        print(f"Limpieza completada. Equipos únicos: {len(self.df['Equipo'].unique())}")
        print(f"Jornadas normalizadas en datos: {sorted(self.df['Jornada'].unique())}")
        
    def get_available_teams(self):
        """Retorna la lista de equipos disponibles"""
        if self.df is None:
            return []
        return sorted(self.df['Equipo'].unique())
    
    def get_available_jornadas(self, equipo=None):
        """Retorna las jornadas disponibles, opcionalmente filtradas por equipo"""
        if self.df is None:
            return []
        
        if equipo:
            filtered_df = self.df[self.df['Equipo'] == equipo]
            return sorted(filtered_df['Jornada'].unique())
        else:
            return sorted(self.df['Jornada'].unique())
    
    def filter_data(self, equipo, jornadas):
        """Filtra los datos por equipo y jornadas específicas"""
        if self.df is None:
            return None
        
        # Normalizar jornadas (pueden venir como 'J1', 'J2' o como números)
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
        
        print(f"Jornadas normalizadas: {normalized_jornadas}")
        print(f"Jornadas únicas en datos: {sorted(self.df['Jornada'].unique())}")
        
        filtered_df = self.df[
            (self.df['Equipo'] == equipo) & 
            (self.df['Jornada'].isin(normalized_jornadas))
        ].copy()
        
        print(f"Datos filtrados: {len(filtered_df)} filas para {equipo}")
        return filtered_df
    
    def load_team_logo(self, equipo):
        """Carga el escudo del equipo"""
        # Intentar diferentes variaciones del nombre
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
                print(f"Escudo encontrado: {logo_path}")
                try:
                    return plt.imread(logo_path)
                except Exception as e:
                    print(f"Error al cargar escudo {logo_path}: {e}")
                    continue
        
        print(f"No se encontró el escudo para: {equipo}")
        return None
    
    def load_ball_image(self):
        """Carga la imagen del balón"""
        ball_path = "assets/balon.png"
        if os.path.exists(ball_path):
            print(f"Balón encontrado: {ball_path}")
            try:
                return plt.imread(ball_path)
            except Exception as e:
                print(f"Error al cargar balón: {e}")
                return None
        else:
            print(f"No se encontró el balón: {ball_path}")
            return None
    
    def load_background(self):
        """Carga el fondo del informe"""
        bg_path = "assets/fondo_informes.png"
        if os.path.exists(bg_path):
            print(f"Fondo encontrado: {bg_path}")
            try:
                return plt.imread(bg_path)
            except Exception as e:
                print(f"Error al cargar fondo: {e}")
                return None
        else:
            print(f"No se encontró el fondo: {bg_path}")
            return None
    
    def create_distances_data(self, filtered_df, jornadas):
        """Procesa los datos de distancias para los gráficos"""
        # Normalizar jornadas de entrada
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
        
        # Verificar si Alias está vacío y usar Nombre en su lugar
        if 'Nombre' in filtered_df.columns:
            mask_empty_alias = filtered_df['Alias'].isna() | (filtered_df['Alias'] == '') | (filtered_df['Alias'].str.strip() == '')
            filtered_df.loc[mask_empty_alias, 'Alias'] = filtered_df.loc[mask_empty_alias, 'Nombre']

        # Agrupar datos por jugador y jornada
        pivot_data = filtered_df.groupby(['Alias', 'Jornada']).agg({
            'Distancia Total': 'sum',
            'Distancia Total 14-21 km / h': 'sum',
            'Distancia Total  21-24 km / h': 'sum',
            'Distancia Total >24 km / h': 'sum',
            'Dorsal': 'first'
        }).reset_index()
        
        # Procesar datos para gráficos
        jugadores_data = {}
        for jugador in pivot_data['Alias'].unique():
            player_data = pivot_data[pivot_data['Alias'] == jugador]
            dorsal = player_data['Dorsal'].iloc[0] if len(player_data) > 0 else 'N/A'
            
            jugadores_data[jugador] = {
                'dorsal': dorsal,
                'jornadas': {},
                'totales': {
                    'total': 0,
                    '14_21': 0,
                    '21_24': 0,
                    'mas_24': 0
                }
            }
            
            for jornada in normalized_jornadas:
                jornada_data = player_data[player_data['Jornada'] == jornada]
                if len(jornada_data) > 0:
                    row = jornada_data.iloc[0]
                    distancias = {
                        'total': float(row['Distancia Total']) / 1000,  # Convertir a km
                        '14_21': float(row['Distancia Total 14-21 km / h']) / 1000,
                        '21_24': float(row['Distancia Total  21-24 km / h']) / 1000,
                        'mas_24': float(row['Distancia Total >24 km / h']) / 1000
                    }
                else:
                    distancias = {'total': 0, '14_21': 0, '21_24': 0, 'mas_24': 0}
                
                jugadores_data[jugador]['jornadas'][jornada] = distancias
                
                # Acumular totales
                for key in distancias:
                    jugadores_data[jugador]['totales'][key] += distancias[key]
        
        return jugadores_data, normalized_jornadas
    
    def create_visualization(self, equipo, jornadas, figsize=(16, 11)):
        """Crea la visualización completa siguiendo el patrón de la imagen"""
        # Filtrar datos
        filtered_df = self.filter_data(equipo, jornadas)
        if filtered_df is None or len(filtered_df) == 0:
            print("No hay datos para los filtros especificados")
            return None
        
        # Crear figura
        fig = plt.figure(figsize=figsize, facecolor='white')
        
        # Cargar y establecer fondo
        background = self.load_background()
        if background is not None:
            try:
                ax_background = fig.add_axes([0, 0, 1, 1], zorder=-1)
                ax_background.imshow(background, extent=[0, 1, 0, 1], aspect='auto', alpha=0.25, zorder=-1)
                ax_background.axis('off')
                ax_background.set_xticks([])
                ax_background.set_yticks([])
                for spine in ax_background.spines.values():
                    spine.set_visible(False)
                print("Fondo aplicado correctamente")
            except Exception as e:
                print(f"Error al aplicar fondo: {e}")
        
        # Configurar grid: header + gráfico principal + 3 gráficos verticales
        gs = fig.add_gridspec(3, 4, 
                             height_ratios=[0.1, 0.6, 0.3], 
                             width_ratios=[1, 1, 1, 1], 
                             hspace=0.15, wspace=0.08,
                             left=0.03, right=0.97, top=0.95, bottom=0.05)
        
        # Área del título (toda la fila superior)
        ax_title = fig.add_subplot(gs[0, :])
        ax_title.axis('off')
        
        # Título principal
        ax_title.text(0.5, 0.8, 'DISTANCIAS RECORRIDAS', 
                     fontsize=24, weight='bold', ha='center', va='center',
                     color='#1e3d59', family='serif')
        ax_title.text(0.5, 0.2, f'ÚLTIMAS {len(jornadas)} JORNADAS', 
                     fontsize=12, ha='center', va='center',
                     color='#2c3e50', weight='bold')
        
        # Balón izquierda
        ball = self.load_ball_image()
        if ball is not None:
            try:
                imagebox = OffsetImage(ball, zoom=0.12)
                ab = AnnotationBbox(imagebox, (0.05, 0.5), frameon=False)
                ax_title.add_artist(ab)
            except Exception as e:
                print(f"Error al aplicar balón: {e}")
        
        # Escudo derecha
        logo = self.load_team_logo(equipo)
        if logo is not None:
            try:
                imagebox = OffsetImage(logo, zoom=0.025)
                ab = AnnotationBbox(imagebox, (0.95, 0.5), frameon=False)
                ax_title.add_artist(ab)
            except Exception as e:
                print(f"Error al aplicar logo: {e}")
        
        # Procesar datos
        jugadores_data, normalized_jornadas = self.create_distances_data(filtered_df, jornadas)
        
        # Gráfico principal de barras horizontales acumulativas (fila central, todas las columnas)
        ax_main = fig.add_subplot(gs[1, :])
        ax_main.set_facecolor('white')
        ax_main.set_title('DISTANCIA TOTAL POR PARTIDO', fontsize=16, weight='bold', 
                         color='#1e3d59', pad=15)
        self.plot_main_stacked_bars(ax_main, jugadores_data, normalized_jornadas)
        
        # Tres gráficos verticales en la fila inferior
        titles = ['DIST. 14-21 km/h', 'DIST. 21-24 km/h', 'DIST. >24 km/h']
        colors = ['#2ecc71', '#f39c12', '#e74c3c']  # Verde, Naranja, Rojo
        data_keys = ['14_21', '21_24', 'mas_24']
        
        for i, (title, color, data_key) in enumerate(zip(titles, colors, data_keys)):
            ax_chart = fig.add_subplot(gs[2, i])
            ax_chart.set_facecolor('white')
            ax_chart.set_title(title, fontsize=12, weight='bold', 
                              color='#1e3d59', pad=10)
            self.plot_vertical_bar_chart(ax_chart, jugadores_data, data_key, color)
        
        return fig
    
    def plot_main_stacked_bars(self, ax, jugadores_data, jornadas):
        """Dibuja el gráfico principal de barras horizontales acumulativas"""
        jugadores = list(jugadores_data.keys())
        if not jugadores:
            ax.text(0.5, 0.5, 'No hay datos disponibles', ha='center', va='center')
            ax.axis('off')
            return
        
        # Ordenar jugadores por total de distancia
        jugadores_ordenados = sorted(jugadores, 
                                   key=lambda x: jugadores_data[x]['totales']['total'], 
                                   reverse=True)
        jugadores_ordenados.reverse()  # Para mostrar de mayor a menor en el gráfico
        
        # Colores para jornadas (misma paleta)
        colors = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c', '#e67e22', '#34495e']
        
        y_positions = np.arange(len(jugadores_ordenados))
        bar_width = 0.8
        
        # Crear barras apiladas
        left_values = np.zeros(len(jugadores_ordenados))
        
        for j_idx, jornada in enumerate(jornadas):
            distancias_jornada = []
            for jugador in jugadores_ordenados:
                distancia = jugadores_data[jugador]['jornadas'].get(jornada, {'total': 0})['total']
                distancias_jornada.append(distancia)
            
            bars = ax.barh(y_positions, distancias_jornada, bar_width, 
                          left=left_values, 
                          label=f'{jornada}', 
                          color=colors[j_idx % len(colors)])
            
            # Añadir valores en segmentos grandes
            for i, (bar, distancia) in enumerate(zip(bars, distancias_jornada)):
                if distancia > 1.5:
                    ax.text(left_values[i] + distancia/2, bar.get_y() + bar.get_height()/2, 
                           f"{distancia:.1f}", ha='center', va='center', 
                           fontsize=8, weight='bold', color='white')
            
            left_values += distancias_jornada
        
        # Etiquetas de jugadores y totales
        for i, (total, jugador) in enumerate(zip(left_values, jugadores_ordenados)):
            if total > 0:
                # Total al final
                ax.text(total + 0.5, i, f"{total:.1f}", 
                       va='center', fontsize=10, color='#1a237e', weight='bold')
                
                # Nombre del jugador
                ax.text(0.5, i, jugador,
                       va='center', ha='left', fontsize=10, color='#1a237e', weight='bold',
                       bbox=dict(facecolor='#ffffff', alpha=0.9, edgecolor='#2c3e50',
                                linewidth=1, pad=2, boxstyle='round,pad=0.1'))
        
        # Configurar ejes
        ax.set_yticks([])
        ax.set_xlabel('Distancia (km)', fontsize=10, color='#2c3e50')
        
        # Ajustar límites
        max_total = max(left_values) if len(left_values) > 0 else 20
        ax.set_xlim(0, max_total * 1.1)
        
        # Leyenda
        ax.legend(loc='upper right', bbox_to_anchor=(0.98, 0.98), 
                 title='Jornadas', fontsize=8, title_fontsize=9)
        
        # Estilo
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.grid(axis='x', alpha=0.3)
    
    def plot_vertical_bar_chart(self, ax, jugadores_data, data_key, color):
        """Dibuja un gráfico de barras verticales para un tipo específico de distancia"""
        jugadores = list(jugadores_data.keys())
        if not jugadores:
            ax.text(0.5, 0.5, 'No hay datos', ha='center', va='center')
            ax.axis('off')
            return
        
        # Ordenar por total del tipo específico
        jugadores_ordenados = sorted(jugadores, 
                                   key=lambda x: jugadores_data[x]['totales'][data_key], 
                                   reverse=True)
        
        # Tomar solo los top 15 para que se vea bien
        jugadores_top = jugadores_ordenados[:15]
        
        # Datos para el gráfico
        valores = [jugadores_data[jugador]['totales'][data_key] for jugador in jugadores_top]
        nombres = jugadores_top
        
        # Crear gráfico de barras
        x_positions = np.arange(len(nombres))
        bars = ax.bar(x_positions, valores, color=color, alpha=0.8, edgecolor='white', linewidth=1)
        
        # Añadir valores encima de las barras
        for bar, valor in zip(bars, valores):
            if valor > 0:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.02, 
                       f"{valor:.1f}", ha='center', va='bottom', 
                       fontsize=8, weight='bold', color='#2c3e50')
        
        # Configurar ejes
        ax.set_xticks(x_positions)
        ax.set_xticklabels(nombres, rotation=45, ha='right', fontsize=8)
        ax.set_ylabel('km', fontsize=9, color='#2c3e50')
        
        # Ajustar límites
        if valores:
            max_val = max(valores)
            ax.set_ylim(0, max_val * 1.15)
        
        # Estilo
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.grid(axis='y', alpha=0.3)
        ax.tick_params(axis='both', labelsize=8)

# Funciones auxiliares (mantener las mismas que antes)
def seleccionar_equipo_interactivo():
    """Permite al usuario seleccionar un equipo de forma interactiva"""
    try:
        report_generator = DistanciasRecorridasReport()
        equipos = report_generator.get_available_teams()
        
        if len(equipos) == 0:
            print("No se encontraron equipos en los datos.")
            return None, None
        
        print("\n=== SELECCIÓN DE EQUIPO ===")
        for i, equipo in enumerate(equipos, 1):
            print(f"{i}. {equipo}")
        
        while True:
            try:
                seleccion = input(f"\nSelecciona un equipo (1-{len(equipos)}): ").strip()
                indice = int(seleccion) - 1
                
                if 0 <= indice < len(equipos):
                    equipo_seleccionado = equipos[indice]
                    break
                else:
                    print(f"Por favor, ingresa un número entre 1 y {len(equipos)}")
            except ValueError:
                print("Por favor, ingresa un número válido")
        
        # Obtener jornadas disponibles
        jornadas_disponibles = report_generator.get_available_jornadas(equipo_seleccionado)
        print(f"\nJornadas disponibles para {equipo_seleccionado}: {jornadas_disponibles}")
        
        # Preguntar cuántas jornadas incluir
        while True:
            try:
                num_jornadas = input(f"¿Cuántas jornadas incluir? (máximo {len(jornadas_disponibles)}): ").strip()
                num_jornadas = int(num_jornadas)
                
                if 1 <= num_jornadas <= len(jornadas_disponibles):
                    jornadas_seleccionadas = sorted(jornadas_disponibles)[-num_jornadas:]
                    break
                else:
                    print(f"Por favor, ingresa un número entre 1 y {len(jornadas_disponibles)}")
            except ValueError:
                print("Por favor, ingresa un número válido")
        
        return equipo_seleccionado, jornadas_seleccionadas
        
    except Exception as e:
        print(f"Error en la selección: {e}")
        return None, None

def main():
    try:
        print("=== GENERADOR DE REPORTES DE DISTANCIAS RECORRIDAS ===")
        
        # Selección interactiva
        equipo, jornadas = seleccionar_equipo_interactivo()
        
        if equipo is None or jornadas is None:
            print("No se pudo completar la selección.")
            return
        
        print(f"\nGenerando reporte para {equipo} - Jornadas: {jornadas}")
        
        # Crear el reporte
        report_generator = DistanciasRecorridasReport()
        fig = report_generator.create_visualization(equipo, jornadas)
        
        if fig:
            # Mostrar en pantalla
            plt.show()
            
            # Guardar como PDF
            equipo_filename = equipo.replace(' ', '_').replace('/', '_')
            output_path = f"reporte_distancias_{equipo_filename}.pdf"
            
            from matplotlib.backends.backend_pdf import PdfPages
            with PdfPages(output_path) as pdf:
                fig.patch.set_alpha(0.0)
                pdf.savefig(fig, bbox_inches='tight', pad_inches=0, 
                          facecolor='none', edgecolor='none', dpi=300,
                          transparent=True)
            
            print(f"✅ Reporte guardado como: {output_path}")
        else:
            print("❌ No se pudo generar la visualización")
            
    except Exception as e:
        print(f"❌ Error en la ejecución: {e}")
        import traceback
        traceback.print_exc()

def generar_reporte_distancias_personalizado(equipo, jornadas, mostrar=True, guardar=True):
    """Función para generar un reporte personalizado de distancias"""
    try:
        report_generator = DistanciasRecorridasReport()
        fig = report_generator.create_visualization(equipo, jornadas)
        
        if fig:
            if mostrar:
                plt.show()
            
            if guardar:
                equipo_filename = equipo.replace(' ', '_').replace('/', '_')
                output_path = f"reporte_distancias_{equipo_filename}.pdf"
                
                from matplotlib.backends.backend_pdf import PdfPages
                with PdfPages(output_path) as pdf:
                    fig.patch.set_alpha(0.0)
                    pdf.savefig(fig, bbox_inches='tight', pad_inches=0, 
                              facecolor='none', edgecolor='none', dpi=300,
                              transparent=True)
                
                print(f"✅ Reporte guardado como: {output_path}")
            
            return fig
        else:
            print("❌ No se pudo generar la visualización")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# Inicialización
print("=== INICIALIZANDO GENERADOR DE REPORTES DE DISTANCIAS ===")
try:
    report_generator = DistanciasRecorridasReport()
    equipos = report_generator.get_available_teams()
    print(f"\n✅ Sistema listo. Equipos disponibles: {len(equipos)}")
    
    if len(equipos) > 0:
        print("📝 Para generar un reporte ejecuta: main()")
        print("📝 Para uso directo: generar_reporte_distancias_personalizado('Nombre_Equipo', [33,34,35])")
    
except Exception as e:
    print(f"❌ Error al inicializar: {e}")

if __name__ == "__main__":
    main()