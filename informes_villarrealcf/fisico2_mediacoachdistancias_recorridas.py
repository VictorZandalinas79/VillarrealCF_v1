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
        
        # Configurar grid: header + 4 gráficos verticales con barras horizontales
        gs = fig.add_gridspec(2, 4, 
                             height_ratios=[0.08, 1], 
                             width_ratios=[1, 1, 1, 1], 
                             hspace=0.12, wspace=0.05,
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
        
        # 4 gráficos verticales con barras horizontales, uno al lado del otro
        titles = ['DISTANCIA TOTAL POR PARTIDO', 'DIST. 14-21 km/h', 'DIST. 21-24 km/h', 'DIST. >24 km/h']
        data_keys = ['total', '14_21', '21_24', 'mas_24']
        chart_types = ['stacked', 'simple', 'simple', 'simple']  # El primero es acumulativo
        
        for i, (title, data_key, chart_type) in enumerate(zip(titles, data_keys, chart_types)):
            ax_chart = fig.add_subplot(gs[1, i])
            ax_chart.set_facecolor('none')  # Sin fondo
            ax_chart.set_title(title, fontsize=10, weight='bold', 
                              color='#1e3d59', pad=10)
            
            if chart_type == 'stacked':
                self.plot_horizontal_stacked_bars(ax_chart, jugadores_data, normalized_jornadas)
            else:
                color = '#2ecc71' if data_key == '14_21' else '#f39c12' if data_key == '21_24' else '#e74c3c'
                self.plot_horizontal_simple_bars(ax_chart, jugadores_data, data_key, color)
        
        return fig
    
    def plot_horizontal_stacked_bars(self, ax, jugadores_data, jornadas):
        """Dibuja barras horizontales acumulativas por jornadas"""
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
                          label=f'J{jornada}', 
                          color=colors[j_idx % len(colors)])
            
            # Añadir valores en segmentos grandes
            for i, (bar, distancia) in enumerate(zip(bars, distancias_jornada)):
                if distancia > 1.0:
                    ax.text(left_values[i] + distancia/2, bar.get_y() + bar.get_height()/2, 
                           f"{distancia:.1f}", ha='center', va='center', 
                           fontsize=6, weight='bold', color='white')
            
            left_values += distancias_jornada
        
        # Etiquetas de jugadores y totales
        for i, (total, jugador) in enumerate(zip(left_values, jugadores_ordenados)):
            if total > 0:
                # Total al final
                ax.text(total + 0.2, i, f"{total:.1f}", 
                       va='center', fontsize=7, color='#1a237e', weight='bold')
                
                # Nombre del jugador
                ax.text(0.2, i, jugador,
                       va='center', ha='left', fontsize=7, color='#1a237e', weight='bold',
                       bbox=dict(facecolor='#ffffff', alpha=0.9, edgecolor='#2c3e50',
                                linewidth=0.5, pad=1, boxstyle='round,pad=0.05'))
        
        # Configurar ejes
        ax.set_yticks([])
        ax.set_xlabel('km', fontsize=8, color='#2c3e50')
        
        # Ajustar límites
        max_total = max(left_values) if len(left_values) > 0 else 20
        ax.set_xlim(0, max_total * 1.08)
        
        # Leyenda compacta
        ax.legend(loc='upper right', bbox_to_anchor=(0.98, 0.98), 
                 fontsize=6, ncol=1, frameon=True, fancybox=True, shadow=True)
        
        # Estilo sin fondo
        ax.set_facecolor('none')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.grid(axis='x', alpha=0.2)
        ax.tick_params(axis='both', labelsize=7)
    
    def plot_horizontal_simple_bars(self, ax, jugadores_data, data_key, color):
        """Dibuja barras horizontales simples para un tipo específico de distancia"""
        jugadores = list(jugadores_data.keys())
        if not jugadores:
            ax.text(0.5, 0.5, 'No hay datos', ha='center', va='center')
            ax.axis('off')
            return
        
        # Ordenar por total del tipo específico
        jugadores_ordenados = sorted(jugadores, 
                                   key=lambda x: jugadores_data[x]['totales'][data_key], 
                                   reverse=True)
        jugadores_ordenados.reverse()  # Para mostrar de mayor a menor en el gráfico
        
        # Datos para el gráfico
        valores = [jugadores_data[jugador]['totales'][data_key] for jugador in jugadores_ordenados]
        
        # Crear gráfico de barras horizontales
        y_positions = np.arange(len(jugadores_ordenados))
        bars = ax.barh(y_positions, valores, color=color, alpha=0.8, 
                      edgecolor='white', linewidth=0.5, height=0.8)
        
        # Añadir valores al final de las barras
        for i, (bar, valor, jugador) in enumerate(zip(bars, valores, jugadores_ordenados)):
            if valor > 0:
                ax.text(valor + 0.02, bar.get_y() + bar.get_height()/2, 
                       f"{valor:.1f}", ha='left', va='center', 
                       fontsize=6, weight='bold', color='#2c3e50')
                
                # Nombre del jugador
                ax.text(0.05, i, jugador,
                       va='center', ha='left', fontsize=7, color='#1a237e', weight='bold',
                       bbox=dict(facecolor='#ffffff', alpha=0.9, edgecolor='#2c3e50',
                                linewidth=0.5, pad=1, boxstyle='round,pad=0.05'))
        
        # Configurar ejes
        ax.set_yticks([])
        ax.set_xlabel('km', fontsize=8, color='#2c3e50')
        
        # Ajustar límites
        if valores:
            max_val = max(valores)
            ax.set_xlim(0, max_val * 1.08)
        
        # Leyenda simple
        ax.legend([f'Total: {sum(valores):.1f} km'], 
                 loc='upper right', bbox_to_anchor=(0.98, 0.98),
                 fontsize=6, frameon=True, fancybox=True, shadow=True)
        
        # Estilo sin fondo
        ax.set_facecolor('none')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.grid(axis='x', alpha=0.2)
        ax.tick_params(axis='both', labelsize=7)

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