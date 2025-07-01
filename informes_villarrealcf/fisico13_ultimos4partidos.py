import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import numpy as np
import os
from difflib import SequenceMatcher
import warnings
warnings.filterwarnings('ignore')

try:
    from mplsoccer import VerticalPitch
except ImportError:
    print("Instalando mplsoccer...")
    import subprocess
    subprocess.check_call(["pip", "install", "mplsoccer"])
    from mplsoccer import VerticalPitch

class ReporteTacticoJornada:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        """
        Inicializa la clase para generar reportes tácticos por jornada
        """
        self.data_path = data_path
        self.df = None
        self.load_data()
        self.clean_team_names()
        
        # 🏟️ FORMACIÓN TÁCTICA 1-4-4-2 (COORDENADAS PARA VERTICALPITCH)
        # VerticalPitch tiene coordenadas 0-120 (largo) x 0-80 (ancho)
        self.formacion_1442 = {
            'PORTERO': [(40, 10)],  # 1 portero
            'DEFENSAS': [(20, 25), (30, 30), (50, 30), (60, 25)],  # 4 defensas 
            'MEDIOS': [(20, 50), (30, 55), (50, 55), (60, 50)],     # 4 medios
            'DELANTEROS': [(35, 80), (45, 80)]  # 2 delanteros
        }
        
        # Mapeo de demarcaciones a posiciones tácticas
        self.demarcacion_to_posicion_tactica = {
            'Portero': 'PORTERO',
            'Defensa - Lateral Izquierdo': 'DEFENSAS',
            'Defensa - Central Izquierdo': 'DEFENSAS',
            'Defensa - Central Derecho': 'DEFENSAS', 
            'Defensa - Lateral Derecho': 'DEFENSAS',
            'Centrocampista - MC Posicional': 'MEDIOS',
            'Centrocampista - MC Box to Box': 'MEDIOS',
            'Centrocampista - MC Organizador': 'MEDIOS',
            'Centrocampista de ataque - Banda Derecha': 'MEDIOS',
            'Centrocampista de ataque - Banda Izquierda': 'MEDIOS',
            'Centrocampista de ataque - Mediapunta': 'MEDIOS',
            'Delantero - Delantero Centro': 'DELANTEROS',
            'Delantero - Segundo Delantero': 'DELANTEROS',
            'Sin Posición': 'MEDIOS'
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
    
    def get_partidos_equipo_jornada(self, equipo, jornada):
        """Obtiene partidos disponibles de un equipo en esa jornada Y las anteriores"""
        if self.df is None:
            return {'local': [], 'visitante': []}
        
        # Normalizar jornada
        if isinstance(jornada, str) and jornada.startswith(('J', 'j')):
            try:
                jornada = int(jornada[1:])
            except ValueError:
                pass
        
        # 🔥 BUSCAR EN LA JORNADA Y LAS ANTERIORES
        jornadas_disponibles = sorted(self.get_available_jornadas())
        jornadas_a_buscar = [j for j in jornadas_disponibles if j <= jornada]
        
        print(f"🔍 Buscando partidos en jornadas: {jornadas_a_buscar}")
        
        # Filtrar por equipo y jornadas múltiples
        filtrado = self.df[
            (self.df['Equipo'] == equipo) & 
            (self.df['Jornada'].isin(jornadas_a_buscar))
        ].copy()
        
        if len(filtrado) == 0:
            return {'local': [], 'visitante': []}
        
        # Clasificar partidos
        partidos_clasificados = {'local': [], 'visitante': []}
        
        for partido in filtrado['Partido'].unique():
            tipo = self.determinar_local_visitante(partido, equipo)
            if tipo in ['local', 'visitante']:
                # Obtener la jornada de este partido
                jornada_partido = filtrado[filtrado['Partido'] == partido]['Jornada'].iloc[0]
                partidos_clasificados[tipo].append(f"{partido} (J{jornada_partido})")
        
        return partidos_clasificados

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
    
    def filtrar_partido_especifico(self, equipo, jornada, tipo_partido, nombre_partido=None, min_minutos=60):
        """Filtra un partido específico buscando en la jornada Y las anteriores"""
        if self.df is None:
            return None
        
        # Normalizar jornada
        if isinstance(jornada, str) and jornada.startswith(('J', 'j')):
            try:
                jornada = int(jornada[1:])
            except ValueError:
                pass
        
        # 🔥 BUSCAR EN MÚLTIPLES JORNADAS (igual que get_partidos_equipo_jornada)
        jornadas_disponibles = sorted(self.get_available_jornadas())
        jornadas_a_buscar = [j for j in jornadas_disponibles if j <= jornada]
        
        # Filtrar por equipo y jornadas múltiples
        filtrado = self.df[
            (self.df['Equipo'] == equipo) & 
            (self.df['Jornada'].isin(jornadas_a_buscar))
        ].copy()
        
        if len(filtrado) == 0:
            print(f"❌ No hay datos para {equipo} en jornadas hasta {jornada}")
            return None
        
        # Si se especifica un partido concreto
        if nombre_partido:
            # Limpiar el nombre del partido (quitar el "(Jx)" si lo tiene)
            nombre_limpio = nombre_partido.split(' (J')[0] if ' (J' in nombre_partido else nombre_partido
            filtrado = filtrado[filtrado['Partido'] == nombre_limpio]
        else:
            # Buscar partidos del tipo especificado en TODAS las jornadas
            partidos_del_tipo = []
            for partido in filtrado['Partido'].unique():
                if self.determinar_local_visitante(partido, equipo) == tipo_partido:
                    partidos_del_tipo.append(partido)
            
            if not partidos_del_tipo:
                print(f"❌ No hay partidos como {tipo_partido} para {equipo} en jornadas hasta {jornada}")
                return None
            
            # Tomar el primer partido del tipo
            filtrado = filtrado[filtrado['Partido'] == partidos_del_tipo[0]]
        
        # Verificar si Alias está vacío y usar Nombre
        if 'Nombre' in filtrado.columns:
            mask_empty_alias = filtrado['Alias'].isna() | (filtrado['Alias'] == '') | (filtrado['Alias'].str.strip() == '')
            filtrado.loc[mask_empty_alias, 'Alias'] = filtrado.loc[mask_empty_alias, 'Nombre']
        
        # Filtrar jugadores con minutos suficientes
        if 'Minutos jugados' in filtrado.columns:
            filtrado = filtrado[filtrado['Minutos jugados'] >= min_minutos]
        
        # Mostrar en qué jornada se encontró el partido
        jornada_encontrada = filtrado['Jornada'].iloc[0] if len(filtrado) > 0 else jornada
        print(f"✅ {equipo} J{jornada_encontrada} ({tipo_partido}): {len(filtrado)} jugadores con {min_minutos}+ minutos")
        return filtrado
    
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
    
    def crear_campo_vertical(self, figsize=(16, 24)):
        """Crea campo vertical usando VerticalPitch"""
        pitch = VerticalPitch(
            half=False,
            pitch_color='grass', 
            line_color='white', 
            stripe=True,
            linewidth=3
        )
        fig, ax = pitch.draw(figsize=figsize)
        return fig, ax
    
    def asignar_jugadores_a_formacion(self, jugadores_df):
        """Asigna jugadores a posiciones de la formación 1-4-4-2"""
        # Rellenar demarcaciones vacías usando histórico
        jugadores_df = self.fill_missing_demarcaciones(jugadores_df)
        
        # Agrupar por posición táctica
        jugadores_por_posicion = {
            'PORTERO': [],
            'DEFENSAS': [], 
            'MEDIOS': [],
            'DELANTEROS': []
        }
        
        for _, jugador in jugadores_df.iterrows():
            demarcacion = jugador.get('Demarcacion', 'Sin Posición')
            posicion_tactica = self.demarcacion_to_posicion_tactica.get(demarcacion, 'MEDIOS')
            jugadores_por_posicion[posicion_tactica].append(jugador.to_dict())
        
        # Ordenar por minutos jugados (descendente)
        for posicion in jugadores_por_posicion:
            jugadores_por_posicion[posicion].sort(
                key=lambda x: x.get('Minutos jugados', 0), reverse=True
            )
        
        # Asignar a coordenadas específicas
        asignacion_final = {}
        
        # Portero (1)
        if jugadores_por_posicion['PORTERO']:
            coord = self.formacion_1442['PORTERO'][0]
            asignacion_final[coord] = jugadores_por_posicion['PORTERO'][0]
        
        # Defensas (4)
        for i, coord in enumerate(self.formacion_1442['DEFENSAS']):
            if i < len(jugadores_por_posicion['DEFENSAS']):
                asignacion_final[coord] = jugadores_por_posicion['DEFENSAS'][i]
        
        # Medios (4)  
        for i, coord in enumerate(self.formacion_1442['MEDIOS']):
            if i < len(jugadores_por_posicion['MEDIOS']):
                asignacion_final[coord] = jugadores_por_posicion['MEDIOS'][i]
        
        # Delanteros (2)
        for i, coord in enumerate(self.formacion_1442['DELANTEROS']):
            if i < len(jugadores_por_posicion['DELANTEROS']):
                asignacion_final[coord] = jugadores_por_posicion['DELANTEROS'][i]
        
        return asignacion_final
    
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
    
    def crear_tabla_jugador(self, jugador, x, y, ax, team_colors, scale=1.0):
        """Crea una tabla moderna con las 9 métricas solicitadas"""
        if not jugador:
            return
        
        # Dimensiones de la tabla (MÁS ALTA para 9 métricas) - Escalable
        ancho_tabla = 14 * scale
        alto_fila = 1.0 * scale
        alto_header = 1.8 * scale
        alto_total = alto_header + len(self.metricas_tabla) * alto_fila
        
        # Fondo principal
        main_rect = plt.Rectangle((x - ancho_tabla/2, y - alto_total/2), 
                                ancho_tabla, alto_total,
                                facecolor='#2c3e50', alpha=0.95, 
                                edgecolor='white', linewidth=1.5)
        ax.add_patch(main_rect)
        
        # Header con nombre y dorsal
        header_rect = plt.Rectangle((x - ancho_tabla/2, y + alto_total/2 - alto_header), 
                                  ancho_tabla, alto_header,
                                  facecolor=team_colors['primary'], alpha=0.9,
                                  edgecolor='white', linewidth=1)
        ax.add_patch(header_rect)
        
        # Nombre del jugador
        nombre = jugador.get('Alias', 'N/A')
        dorsal = jugador.get('Dorsal', 'N/A')
        
        ax.text(x, y + alto_total/2 - alto_header/2, f"{dorsal}. {nombre}", 
                fontsize=9 * scale, weight='bold', color=team_colors['text'],
                ha='center', va='center')
        
        # ✅ MÉTRICAS COMPLETAS
        for i, metrica in enumerate(self.metricas_tabla):
            metrica_y = y + alto_total/2 - alto_header - (i + 0.5) * alto_fila
            
            # Fondo alternado
            if i % 2 == 0:
                row_rect = plt.Rectangle((x - ancho_tabla/2, metrica_y - alto_fila/2), 
                                       ancho_tabla, alto_fila,
                                       facecolor='#3c566e', alpha=0.3)
                ax.add_patch(row_rect)
            
            # Nombre de métrica (ABREVIADO para que quepa)
            metrica_corta = metrica.replace('Distancia Total', 'Dist').replace('Velocidad Máxima Total', 'V.Max').replace(' / min', '/min')
            ax.text(x - ancho_tabla/2 + 1, metrica_y, metrica_corta, 
                    fontsize=5.5 * scale, weight='bold', color='white',
                    ha='left', va='center')
            
            # Valor
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
            
            ax.text(x + ancho_tabla/2 - 1, metrica_y, valor_format, 
                    fontsize=6.5 * scale, weight='bold', color='#FFD700',
                    ha='right', va='center')
    
    def crear_visualization(self, equipo, jornada, tipo_partido, nombre_partido=None, figsize=(20, 28)):
        """Crea UNA visualización específica (local o visitante)"""
        
        # Filtrar datos del partido específico
        partido_data = self.filtrar_partido_especifico(equipo, jornada, tipo_partido, nombre_partido)
        
        if partido_data is None or len(partido_data) == 0:
            print(f"❌ No hay datos para {equipo} en jornada {jornada} como {tipo_partido}")
            return None
        
        # Crear campo vertical
        fig, ax = self.crear_campo_vertical(figsize)
        
        # Obtener colores y logo del equipo
        team_colors = self.get_team_colors(equipo)
        team_logo = self.load_team_logo(equipo)
        
        # Asignar jugadores a formación
        asignacion = self.asignar_jugadores_a_formacion(partido_data)
        
        # Crear tablas de jugadores en el campo
        for coord, jugador in asignacion.items():
            self.crear_tabla_jugador(jugador, coord[0], coord[1], ax, team_colors)
        
        # Obtener nombre del partido para el título
        nombre_partido_real = partido_data['Partido'].iloc[0] if len(partido_data) > 0 else f"{equipo} J{jornada}"
        
        # Título del partido
        tipo_display = "LOCAL" if tipo_partido == 'local' else "VISITANTE"
        fig.suptitle(f'{equipo.upper()} - JORNADA {jornada} ({tipo_display})\n{nombre_partido_real}', 
                    fontsize=16, color='black', weight='bold', y=0.95)
        
        # Añadir escudo en esquina
        if team_logo is not None:
            imagebox = OffsetImage(team_logo, zoom=0.08)
            ab = AnnotationBbox(imagebox, (70, 10), frameon=False)
            ax.add_artist(ab)
        
        return fig

    def crear_4_partidos_visualization(self, equipo, jornada_maxima, tipo_partido_filter=None, figsize=(80, 60)):
        """FUNCIÓN MODIFICADA: Crea 4 campos MÁS ANCHOS en layout 2x2 con DISTANCIA MÍNIMA"""
        
        tipo_display = tipo_partido_filter.upper() if tipo_partido_filter else "TODOS"
        print(f"\n🔄 Generando visualización 2x2 ANCHA de 4 partidos {tipo_display} para {equipo} hasta jornada {jornada_maxima}")
        
        # Obtener últimos 4 partidos filtrados por tipo
        partidos = self.get_ultimos_4_partidos(equipo, jornada_maxima, tipo_partido_filter)
        
        if len(partidos) == 0:
            print(f"❌ No hay partidos {tipo_display} para {equipo} hasta jornada {jornada_maxima}")
            return None
        
        print(f"📊 Se crearán {len(partidos)} campos de fútbol ANCHOS en layout 2x2")
        if len(partidos) < 4:
            print(f"⚠️ Solo {len(partidos)} partidos {tipo_display} disponibles. Las {4-len(partidos)} celdas restantes quedarán vacías")
        
        # 🔥 CREAR FIGURA MÁS ANCHA con subplots 2x2 y ESPACIO MÍNIMO
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        
        # Cargar imagen de fondo
        background_img = self.load_background_image()
        if background_img is not None:
            fig.figimage(background_img, alpha=0.2, resize=True)
        
        # Aplanar axes para acceso fácil
        axes_flat = axes.flatten()
        
        # Obtener colores del equipo principal
        team_colors = self.get_team_colors(equipo)
        team_logo = self.load_team_logo(equipo)
        
        # Crear hasta 4 campos (una celda por partido)
        for i in range(4):
            ax = axes_flat[i]
            
            if i < len(partidos):
                # Hay partido para mostrar
                partido_info = partidos[i]
                print(f"🏟️ Celda {i+1}: J{partido_info['jornada']} - {partido_info['partido']} ({partido_info['tipo']})")
                
                # Crear campo vertical
                pitch = VerticalPitch(
                    half=False,
                    pitch_color='grass', 
                    line_color='white', 
                    stripe=True,
                    linewidth=2
                )
                pitch.draw(ax=ax)

                # CORTAR CAMPO EN 3/4 - Mostrar solo desde portería hasta 3/4 del campo
                ax.set_ylim(0, 90)  # En lugar de 0-120, mostrar solo 0-90 (3/4 del campo)
                
                # Asignar jugadores a formación
                asignacion = self.asignar_jugadores_a_formacion(partido_info['datos'])
                
                # Escala para las tablas (ajustada para layout 2x2 ANCHO)
                escala = 1.1  # ⬆️ AUMENTADO de 0.9 a 1.1 para tablas más grandes
                
                # Crear tablas de jugadores
                for coord, jugador in asignacion.items():
                    self.crear_tabla_jugador(jugador, coord[0], coord[1], ax, team_colors, escala)
                
                # Título del partido
                tipo_display_partido = "LOCAL" if partido_info['tipo'] == 'local' else "VISITANTE"
                ax.set_title(f"J{partido_info['jornada']} - {equipo} vs {partido_info['rival']} ({tipo_display_partido})", 
                            fontsize=16, color='black', weight='bold', pad=20)  # ⬆️ Fuente más grande
                
                # Escudos del partido (más grandes para layout 2x2 ancho)
                if team_logo is not None:
                    # Escudo del equipo principal (izquierda)
                    imagebox1 = OffsetImage(team_logo, zoom=0.065)  # ⬆️ AUMENTADO de 0.045 a 0.065
                    ab1 = AnnotationBbox(imagebox1, (15, 10), frameon=False)
                    ax.add_artist(ab1)
                
                # Escudo del rival (derecha)
                rival_logo = self.load_team_logo(partido_info['rival'])
                if rival_logo is not None:
                    imagebox2 = OffsetImage(rival_logo, zoom=0.065)  # ⬆️ AUMENTADO de 0.045 a 0.065
                    ab2 = AnnotationBbox(imagebox2, (65, 10), frameon=False)
                    ax.add_artist(ab2)
            else:
                # Espacio vacío - mostrar mensaje
                print(f"⬜ Celda {i+1}: Vacía (sin partido {tipo_display} disponible)")
                ax.set_xlim(0, 80)
                ax.set_ylim(0, 120)
                ax.text(40, 60, f'Sin partido {tipo_display.lower()}\ndisponible', 
                    ha='center', va='center', fontsize=14, color='gray', style='italic')  # ⬆️ Fuente más grande
                ax.set_facecolor('lightgray')
                ax.set_alpha(0.3)
        
        # Título general
        fig.suptitle(f'{equipo.upper()} - ÚLTIMOS 4 PARTIDOS {tipo_display} (hasta J{jornada_maxima})', 
                    fontsize=20, color='black', weight='bold', y=0.96)  # ⬆️ Fuente más grande
        
        # 🔥 AJUSTAR ESPACIADO MÍNIMO ENTRE SUBPLOTS - CAMPOS MÁS ANCHOS
        plt.tight_layout()
        plt.subplots_adjust(
            top=0.93,      # ⬆️ Más espacio arriba para título
            bottom=0.05,   # ⬇️ Menos margen abajo  
            left=0.02,     # ⬅️ Menos margen izquierda
            right=0.98,    # ➡️ Menos margen derecha
            hspace=0.01,   # ⬇️ ESPACIO VERTICAL MÍNIMO entre filas (era 0.1)
            wspace=0.001    # ⬅️➡️ ESPACIO HORIZONTAL MÍNIMO entre columnas (era 0.05)
        )
        
        # Información adicional
        fig.text(0.5, 0.01, f'Layout 2x2 ANCHO: Partidos {tipo_display.lower()} con distancia mínima', 
                ha='center', fontsize=12, style='italic')
        
        print("✅ Visualización 2x2 ANCHA con distancia mínima creada correctamente")
        return fig
    
    def get_partidos_disponibles(self, equipo, jornada):
        """Lista partidos disponibles para selección"""
        partidos = self.get_partidos_equipo_jornada(equipo, jornada)
        return partidos

def seleccionar_equipo_jornada_tipo():
    """Selección interactiva de equipo, jornada y tipo (local/visitante)"""
    try:
        report_gen = ReporteTacticoJornada()
        equipos = report_gen.get_available_teams()
        
        if not equipos:
            print("❌ No hay equipos disponibles")
            return None, None, None
        
        print("\n🏟️ === REPORTE TÁCTICO POR JORNADA ===")
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
        
        # Seleccionar jornada
        jornadas = report_gen.get_available_jornadas()
        print(f"\nJornadas disponibles: {jornadas}")
        
        while True:
            try:
                jornada_input = input("Selecciona jornada: ").strip()
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
        
        # Mostrar partidos disponibles y seleccionar tipo
        partidos_disponibles = report_gen.get_partidos_disponibles(equipo_seleccionado, jornada)
        
        print(f"\n📋 Partidos disponibles para {equipo_seleccionado} en Jornada {jornada}:")
        opciones = []
        
        if partidos_disponibles['local']:
            for i, partido in enumerate(partidos_disponibles['local']):
                print(f"  LOCAL {i+1}: {partido}")
                opciones.append(('local', partido))
                
        if partidos_disponibles['visitante']:
            for i, partido in enumerate(partidos_disponibles['visitante']):
                print(f"  VISITANTE {i+1}: {partido}")
                opciones.append(('visitante', partido))
        
        if not opciones:
            print("❌ No hay partidos disponibles")
            return None, None, None
        
        print(f"\n🎯 Selecciona tipo de partido:")
        print("1. LOCAL")
        print("2. VISITANTE")
        
        while True:
            try:
                tipo_seleccion = input("Selecciona tipo (1-2): ").strip()
                if tipo_seleccion == "1":
                    tipo_partido = "local"
                    break
                elif tipo_seleccion == "2":
                    tipo_partido = "visitante"
                    break
                else:
                    print("❌ Selecciona 1 o 2")
            except ValueError:
                print("❌ Ingresa 1 o 2")
        
        return equipo_seleccionado, jornada, tipo_partido
        
    except Exception as e:
        print(f"❌ Error en selección: {e}")
        return None, None, None

def seleccionar_equipo_para_4_partidos():
    """NUEVA FUNCIÓN: Selección interactiva para reporte de 4 partidos con filtro de tipo"""
    try:
        report_gen = ReporteTacticoJornada()
        equipos = report_gen.get_available_teams()
        
        if not equipos:
            print("❌ No hay equipos disponibles")
            return None, None, None
        
        print("\n🏟️ === REPORTE 4 ÚLTIMOS PARTIDOS ===")
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
        
        # NUEVA SELECCIÓN: Tipo de partido
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
        
        return equipo_seleccionado, jornada, tipo_partido_filter
        
    except Exception as e:
        print(f"❌ Error en selección: {e}")
        return None, None, None

def main_reporte_tactico():
    """Función principal"""
    try:
        equipo, jornada, tipo_partido = seleccionar_equipo_jornada_tipo()
        
        if not equipo or not jornada or not tipo_partido:
            print("❌ Selección incompleta")
            return
        
        print(f"\n🔄 Generando reporte táctico para {equipo} - Jornada {jornada} ({tipo_partido.upper()})")
        
        report_gen = ReporteTacticoJornada()
        fig = report_gen.crear_visualization(equipo, jornada, tipo_partido)
        
        if fig:
            plt.show()
            
            # Guardar
            filename = f"reporte_tactico_{equipo.replace(' ', '_')}_J{jornada}_{tipo_partido}.pdf"
            fig.savefig(filename, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            print(f"✅ Guardado: {filename}")
        else:
            print("❌ No se pudo generar el reporte")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main_reporte_4_partidos():
    """NUEVA FUNCIÓN: Función principal para generar reporte de 4 partidos con filtro de tipo"""
    try:
        equipo, jornada, tipo_partido_filter = seleccionar_equipo_para_4_partidos()
        
        if not equipo or not jornada:
            print("❌ Selección incompleta")
            return
        
        tipo_display = tipo_partido_filter.upper() if tipo_partido_filter else "TODOS"
        print(f"\n🔄 Generando reporte de 4 partidos {tipo_display} para {equipo} (hasta J{jornada})")
        
        report_gen = ReporteTacticoJornada()
        fig = report_gen.crear_4_partidos_visualization(equipo, jornada, tipo_partido_filter)
        
        if fig:
            plt.show()
            
            # Guardar
            tipo_filename = f"_{tipo_partido_filter}" if tipo_partido_filter else "_todos"
            filename = f"reporte_4_partidos_{equipo.replace(' ', '_')}_hasta_J{jornada}{tipo_filename}.pdf"
            fig.savefig(filename, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            print(f"✅ Guardado: {filename}")
        else:
            print("❌ No se pudo generar el reporte")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def generar_reporte_personalizado(equipo, jornada, tipo_partido, mostrar=True, guardar=True):
    """Función para uso directo"""
    try:
        report_gen = ReporteTacticoJornada()
        fig = report_gen.crear_visualization(equipo, jornada, tipo_partido)
        
        if fig:
            if mostrar:
                plt.show()
            if guardar:
                filename = f"reporte_tactico_{equipo.replace(' ', '_')}_J{jornada}_{tipo_partido}.pdf"
                fig.savefig(filename, dpi=300, bbox_inches='tight', 
                           facecolor='white', edgecolor='none')
                print(f"✅ Guardado: {filename}")
            return fig
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def generar_4_partidos_personalizado(equipo, jornada_maxima, tipo_partido_filter=None, mostrar=True, guardar=True):
    """NUEVA FUNCIÓN: Función para uso directo de 4 partidos con filtro de tipo"""
    try:
        report_gen = ReporteTacticoJornada()
        fig = report_gen.crear_4_partidos_visualization(equipo, jornada_maxima, tipo_partido_filter)
        
        if fig:
            if mostrar:
                plt.show()
            if guardar:
                tipo_filename = f"_{tipo_partido_filter}" if tipo_partido_filter else "_todos"
                filename = f"reporte_4_partidos_{equipo.replace(' ', '_')}_hasta_J{jornada_maxima}{tipo_filename}.pdf"
                fig.savefig(filename, dpi=300, bbox_inches='tight', 
                           facecolor='white', edgecolor='none')
                print(f"✅ Guardado: {filename}")
            return fig
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def rapido(equipo, jornada=35, tipo_partido=None):
    """🚀 FUNCIÓN RÁPIDA: Genera 4 partidos con valores por defecto y filtro de tipo"""
    tipo_display = tipo_partido.upper() if tipo_partido else "TODOS"
    print(f"🚀 Generación rápida: {equipo} hasta jornada {jornada} - Partidos {tipo_display}")
    return generar_4_partidos_personalizado(equipo, jornada, tipo_partido)

def sevilla(tipo_partido=None):
    """⚡ ACCESO ULTRA-RÁPIDO: Sevilla FC hasta jornada 35 con filtro opcional"""
    return rapido("Sevilla FC", 35, tipo_partido)

def sevilla_local():
    """⚡ SEVILLA SOLO LOCALES"""
    return sevilla("local")

def sevilla_visitante():
    """⚡ SEVILLA SOLO VISITANTES"""
    return sevilla("visitante")

def madrid(tipo_partido=None):
    """⚡ ACCESO ULTRA-RÁPIDO: Real Madrid hasta jornada 35 con filtro opcional"""
    return rapido("Real Madrid", 35, tipo_partido)

def madrid_local():
    """⚡ MADRID SOLO LOCALES"""
    return madrid("local")

def madrid_visitante():
    """⚡ MADRID SOLO VISITANTES"""
    return madrid("visitante")

def barcelona(tipo_partido=None):
    """⚡ ACCESO ULTRA-RÁPIDO: FC Barcelona hasta jornada 35 con filtro opcional"""
    return rapido("FC Barcelona", 35, tipo_partido)

# Inicialización
print("🏟️ === REPORTE TÁCTICO CON VERTICALPITCH INICIALIZADO ===")
try:
    report_gen = ReporteTacticoJornada()
    equipos = report_gen.get_available_teams()
    jornadas = report_gen.get_available_jornadas()
    print(f"✅ Sistema listo: {len(equipos)} equipos, {len(jornadas)} jornadas")
    print("📝 PARA USAR:")
    print("   → main_reporte_4_partidos() - REPORTE 2x2 CON FILTRO (RECOMENDADO)")
    print("   → main_reporte_tactico() - REPORTE INDIVIDUAL") 
    print("   → sevilla_local() - DIRECTO: Sevilla solo locales")
    print("   → sevilla_visitante() - DIRECTO: Sevilla solo visitantes")
    print("   → generar_4_partidos_personalizado('Sevilla FC', 35, 'local')")
    print("   → generar_reporte_personalizado('Sevilla FC', 35, 'local') - INDIVIDUAL")
    print("\n🎯 CARACTERÍSTICAS ACTUALIZADAS:")
    print("   • Campo vertical con VerticalPitch de mplsoccer")
    print("   • 9 métricas completas en cada tabla de jugador")
    print("   • Selección específica: LOCAL o VISITANTE")
    print("   • Filtro de 60+ minutos jugados")
    print("   • ✨ NUEVO: Layout 2x2 (2 filas, 2 columnas)")
    print("   • ✨ NUEVO: Filtro por tipo de partido (solo locales/solo visitantes)")
    print("   • ✨ NUEVO: Espacios vacíos si hay menos de 4 partidos del tipo seleccionado")
    print("   • ✨ NUEVO: Fondo personalizado con assets/fondo_informes.png")
    print("   • ✨ NUEVO: Escudos de ambos equipos en cada celda")
    # Ejemplo de uso inmediato al cargar el script:
    print("\n" + "="*70)
    print("🚀 EJEMPLOS DE USO RÁPIDO - LAYOUT 2x2:")
    print("="*70)
    print("⚡ ULTRA-RÁPIDO (comandos directos):")
    print("   sevilla()           # Sevilla FC todos los partidos")
    print("   sevilla_local()     # Sevilla FC solo locales")
    print("   sevilla_visitante() # Sevilla FC solo visitantes")
    print("   madrid_local()      # Real Madrid solo locales")
    print("   madrid_visitante()  # Real Madrid solo visitantes")
    print("\n🎯 PERSONALIZADO:")
    print("   rapido('Athletic Club', 20, 'local')     # Solo locales")
    print("   rapido('Valencia CF', 15, 'visitante')   # Solo visitantes")
    print("   generar_4_partidos_personalizado('Sevilla FC', 35, 'local')")
    print("\n🔧 INTERACTIVO (guiado paso a paso con selección de tipo):")
    print("   main_reporte_4_partidos()")
    print("\n📋 UN SOLO PARTIDO (función original):")
    print("   generar_reporte_personalizado('Sevilla FC', 35, 'local')")
    print("="*70)
    print("💡 TIP: Para Sevilla solo locales, escribe: sevilla_local()")
    print("🎯 NUEVO: Layout 2x2 (2 filas, 2 columnas) + Filtro local/visitante")
    print("="*70)
except Exception as e:
    print(f"❌ Error inicialización: {e}")

if __name__ == "__main__":
    # Por defecto ejecutar reporte de 4 partidos
    main_reporte_4_partidos()