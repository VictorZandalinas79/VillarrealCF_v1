# Script de diagnóstico para el problema de jornadas de Sevilla FC

import pandas as pd

def diagnosticar_problema_jornadas(data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
    """
    Función de diagnóstico para entender por qué Sevilla FC solo muestra 2 jornadas
    """
    print("=== DIAGNÓSTICO DEL PROBLEMA DE JORNADAS ===\n")
    
    # 1. Cargar datos originales
    try:
        df = pd.read_parquet(data_path)
        print(f"✅ Datos cargados: {df.shape[0]} filas, {df.shape[1]} columnas")
    except Exception as e:
        print(f"❌ Error al cargar datos: {e}")
        return
    
    # 2. Verificar columnas disponibles
    print(f"\n📋 Columnas disponibles: {list(df.columns)}")
    
    # 3. Verificar equipos únicos ANTES de la limpieza
    equipos_originales = df['equipo'].unique()
    print(f"\n🏟️ Equipos únicos ORIGINALES ({len(equipos_originales)}):")
    sevilla_variants = [eq for eq in equipos_originales if 'sevilla' in eq.lower()]
    print(f"   Variantes de Sevilla encontradas: {sevilla_variants}")
    
    # 4. Verificar jornadas únicas ANTES de normalización
    jornadas_originales = df['jornada'].unique()
    print(f"\n📅 Jornadas únicas ORIGINALES: {sorted(jornadas_originales)}")
    
    # 5. Verificar datos específicos de Sevilla ANTES de limpieza
    for sevilla_variant in sevilla_variants:
        sevilla_data = df[df['equipo'] == sevilla_variant]
        jornadas_sevilla = sevilla_data['jornada'].unique()
        print(f"\n🔍 {sevilla_variant}:")
        print(f"   - Registros: {len(sevilla_data)}")
        print(f"   - Jornadas: {sorted(jornadas_sevilla)}")
        print(f"   - Jugadores únicos: {len(sevilla_data['Alias'].unique()) if 'Alias' in sevilla_data.columns else 'N/A'}")
    
    # 6. Aplicar la lógica de limpieza paso a paso
    print(f"\n=== APLICANDO LIMPIEZA DE NOMBRES ===")
    
    from difflib import SequenceMatcher
    
    def similarity(a, b):
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    # Limpiar nombres de equipos
    unique_teams = df['equipo'].unique()
    team_mapping = {}
    processed_teams = set()
    
    for team in unique_teams:
        if team in processed_teams:
            continue
            
        # Buscar equipos similares
        similar_teams = [team]
        for other_team in unique_teams:
            if other_team != team and other_team not in processed_teams:
                sim_score = similarity(team, other_team)
                if sim_score > 0.7:  # 70% de similitud
                    similar_teams.append(other_team)
                    print(f"   📎 Similitud {sim_score:.2f}: '{team}' <-> '{other_team}'")
        
        # Elegir el nombre más largo como representativo
        canonical_name = max(similar_teams, key=len)
        
        # Mapear todos los nombres similares al canónico
        for similar_team in similar_teams:
            team_mapping[similar_team] = canonical_name
            processed_teams.add(similar_team)
    
    # Mostrar mapeo para Sevilla
    print(f"\n🔄 Mapeo de equipos relacionados con Sevilla:")
    for original, mapped in team_mapping.items():
        if 'sevilla' in original.lower() or 'sevilla' in mapped.lower():
            print(f"   '{original}' -> '{mapped}'")
    
    # 7. Aplicar mapeo y verificar resultado
    df_cleaned = df.copy()
    df_cleaned['equipo'] = df_cleaned['equipo'].map(team_mapping)
    
    # 8. Normalizar jornadas
    def normalize_jornada(jornada):
        if isinstance(jornada, str) and jornada.startswith('J'):
            try:
                return int(jornada[1:])
            except ValueError:
                return jornada
        return jornada
    
    df_cleaned['jornada'] = df_cleaned['jornada'].apply(normalize_jornada)
    
    # 9. Verificar resultado final para Sevilla
    equipos_finales = df_cleaned['equipo'].unique()
    sevilla_finales = [eq for eq in equipos_finales if 'sevilla' in eq.lower()]
    
    print(f"\n✅ RESULTADO FINAL:")
    print(f"   Equipos con 'Sevilla': {sevilla_finales}")
    
    for sevilla_final in sevilla_finales:
        sevilla_final_data = df_cleaned[df_cleaned['equipo'] == sevilla_final]
        jornadas_final = sevilla_final_data['jornada'].unique()
        print(f"\n🎯 {sevilla_final}:")
        print(f"   - Registros: {len(sevilla_final_data)}")
        print(f"   - Jornadas: {sorted(jornadas_final)}")
        
        # Mostrar detalles por jornada
        for jornada in sorted(jornadas_final):
            jornada_data = sevilla_final_data[sevilla_final_data['jornada'] == jornada]
            jugadores_jornada = jornada_data['Alias'].nunique() if 'Alias' in jornada_data.columns else 'N/A'
            print(f"     - Jornada {jornada}: {len(jornada_data)} registros, {jugadores_jornada} jugadores")
    
    return df_cleaned, team_mapping

def corregir_clase_original():
    """
    Versión corregida de la clase con mejor diagnóstico
    """
    print("\n=== CLASE CORREGIDA CON DIAGNÓSTICO ===")
    
    codigo_corregido = """
class MinutosJugadosReport:
    def __init__(self, data_path="prueba_extraccion/data/rendimiento_fisico.parquet"):
        self.data_path = data_path
        self.df = None
        self.team_mapping = {}  # Añadir para tracking
        self.load_data()
        self.clean_team_names()
        
    def load_data(self):
        try:
            self.df = pd.read_parquet(self.data_path)
            print(f"Datos cargados exitosamente: {self.df.shape[0]} filas, {self.df.shape[1]} columnas")
        except Exception as e:
            print(f"Error al cargar los datos: {e}")
            
    def clean_team_names(self):
        if self.df is None:
            return
        
        print(f"\\n🔧 INICIANDO LIMPIEZA DE NOMBRES...")
        print(f"Equipos originales: {len(self.df['equipo'].unique())}")
        
        # Verificar Sevilla antes de limpieza
        sevilla_before = [eq for eq in self.df['equipo'].unique() if 'sevilla' in eq.lower()]
        print(f"Variantes de Sevilla ANTES: {sevilla_before}")
        
        # Limpiar nombres de equipos
        unique_teams = self.df['equipo'].unique()
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
                        print(f"  📎 Agrupando: '{team}' con '{other_team}'")
            
            # Elegir el nombre más largo como representativo
            canonical_name = max(similar_teams, key=len)
            
            # Mapear todos los nombres similares al canónico
            for similar_team in similar_teams:
                team_mapping[similar_team] = canonical_name
                processed_teams.add(similar_team)
        
        # Guardar mapeo para debugging
        self.team_mapping = team_mapping
        
        # Aplicar el mapeo
        self.df['equipo'] = self.df['equipo'].map(team_mapping)
        
        # Verificar Sevilla después de limpieza
        sevilla_after = [eq for eq in self.df['equipo'].unique() if 'sevilla' in eq.lower()]
        print(f"Variantes de Sevilla DESPUÉS: {sevilla_after}")
        
        # Normalizar jornadas
        def normalize_jornada(jornada):
            if isinstance(jornada, str) and jornada.startswith('J'):
                try:
                    return int(jornada[1:])
                except ValueError:
                    return jornada
            return jornada
        
        jornadas_before = self.df['jornada'].unique()
        self.df['jornada'] = self.df['jornada'].apply(normalize_jornada)
        jornadas_after = self.df['jornada'].unique()
        
        print(f"Jornadas ANTES: {sorted(jornadas_before)}")
        print(f"Jornadas DESPUÉS: {sorted(jornadas_after)}")
        
        print(f"✅ Limpieza completada. Equipos únicos: {len(self.df['equipo'].unique())}")
        
        # DIAGNÓSTICO ESPECÍFICO PARA SEVILLA
        self.diagnosticar_sevilla()
    
    def diagnosticar_sevilla(self):
        print(f"\\n🔍 DIAGNÓSTICO ESPECÍFICO PARA SEVILLA:")
        sevilla_teams = [eq for eq in self.df['equipo'].unique() if 'sevilla' in eq.lower()]
        
        for sevilla_team in sevilla_teams:
            sevilla_data = self.df[self.df['equipo'] == sevilla_team]
            jornadas = sevilla_data['jornada'].unique()
            print(f"  {sevilla_team}:")
            print(f"    - Total registros: {len(sevilla_data)}")
            print(f"    - Jornadas: {sorted(jornadas)}")
            
            # Detalle por jornada
            for jornada in sorted(jornadas):
                jornada_data = sevilla_data[sevilla_data['jornada'] == jornada]
                jugadores = jornada_data['Alias'].nunique() if 'Alias' in jornada_data.columns else 'N/A'
                print(f"      Jornada {jornada}: {len(jornada_data)} registros, {jugadores} jugadores")
    
    def similarity(self, a, b):
        from difflib import SequenceMatcher
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    def get_available_jornadas(self, equipo=None):
        if self.df is None:
            return []
        
        if equipo:
            print(f"\\n📊 Buscando jornadas para: '{equipo}'")
            filtered_df = self.df[self.df['equipo'] == equipo]
            jornadas = sorted(filtered_df['jornada'].unique())
            print(f"   Jornadas encontradas: {jornadas}")
            print(f"   Total registros: {len(filtered_df)}")
            return jornadas
        else:
            return sorted(self.df['jornada'].unique())
"""
    
    print(codigo_corregido)

# Función principal de diagnóstico
def main_diagnostico():
    print("🔧 Ejecutando diagnóstico completo...")
    
    # 1. Diagnóstico detallado
    df_cleaned, team_mapping = diagnosticar_problema_jornadas()
    
    print(f"\n" + "="*50)
    print("RESUMEN DEL DIAGNÓSTICO:")
    print("="*50)
    
    # 2. Recomendaciones
    print(f"\n💡 POSIBLES SOLUCIONES:")
    print(f"1. Verificar que el archivo parquet contiene todos los datos esperados")
    print(f"2. Revisar si hay problemas en el agrupamiento de equipos similares")
    print(f"3. Verificar que 'Sevilla FC' es exactamente el nombre en los datos")
    print(f"4. Comprobar si hay registros con valores nulos en jornada")
    
    # 3. Mostrar clase corregida
    corregir_clase_original()

if __name__ == "__main__":
    main_diagnostico()