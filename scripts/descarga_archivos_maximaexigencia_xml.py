#!/usr/bin/env python3
"""
Extractor de archivos XML de MediaCoach (maximaexigenciarevisada, beyondstats) a Parquet

ESTRUCTURA IDENTIFICADA:
- maximaexigenciarevisada: Ventanas de exigencia física por jugador
- beyondstats: Posibles estadísticas avanzadas (similar estructura esperada)

DATOS EXTRAÍDOS:
- Ventanas de velocidad: >0km/h, >21km/h, >24km/h
- Duraciones: 1min, 3min, 5min
- Jugadores y equipos
- Tiempos de inicio/fin
- Periodos de juego
"""

import pandas as pd
import xml.etree.ElementTree as ET
import os
import glob
from typing import List, Dict, Tuple
import re
import warnings
warnings.filterwarnings('ignore')

def extraer_informacion_label(instance):
    """
    Extrae información de los elementos <label> de una instancia
    """
    jugador = None
    equipo = None
    periodo = None
    
    # Buscar todos los elementos label
    labels = instance.findall('label')
    
    for label in labels:
        # Buscar elemento <text> directo
        text_elem = label.find('text')
        if text_elem is not None:
            text_value = text_elem.text
            
            # Verificar si hay un <group> que indique que es equipo
            group_elem = label.find('group')
            if group_elem is not None and group_elem.text == 'Equipo':
                equipo = text_value
            elif text_value in ['P1', 'P2']:
                periodo = text_value
            else:
                # Si no hay group, probablemente es nombre de jugador
                jugador = text_value
        
        # Si no encontró text directo, buscar solo text
        elif len(label) == 1 and label[0].tag == 'text':
            text_value = label[0].text
            if text_value in ['P1', 'P2']:
                periodo = text_value
            else:
                jugador = text_value
    
    return jugador, equipo, periodo

def parsear_codigo_ventana(codigo):
    """
    Parsea el código de ventana para extraer duración y velocidad
    Ejemplo: "Ventana 1min y >21km/h" -> (1, 21)
    """
    if codigo == "Inicio":
        return None, None, "Inicio"
    
    # Buscar patrón: Ventana {N}min y >{V}km/h
    patron = r'Ventana (\d+)min y >(\d+)km/h'
    match = re.search(patron, codigo)
    
    if match:
        duracion_min = int(match.group(1))
        velocidad_kmh = int(match.group(2))
        return duracion_min, velocidad_kmh, "Ventana"
    
    return None, None, "Desconocido"

def procesar_archivo_xml(ruta_archivo: str) -> pd.DataFrame:
    """
    Procesa un archivo XML de MediaCoach y devuelve un DataFrame
    """
    print(f"  📄 Procesando: {os.path.basename(ruta_archivo)}")
    
    try:
        # Parsear XML
        tree = ET.parse(ruta_archivo)
        root = tree.getroot()
        
        # Buscar todas las instancias
        instances = root.findall('.//instance')
        print(f"    🔍 Instancias encontradas: {len(instances)}")
        
        if not instances:
            print(f"    ⚠️  No se encontraron instancias en el archivo")
            return None
        
        # Lista para almacenar datos
        datos = []
        
        for instance in instances:
            # Extraer elementos básicos
            id_elem = instance.find('ID')
            start_elem = instance.find('start')
            end_elem = instance.find('end')
            code_elem = instance.find('code')
            
            if not all([id_elem is not None, start_elem is not None, 
                       end_elem is not None, code_elem is not None]):
                continue
            
            # Valores básicos
            instance_id = id_elem.text
            start_time = float(start_elem.text)
            end_time = float(end_elem.text)
            code = code_elem.text
            
            # Extraer información de labels
            jugador, equipo, periodo = extraer_informacion_label(instance)
            
            # Parsear código de ventana
            duracion_min, velocidad_kmh, tipo_evento = parsear_codigo_ventana(code)
            
            # Calcular duración
            duracion_segundos = end_time - start_time
            
            # Crear registro
            registro = {
                'id_instancia': instance_id,
                'jugador': jugador,
                'equipo': equipo,
                'periodo': periodo,
                'tipo_evento': tipo_evento,
                'codigo_completo': code,
                'duracion_ventana_min': duracion_min,
                'velocidad_minima_kmh': velocidad_kmh,
                'inicio_segundos': start_time,
                'fin_segundos': end_time,
                'duracion_segundos': duracion_segundos,
                'archivo_fuente': os.path.basename(ruta_archivo)
            }
            
            datos.append(registro)
        
        # Crear DataFrame
        df = pd.DataFrame(datos)
        print(f"    ✅ Registros extraídos: {len(df)}")
        
        # Mostrar estadísticas básicas
        if not df.empty:
            tipos_evento = df['tipo_evento'].value_counts()
            print(f"    📊 Tipos de evento: {tipos_evento.to_dict()}")
            
            if 'jugador' in df.columns:
                jugadores_unicos = df['jugador'].nunique()
                equipos_unicos = df['equipo'].nunique()
                print(f"    👥 Jugadores únicos: {jugadores_unicos}")
                print(f"    ⚽ Equipos únicos: {equipos_unicos}")
        
        return df
        
    except ET.ParseError as e:
        print(f"    ❌ Error parseando XML: {e}")
        return None
    except Exception as e:
        print(f"    ❌ Error procesando archivo: {e}")
        return None

def procesar_carpeta_xml(ruta_carpeta: str) -> str:
    """
    Procesa todos los archivos XML de una carpeta y consolida en parquet
    """
    nombre_carpeta = os.path.basename(ruta_carpeta)
    print(f"\n{'='*60}")
    print(f"📁 PROCESANDO CARPETA XML: {nombre_carpeta}")
    print(f"📂 Ruta: {ruta_carpeta}")
    print(f"{'='*60}")
    
    # Buscar archivos XML
    archivos_xml = glob.glob(os.path.join(ruta_carpeta, "*.xml"))
    
    if not archivos_xml:
        print("❌ No se encontraron archivos XML")
        return None
    
    print(f"✅ Archivos XML encontrados: {len(archivos_xml)}")
    
    # Procesar cada archivo
    dataframes_procesados = []
    archivos_exitosos = 0
    
    for i, archivo in enumerate(archivos_xml, 1):
        print(f"\n📄 Procesando ({i}/{len(archivos_xml)}): {os.path.basename(archivo)}")
        
        df = procesar_archivo_xml(archivo)
        
        if df is not None and not df.empty:
            dataframes_procesados.append(df)
            archivos_exitosos += 1
        else:
            print(f"    ⚠️  Archivo omitido (vacío o error)")
    
    if not dataframes_procesados:
        print("❌ No se pudo procesar ningún archivo XML")
        return None
    
    # Consolidar DataFrames
    print(f"\n🔗 CONSOLIDANDO {len(dataframes_procesados)} DATAFRAMES...")
    
    try:
        df_consolidado = pd.concat(dataframes_procesados, ignore_index=True, sort=False)
        print(f"✅ Consolidación exitosa: {df_consolidado.shape}")
        
        # Limpiar y optimizar datos
        print("🧹 Limpiando y optimizando datos...")
        
        # Convertir tipos de datos
        if 'id_instancia' in df_consolidado.columns:
            df_consolidado['id_instancia'] = pd.to_numeric(df_consolidado['id_instancia'], errors='ignore')
        
        for col in ['duracion_ventana_min', 'velocidad_minima_kmh', 'inicio_segundos', 'fin_segundos', 'duracion_segundos']:
            if col in df_consolidado.columns:
                df_consolidado[col] = pd.to_numeric(df_consolidado[col], errors='ignore')
        
        # Ordenar por archivo fuente e id
        df_consolidado = df_consolidado.sort_values(['archivo_fuente', 'id_instancia'], na_position='last')
        
        # Generar archivo consolidado
        archivo_consolidado = os.path.join(ruta_carpeta, f"{nombre_carpeta}_XML_CONSOLIDADO.parquet")
        df_consolidado.to_parquet(archivo_consolidado, index=False)
        print(f"💾 Archivo consolidado guardado: {os.path.basename(archivo_consolidado)}")
        
        # Estadísticas finales
        print(f"\n📊 ESTADÍSTICAS FINALES:")
        print(f"   • Total registros: {len(df_consolidado):,}")
        print(f"   • Total columnas: {len(df_consolidado.columns)}")
        print(f"   • Archivos procesados: {df_consolidado['archivo_fuente'].nunique()}")
        print(f"   • Tamaño archivo: {os.path.getsize(archivo_consolidado) / 1024 / 1024:.2f} MB")
        
        # Análisis de contenido
        print(f"\n🔍 ANÁLISIS DE CONTENIDO:")
        
        # Tipos de evento
        if 'tipo_evento' in df_consolidado.columns:
            tipos = df_consolidado['tipo_evento'].value_counts()
            print(f"   📋 Tipos de evento:")
            for tipo, count in tipos.items():
                print(f"     • {tipo}: {count:,} registros")
        
        # Jugadores únicos
        if 'jugador' in df_consolidado.columns:
            jugadores_validos = df_consolidado['jugador'].dropna()
            if len(jugadores_validos) > 0:
                print(f"   👥 Jugadores únicos: {jugadores_validos.nunique()}")
                # Mostrar algunos ejemplos
                ejemplos_jugadores = jugadores_validos.unique()[:5]
                print(f"     Ejemplos: {list(ejemplos_jugadores)}")
        
        # Equipos únicos
        if 'equipo' in df_consolidado.columns:
            equipos_validos = df_consolidado['equipo'].dropna()
            if len(equipos_validos) > 0:
                equipos_unicos = equipos_validos.unique()
                print(f"   ⚽ Equipos: {list(equipos_unicos)}")
        
        # Ventanas de velocidad
        if 'velocidad_minima_kmh' in df_consolidado.columns:
            velocidades = df_consolidado['velocidad_minima_kmh'].dropna().value_counts().sort_index()
            if len(velocidades) > 0:
                print(f"   🏃 Velocidades mínimas (km/h):")
                for vel, count in velocidades.items():
                    print(f"     • >{vel} km/h: {count:,} registros")
        
        # Duraciones de ventana
        if 'duracion_ventana_min' in df_consolidado.columns:
            duraciones = df_consolidado['duracion_ventana_min'].dropna().value_counts().sort_index()
            if len(duraciones) > 0:
                print(f"   ⏱️  Duraciones de ventana (min):")
                for dur, count in duraciones.items():
                    print(f"     • {dur} min: {count:,} registros")
        
        # Muestra de datos
        print(f"\n👀 MUESTRA DE DATOS:")
        columnas_importantes = ['jugador', 'equipo', 'tipo_evento', 'duracion_ventana_min', 'velocidad_minima_kmh']
        columnas_muestra = [col for col in columnas_importantes if col in df_consolidado.columns]
        
        if columnas_muestra:
            muestra = df_consolidado[columnas_muestra].head(3)
            for col in columnas_muestra:
                valores = muestra[col].tolist()
                print(f"   {col}: {valores}")
        
        return archivo_consolidado
        
    except Exception as e:
        print(f"❌ Error en consolidación: {e}")
        return None

def procesar_todas_las_carpetas_xml(ruta_base: str = "./VCF_Mediacoach_Data") -> dict:
    """
    Busca y procesa todas las carpetas con archivos XML
    """
    print(f"⚽ EXTRACTOR XML MEDIACOACH")
    print(f"📂 Ruta base: {ruta_base}")
    print("="*80)
    
    resultados = {
        'carpetas_procesadas': 0,
        'extracciones_exitosas': 0,
        'archivos_creados': []
    }
    
    if not os.path.exists(ruta_base):
        print(f"❌ La ruta {ruta_base} no existe")
        return resultados
    
    # Buscar carpetas con archivos XML
    carpetas_xml = []
    
    for root, dirs, files in os.walk(ruta_base):
        for dir_name in dirs:
            if any(keyword in dir_name.lower() for keyword in ['beyondstats', 'maximaexigencia']):
                ruta_completa = os.path.join(root, dir_name)
                # Verificar si tiene archivos XML
                archivos_xml = glob.glob(os.path.join(ruta_completa, "*.xml"))
                if archivos_xml:
                    carpetas_xml.append(ruta_completa)
    
    print(f"🔍 Carpetas con XML encontradas: {len(carpetas_xml)}")
    for carpeta in carpetas_xml:
        archivos_count = len(glob.glob(os.path.join(carpeta, "*.xml")))
        print(f"   📁 {os.path.basename(carpeta)}: {archivos_count} archivos XML")
    
    if not carpetas_xml:
        print("❌ No se encontraron carpetas con archivos XML")
        print("💡 Busca carpetas: beyondstats, maximaexigencia, maximaexigenciarevisada")
        return resultados
    
    # Procesar cada carpeta
    for carpeta in carpetas_xml:
        archivo_consolidado = procesar_carpeta_xml(carpeta)
        resultados['carpetas_procesadas'] += 1
        
        if archivo_consolidado:
            resultados['extracciones_exitosas'] += 1
            resultados['archivos_creados'].append(archivo_consolidado)
    
    return resultados

def generar_reporte_xml(resultados: dict):
    """
    Genera un reporte final de la extracción XML
    """
    print(f"\n{'='*80}")
    print(f"📋 REPORTE FINAL DE EXTRACCIÓN XML")
    print(f"{'='*80}")
    
    print(f"📊 Estadísticas:")
    print(f"   • Carpetas procesadas: {resultados['carpetas_procesadas']}")
    print(f"   • Extracciones exitosas: {resultados['extracciones_exitosas']}")
    print(f"   • Archivos parquet creados: {len(resultados['archivos_creados'])}")
    
    if resultados['archivos_creados']:
        print(f"\n📁 Archivos consolidados creados:")
        for archivo in resultados['archivos_creados']:
            tamaño_mb = os.path.getsize(archivo) / 1024 / 1024
            print(f"   ✅ {os.path.basename(archivo)}")
            print(f"      📂 Ubicación: {archivo}")
            print(f"      📏 Tamaño: {tamaño_mb:.2f} MB")
    
    tasa_exito = (resultados['extracciones_exitosas'] / resultados['carpetas_procesadas'] * 100) if resultados['carpetas_procesadas'] > 0 else 0
    print(f"\n🎯 Tasa de éxito: {tasa_exito:.1f}%")
    
    if resultados['archivos_creados']:
        print(f"\n💡 DATOS EXTRAÍDOS:")
        print(f"   📊 maximaexigenciarevisada: Ventanas de exigencia física")
        print(f"      • Velocidades: >0, >21, >24 km/h")
        print(f"      • Duraciones: 1, 3, 5 minutos")
        print(f"      • Jugadores y equipos")
        print(f"      • Tiempos de inicio/fin")
        
        print(f"\n📋 PRÓXIMOS PASOS:")
        print(f"   1. Revisa los archivos *_XML_CONSOLIDADO.parquet")
        print(f"   2. Analiza las ventanas de máxima exigencia por jugador")
        print(f"   3. Cruza con datos de otros archivos (CSV, Excel)")
        print(f"   4. ¡Datos listos para análisis de rendimiento físico!")

def main():
    """Función principal"""
    print("⚽ EXTRACTOR XML MEDIACOACH")
    print("="*50)
    print("📊 Extrae datos de archivos XML (maximaexigenciarevisada, beyondstats)")
    print("🔄 Convierte a parquet consolidado")
    print("📈 Datos de exigencia física y estadísticas avanzadas")
    print("="*50)
    
    # Preguntar confirmación
    respuesta = input("\n¿Proceder con la extracción de archivos XML? (s/n): ").lower().strip()
    
    if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
        # Ejecutar extracción
        resultados = procesar_todas_las_carpetas_xml()
        
        # Generar reporte
        generar_reporte_xml(resultados)
        
        print(f"\n🎉 ¡EXTRACCIÓN XML COMPLETADA!")
        
    else:
        print("❌ Extracción cancelada")

if __name__ == "__main__":
    main()