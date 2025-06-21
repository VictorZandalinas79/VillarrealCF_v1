#!/usr/bin/env python3
"""
Extractor de archivos XLSX de MediaCoach (Rendimiento y Máxima Exigencia) a Parquet

ESTRUCTURA IDENTIFICADA:
- Información del partido en columna E: "LALIGA EA SPORTS | Temporada 2024 - 2025 | J1 | Villarreal CF 2 - 2 Atlético de Madrid (2024-08-19)"
- Headers de métricas en fila 5
- Datos de rendimiento individual por jugador
- Múltiples métricas físicas y técnicas

DATOS EXTRAÍDOS:
- Información del partido (liga, temporada, jornada, equipos, resultado, fecha)
- Datos individuales de jugadores
- Métricas de rendimiento físico y técnico
- Consolidación de múltiples partidos
"""

import pandas as pd
import openpyxl
import os
import glob
import re
from typing import List, Dict, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

def extraer_informacion_partido(df: pd.DataFrame) -> Dict[str, str]:
    """
    Extrae información del partido desde la columna E del DataFrame
    Formato esperado: "LALIGA EA SPORTS | Temporada 2024 - 2025 | J1 | Villarreal CF 2 - 2 Atlético de Madrid (2024-08-19)"
    """
    info_partido = {
        'liga': None,
        'temporada': None,
        'jornada': None,
        'equipo_local': None,
        'equipo_visitante': None,
        'goles_local': None,
        'goles_visitante': None,
        'fecha': None,
        'partido_completo': None
    }
    
    # Buscar en las primeras filas y columna E (índice 4)
    for i in range(min(10, len(df))):
        if i < len(df) and len(df.columns) > 4:
            valor = df.iloc[i, 4]  # Columna E (índice 4)
            
            if pd.notna(valor) and isinstance(valor, str) and '|' in valor:
                print(f"    🔍 Información del partido encontrada en fila {i+1}")
                texto = valor.strip()
                info_partido['partido_completo'] = texto
                
                # Dividir por |
                partes = [parte.strip() for parte in texto.split('|')]
                
                if len(partes) >= 4:
                    # Parte 1: Liga
                    info_partido['liga'] = partes[0]
                    
                    # Parte 2: Temporada (extraer años)
                    temporada_match = re.search(r'Temporada (\d{4}\s*-\s*\d{4})', partes[1])
                    if temporada_match:
                        info_partido['temporada'] = temporada_match.group(1)
                    
                    # Parte 3: Jornada
                    jornada_match = re.search(r'(J\d+)', partes[2])
                    if jornada_match:
                        info_partido['jornada'] = jornada_match.group(1)
                    
                    # Parte 4: Partido con resultado y fecha
                    if len(partes) >= 4:
                        partido_parte = partes[3]
                        
                        # Extraer fecha (entre paréntesis al final)
                        fecha_match = re.search(r'\((\d{4}-\d{2}-\d{2})\)', partido_parte)
                        if fecha_match:
                            info_partido['fecha'] = fecha_match.group(1)
                            # Remover la fecha del texto del partido
                            partido_sin_fecha = re.sub(r'\s*\(\d{4}-\d{2}-\d{2}\)', '', partido_parte).strip()
                        else:
                            partido_sin_fecha = partido_parte.strip()
                        
                        # Extraer equipos y resultado: "Villarreal CF 2 - 2 Atlético de Madrid"
                        resultado_match = re.search(r'^(.+?)\s+(\d+)\s*-\s*(\d+)\s+(.+)$', partido_sin_fecha)
                        if resultado_match:
                            info_partido['equipo_local'] = resultado_match.group(1).strip()
                            info_partido['goles_local'] = int(resultado_match.group(2))
                            info_partido['goles_visitante'] = int(resultado_match.group(3))
                            info_partido['equipo_visitante'] = resultado_match.group(4).strip()
                
                break
    
    return info_partido

def encontrar_fila_headers(df: pd.DataFrame) -> Optional[int]:
    """
    Encuentra la fila que contiene los headers (debería ser la fila 5, índice 4)
    """
    # Probar fila 5 primero (índice 4)
    if len(df) > 4:
        fila_candidata = df.iloc[4]
        # Verificar si tiene valores que parecen headers
        valores_no_vacios = fila_candidata.dropna()
        if len(valores_no_vacios) > 5:  # Debe tener al menos 5 columnas con datos
            # Verificar si contiene palabras típicas de headers
            texto_fila = ' '.join([str(v).lower() for v in valores_no_vacios if isinstance(v, str)])
            palabras_header = ['dorsal', 'nombre', 'apellido', 'demarcacion', 'velocidad', 'distancia']
            if any(palabra in texto_fila for palabra in palabras_header):
                return 4  # Fila 5 (índice 4)
    
    # Buscar en un rango más amplio si no se encuentra en fila 5
    for i in range(min(15, len(df))):
        fila_candidata = df.iloc[i]
        valores_no_vacios = fila_candidata.dropna()
        
        if len(valores_no_vacios) > 5:
            texto_fila = ' '.join([str(v).lower() for v in valores_no_vacios if isinstance(v, str)])
            palabras_header = ['dorsal', 'nombre', 'apellido', 'demarcacion', 'velocidad', 'distancia']
            if any(palabra in texto_fila for palabra in palabras_header):
                return i
    
    return None

def limpiar_nombre_columna(nombre: str) -> str:
    """
    Limpia y normaliza nombres de columnas
    """
    if pd.isna(nombre):
        return 'columna_sin_nombre'
    
    nombre = str(nombre).strip()
    
    # Reemplazar caracteres problemáticos
    nombre = re.sub(r'[^\w\s\(\)\%\-]', '_', nombre)
    nombre = re.sub(r'\s+', '_', nombre)
    nombre = re.sub(r'_+', '_', nombre)
    nombre = nombre.strip('_')
    
    # Si queda vacío, dar nombre genérico
    if not nombre:
        return 'columna_sin_nombre'
    
    return nombre

def procesar_archivo_xlsx(ruta_archivo: str) -> pd.DataFrame:
    """
    Procesa un archivo XLSX de MediaCoach y devuelve un DataFrame
    """
    print(f"  📄 Procesando: {os.path.basename(ruta_archivo)}")
    
    try:
        # Leer archivo Excel sin headers inicialmente
        df_raw = pd.read_excel(ruta_archivo, header=None, engine='openpyxl')
        print(f"    📊 Dimensiones brutas: {df_raw.shape}")
        
        if df_raw.empty:
            print(f"    ⚠️  Archivo vacío")
            return None
        
        # Extraer información del partido
        info_partido = extraer_informacion_partido(df_raw)
        
        if info_partido['partido_completo']:
            print(f"    ⚽ Partido: {info_partido['equipo_local']} vs {info_partido['equipo_visitante']}")
            print(f"    📅 Fecha: {info_partido['fecha']} | Jornada: {info_partido['jornada']}")
        else:
            print(f"    ⚠️  No se pudo extraer información del partido")
        
        # Encontrar fila de headers
        fila_headers = encontrar_fila_headers(df_raw)
        
        if fila_headers is None:
            print(f"    ❌ No se pudo encontrar la fila de headers")
            return None
        
        print(f"    📋 Headers encontrados en fila {fila_headers + 1}")
        
        # Extraer headers y limpiarlos
        headers_raw = df_raw.iloc[fila_headers].tolist()
        headers = []
        
        for i, header in enumerate(headers_raw):
            if pd.notna(header) and str(header).strip():
                headers.append(limpiar_nombre_columna(header))
            else:
                headers.append(f'columna_{i+1}')
        
        print(f"    🏷️  Columnas detectadas: {len(headers)}")
        
        # Extraer datos (desde la fila siguiente a los headers)
        datos_inicio = fila_headers + 1
        df_datos = df_raw.iloc[datos_inicio:].copy()
        
        # Ajustar número de columnas
        if len(headers) > len(df_datos.columns):
            headers = headers[:len(df_datos.columns)]
        elif len(headers) < len(df_datos.columns):
            for i in range(len(headers), len(df_datos.columns)):
                headers.append(f'columna_{i+1}')
        
        # Asignar headers
        df_datos.columns = headers
        
        # Eliminar filas completamente vacías
        df_datos = df_datos.dropna(how='all')
        
        if df_datos.empty:
            print(f"    ⚠️  No hay datos después de los headers")
            return None
        
        print(f"    ✅ Datos extraídos: {df_datos.shape}")
        
        # Añadir información del partido a cada fila
        for key, value in info_partido.items():
            df_datos[f'partido_{key}'] = value
        
        # Añadir metadatos
        df_datos['archivo_fuente'] = os.path.basename(ruta_archivo)
        df_datos['fila_headers_original'] = fila_headers + 1
        
        # Mostrar muestra de columnas
        columnas_importantes = ['dorsal', 'nombre', 'apellido', 'demarcacion']
        columnas_encontradas = [col for col in df_datos.columns 
                              if any(palabra in col.lower() for palabra in columnas_importantes)]
        
        if columnas_encontradas:
            print(f"    👥 Columnas clave: {columnas_encontradas[:5]}")
            
            # Mostrar algunos jugadores como ejemplo
            if not df_datos.empty:
                filas_con_datos = df_datos.dropna(subset=columnas_encontradas[:2], how='all')
                if not filas_con_datos.empty:
                    jugadores_ejemplo = filas_con_datos.head(3)
                    for col in columnas_encontradas[:3]:
                        if col in jugadores_ejemplo.columns:
                            valores = jugadores_ejemplo[col].dropna().tolist()[:3]
                            if valores:
                                print(f"      {col}: {valores}")
        
        return df_datos
        
    except Exception as e:
        print(f"    ❌ Error procesando archivo: {e}")
        return None

def procesar_carpeta_xlsx(ruta_carpeta: str) -> str:
    """
    Procesa todos los archivos XLSX de una carpeta y consolida en parquet
    """
    nombre_carpeta = os.path.basename(ruta_carpeta)
    print(f"\n{'='*60}")
    print(f"📁 PROCESANDO CARPETA XLSX: {nombre_carpeta}")
    print(f"📂 Ruta: {ruta_carpeta}")
    print(f"{'='*60}")
    
    # Buscar archivos XLSX
    archivos_xlsx = glob.glob(os.path.join(ruta_carpeta, "*.xlsx"))
    
    if not archivos_xlsx:
        print("❌ No se encontraron archivos XLSX")
        return None
    
    print(f"✅ Archivos XLSX encontrados: {len(archivos_xlsx)}")
    for archivo in archivos_xlsx[:5]:  # Mostrar algunos ejemplos
        print(f"   📄 {os.path.basename(archivo)}")
    if len(archivos_xlsx) > 5:
        print(f"   ... y {len(archivos_xlsx) - 5} más")
    
    # Procesar cada archivo
    dataframes_procesados = []
    archivos_exitosos = 0
    
    for i, archivo in enumerate(archivos_xlsx, 1):
        print(f"\n📄 Procesando ({i}/{len(archivos_xlsx)}): {os.path.basename(archivo)}")
        
        df = procesar_archivo_xlsx(archivo)
        
        if df is not None and not df.empty:
            dataframes_procesados.append(df)
            archivos_exitosos += 1
        else:
            print(f"    ⚠️  Archivo omitido (vacío o error)")
    
    if not dataframes_procesados:
        print("❌ No se pudo procesar ningún archivo XLSX")
        return None
    
    # Consolidar DataFrames
    print(f"\n🔗 CONSOLIDANDO {len(dataframes_procesados)} DATAFRAMES...")
    
    try:
        # Obtener todas las columnas únicas
        todas_las_columnas = set()
        for df in dataframes_procesados:
            todas_las_columnas.update(df.columns)
        
        print(f"    📊 Total de columnas únicas: {len(todas_las_columnas)}")
        
        # Asegurar que todos los DataFrames tengan las mismas columnas
        for i, df in enumerate(dataframes_procesados):
            for col in todas_las_columnas:
                if col not in df.columns:
                    df[col] = None
            
            # Reordenar columnas consistentemente
            dataframes_procesados[i] = df[sorted(todas_las_columnas)]
        
        # Concatenar
        df_consolidado = pd.concat(dataframes_procesados, ignore_index=True, sort=False)
        print(f"✅ Consolidación exitosa: {df_consolidado.shape}")
        
        # Limpiar y optimizar datos
        print("🧹 Limpiando y optimizando datos...")
        
        # Convertir tipos numéricos donde sea posible
        for col in df_consolidado.columns:
            if col.startswith('partido_') or col in ['archivo_fuente', 'fila_headers_original']:
                continue
            
            # Intentar convertir a numérico
            if df_consolidado[col].dtype == 'object':
                numeric_series = pd.to_numeric(df_consolidado[col], errors='ignore')
                if not numeric_series.equals(df_consolidado[col]):
                    df_consolidado[col] = numeric_series
        
        # Ordenar por partido y archivo
        columnas_orden = ['partido_fecha', 'partido_jornada', 'archivo_fuente']
        columnas_orden_existentes = [col for col in columnas_orden if col in df_consolidado.columns]
        
        if columnas_orden_existentes:
            df_consolidado = df_consolidado.sort_values(columnas_orden_existentes, na_position='last')
        
        # Generar archivo consolidado
        archivo_consolidado = os.path.join(ruta_carpeta, f"{nombre_carpeta}_XLSX_CONSOLIDADO.parquet")
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
        
        # Partidos únicos
        if 'partido_completo' in df_consolidado.columns:
            partidos_unicos = df_consolidado['partido_completo'].dropna().nunique()
            print(f"   ⚽ Partidos únicos: {partidos_unicos}")
            
            # Mostrar algunos ejemplos
            ejemplos_partidos = df_consolidado['partido_completo'].dropna().unique()[:3]
            for partido in ejemplos_partidos:
                print(f"     • {partido}")
        
        # Jugadores únicos (si hay columna nombre)
        columnas_nombre = [col for col in df_consolidado.columns 
                          if 'nombre' in col.lower() and not col.startswith('partido_')]
        
        if columnas_nombre:
            col_nombre = columnas_nombre[0]
            jugadores_validos = df_consolidado[col_nombre].dropna()
            if len(jugadores_validos) > 0:
                print(f"   👥 Jugadores únicos: {jugadores_validos.nunique()}")
                ejemplos_jugadores = jugadores_validos.unique()[:5]
                print(f"     Ejemplos: {list(ejemplos_jugadores)}")
        
        # Temporadas y jornadas
        if 'partido_temporada' in df_consolidado.columns:
            temporadas = df_consolidado['partido_temporada'].dropna().unique()
            print(f"   📅 Temporadas: {list(temporadas)}")
        
        if 'partido_jornada' in df_consolidado.columns:
            jornadas = df_consolidado['partido_jornada'].dropna().unique()
            print(f"   🗓️  Jornadas: {sorted(list(jornadas))}")
        
        # Columnas de métricas principales
        columnas_metricas = [col for col in df_consolidado.columns 
                           if not col.startswith('partido_') and col not in ['archivo_fuente', 'fila_headers_original']
                           and any(palabra in col.lower() for palabra in ['velocidad', 'distancia', 'intensidad', 'vcs'])]
        
        if columnas_metricas:
            print(f"   📈 Métricas principales: {len(columnas_metricas)}")
            for metrica in columnas_metricas[:5]:
                print(f"     • {metrica}")
            if len(columnas_metricas) > 5:
                print(f"     ... y {len(columnas_metricas) - 5} más")
        
        return archivo_consolidado
        
    except Exception as e:
        print(f"❌ Error en consolidación: {e}")
        return None

def procesar_todas_las_carpetas_xlsx(ruta_base: str = "./VCF_Mediacoach_Data") -> dict:
    """
    Busca y procesa todas las carpetas con archivos XLSX
    """
    print(f"⚽ EXTRACTOR XLSX MEDIACOACH")
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
    
    # Buscar carpetas con archivos XLSX
    carpetas_xlsx = []
    
    for root, dirs, files in os.walk(ruta_base):
        for dir_name in dirs:
            if any(keyword in dir_name.lower() for keyword in ['rendimiento', 'maxima', 'exigencia']):
                ruta_completa = os.path.join(root, dir_name)
                # Verificar si tiene archivos XLSX
                archivos_xlsx = glob.glob(os.path.join(ruta_completa, "*.xlsx"))
                if archivos_xlsx:
                    carpetas_xlsx.append(ruta_completa)
    
    print(f"🔍 Carpetas con XLSX encontradas: {len(carpetas_xlsx)}")
    for carpeta in carpetas_xlsx:
        archivos_count = len(glob.glob(os.path.join(carpeta, "*.xlsx")))
        print(f"   📁 {os.path.basename(carpeta)}: {archivos_count} archivos XLSX")
    
    if not carpetas_xlsx:
        print("❌ No se encontraron carpetas con archivos XLSX")
        print("💡 Busca carpetas: rendimiento, maxima, exigencia")
        return resultados
    
    # Procesar cada carpeta
    for carpeta in carpetas_xlsx:
        archivo_consolidado = procesar_carpeta_xlsx(carpeta)
        resultados['carpetas_procesadas'] += 1
        
        if archivo_consolidado:
            resultados['extracciones_exitosas'] += 1
            resultados['archivos_creados'].append(archivo_consolidado)
    
    return resultados

def generar_reporte_xlsx(resultados: dict):
    """
    Genera un reporte final de la extracción XLSX
    """
    print(f"\n{'='*80}")
    print(f"📋 REPORTE FINAL DE EXTRACCIÓN XLSX")
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
        print(f"   📊 Rendimiento individual por jugador")
        print(f"      • Datos básicos: Dorsal, Nombre, Demarcación")
        print(f"      • Métricas físicas: Velocidad, Distancia, VCS")
        print(f"      • Información del partido: Liga, Temporada, Jornada")
        print(f"      • Resultados y fechas de partidos")
        
        print(f"\n📋 PRÓXIMOS PASOS:")
        print(f"   1. Revisa los archivos *_XLSX_CONSOLIDADO.parquet")
        print(f"   2. Analiza rendimiento individual por jugador")
        print(f"   3. Compara métricas entre partidos y temporadas")
        print(f"   4. Cruza con datos XML (exigencia física y acciones tácticas)")
        print(f"   5. ¡Datos completos para análisis de rendimiento!")

def main():
    """Función principal"""
    print("⚽ EXTRACTOR XLSX MEDIACOACH")
    print("="*50)
    print("📊 Extrae datos de archivos XLSX (Rendimiento y Máxima Exigencia)")
    print("🔄 Convierte a parquet consolidado")
    print("📈 Datos de rendimiento individual por jugador")
    print("⚽ Información completa de partidos")
    print("="*50)
    
    # Preguntar confirmación
    respuesta = input("\n¿Proceder con la extracción de archivos XLSX? (s/n): ").lower().strip()
    
    if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
        # Ejecutar extracción
        resultados = procesar_todas_las_carpetas_xlsx()
        
        # Generar reporte
        generar_reporte_xlsx(resultados)
        
        print(f"\n🎉 ¡EXTRACCIÓN XLSX COMPLETADA!")
        
    else:
        print("❌ Extracción cancelada")

if __name__ == "__main__":
    main()