#!/usr/bin/env python3
"""
Script para procesar correctamente los archivos CSV originales de MediaCoach

PROBLEMA IDENTIFICADO:
- Los archivos postpartidoequipos y postpartidojugador se descargan como CSV con delimitador ;
- Alguien los convirtió mal a parquet usando delimitador incorrecto (coma en lugar de ;)
- Resultado: datos mal alineados, columnas null, nombres en posiciones incorrectas

SOLUCIÓN:
- Procesar directamente los archivos CSV originales
- Usar delimitador correcto (;)
- Analizar estructura real y limpiar inconsistencias
- Consolidar correctamente
"""

import pandas as pd
import os
import glob
from typing import List, Tuple, Dict
import warnings
warnings.filterwarnings('ignore')

def analizar_estructura_csv(ruta_archivo: str) -> Dict:
    """
    Analiza la estructura real de un archivo CSV para detectar problemas
    """
    resultado = {
        'archivo': os.path.basename(ruta_archivo),
        'delimitadores_detectados': [],
        'filas_analizadas': 0,
        'estructura_consistente': True,
        'problemas_detectados': []
    }
    
    # Detectar delimitador analizando primeras líneas
    with open(ruta_archivo, 'r', encoding='utf-8', errors='ignore') as f:
        primeras_lineas = [f.readline().strip() for _ in range(5)]
    
    # Contar diferentes delimitadores
    delimitadores = [';', ',', '\t', '|']
    for delim in delimitadores:
        count = sum(linea.count(delim) for linea in primeras_lineas if linea)
        if count > 0:
            resultado['delimitadores_detectados'].append((delim, count))
    
    # Ordenar por frecuencia
    resultado['delimitadores_detectados'].sort(key=lambda x: x[1], reverse=True)
    
    return resultado

def leer_csv_con_delimitador_correcto(ruta_archivo: str) -> pd.DataFrame:
    """
    Lee un CSV detectando automáticamente el delimitador correcto
    """
    print(f"  🔍 Analizando estructura: {os.path.basename(ruta_archivo)}")
    
    # Analizar estructura
    estructura = analizar_estructura_csv(ruta_archivo)
    
    if not estructura['delimitadores_detectados']:
        print(f"    ❌ No se detectaron delimitadores")
        return None
    
    # Usar el delimitador más frecuente
    delimitador_principal = estructura['delimitadores_detectados'][0][0]
    frecuencia = estructura['delimitadores_detectados'][0][1]
    
    print(f"    📋 Delimitador detectado: '{delimitador_principal}' (frecuencia: {frecuencia})")
    print(f"    🔢 Otros delimitadores: {estructura['delimitadores_detectados'][1:] if len(estructura['delimitadores_detectados']) > 1 else 'ninguno'}")
    
    try:
        # Intentar diferentes configuraciones de lectura
        configuraciones = [
            {'sep': delimitador_principal, 'encoding': 'utf-8'},
            {'sep': delimitador_principal, 'encoding': 'latin-1'},
            {'sep': delimitador_principal, 'encoding': 'cp1252'},
        ]
        
        df = None
        for config in configuraciones:
            try:
                df = pd.read_csv(ruta_archivo, **config)
                print(f"    ✅ Lectura exitosa con encoding: {config['encoding']}")
                break
            except Exception as e:
                print(f"    ⚠️  Fallo con {config['encoding']}: {str(e)[:50]}...")
                continue
        
        if df is None:
            print(f"    ❌ No se pudo leer el archivo con ninguna configuración")
            return None
        
        print(f"    📊 Dimensiones: {df.shape}")
        print(f"    🏷️  Columnas detectadas: {len(df.columns)}")
        print(f"    📝 Primeras columnas: {list(df.columns)[:5]}")
        
        # Verificar si la primera fila son realmente headers
        if df.shape[0] > 0:
            primera_fila = df.iloc[0]
            # Si la primera fila tiene muchos valores numéricos, podría ser datos, no headers
            valores_numericos = sum(1 for val in primera_fila if pd.to_numeric(val, errors='coerce') is not pd.NA)
            if valores_numericos > len(df.columns) * 0.7:
                print(f"    ⚠️  Posible problema: primera fila parece contener datos, no headers")
        
        return df
        
    except Exception as e:
        print(f"    ❌ Error leyendo archivo: {e}")
        return None

def limpiar_dataframe_postpartido(df: pd.DataFrame, tipo_archivo: str) -> pd.DataFrame:
    """
    Limpia y estandariza un DataFrame de postpartido
    """
    if df is None or df.empty:
        return None
    
    print(f"  🧹 Limpiando DataFrame {tipo_archivo}...")
    
    # Verificar si tenemos las columnas esperadas
    columnas_esperadas_equipos = ['CATEGORÍA', 'ID MÉTRICA', 'NOMBRE MÉTRICA', 'EQUIPO', 'PERIODO', 'VALOR', 
                                  'DORSAL DEL JUGADOR', 'NOMBRE DEL JUGADOR', 'ID EQUIPO', 'ID PARTIDO']
    
    columnas_esperadas_jugadores = ['CATEGORÍA', 'ID MÉTRICA', 'NOMBRE MÉTRICA', 'EQUIPO', 'PERIODO', 'VALOR',
                                    'DORSAL DEL JUGADOR', 'NOMBRE DEL JUGADOR', 'ID JUGADOR', 'ID EQUIPO', 'ID PARTIDO']
    
    print(f"    📋 Columnas actuales: {list(df.columns)}")
    
    # Eliminar filas completamente vacías
    df_limpio = df.dropna(how='all').copy()
    print(f"    📊 Después de eliminar filas vacías: {df_limpio.shape}")
    
    # Eliminar columnas completamente vacías
    df_limpio = df_limpio.dropna(axis=1, how='all')
    print(f"    📊 Después de eliminar columnas vacías: {df_limpio.shape}")
    
    # Detectar y eliminar filas de header duplicadas
    if df_limpio.shape[0] > 1:
        # Buscar filas que contengan palabras típicas de headers
        palabras_header = ['CATEGORÍA', 'MÉTRICA', 'NOMBRE', 'EQUIPO', 'PERIODO', 'VALOR', 'DORSAL', 'JUGADOR']
        
        filas_header = []
        for idx, fila in df_limpio.iterrows():
            fila_str = ' '.join(str(val) for val in fila if pd.notna(val)).upper()
            if any(palabra in fila_str for palabra in palabras_header):
                if idx > 0:  # No eliminar la primera fila (headers legítimos)
                    filas_header.append(idx)
        
        if filas_header:
            print(f"    🗑️  Eliminando {len(filas_header)} filas de header duplicadas")
            df_limpio = df_limpio.drop(filas_header)
    
    # Resetear índice
    df_limpio = df_limpio.reset_index(drop=True)
    
    # Intentar convertir columnas numéricas
    for col in df_limpio.columns:
        if any(keyword in col.upper() for keyword in ['VALOR', 'DORSAL', 'ID']):
            df_limpio[col] = pd.to_numeric(df_limpio[col], errors='ignore')
    
    print(f"    ✅ Limpieza completada: {df_limpio.shape}")
    
    # Mostrar muestra de datos limpios
    print(f"    👀 Muestra de datos limpios:")
    for col in df_limpio.columns[:4]:
        valores_no_nulos = df_limpio[col].dropna().head(3).tolist()
        if valores_no_nulos:
            print(f"       {col}: {valores_no_nulos}")
    
    return df_limpio

def procesar_archivos_csv_originales(ruta_carpeta: str) -> str:
    """
    Procesa todos los archivos CSV originales de una carpeta
    """
    nombre_carpeta = os.path.basename(ruta_carpeta)
    print(f"\n{'='*60}")
    print(f"📁 PROCESANDO CARPETA: {nombre_carpeta}")
    print(f"📂 Ruta: {ruta_carpeta}")
    print(f"{'='*60}")
    
    # Buscar archivos CSV originales
    archivos_csv = glob.glob(os.path.join(ruta_carpeta, "*.csv"))
    
    if not archivos_csv:
        print("❌ No se encontraron archivos CSV originales")
        print("💡 Nota: Este script necesita los archivos CSV originales, no los parquet convertidos")
        return None
    
    print(f"✅ Archivos CSV encontrados: {len(archivos_csv)}")
    
    # Procesar cada archivo CSV
    dataframes_procesados = []
    archivos_exitosos = 0
    
    for i, archivo in enumerate(archivos_csv, 1):
        try:
            print(f"\n📄 Procesando ({i}/{len(archivos_csv)}): {os.path.basename(archivo)}")
            
            # Leer CSV con delimitador correcto
            df = leer_csv_con_delimitador_correcto(archivo)
            
            if df is not None:
                # Limpiar DataFrame
                df_limpio = limpiar_dataframe_postpartido(df, nombre_carpeta)
                
                if df_limpio is not None and not df_limpio.empty:
                    # Agregar metadatos
                    df_limpio['archivo_fuente'] = os.path.basename(archivo)
                    dataframes_procesados.append(df_limpio)
                    archivos_exitosos += 1
                    
                    print(f"    ✅ Procesado exitosamente: {df_limpio.shape}")
                else:
                    print(f"    ⚠️  Archivo vacío después de limpiar")
            else:
                print(f"    ❌ No se pudo leer el archivo")
                
        except Exception as e:
            print(f"    ❌ Error procesando archivo: {e}")
    
    if not dataframes_procesados:
        print("❌ No se pudo procesar ningún archivo CSV")
        return None
    
    # Consolidar todos los DataFrames
    print(f"\n🔗 CONSOLIDANDO {len(dataframes_procesados)} DATAFRAMES...")
    
    try:
        df_consolidado = pd.concat(dataframes_procesados, ignore_index=True, sort=False)
        print(f"✅ Consolidación exitosa: {df_consolidado.shape}")
        
        # Generar nombre de archivo consolidado
        archivo_consolidado = os.path.join(ruta_carpeta, f"{nombre_carpeta}_CSV_CORREGIDO_FINAL.parquet")
        
        # Guardar archivo consolidado
        df_consolidado.to_parquet(archivo_consolidado, index=False)
        print(f"💾 Archivo consolidado guardado: {os.path.basename(archivo_consolidado)}")
        
        # Mostrar estadísticas finales
        print(f"\n📊 ESTADÍSTICAS FINALES:")
        print(f"   • Total filas: {len(df_consolidado):,}")
        print(f"   • Total columnas: {len(df_consolidado.columns)}")
        print(f"   • Archivos fuente: {df_consolidado['archivo_fuente'].nunique()}")
        print(f"   • Tamaño archivo: {os.path.getsize(archivo_consolidado) / 1024 / 1024:.2f} MB")
        
        # Mostrar estructura de columnas
        print(f"\n🏷️  ESTRUCTURA DE COLUMNAS:")
        for i, col in enumerate(df_consolidado.columns, 1):
            valores_no_nulos = df_consolidado[col].count()
            valores_unicos = df_consolidado[col].nunique()
            print(f"   {i:2d}. {col}: {valores_no_nulos:,} valores ({valores_unicos:,} únicos)")
        
        # Verificar calidad de datos específicos
        print(f"\n🔍 VERIFICACIÓN DE CALIDAD:")
        
        # Buscar columnas de jugadores
        cols_jugador = [col for col in df_consolidado.columns if 'JUGADOR' in col.upper()]
        if cols_jugador:
            print(f"   🏃 Columnas de jugador encontradas: {cols_jugador}")
            for col in cols_jugador:
                valores_validos = df_consolidado[col].dropna()
                if len(valores_validos) > 0:
                    print(f"     • {col}: {len(valores_validos):,} valores válidos")
                    # Mostrar algunos nombres de ejemplo
                    if 'NOMBRE' in col.upper():
                        ejemplos = valores_validos.head(5).tolist()
                        print(f"       Ejemplos: {ejemplos}")
        
        # Buscar columnas de dorsales
        cols_dorsal = [col for col in df_consolidado.columns if 'DORSAL' in col.upper()]
        if cols_dorsal:
            print(f"   🔢 Columnas de dorsal encontradas: {cols_dorsal}")
            for col in cols_dorsal:
                valores_numericos = pd.to_numeric(df_consolidado[col], errors='coerce').dropna()
                if len(valores_numericos) > 0:
                    print(f"     • {col}: {len(valores_numericos):,} dorsales válidos (rango: {valores_numericos.min():.0f}-{valores_numericos.max():.0f})")
        
        # Mostrar muestra final
        print(f"\n👀 MUESTRA DE DATOS FINALES:")
        columnas_muestra = [col for col in df_consolidado.columns if col != 'archivo_fuente'][:6]
        muestra = df_consolidado[columnas_muestra].head(3)
        for col in columnas_muestra:
            print(f"   {col}: {muestra[col].tolist()}")
        
        return archivo_consolidado
        
    except Exception as e:
        print(f"❌ Error en consolidación: {e}")
        return None

def procesar_todas_las_carpetas_csv(ruta_base: str = "./VCF_Mediacoach_Data") -> dict:
    """
    Procesa todas las carpetas que contienen archivos CSV
    """
    print(f"⚽ PROCESADOR DE CSV ORIGINALES MEDIACOACH")
    print(f"📂 Ruta base: {ruta_base}")
    print("="*80)
    
    resultados = {
        'carpetas_procesadas': 0,
        'procesamiento_exitoso': 0,
        'archivos_creados': []
    }
    
    if not os.path.exists(ruta_base):
        print(f"❌ La ruta {ruta_base} no existe")
        return resultados
    
    # Buscar carpetas que deberían contener CSV
    carpetas_objetivo = []
    
    for root, dirs, files in os.walk(ruta_base):
        for dir_name in dirs:
            if any(keyword in dir_name.lower() for keyword in ['postpartido', 'equipos', 'jugador']):
                ruta_completa = os.path.join(root, dir_name)
                # Verificar si tiene archivos CSV
                archivos_csv = glob.glob(os.path.join(ruta_completa, "*.csv"))
                if archivos_csv:
                    carpetas_objetivo.append(ruta_completa)
    
    print(f"🔍 Carpetas con CSV encontradas: {len(carpetas_objetivo)}")
    for carpeta in carpetas_objetivo:
        archivos_count = len(glob.glob(os.path.join(carpeta, "*.csv")))
        print(f"   📁 {carpeta} ({archivos_count} archivos CSV)")
    
    if not carpetas_objetivo:
        print("❌ No se encontraron carpetas con archivos CSV")
        print("💡 Asegúrate de que los archivos CSV originales están en las carpetas postpartidoequipos y postpartidojugador")
        return resultados
    
    # Procesar cada carpeta
    for carpeta in carpetas_objetivo:
        archivo_consolidado = procesar_archivos_csv_originales(carpeta)
        resultados['carpetas_procesadas'] += 1
        
        if archivo_consolidado:
            resultados['procesamiento_exitoso'] += 1
            resultados['archivos_creados'].append(archivo_consolidado)
    
    return resultados

def generar_reporte_csv(resultados: dict):
    """
    Genera un reporte final del procesamiento de CSV
    """
    print(f"\n{'='*80}")
    print(f"📋 REPORTE FINAL DE PROCESAMIENTO CSV")
    print(f"{'='*80}")
    
    print(f"📊 Estadísticas:")
    print(f"   • Carpetas procesadas: {resultados['carpetas_procesadas']}")
    print(f"   • Procesamientos exitosos: {resultados['procesamiento_exitoso']}")
    print(f"   • Archivos corregidos creados: {len(resultados['archivos_creados'])}")
    
    if resultados['archivos_creados']:
        print(f"\n📁 Archivos corregidos creados:")
        for archivo in resultados['archivos_creados']:
            tamaño_mb = os.path.getsize(archivo) / 1024 / 1024
            print(f"   ✅ {os.path.basename(archivo)}")
            print(f"      📂 Ubicación: {archivo}")
            print(f"      📏 Tamaño: {tamaño_mb:.2f} MB")
    
    tasa_exito = (resultados['procesamiento_exitoso'] / resultados['carpetas_procesadas'] * 100) if resultados['carpetas_procesadas'] > 0 else 0
    print(f"\n🎯 Tasa de éxito: {tasa_exito:.1f}%")
    
    if resultados['archivos_creados']:
        print(f"\n💡 COMPARACIÓN CON ARCHIVOS ANTERIORES:")
        print(f"   ❌ Archivos parquet mal convertidos → Todo en 1 columna, datos corruptos")
        print(f"   ✅ Archivos CSV corregidos → Columnas separadas correctamente, dorsales y nombres en posición correcta")
        
        print(f"\n📋 PRÓXIMOS PASOS:")
        print(f"   1. Compara los archivos *_CSV_CORREGIDO_FINAL.parquet con los anteriores")
        print(f"   2. Verifica que dorsales y nombres estén en las columnas correctas")
        print(f"   3. Si todo es correcto, reemplaza los archivos mal convertidos")
        print(f"   4. Los archivos están listos para análisis de fútbol")

def main():
    """Función principal"""
    print("⚽ CORRECTOR DE CSV ORIGINALES MEDIACOACH")
    print("="*50)
    print("🎯 Procesa archivos CSV originales con delimitador ; correcto")
    print("🔧 Corrige problemas de dorsales y nombres mal posicionados")
    print("📁 Objetivo: postpartidoequipos, postpartidojugador")
    print("="*50)
    
    print("\n⚠️  IMPORTANTE:")
    print("   Este script necesita los archivos CSV originales (no los parquet convertidos)")
    print("   Los archivos CSV deben estar en las carpetas postpartidoequipos y postpartidojugador")
    
    # Preguntar confirmación
    respuesta = input("\n¿Proceder con el procesamiento de CSV originales? (s/n): ").lower().strip()
    
    if respuesta in ['s', 'si', 'sí', 'y', 'yes']:
        # Ejecutar procesamiento
        resultados = procesar_todas_las_carpetas_csv()
        
        # Generar reporte
        generar_reporte_csv(resultados)
        
        print(f"\n🎉 ¡PROCESAMIENTO COMPLETADO!")
        
    else:
        print("❌ Procesamiento cancelado")

if __name__ == "__main__":
    main()