import pandas as pd
import os
import re
from pathlib import Path
from datetime import datetime
import warnings
import zipfile
import io
warnings.filterwarnings('ignore')

def extraer_info_partido(texto):
    """Extrae información del partido"""
    try:
        partes = [parte.strip() for parte in texto.split('|')]
        if len(partes) >= 4:
            liga = partes[0]
            temporada = partes[1].replace('Temporada ', '')
            jornada = partes[2]
            partido_y_fecha = partes[3]
            
            match_fecha = re.search(r'\((\d{4}-\d{2}-\d{2})\)$', partido_y_fecha)
            if match_fecha:
                fecha = match_fecha.group(1)
                partido = partido_y_fecha[:match_fecha.start()].strip()
            else:
                fecha = None
                partido = partido_y_fecha
            
            return {
                'liga': liga,
                'temporada': temporada,
                'jornada': jornada,
                'partido': partido,
                'fecha': fecha
            }
        return None
    except:
        return None

def procesar_con_xlrd(archivo_path):
    """Intenta procesar con xlrd que es más tolerante"""
    try:
        import xlrd
        print("  🔧 Intentando con xlrd...")
        
        # Leer con xlrd
        workbook = xlrd.open_workbook(archivo_path)
        sheet = workbook.sheet_by_index(0)
        
        # Convertir a lista de listas
        data = []
        for row_idx in range(sheet.nrows):
            row = []
            for col_idx in range(sheet.ncols):
                try:
                    cell = sheet.cell(row_idx, col_idx)
                    value = cell.value
                    if cell.ctype == xlrd.XL_CELL_DATE:
                        # Convertir fechas
                        value = xlrd.xldate_as_datetime(value, workbook.datemode)
                    row.append(value)
                except:
                    row.append(None)
            data.append(row)
        
        # Buscar información del partido
        info_partido = None
        for row_idx, row in enumerate(data[:15]):  # Primeras 15 filas
            for cell_value in row:
                if cell_value and isinstance(cell_value, str) and '|' in cell_value and 'Temporada' in cell_value:
                    info_partido = extraer_info_partido(cell_value)
                    if info_partido:
                        break
            if info_partido:
                break
        
        if not info_partido:
            print("  ❌ No se encontró información del partido")
            return None
        
        # Buscar fila de headers
        fila_headers = None
        for row_idx, row in enumerate(data[:20]):  # Primeras 20 filas
            for cell_value in row:
                if cell_value and str(cell_value).strip() == 'Id Jugador':
                    fila_headers = row_idx
                    break
            if fila_headers is not None:
                break
        
        if fila_headers is None:
            print("  ❌ No se encontró fila de headers")
            return None
        
        # Crear DataFrame
        headers = data[fila_headers][1:]  # Desde columna 2
        data_rows = []
        
        for row in data[fila_headers + 1:]:
            if len(row) > 1:
                row_data = row[1:]  # Desde columna 2
                # Verificar que la fila no esté vacía
                if any(val is not None and val != '' for val in row_data):
                    data_rows.append(row_data)
        
        if not data_rows:
            print("  ❌ No hay datos")
            return None
        
        # Crear DataFrame con el tamaño correcto
        max_cols = max(len(headers), max(len(row) for row in data_rows) if data_rows else 0)
        
        # Ajustar headers
        while len(headers) < max_cols:
            headers.append(f'columna_{len(headers) + 2}')
        
        # Ajustar filas
        for row in data_rows:
            while len(row) < max_cols:
                row.append(None)
        
        df = pd.DataFrame(data_rows, columns=headers[:max_cols])
        
        # Agregar información del partido
        for key, value in info_partido.items():
            df[key] = value
        df['archivo_origen'] = Path(archivo_path).name
        
        print(f"  ✅ Procesado con xlrd: {len(df)} filas")
        return df
        
    except ImportError:
        print("  ⚠️ xlrd no está instalado. Instala con: pip install xlrd")
        return None
    except Exception as e:
        print(f"  ❌ Error con xlrd: {e}")
        return None

def procesar_ignorando_estilos(archivo_path):
    """Intenta leer ignorando completamente los estilos"""
    try:
        print("  🔧 Intentando leer como CSV desde Excel...")
        
        # Convertir a CSV en memoria usando pandas
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            try:
                # Intenta leer y convertir a CSV
                df_temp = pd.read_excel(archivo_path, engine='openpyxl', header=None)
                df_temp.to_csv(tmp.name, index=False, header=False)
                
                # Leer el CSV
                df = pd.read_csv(tmp.name, header=None)
                
                # Buscar información del partido
                info_partido = None
                for idx, row in df.iterrows():
                    if idx > 15:
                        break
                    for cell_value in row:
                        if pd.notna(cell_value) and isinstance(cell_value, str) and '|' in cell_value and 'Temporada' in cell_value:
                            info_partido = extraer_info_partido(cell_value)
                            if info_partido:
                                break
                    if info_partido:
                        break
                
                if not info_partido:
                    return None
                
                # Buscar fila de headers
                fila_headers = None
                for idx, row in df.iterrows():
                    if idx > 20:
                        break
                    for cell_value in row:
                        if pd.notna(cell_value) and str(cell_value).strip() == 'Id Jugador':
                            fila_headers = idx
                            break
                    if fila_headers is not None:
                        break
                
                if fila_headers is None:
                    return None
                
                # Crear DataFrame final
                headers = df.iloc[fila_headers, 1:].tolist()  # Desde columna 2
                df_final = df.iloc[fila_headers + 1:, 1:].copy()  # Desde columna 2
                df_final.columns = [str(h) if pd.notna(h) else f'columna_{i+2}' for i, h in enumerate(headers)]
                df_final = df_final.dropna(how='all')
                
                # Agregar información del partido
                for key, value in info_partido.items():
                    df_final[key] = value
                df_final['archivo_origen'] = Path(archivo_path).name
                
                print(f"  ✅ Procesado vía CSV: {len(df_final)} filas")
                return df_final
                
            finally:
                # Limpiar archivo temporal
                try:
                    os.unlink(tmp.name)
                except:
                    pass
                    
    except Exception as e:
        print(f"  ❌ Error con método CSV: {e}")
        return None

def procesar_archivo_xlsx(archivo_path):
    """Procesa un archivo xlsx con múltiples métodos"""
    print(f"Procesando: {Path(archivo_path).name}")
    
    # Método 1: xlrd (más tolerante)
    df = procesar_con_xlrd(archivo_path)
    if df is not None:
        return df
    
    # Método 2: Conversión vía CSV
    df = procesar_ignorando_estilos(archivo_path)
    if df is not None:
        return df
    
    print(f"  ❌ No se pudo procesar el archivo")
    return None

def main():
    """Función principal"""
    directorio_actual = Path('.')
    archivos_xlsx = list(directorio_actual.glob('*.xlsx'))
    
    if not archivos_xlsx:
        print("❌ No se encontraron archivos xlsx")
        return
    
    print(f"🔍 Encontrados {len(archivos_xlsx)} archivos xlsx")
    print("=" * 50)
    
    todos_los_dfs = []
    procesados = 0
    errores = 0
    
    for archivo in archivos_xlsx:
        df = procesar_archivo_xlsx(archivo)
        if df is not None and not df.empty:
            todos_los_dfs.append(df)
            procesados += 1
        else:
            errores += 1
        print("-" * 50)
    
    print(f"\n📊 Resumen:")
    print(f"  ✅ Archivos procesados: {procesados}")
    print(f"  ❌ Archivos con errores: {errores}")
    
    if not todos_los_dfs:
        print("\n❌ No se obtuvieron datos")
        print("\n💡 Recomendación: Usar la versión Node.js con SheetJS")
        return
    
    # Combinar DataFrames
    df_final = pd.concat(todos_los_dfs, ignore_index=True)
    
    # Reordenar columnas
    columnas_info = ['liga', 'temporada', 'jornada', 'partido', 'fecha', 'archivo_origen']
    otras_columnas = [col for col in df_final.columns if col not in columnas_info]
    df_final = df_final[columnas_info + otras_columnas]
    
    # Guardar
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        import pyarrow
        nombre_archivo = f"datos_combinados_{timestamp}.parquet"
        df_final.to_parquet(nombre_archivo, index=False)
        print(f"📁 Guardado como parquet: {nombre_archivo}")
    except ImportError:
        nombre_archivo = f"datos_combinados_{timestamp}.csv"
        df_final.to_csv(nombre_archivo, index=False)
        print(f"📁 Guardado como CSV: {nombre_archivo}")
    
    print(f"📊 Total de filas: {len(df_final)}")
    
    # Resumen por partido
    print(f"\n📈 Resumen por partido:")
    resumen = df_final.groupby(['liga', 'jornada', 'partido']).size().reset_index(name='num_filas')
    for _, row in resumen.iterrows():
        print(f"  ⚽ {row['jornada']} | {row['partido']}: {row['num_filas']} filas")

if __name__ == "__main__":
    main()