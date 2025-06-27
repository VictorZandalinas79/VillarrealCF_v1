# CÓDIGO PARA VERIFICAR LAS COLUMNAS DISPONIBLES
# Ejecuta esto primero para ver qué columnas tienes

import pandas as pd

def verificar_columnas_distancias():
    """Verifica las columnas disponibles relacionadas con distancias"""
    try:
        # Cargar el dataset
        df = pd.read_parquet("prueba_extraccion/data/rendimiento_fisico.parquet")
        
        print("=== TODAS LAS COLUMNAS DISPONIBLES ===")
        print(f"Total de columnas: {len(df.columns)}")
        for i, col in enumerate(df.columns, 1):
            print(f"{i:2d}. {col}")
        
        print("\n=== COLUMNAS RELACIONADAS CON DISTANCIA ===")
        distance_cols = [col for col in df.columns if 'istancia' in col or 'Distance' in col]
        for col in distance_cols:
            print(f"✅ {col}")
        
        print("\n=== COLUMNAS RELACIONADAS CON VELOCIDAD/KM ===")
        speed_cols = [col for col in df.columns if 'km' in col.lower() or 'speed' in col.lower() or 'vel' in col.lower()]
        for col in speed_cols:
            print(f"🏃 {col}")
        
        print("\n=== MUESTRA DE DATOS (primeras 3 filas) ===")
        distance_and_speed_cols = list(set(distance_cols + speed_cols))
        if distance_and_speed_cols:
            print(df[['Alias'] + distance_and_speed_cols].head(3))
        
        return df.columns.tolist()
        
    except Exception as e:
        print(f"Error al verificar columnas: {e}")
        return []

# Ejecutar la verificación
if __name__ == "__main__":
    columnas = verificar_columnas_distancias()