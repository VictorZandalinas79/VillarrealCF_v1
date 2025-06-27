# Villarreal CF - Sistema de Informes

## 📋 Descripción
Sistema de informes interactivo para análisis de fútbol basado en datos de MediaCoach y Opta. Permite generar informes de prepartido y postpartido con análisis táctico, ABP (Análisis Basado en Posición) y físico.

## 🚀 Instalación

1. **Instalar dependencias:**
```bash
pip install -r requirements.txt
```

2. **Crear estructura de directorios:**
```bash
mkdir -p assets data/mediacoach data/opta data/processed
```

3. **Añadir imágenes necesarias:**
   - `assets/fondo_informes.png` - Fondo para informes
   - `assets/balon.png` - Imagen de balón
   - `assets/villarreal_logo.png` - Logo del club

4. **Ejecutar la aplicación:**
```bash
python app.py
```

## 📁 Estructura del Proyecto

### Archivos Principales
- `app.py` - Aplicación principal
- `login.py` - Sistema de login (ya existente)
- `requirements.txt` - Dependencias

### Directorios
- `pages/` - Páginas principales (menú, prepartido, postpartido)
- `components/` - Componentes reutilizables
- `reports/` - Informes organizados por sección
- `data/` - Fuentes de datos
- `assets/` - CSS, imágenes y recursos estáticos
- `utils/` - Utilidades y configuraciones

## 🔧 Cómo Añadir Nuevos Informes

### 1. Crear el archivo del informe
Crea un nuevo archivo en la estructura correspondiente:

```
reports/[seccion]/[tipo]/[nombre_informe].py
```

Ejemplo: `reports/tactico/prepartido/formaciones.py`

### 2. Estructura del informe
Cada informe debe tener una función `get_layout()`:

```python
from dash import html
import dash_bootstrap_components as dbc

def get_layout():
    """
    Retorna el layout del informe
    """
    return html.Div([
        html.H2("Título del Informe"),
        # Contenido del informe...
    ])
```

### 3. Registrar el informe
Añade el informe en `utils/config.py` en la sección `REPORTS_STRUCTURE`:

```python
'tactico': [
    {'id': 'nombre_archivo', 'name': 'Nombre Mostrado', 'data_source': 'opta'},
    # ...
]
```

### 4. Ejemplo completo
Ver `reports/tactico/prepartido/analisis_rival.py` como ejemplo de implementación.

## 📊 Trabajar con Datos

### MediaCoach
- Colocar archivos CSV/Excel en `data/mediacoach/`
- Usar `data.mediacoach.loader` para cargar datos

### Opta
- Colocar archivos JSON/XML en `data/opta/`
- Usar `data.opta.loader` para cargar datos

## 🎨 Personalización

### Colores y Estilos
Editar `utils/config.py` en la sección `COLORS`:

```python
COLORS = {
    'primary': '#FFD700',      # Amarillo Villarreal
    'secondary': '#003366',    # Azul marino
    # ...
}
```

### Configuración de PDF
Ajustar `PDF_CONFIG` en `utils/config.py` para personalizar exportaciones.

## 📱 Navegación

### Páginas Principales
- **Menú** (`/menu`) - Página de inicio
- **Prepartido** (`/prepartido`) - Análisis previo al partido
- **Postpartido** (`/postpartido`) - Análisis posterior al partido

### Secciones
Cada página tiene 3 secciones:
- **Táctico** - Análisis táctico y formaciones
- **ABP** - Análisis basado en posición
- **Físico** - Análisis físico y rendimiento

### Navegación entre Informes
- Usar flechas ← → para navegar entre informes
- Cada sección mantiene su propio estado de navegación

## 📄 Exportación PDF

Los botones de exportación permiten:
- Exportar solo prepartido
- Exportar solo postpartido  
- Exportar informe completo

## 🛠️ Desarrollo

### Añadir Nuevas Funcionalidades
1. **Componentes** - Crear en `components/`
2. **Utilidades** - Añadir en `utils/`
3. **Callbacks** - Usar decorador `@callback`

### Debugging
- Activar modo debug en `utils/config.py`
- Logs detallados en consola del navegador

## 📞 Soporte

Para añadir informes o modificar funcionalidades, seguir la estructura modular establecida. Cada informe es independiente y puede desarrollarse por separado.

---
**Villarreal CF - Temporada 2024-25**