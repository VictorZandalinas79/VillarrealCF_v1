import os

# Configuración de autenticación
AUTH_CONFIG = {
    "ADMIN_USER": "admin",
    "ADMIN_PASSWORD": "sdc2025",
    "SESSION_TIMEOUT": 24  # horas
}

# Rutas de archivos
DATA_PATHS = {
    "PARQUET_DIR": "parquet_files/",
    "MATCH_DATA": "parquet_files/match_data/",
    "PLAYER_STATS": "parquet_files/player_stats/",
    "TEAM_PERFORMANCE": "parquet_files/team_performance/"
}

# Configuración de la app
APP_CONFIG = {
    "DEBUG": True,
    "HOST": "127.0.0.1",
    "PORT": 8050
}

# Colores del Villarreal CF
COLORS = {
    "PRIMARY_YELLOW": "#FFD700",      # Amarillo principal
    "SECONDARY_YELLOW": "#FFA500",    # Amarillo naranja
    "LIGHT_YELLOW": "#FFFF99",        # Amarillo claro
    "PRIMARY_BLUE": "#1E90FF",        # Azul principal
    "SECONDARY_BLUE": "#4169E1",      # Azul real
    "NAVY_BLUE": "#003366",           # Azul marino
    "SUCCESS": "#28A745",
    "DANGER": "#DC3545",
    "WHITE": "#FFFFFF",
    "LIGHT_GRAY": "#F8F9FA",
    "DARK_GRAY": "#343A40"
}

# URLs de imágenes por defecto
DEFAULT_IMAGES = {
    "LOGIN_BACKGROUND": "https://www.tauceramica.com/wp-content/webp-express/webp-images/uploads/2020/10/estadio_ceramica-5.jpg.webp",  # Estadio de la Cerámica oficial
    "STADIUM": "https://www.tauceramica.com/wp-content/webp-express/webp-images/uploads/2020/10/estadio_ceramica-5.jpg.webp"
}