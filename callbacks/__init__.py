from .auth_callbacks import register_auth_callbacks
# from .logout_callbacks import register_logout_callbacks  # Temporalmente deshabilitado
from .navigation_callbacks import register_navigation_callbacks

def register_all_callbacks(app):
    """Registrar todos los callbacks de la aplicación"""
    register_auth_callbacks(app)
    # register_logout_callbacks(app)  # Temporalmente deshabilitado
    register_navigation_callbacks(app)