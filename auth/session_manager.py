from dash import dcc
from datetime import datetime, timedelta
import uuid

# Almacenamiento temporal de sesiones (en producción usar Redis o base de datos)
sessions = {}

def init_session_store():
    """Inicializar el store de sesión"""
    return dcc.Store(id="session-id", storage_type="session")

def create_session(username):
    """Crear una nueva sesión"""
    session_id = str(uuid.uuid4())
    sessions[session_id] = {
        "user": username,
        "login_time": datetime.now()
    }
    return session_id

def validate_session(session_id):
    """Validar si la sesión es válida y no ha expirado"""
    if not session_id or session_id not in sessions:
        return False
    
    from config import AUTH_CONFIG
    login_time = sessions[session_id]["login_time"]
    session_timeout = timedelta(hours=AUTH_CONFIG["SESSION_TIMEOUT"])
    
    if datetime.now() - login_time > session_timeout:
        # Sesión expirada, eliminarla
        if session_id in sessions:
            del sessions[session_id]
        return False
    
    return True

def destroy_session(session_id):
    """Destruir una sesión"""
    if session_id and session_id in sessions:
        del sessions[session_id]
        return True
    return False

def get_session_user(session_id):
    """Obtener el usuario de una sesión"""
    if session_id and session_id in sessions:
        return sessions[session_id]["user"]
    return None

def get_active_sessions():
    """Obtener número de sesiones activas"""
    return len(sessions)