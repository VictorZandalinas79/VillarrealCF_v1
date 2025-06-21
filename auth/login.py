from dash import html, dcc
import dash_bootstrap_components as dbc
from config import AUTH_CONFIG, DEFAULT_IMAGES

def validate_credentials(username, password):
    """Validar credenciales de usuario"""
    return (username == AUTH_CONFIG["ADMIN_USER"] and 
            password == AUTH_CONFIG["ADMIN_PASSWORD"])

def create_login_layout():
    """Crear layout de login con diseño moderno y animado"""
    return html.Div([
        # Fondo animado
        html.Div(
            className="login-background",
            style={
                "background-image": f"url('{DEFAULT_IMAGES['LOGIN_BACKGROUND']}')"
            }
        ),
        
        # Logo SDC en la esquina superior derecha - CON ESTILOS INLINE FORZADOS
        html.Div([
            html.Img(
                src="/assets/SDC_horizontal_texto_blanco.svg",
                style={
                    "height": "60px",
                    "max-width": "200px",
                    "object-fit": "contain",
                    "display": "block",
                    "width": "auto"
                }
            )
        ], style={
            "position": "absolute",
            "top": "20px",
            "right": "20px", 
            "z-index": "9999",
            "background": "rgba(255, 0, 0, 0.5)",  # Fondo rojo temporal para ver si aparece
            "padding": "10px",
            "border": "2px solid yellow",  # Borde amarillo temporal
            "border-radius": "8px"
        }),
        
        # Formulario de login
        html.Div([
            # Logo y título
            html.Div([
                html.Div([
                    html.Img(
                        src="/assets/escudo_villarreal_datos.png",
                        style={
                            "height": "250px",
                            "width": "180px",
                            "margin-bottom": "15px"
                        }
                    )
                ], style={"text-align": "center"}),
                html.H1(
                    "Villarreal CF", 
                    className="login-title"
                ),
                html.H3(
                    "Informes de Datos", 
                    className="login-subtitle"
                ),
                html.Hr(className="login-divider")
            ], className="login-header"),
            
            # Formulario
            html.Div([
                dcc.Input(
                    id="username-input",
                    type="text",
                    placeholder="👤 Usuario",
                    className="login-input",
                    autoFocus=True
                ),
                dcc.Input(
                    id="password-input",
                    type="password",
                    placeholder="🔒 Contraseña",
                    className="login-input"
                ),
                html.Button(
                    [
                        html.Span("🚀", style={"margin-right": "10px"}),
                        "Iniciar Sesión"
                    ],
                    id="login-button",
                    className="login-btn",
                    n_clicks=0
                )
            ], className="login-form-inputs"),
            
            # Mensaje de estado
            html.Div(id="login-message", className="login-message")
            
        ], className="login-form-container"),
        
        # Footer
        html.Div([
            html.P([
                "© 2025 Villarreal CF - Sistema de Análisis de Datos",
                html.Br(),
                "Acceso autorizado únicamente"
            ], className="login-footer-text")
        ], className="login-footer")
        
    ], className="login-page")