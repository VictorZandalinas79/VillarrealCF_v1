from dash import Input, Output, callback, html
import dash_bootstrap_components as dbc
from auth.login import create_login_layout
from auth.session_manager import validate_session
from pages.home import create_home_layout

def register_navigation_callbacks(app):
    """Registrar callbacks de navegación"""
    
    @app.callback(
        Output("page-content", "children"),
        [Input("url", "pathname"),
         Input("session-id", "data")]
    )
    def display_page(pathname, session_id):
        """Mostrar la página correspondiente según la ruta y sesión"""
        
        # Verificar si hay sesión activa
        if validate_session(session_id):
            # Usuario autenticado - mostrar páginas de la app
            if pathname == "/" or pathname is None:
                return create_home_layout()
            elif pathname == "/analytics":
                return create_placeholder_page("Analytics", "chart-bar")
            elif pathname == "/reports":
                return create_placeholder_page("Informes", "file-alt")
            elif pathname == "/players":
                return create_placeholder_page("Jugadores", "users")
            elif pathname == "/matches":
                return create_placeholder_page("Partidos", "futbol")
            else:
                return create_home_layout()
        else:
            # Usuario no autenticado - mostrar login
            return create_login_layout()

def create_placeholder_page(title, icon):
    """Crear página placeholder para futuras funcionalidades"""
    from components.navbar import create_navbar
    
    return html.Div([
        create_navbar(),
        html.Div([
            html.Div([
                html.H1([
                    html.I(className=f"fas fa-{icon}", style={"margin-right": "15px"}),
                    title
                ], className="page-title"),
                html.P(f"Página de {title} - En desarrollo", className="page-subtitle")
            ], className="page-header"),
            
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        html.I(className=f"fas fa-{icon} fa-5x", style={"color": "#FFD700", "margin-bottom": "20px"}),
                        html.H3(f"Módulo de {title}"),
                        html.P("Esta funcionalidad estará disponible próximamente."),
                        html.Hr(),
                        html.P("Funciones planificadas:", className="fw-bold"),
                        html.Ul([
                            html.Li(f"Análisis detallado de {title.lower()}"),
                            html.Li("Visualizaciones interactivas"),
                            html.Li("Exportación de datos"),
                            html.Li("Informes personalizados")
                        ])
                    ], className="text-center")
                ])
            ], className="placeholder-card")
        ], className="container-fluid page-container")
    ])