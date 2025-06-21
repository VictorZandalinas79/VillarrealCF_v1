from dash import html
import dash_bootstrap_components as dbc

def create_navbar():
    """Crear barra de navegación"""
    return dbc.Navbar([
        html.Div([
            # Logo y título
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.Img(
                            src="/assets/escudo_villarreal_datos.png",
                            style={
                                "height": "35px",
                                "width": "35px", 
                                "margin-right": "10px"
                            }
                        ),
                        html.Span(
                            "Villarreal CF", 
                            className="navbar-brand-text"
                        ),
                        html.Small(
                            " | Informes de Datos", 
                            className="navbar-subtitle"
                        )
                    ], className="navbar-brand-container")
                ], width="auto"),
            ], align="center", className="g-0 w-100 justify-content-between")
        ], className="container-fluid"),
        
        # Menú de navegación
        dbc.Nav([
            dbc.NavItem([
                dbc.NavLink([
                    html.I(className="fas fa-home", style={"margin-right": "5px"}),
                    "Inicio"
                ], href="/", active="exact", className="nav-link-custom")
            ]),
            dbc.NavItem([
                dbc.NavLink([
                    html.I(className="fas fa-chart-bar", style={"margin-right": "5px"}),
                    "Analytics"
                ], href="/analytics", className="nav-link-custom")
            ]),
            dbc.NavItem([
                dbc.NavLink([
                    html.I(className="fas fa-file-alt", style={"margin-right": "5px"}),
                    "Informes"
                ], href="/reports", className="nav-link-custom")
            ]),
            dbc.NavItem([
                dbc.NavLink([
                    html.I(className="fas fa-users", style={"margin-right": "5px"}),
                    "Jugadores"
                ], href="/players", className="nav-link-custom")
            ]),
            dbc.NavItem([
                dbc.NavLink([
                    html.I(className="fas fa-futbol", style={"margin-right": "5px"}),
                    "Partidos"
                ], href="/matches", className="nav-link-custom")
            ])
        ], className="me-auto navbar-nav-custom", navbar=True),
        
        # Botón de logout
        html.Div([
            dbc.Button([
                html.I(className="fas fa-sign-out-alt", style={"margin-right": "5px"}),
                "Cerrar Sesión"
            ], 
            id="logout-button", 
            color="danger", 
            size="sm", 
            className="logout-btn",
            n_clicks=0)
        ])
        
    ], 
    className="custom-navbar",
    color="light",
    light=True,
    sticky="top")