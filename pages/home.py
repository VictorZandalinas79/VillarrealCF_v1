from dash import html, dcc
import dash_bootstrap_components as dbc
from datetime import datetime
from components.navbar import create_navbar

def create_stats_card(title, value, subtitle, color, icon):
    """Crear tarjeta de estadística"""
    return dbc.Card([
        dbc.CardBody([
            html.Div([
                html.Div([
                    html.H4(title, className="card-title"),
                    html.H2(value, className="card-value", style={"color": color}),
                    html.P(subtitle, className="card-subtitle")
                ], className="col-8"),
                html.Div([
                    html.I(className=f"fas fa-{icon} fa-3x", style={"color": color})
                ], className="col-4 text-end")
            ], className="row align-items-center")
        ])
    ], className="stats-card h-100", style={"border-left": f"5px solid {color}"})

def create_home_layout():
    """Crear layout de la página principal"""
    return html.Div([
        # Navbar
        create_navbar(),
        
        # Contenido principal
        html.Div([
            # Header
            html.Div([
                html.H1(
                    [
                        html.I(className="fas fa-tachometer-alt", style={"margin-right": "15px"}),
                        "Panel de Control"
                    ], 
                    className="page-title"
                ),
                html.P(
                    f"Bienvenido al sistema de análisis - {datetime.now().strftime('%A, %d de %B de %Y')}",
                    className="page-subtitle"
                )
            ], className="page-header"),
            
            # Tarjetas de estadísticas
            html.Div([
                dbc.Row([
                    dbc.Col([
                        create_stats_card(
                            "Partidos Analizados",
                            "38",
                            "Temporada 2024/25",
                            "#FFD700",
                            "futbol"
                        )
                    ], md=3),
                    dbc.Col([
                        create_stats_card(
                            "Informes Generados",
                            "156",
                            "Total acumulado",
                            "#1E90FF",
                            "file-alt"
                        )
                    ], md=3),
                    dbc.Col([
                        create_stats_card(
                            "Jugadores Analizados",
                            "28",
                            "Plantilla actual",
                            "#28A745",
                            "users"
                        )
                    ], md=3),
                    dbc.Col([
                        create_stats_card(
                            "Última Actualización",
                            "Hoy",
                            datetime.now().strftime('%H:%M'),
                            "#FFA500",
                            "sync-alt"
                        )
                    ], md=3)
                ], className="mb-4")
            ]),
            
            # Sección principal
            html.Div([
                dbc.Row([
                    # Panel izquierdo
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader([
                                html.H4([
                                    html.I(className="fas fa-chart-line", style={"margin-right": "10px"}),
                                    "Resumen de Rendimiento"
                                ])
                            ]),
                            dbc.CardBody([
                                html.H5("Estado del Sistema", className="text-success"),
                                html.P("Todos los sistemas funcionando correctamente"),
                                html.Hr(),
                                html.H6("Funcionalidades Disponibles:"),
                                html.Ul([
                                    html.Li("📊 Análisis de datos de Mediacoach"),
                                    html.Li("📋 Generación de informes automáticos"),
                                    html.Li("📈 Visualizaciones interactivas"),
                                    html.Li("⚽ Estadísticas de jugadores"),
                                    html.Li("🏆 Análisis de partidos"),
                                    html.Li("📊 Dashboard en tiempo real")
                                ]),
                                html.Hr(),
                                dbc.Alert([
                                    html.H5("¡Sistema Activo!", className="alert-heading"),
                                    html.P("La aplicación está lista para procesar datos y generar informes.")
                                ], color="success")
                            ])
                        ])
                    ], md=8),
                    
                    # Panel derecho
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader([
                                html.H5([
                                    html.I(className="fas fa-info-circle", style={"margin-right": "10px"}),
                                    "Información Rápida"
                                ])
                            ]),
                            dbc.CardBody([
                                html.Div([
                                    html.H6("🏟️ Próximo Partido"),
                                    html.P("Por definir", className="text-muted"),
                                    html.Hr(),
                                    html.H6("📅 Temporada Actual"),
                                    html.P("2024/25", className="text-primary"),
                                    html.Hr(),
                                    html.H6("🎯 Liga"),
                                    html.P("LaLiga EA Sports", className="text-info"),
                                    html.Hr(),
                                    html.H6("⚙️ Estado del Sistema"),
                                    dbc.Badge("OPERATIVO", color="success", className="badge-lg")
                                ])
                            ])
                        ], className="h-100")
                    ], md=4)
                ])
            ], className="main-content")
            
        ], className="container-fluid page-container")
    ])