import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from auth.session_manager import init_session_store
from callbacks import register_all_callbacks

# Inicializar app
app = dash.Dash(__name__, 
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                suppress_callback_exceptions=True,
                assets_folder="assets")

app.title = "Villarreal CF - Informes Datos"
server = app.server

# Configurar favicon personalizado
app._favicon = "escudo_villarreal_datos.png"

# Layout principal
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    init_session_store(),
    html.Div(id="page-content")
])

# Registrar todos los callbacks
register_all_callbacks(app)

if __name__ == "__main__":
    from config import APP_CONFIG
    app.run_server(
        debug=APP_CONFIG["DEBUG"], 
        host=APP_CONFIG["HOST"], 
        port=APP_CONFIG["PORT"]
    )