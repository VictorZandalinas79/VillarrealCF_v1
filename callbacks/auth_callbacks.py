from dash import Input, Output, State, callback, no_update, html
import dash_bootstrap_components as dbc
from auth.login import validate_credentials
from auth.session_manager import create_session

def register_auth_callbacks(app):
    """Registrar callbacks de autenticación"""
    
    @app.callback(
        [Output("session-id", "data"),
         Output("login-message", "children")],
        [Input("login-button", "n_clicks")],
        [State("username-input", "value"),
         State("password-input", "value")],
        prevent_initial_call=True
    )
    def handle_login(n_clicks, username, password):
        """Manejar el proceso de login"""
        print(f"🔍 Login callback triggered: n_clicks={n_clicks}, username={username}")
        
        if n_clicks and n_clicks > 0:
            if username and password:
                print(f"🔍 Validating credentials: {username}")
                if validate_credentials(username, password):
                    session_id = create_session(username)
                    print(f"✅ Login successful, session_id: {session_id}")
                    return session_id, dbc.Alert([
                        html.Div([
                            html.I(className="fas fa-check-circle", style={"margin-right": "10px"}),
                            "¡Login exitoso! Redirigiendo..."
                        ])
                    ], color="success", className="login-alert")
                else:
                    print("❌ Invalid credentials")
                    return no_update, dbc.Alert([
                        html.Div([
                            html.I(className="fas fa-exclamation-triangle", style={"margin-right": "10px"}),
                            "Usuario o contraseña incorrectos"
                        ])
                    ], color="danger", className="login-alert")
            else:
                print("❌ Missing username or password")
                return no_update, dbc.Alert([
                    html.Div([
                        html.I(className="fas fa-info-circle", style={"margin-right": "10px"}),
                        "Por favor, complete todos los campos"
                    ])
                ], color="warning", className="login-alert")
        
        print("🔍 No action taken")
        return no_update, no_update