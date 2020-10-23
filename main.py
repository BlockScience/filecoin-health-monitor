# Dependences
import dash
import dash_auth
import dash_core_components as dcc
import dash_html_components as html
from figures import FIGURES

# Dash parameters
VALID_USERNAME_PASSWORD_PAIRS = {
    'file': 'coin'
}
external_stylesheets = []

# Create Dash instance
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
server = app.server

# Create a basic login screen
auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
)

# Create a image for each key-value on figures.py
app.layout = html.Div(children=[
    html.Img(src="assets/fil-health-monitor.png"),
    *(dcc.Graph(figure=fig) for fig in FIGURES if fig is not None)
])

# Run Dash
if __name__ == '__main__':
    app.run_server(debug=True, host="0.0.0.0", use_reloader=False)