import sqlite3
import pandas as pd
import yfinance as yf
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, timedelta
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

# --------------------------------------------------------------------------------------- DIRECCIÓN ARCHIVO TERMINAL y COMANDO RUN
# D:\DadAnalysisApp
# streamlit run streamlit_app.py
# --------------------------------------------------------------------------------------- TICKERS
data_tickers = {
    'ticker': ["^GSPC",  "^IBEX",  "^GDAXIP", "^GDAXI", "^FCHI",  "^STOXX",  "^IXIC" ],
    'nombreTicker': ["SyP_500", "IBEX_35",  "DAX", "DAX_TR", "CAC40",  "Eurostoxx600",  "NASDAQ_TR"]
}
df_tickers = pd.DataFrame(data_tickers)

datos_historicos_dict = {}


# Iterar sobre los tickers y sus nombres
for ticker, nombre in zip(data_tickers['ticker'], data_tickers['nombreTicker']):
    # Obtener los datos históricos usando yfinance
    datos_historicos = yf.Ticker(ticker).history(period="max")
    
    # Seleccionar solo la columna "Close"
    datos_close = datos_historicos[['Close']].reset_index()
    
    # Guardar el DataFrame en el diccionario con la clave como el nombre del ticker
    datos_historicos_dict[f'datosHistoricos_{nombre}'] = datos_close

# --------------------------------------------------------------------------------------- CREAR Y ACTUALIZAR BASE DE DATOS

# Conectar a la base de datos
conexion = sqlite3.connect('macroeconomic_data.db', timeout=10)
cursor = conexion.cursor()

#Borrar todas las tablas
#cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#tables = cursor.fetchall()
#
#for table in tables:
#    cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")

# Crear la tabla de nuevo
cursor.execute('''CREATE TABLE IF NOT EXISTS tickers (ticker TEXT PRIMARY KEY,nombreTicker TEXT)''')

# Insertar los datos del DataFrame en la tabla
# Convertir el DataFrame a una lista de tuplas
data_para_insertar = list(df_tickers.itertuples(index=False, name=None))

# Usar executemany para insertar múltiples filas
cursor.executemany('''INSERT OR REPLACE INTO tickers (ticker, nombreTicker)VALUES (?, ?)''', data_para_insertar)

#Iterar sobre los DataFrames en el diccionario y crear tablas
for nombre in data_tickers['nombreTicker']:
    # Formatear el nombre para que sea un nombre de tabla válido

    # Crear la tabla en la base de datos
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {nombre} (fecha DATE PRIMARY KEY,close REAL)''')

    # Insertar los datos en la tabla
    df = datos_historicos_dict[f'datosHistoricos_{nombre}'].reset_index()
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')  # Convertir y formatear la fecha
    
    # Crear una lista de tuplas para insertar
    df_insertar = list(zip(df['Date'], df['Close']))  # Crear tuplas de (fecha, close)

    # Usar executemany para insertar los datos en la tabla
    cursor.executemany(f'''INSERT OR REPLACE INTO {nombre} (fecha, close) VALUES (?, ?)''', df_insertar)

# Guardar los cambios
conexion.commit()

# Cerrar la conexión
conexion.close()

# --------------------------------------------------------------------------------------- VISUALIZACIÓN

# Configuración del tema
st.set_page_config(
    layout="wide",  # Modo ancho
    page_title="Dad Analysis App",
    page_icon="🧮",  # Icono para la página
    initial_sidebar_state="auto",
)

st.markdown("""
    <style>
        .title {
            text-align: center;
            font-size: 30px;
            font-weight: bold;
        }
        .subtitle {
            text-align: center;
            font-size: 20px;
            font-style: italic;
        }
    </style>
""", unsafe_allow_html=True)


# Selección de índices para visualizar
selected_tickers = st.multiselect(
    "Selecciona los índices para visualizar",
    options=df_tickers['nombreTicker'].tolist(),
)



# Aplicamos CSS para cambiar el color de fondo de los contenedores
st.markdown(
    """
    <style>
    /* Estilo para los botones */
    .stButton>button {
        background-color:rgb(94, 222, 98); /* Fondo verde para los botones */
        color: white; /* Texto blanco */
        border-radius: 5px;
        border: none;
        padding: 10px 20px;
        cursor: pointer;
    }

    .stButton>button:hover {
        background-color:rgb(51, 119, 54); /* Fondo más oscuro cuando pasa el ratón */
    }
    </style>
""", unsafe_allow_html=True)


# Crear las columnas para los botones de selección de tipo de gráfico
col1, col2, col3 = st.columns(3)

# Inicializar la opción de gráfico si no está definida
if 'graph_option' not in st.session_state:
    st.session_state.graph_option = 'Gráfico Lineal'

# Botón para "Gráfico Lineal"
with col1:
    if st.button('Gráfico Lineal'):
        st.session_state.graph_option = 'Gráfico Lineal'

# Botón para "Gráfico de Índices"
with col2:
    if st.button('Gráfico de Índices'):
        st.session_state.graph_option = 'Gráfico de Índices'

# Inicializar start_date si no está definido
if 'start_date' not in st.session_state:
    st.session_state.start_date = datetime(1900, 1, 1)


# Crear un marcador de lugar para el gráfico
graph_placeholder = st.empty()

def render_graph(key):
    # Usar la fecha seleccionada
    start_date = st.session_state.start_date

    # Conectar a la base de datos
    conn = sqlite3.connect('macroeconomic_data.db')

    # Crear una figura interactiva
    fig = go.Figure()

    # Graficar los datos de los índices seleccionados
    for ticker_nombre in selected_tickers:
        # Consultar los datos del rango seleccionado
        query = f"SELECT fecha, close FROM {ticker_nombre} WHERE fecha >= '{start_date.strftime('%Y-%m-%d')}'"
        df = pd.read_sql(query, conn)
        df['fecha'] = pd.to_datetime(df['fecha'])

        # Normalizar si se selecciona el gráfico de índices
        if st.session_state.graph_option == 'Gráfico de Índices':
            df['close'] = (df['close'] / df['close'].iloc[0]) * 100

        # Obtener la moneda del ticker utilizando yfinance
        ticker_symbol = data_tickers['ticker'][data_tickers['nombreTicker'].index(ticker_nombre)]
        ticker_data = yf.Ticker(ticker_symbol)
        currency = ticker_data.info['currency']

        # Añadir los datos al gráfico
        fig.add_trace(go.Scatter(
            x=df['fecha'],
            y=df['close'],
            mode='lines',
            name=f"{ticker_nombre} ({currency})"
        ))

    # Ajustar el rango de fechas en función del valor de time_option y months_input
    fig.update_layout(
        title="Evolución Precio",
        xaxis_title="Fecha",
        yaxis_title="Índice Normalizado" if st.session_state.graph_option == 'Gráfico de Índices' else "Precio de Cierre",
        xaxis=dict(
            showgrid=True,
            tickformat="%e %b %Y",  # Muestra día, mes y año
            rangeslider=dict(visible=True),  # Añadir un slider interactivo para el rango de fechas
        ),
        yaxis=dict(
            showgrid=True
        ),
        hovermode="x unified",  # Al pasar el cursor, ver todos los valores en esa fecha
    )

    # Mostrar la gráfica interactiva en el marcador de lugar con una clave única
    graph_placeholder.plotly_chart(fig, key=key)
    conn.close()


# Renderizar el gráfico por primera vez con una clave inicial
render_graph(key="initial_graph")

# **Botones y entrada de fecha**
col1, col2, col3, col4 = st.columns(4)

# Actualizar start_date basado en las interacciones
with col1:
    if st.button('1 Año'):
        st.session_state.start_date = datetime.today() - timedelta(days=365)

with col2:
    if st.button('5 Años'):
        st.session_state.start_date = datetime.today() - timedelta(days=5 * 365)

with col3:
    if st.button('Todos los tiempos'):
        st.session_state.start_date = datetime(1900, 1, 1)

with col4:
    custom_date = st.date_input("Selecciona la fecha de inicio", st.session_state.start_date)
    if custom_date != st.session_state.start_date.date():
        st.session_state.start_date = datetime.combine(custom_date, datetime.min.time())

# Renderizar el gráfico después de las interacciones con una clave actualizada
render_graph(key="updated_graph")
