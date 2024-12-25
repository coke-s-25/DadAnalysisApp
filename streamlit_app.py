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


# Crear columnas: una para cada botón y la cuarta para el input del número de meses
col1, col2, col3, col4 = st.columns(4)

# Si no existe, inicializamos el estado de session_state
if 'time_option' not in st.session_state:
    st.session_state.time_option = "Todos los tiempos"
if 'months_input' not in st.session_state:
    st.session_state.months_input = 0  # Inicializamos como 0 para "Todos los tiempos"

# Colocar los botones en sus respectivas columnas
with col1:
    if st.button('1 Año'):
        st.session_state.time_option = "1 Año"
        st.session_state.months_input = 12  # Reiniciar a 12 meses al seleccionar "1 Año"
with col2:
    if st.button('5 Años'):
        st.session_state.time_option = "5 Años"
        st.session_state.months_input = 60  # Reiniciar a 60 meses al seleccionar "5 Años"
with col3:
    if st.button('Todos los tiempos'):
        st.session_state.time_option = "Todos los tiempos"
        st.session_state.months_input = 0  # Reiniciar a 0 meses al seleccionar "Todos los tiempos"
with col4:
    # Opción para ingresar el número de meses
    st.session_state.months_input = st.number_input(
        "Número de meses a graficar", 
        min_value=0, 
        max_value=500,  # Máximo 10 años
        value=st.session_state.months_input,  # Valor por defecto (0 o 12, dependiendo de la selección)
        step=1          # Paso de 1 mes
    )

# Si el campo de meses tiene un valor (es decir, no es 0), actualizamos time_option
if st.session_state.months_input > 0:
    st.session_state.time_option = f"{st.session_state.months_input} Meses"


# Establecer el rango de fechas en función del valor de time_option y months_input
today = datetime.today()

# Si el número de meses es mayor que 0, usamos ese valor
if st.session_state.months_input > 0:
    start_date = today - timedelta(days=st.session_state.months_input * 30)  # Aproximadamente 30 días por mes
else:
    # Si "Todos los tiempos" es seleccionado, usamos una fecha muy antigua
    start_date = datetime(1900, 1, 1)  # Todos los tiempos
    if st.session_state.time_option == "1 Año":
        start_date = today - timedelta(days=365)
    elif st.session_state.time_option == "5 Años":
        start_date = today - timedelta(days=5 * 365)



# Conectar a la base de datos
conn = sqlite3.connect('macroeconomic_data.db')

# Crear una figura interactiva
fig = go.Figure()

# Graficar los datos de los índices seleccionados
for ticker_nombre in selected_tickers:
    query = f"SELECT fecha, close FROM {ticker_nombre} WHERE fecha >= '{start_date.strftime('%Y-%m-%d')}'"
    df = pd.read_sql(query, conn)
    df['fecha'] = pd.to_datetime(df['fecha'])
    
    # Obtener la moneda del ticker utilizando yfinance
    ticker_symbol = data_tickers['ticker'][data_tickers['nombreTicker'].index(ticker_nombre)]
    ticker_data = yf.Ticker(ticker_symbol)
    currency = ticker_data.info['currency']
    
    # Añadir la línea de datos a la figura
    fig.add_trace(go.Scatter(x=df['fecha'], y=df['close'], mode='lines', name=f"{ticker_nombre} ({currency})"))

# Ajustar el rango de fechas en función del valor de time_option y months_input
fig.update_layout(
    title="Evolución Precio",
    xaxis_title="Fecha",
    yaxis_title="Precio de Cierre",
    xaxis=dict(
        showgrid=True,
        tickformat="%b %Y",  # Muestra mes y año
        rangeslider=dict(visible=True),  # Añadir un slider interactivo para el rango de fechas
    ),
    yaxis=dict(
        showgrid=True
    ),
    hovermode="x unified",  # Al pasar el cursor, ver todos los valores en esa fecha
)

# Mostrar la gráfica interactiva en Streamlit
st.plotly_chart(fig)

# Cerrar la conexión a la base de datos
conn.close()



