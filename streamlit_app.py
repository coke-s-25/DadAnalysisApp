import sqlite3
import pandas as pd
import scrapy
import yfinance as yf
import streamlit as st


# --------------------------------------------------------------------------------------- TICKERS
data_tickers={'ticker': ["^GSPC","^IBEX"], "nombreTicker": ["SyP_500", "IBEX_35"]}
df_tickers = pd.DataFrame(data_tickers)

datos_historicos_dict = {}

# Iterar sobre los tickers y sus nombres
for ticker, nombre in zip(data_tickers['ticker'], data_tickers['nombreTicker']):
    # Obtener los datos históricos usando yfinance
    datos_historicos = yf.Ticker(ticker).history(period="max")
    
    # Seleccionar solo la columna "Close"
    datos_close = datos_historicos[['Close']].reset_index()
    
    # Guardar el DataFrame en el diccionario con la clave como el nombre del ticker
    datos_historicos_dict[f'datosHistoricos_{nombre.replace(" ", "_")}'] = datos_close

# --------------------------------------------------------------------------------------- CREAR Y ACTUALIZAR BASE DE DATOS

# Conectar a la base de datos
conexion = sqlite3.connect('macroeconomic_data.db', timeout=10)
cursor = conexion.cursor()

# Crear la tabla de nuevo
cursor.execute('''CREATE TABLE IF NOT EXISTS tickers (ticker TEXT PRIMARY KEY,nombreTicker TEXT)''')

# Insertar los datos del DataFrame en la tabla
# Convertir el DataFrame a una lista de tuplas
data_para_insertar = list(df_tickers.itertuples(index=False, name=None))

# Usar executemany para insertar múltiples filas
cursor.executemany('''INSERT INTO tickers (ticker, nombreTicker)VALUES (?, ?)''', data_para_insertar)

#Iterar sobre los DataFrames en el diccionario y crear tablas
for nombre in data_tickers['nombreTicker']:
    # Formatear el nombre para que sea un nombre de tabla válido
    nombre_tabla = nombre.replace(" ", "_")  # Reemplazar espacios con guiones bajos

    # Crear la tabla en la base de datos
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {nombre_tabla} (fecha DATE PRIMARY KEY,close REAL)''')

    # Insertar los datos en la tabla
    df = datos_historicos_dict[f'datosHistoricos_{nombre_tabla}'].reset_index()
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')  # Convertir y formatear la fecha
    
    # Crear una lista de tuplas para insertar
    df_insertar = list(zip(df['Date'], df['Close']))  # Crear tuplas de (fecha, close)

    # Usar executemany para insertar los datos en la tabla
    cursor.executemany(f'''INSERT OR REPLACE INTO {nombre_tabla} (fecha, close) VALUES (?, ?)''', df_insertar)

# Guardar los cambios
conexion.commit()

# Cerrar la conexión
conexion.close()

# --------------------------------------------------------------------------------------- VISUALIZACIÓN

st.markdown("""
    <style>
        .title {
            text-align: center;
            font-size: 45px;
            font-weight: bold;
        }
        .subtitle {
            text-align: center;
            font-size: 20px;
            font-style: italic;
        }
    </style>
""", unsafe_allow_html=True)

st.markdown("""
    <div class="title">
        Movie Success Prediction
    </div>
    <div class="subtitle">
        Created by: Ali Alhaj Hassan, Ana Real, Amelia Lane, Jorge Saurina
    </div>
""", unsafe_allow_html=True)



# Conectar a la base de datos
conn = sqlite3.connect('macroeconomic_data.db')

# Consultar los datos
query = "SELECT * FROM SyP_500"
df = pd.read_sql(query, conn)

# Mostrar los datos
print(df)

# Cerrar la conexión a la base de datos
conn.close()




