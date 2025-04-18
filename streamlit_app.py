import sqlite3
import pandas as pd
import yfinance as yf
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, timedelta
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

# --------------------------------------------------------------------------------------- DIRECCIÓN ARCHIVO TERMINAL y COMANDO RUN
# cd D:\DadAnalysisApp 
# cd C:\Users\Usuario\Desktop\DadAnalysisApp
# streamlit run streamlit_app.py
# --------------------------------------------------------------------------------------- TICKERS

data_tickers_indices = {
    'ticker': ["^GSPC", "^IBEX", "^GDAXI", "^FCHI", "^STOXX", "^IXIC", "MCHI", "EWG"],
    'nombreTicker': ["SyP_500", "IBEX_35", "DAX", "CAC40", "Eurostoxx600", "NASDAQ", "MSCI_China", "MSCI_Alemania"]
}

data_tickers_bancos = {
    'ticker': [
        "HSBA.L",       # HSBC (UK)
        "SAN.MC",       # Banco Santander (España)
        "BNP.PA",       # BNP Paribas (Francia)
        "DBK.DE",       # Deutsche Bank (Alemania)
        "INGA.AS",      # ING (Países Bajos)
        "CS.PA",        # Crédit Agricole (Francia)
        "UBSG.SW",      # UBS (Suiza)
        "BARC.L",       # Barclays (UK)
        "ISP.MI",       # Intesa Sanpaolo (Italia)
        "BBVA.MC",      # BBVA (España)
        "NDX1.DE",      # Nordea (Finlandia vía Xetra)
        "DANSKE.CO",    # Danske Bank (Dinamarca)
        "SWEDAS.XD"      # Swedbank (Suecia)
    ],
    'nombreTicker': [
        "HSBC", "Banco_Santander", "BNP_Paribas", "Deutsche_Bank", "ING", "Credit_Agricole",
        "UBS", "Barclays", "Intesa_Sanpaolo", "BBVA", "Nordea",
        "Danske_Bank", "Swedbank"
    ]
}


df_indices = pd.DataFrame(data_tickers_indices)
df_bancos = pd.DataFrame(data_tickers_bancos)

df_tickers = pd.concat([df_indices, df_bancos], ignore_index=True)
datos_historicos_dict = {}


# --------------------------------------------------------------------------------------- CREAR Y ACTUALIZAR BASE DE DATOS CON VALORES CIERRES


with sqlite3.connect('macroeconomic_data.db', timeout=10, check_same_thread=False) as conexion:
    cursor = conexion.cursor()

    ##Borrar todas las tablas
    #cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    #tables = cursor.fetchall()
    #
    #for table in tables:
    #    cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")


    for ticker, nombre in zip(df_tickers['ticker'], df_tickers['nombreTicker']):
        # Verificar si la tabla ya existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (nombre,))
        tabla_existe = cursor.fetchone()

        if tabla_existe:
            cursor.execute(f"SELECT MAX(fecha) FROM {nombre}")
            ultima_fecha_en_db = cursor.fetchone()[0]
        else:
            ultima_fecha_en_db = None

        # Lógica de descarga
        if not ultima_fecha_en_db:
            datos_historicos = yf.Ticker(ticker).history(period="max")
        else:
            fecha_inicio = pd.to_datetime(ultima_fecha_en_db) + timedelta(days=1)
            if fecha_inicio.date() > datetime.today().date():
                continue
            datos_historicos = yf.Ticker(ticker).history(start=fecha_inicio)

        if datos_historicos.empty:
            continue

        datos_close = datos_historicos[['Close']].reset_index()
        datos_historicos_dict[f'datosHistoricos_{nombre}'] = datos_close

    # Crear tabla de tickers
    cursor.execute('''CREATE TABLE IF NOT EXISTS tickers (ticker TEXT PRIMARY KEY,nombreTicker TEXT)''')
    data_para_insertar = list(df_tickers.itertuples(index=False, name=None))
    cursor.executemany('''INSERT OR REPLACE INTO tickers (ticker, nombreTicker) VALUES (?, ?)''', data_para_insertar)

    for nombre in df_tickers['nombreTicker']:
        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {nombre} (fecha DATE PRIMARY KEY,close REAL)''')

        if f'datosHistoricos_{nombre}' not in datos_historicos_dict:
            continue

        df = datos_historicos_dict[f'datosHistoricos_{nombre}'].reset_index()
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        df_insertar = list(zip(df['Date'], df['Close']))
        cursor.executemany(f'''INSERT OR REPLACE INTO {nombre} (fecha, close) VALUES (?, ?)''', df_insertar)

    conexion.commit()


## Crear la tabla de nuevo
#cursor.execute('''CREATE TABLE IF NOT EXISTS tickers (ticker TEXT PRIMARY KEY,nombreTicker TEXT)''')
#
## Insertar los datos del DataFrame en la tabla
## Convertir el DataFrame a una lista de tuplas
#data_para_insertar = list(df_tickers.itertuples(index=False, name=None))
#
## Usar executemany para insertar múltiples filas
#cursor.executemany('''INSERT OR REPLACE INTO tickers (ticker, nombreTicker)VALUES (?, ?)''', data_para_insertar)
#
##Iterar sobre los DataFrames en el diccionario y crear tablas
#for nombre in data_tickers['nombreTicker']:
#    # Crear la tabla en la base de datos
#    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {nombre} (fecha DATE PRIMARY KEY,close REAL)''')
#
#    # Insertar los datos en la tabla
#    if f'datosHistoricos_{nombre}' not in datos_historicos_dict:
#        continue
#
#    df = datos_historicos_dict[f'datosHistoricos_{nombre}'].reset_index()
#    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')  # Convertir y formatear la fecha
#    
#    # Crear una lista de tuplas para insertar
#    df_insertar = list(zip(df['Date'], df['Close']))  # Crear tuplas de (fecha, close)
#
#    # Usar executemany para insertar los datos en la tabla
#    cursor.executemany(f'''INSERT OR REPLACE INTO {nombre} (fecha, close) VALUES (?, ?)''', df_insertar)
#
## Guardar los cambios
#conexion.commit()
#
## Cerrar la conexión
#conexion.close()

# --------------------------------------------------------------------------------------- CREAR Y ACTUALIZAR BASE DE DATOS CON COMPONENTES ÍNDICES



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

pagina = st.sidebar.radio("Selecciona una pestaña:", ["📈 Gráficar", "📊 Estudio Índices", "💰 Inversión", "🧑🏻‍🤝‍🧑🏽 Estudio Países", "🗺️ Estudio Regiones"])

if pagina == "📈 Gráficar":

    ## Selección de índices para visualizar
    #selected_tickers = st.multiselect(
    #    "Selecciona los índices para visualizar",
    #    options=df_tickers['nombreTicker'].tolist(),
    #)

    indices = df_tickers[df_tickers['nombreTicker'].str.contains("SyP|IBEX|DAX|CAC|Eurostoxx|NASDAQ|MSCI")]
    bancos = df_tickers[~df_tickers['nombreTicker'].isin(indices['nombreTicker'])]


    selected_tickers = []

    with st.expander("Selecciona activos para visualizar"):
        st.markdown("#### 🧮 Índices")
        cols_indices = st.columns(4)
        for i, nombre in enumerate(indices['nombreTicker']):
            with cols_indices[i % 4]:
                if st.checkbox(nombre, key=f"indice_{nombre}"):
                    selected_tickers.append(nombre)

        st.markdown("#### 🏦 Bancos")
        cols_bancos = st.columns(4)
        for i, nombre in enumerate(bancos['nombreTicker']):
            with cols_bancos[i % 4]:
                if st.checkbox(nombre, key=f"banco_{nombre}"):
                    selected_tickers.append(nombre)



    # Aplicamos CSS para cambiar el color de fondo de los contenedores
    st.markdown(
        """
        <style>
        /* Obtener el color primario del tema */
        :root {
            --primary-color: #2BA846; /* Color principal definido en config.toml */
            --primary-color-dark: #168F40; /* Versión más oscura al pasar el ratón */
            --text-color: white; /* Color del texto */
        }

        /* Estilo para los botones */
        .stButton>button {
            background-color: var(--primary-color);
            color: var(--text-color);
            border-radius: 5px;
            border: none;
            padding: 10px 20px;
            cursor: pointer;
            transition: background-color 0.3s ease;
        }

        /* Cambio de color al pasar el ratón */
        .stButton>button:hover {
            background-color: var(--primary-color-dark);
            color: var(--text-color); /* Mantener el texto en blanco */
        }
        </style>
        """,
        unsafe_allow_html=True
    )


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
            ticker_symbol = df_tickers.loc[df_tickers['nombreTicker'] == ticker_nombre, 'ticker'].values[0]

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


    # Sección Resumen
    if selected_tickers:
        st.markdown("### 📊 Datos Financieros Clave")

        # Estilos CSS para los boxes
        st.markdown("""
            <style>
            .box-container {
                border: 1px solid #444;
                border-radius: 10px;
                padding: 15px;
                background-color: #1e1e1e;
                color: white;
                text-align: center;
                margin-bottom: 20px;
            }
            .box-title {
                font-weight: bold;
                font-size: 18px;
                margin-bottom: 10px;
            }
            .box-item {
                margin: 5px 0;
                font-size: 16px;
            }
            </style>
        """, unsafe_allow_html=True)

        # Mostrar de a 3 columnas por fila
        cols = st.columns(3)
        for i, nombre in enumerate(selected_tickers):
            col = cols[i % 3]  # seleccionar columna según posición

            ticker_symbol = df_tickers.loc[df_tickers['nombreTicker'] == nombre, 'ticker'].values[0]
            ticker_data = yf.Ticker(ticker_symbol)

            try:
                info = ticker_data.info
                pb_ratio = info.get('priceToBook', 'N/A')
                earnings = info.get('netIncomeToCommon', 'N/A')
                total_assets = info.get('totalAssets', 'N/A')

                html_content = f"""
                <div class='box-container'>
                    <div class='box-title'>{nombre} ({ticker_symbol})</div>
                    <div class='box-item'>📘 P/B Ratio: {pb_ratio}</div>
                    <div class='box-item'>💰 Earnings After Tax: {earnings}</div>
                    <div class='box-item'>🏦 Total Assets: {total_assets}</div>
                </div>
                """

                col.markdown(html_content, unsafe_allow_html=True)

            except Exception as e:
                col.warning(f"No se pudo obtener información para {nombre}")






#if pagina == "📊 Estudio Índices":
#    st.write(df_componentes.head(10))