#!/usr/bin/env python
# coding: utf-8

# Librerias

import socket
from contextlib import closing
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from dash import Dash, dcc, html, Input, Output, dash_table


# Tema Oscuro
external_stylesheets = [{
    'href': 'https://fonts.googleapis.com/css2?family=Goldman&display=swap',
    'rel': 'stylesheet'
}]

# Configuración de colores
DARK_THEME = {
    'background': '#000000',
    'text': '#a49b67',  # Dorado
    'card_background': '#1E1E1E',
    'border': '#333333',
    'accent': '#a49b67',
    'secondary_text': '#AAAAAA'
}

# Configurar Plotly para tema oscuro
plotly_template = 'plotly_dark'
px.defaults.template = plotly_template
px.defaults.color_discrete_sequence = ['#FFD700', '#FFA500', '#FF8C00', '#FF6347']


# Conexion
uri = "mongodb://bdne_user:bdne_user@86.38.203.56:27017/gymdata?authSource=admin&authMechanism=SCRAM-SHA-256"
client = MongoClient(uri)
db = client['gymdata']


# Procesamiento de Datos
def get_municipios():
    """Obtiene todos los municipios únicos de los usuarios"""
    pipeline = [
        {"$match": {"direccion.municipio": {"$exists": True}}},
        {"$group": {"_id": "$direccion.municipio"}},
        {"$sort": {"_id": 1}}
    ]
    result = list(db.users.aggregate(pipeline))
    municipios = [{'label': 'Todos los municipios', 'value': 'all'}] + [{'label': m['_id'], 'value': m['_id']} for m in result]
    return municipios

def get_unique_activities():
    """Obtiene todas las actividades únicas de los gimnasios"""
    pipeline = [
        {"$unwind": "$actividades"},
        {"$group": {"_id": "$actividades"}},
        {"$sort": {"_id": 1}}
    ]
    result = list(db.gyms.aggregate(pipeline))
    return [{'label': act['_id'], 'value': act['_id']} for act in result]

def get_gym_data(municipio=None):
    """Obtiene datos de gimnasios, opcionalmente filtrados por municipio"""
    query = {}
    if municipio and municipio != 'all':
        # Obtener coordenadas del municipio
        user = db.users.find_one({"direccion.municipio": municipio})
        if user and 'direccion' in user and 'ubicacion' in user['direccion']:
            coords = user['direccion']['ubicacion']['coordinates']
            query = {
                "ubicacion": {
                    "$near": {
                        "$geometry": {
                            "type": "Point",
                            "coordinates": coords
                        },
                        "$maxDistance": 10000  # 10km radio
                    }
                }
            }
    
    gyms = list(db.gyms.find(query))
    
    # Procesar datos
    processed_gyms = []
    for gym in gyms:
        processed_gym = gym.copy()
        # Categorizar rating
        rating = processed_gym.get('averageRating', 0)
        if 0 <= rating < 1: processed_gym['rating_group'] = "0-1 ★"
        elif 1 <= rating < 2: processed_gym['rating_group'] = "1-2 ★"
        elif 2 <= rating < 3: processed_gym['rating_group'] = "2-3 ★"
        elif 3 <= rating < 4: processed_gym['rating_group'] = "3-4 ★"
        else: processed_gym['rating_group'] = "4-5 ★"
        
        processed_gyms.append(processed_gym)
    
    return processed_gyms

def get_user_count(municipio=None):
    """Obtiene cantidad de usuarios por municipio"""
    match = {"direccion.municipio": {"$exists": True}}
    if municipio and municipio != 'all':
        match["direccion.municipio"] = municipio
    
    return db.users.count_documents(match)

def create_main_figure(df, graph_type, selected_municipio):
    """Crea el gráfico principal según el tipo seleccionado"""
    location_label = selected_municipio if selected_municipio != 'all' else 'todos los municipios'
    
    if graph_type == 'actividades' and 'actividades' in df.columns:
        actividades_df = df.explode('actividades')
        actividades_count = actividades_df.groupby('actividades').size().reset_index(name='count')
        if not actividades_count.empty:
            fig = px.bar(
                actividades_count,
                x='actividades',
                y='count',
                title=f"Actividades más populares en {location_label}",
                color='actividades',
                labels={'actividades': 'Actividad', 'count': 'Cantidad'},
                height=500
            )
            fig.update_layout(showlegend=False)
            return fig
    
    elif graph_type == 'precios' and 'precio' in df.columns:
        fig = px.box(
            df,
            y='precio',
            title=f"Distribución de precios en {location_label}",
            labels={'precio': 'Precio ($)'},
            height=500
        )
        return fig
    
    elif graph_type == 'gimnasios' and 'createdAt' in df.columns:
        df['year'] = pd.to_datetime(df['createdAt']).dt.year
        count_by_year = df.groupby('year').size().reset_index(name='count')
        if not count_by_year.empty:
            fig = px.line(
                count_by_year,
                x='year',
                y='count',
                title=f"Gimnasios creados por año en {location_label}",
                labels={'year': 'Año', 'count': 'Cantidad de gimnasios'},
                height=500
            )
            return fig
    
    elif graph_type == 'ratings' and 'rating_group' in df.columns:
        rating_count = df.groupby('rating_group').size().reset_index(name='count')
        if not rating_count.empty:
            fig = px.pie(
                rating_count,
                names='rating_group',
                values='count',
                title=f"Distribución de ratings en {location_label}",
                labels={'rating_group': 'Rating', 'count': 'Cantidad'},
                height=500
            )
            return fig
    
    return px.bar(title="No hay datos disponibles para este gráfico", height=500)

def create_secondary_figure(df, selected_municipio):
    """Crea el gráfico secundario de tendencia de precios"""
    location_label = selected_municipio if selected_municipio != 'all' else 'todos los municipios'
    
    if 'createdAt' in df.columns and 'precio' in df.columns:
        df['year'] = pd.to_datetime(df['createdAt']).dt.year
        price_trend = df.groupby('year')['precio'].mean().reset_index()
        if not price_trend.empty:
            fig = px.line(
                price_trend,
                x='year',
                y='precio',
                title=f"Tendencia de precios promedio por año en {location_label}",
                labels={'year': 'Año', 'precio': 'Precio promedio ($)'},
                height=400
            )
            return fig
    
    return px.line(title="No hay datos suficientes para mostrar la tendencia", height=400)

def prepare_table_data(df):
    """Prepara los datos para la tabla, asegurando que los campos existan y sean del tipo correcto"""
    table_df = df.copy()
    
    # Columnas requeridas
    columns_info = {
        'nombre': {'default': 'Desconocido', 'type': 'text'},
        'precio': {'default': '$0.00', 'type': 'text', 'format': lambda x: f"${x:.2f}" if isinstance(x, (int, float)) else '$0.00'},
        'averageRating': {'default': '0.0 ★', 'type': 'text', 'format': lambda x: f"{x:.1f} ★" if isinstance(x, (int, float)) else '0.0 ★'},
        'actividades': {'default': 'Ninguna', 'type': 'text', 'format': lambda x: ', '.join(x) if isinstance(x, list) else str(x)}
    }
    
    for col, info in columns_info.items():
        if col not in table_df.columns:
            table_df[col] = info['default']
        else:
            if 'format' in info:
                table_df[col] = table_df[col].apply(info['format'])
            table_df[col] = table_df[col].fillna(info['default'])
    
    return table_df[list(columns_info.keys())].to_dict('records')

def create_gym_map(df, selected_municipio):
    """Crea un mapa interactivo con los gimnasios"""
    location_label = selected_municipio if selected_municipio != 'all' else 'Ciudad de México'
    
    if df.empty or 'ubicacion' not in df.columns:
        # Mapa vacío centrado en CDMX como fallback
        fig = px.scatter_map(
            title=f"No hay gimnasios para mostrar en {location_label}",
            height=600
        )
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox_zoom=10,
            mapbox_center={"lat": 19.4326, "lon": -99.1332},
            margin={"r":0,"t":40,"l":0,"b":0}
        )
        return fig
    
    # Extraer coordenadas de los gimnasios
    df['lat'] = df['ubicacion'].apply(lambda x: x['coordinates'][1] if isinstance(x, dict) else None)
    df['lon'] = df['ubicacion'].apply(lambda x: x['coordinates'][0] if isinstance(x, dict) else None)
    
    # Eliminar filas sin coordenadas válidas
    df = df.dropna(subset=['lat', 'lon'])
    
    if df.empty:
        # Mapa vacío centrado en CDMX como fallback
        fig = px.scatter_map(
            title=f"No hay gimnasios con ubicación válida en {location_label}",
            height=600
        )
        fig.update_layout(
            mapbox_style="open-street-map",
            mapbox_zoom=10,
            mapbox_center={"lat": 19.4326, "lon": -99.1332},
            margin={"r":0,"t":40,"l":0,"b":0}
        )
        return fig
    
    # Crear el mapa con los gimnasios
    fig = px.scatter_map(
        df,
        lat="lat",
        lon="lon",
        hover_name="nombre",
        hover_data={
            "precio": True,
            "averageRating": True,
            "actividades": True,
            "lat": False,
            "lon": False
        },
        color="averageRating",
        color_continuous_scale=px.colors.sequential.YlOrBr,  # Cambiado a una paleta válida
        size_max=15,
        zoom=10,
        title=f"Gimnasios en {location_label}",
        height=600
    )
    
    # Configuración del mapa
    fig.update_layout(
        mapbox_style="open-street-map",
        mapbox_center={"lat": 19.4326, "lon": -99.1332},
        margin={"r":0,"t":40,"l":0,"b":0},
        coloraxis_colorbar={
            'title': 'Rating',
            'tickvals': [0, 1, 2, 3, 4, 5],
            'ticktext': ['0 ★', '1 ★', '2 ★', '3 ★', '4 ★', '5 ★']
        }
    )
    
    # Personalizar tooltips
    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br><br>" +
                     "Precio: $%{customdata[0]:.2f}<br>" +
                     "Rating: %{customdata[1]:.1f} ★<br>" +
                     "Actividades: %{customdata[2]}<extra></extra>"
    )
    
    return fig


# Dashbord
app = Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(style={
    'backgroundColor': DARK_THEME['background'],
    'color': DARK_THEME['text'],
    'minHeight': '100vh',
    'padding': '20px',
    'fontFamily': 'Goldman, sans-serif'
}, children=[
    # Sección de imagen (NUEVO)
    html.Div(style={
        'textAlign': 'center',
        'marginBottom': '30px',
        'borderRadius': '15px',
        'overflow': 'hidden'
    }, children=[
        html.Img(
            src='https://i.ibb.co/zWP36zFQ/Screenshot-2025-05-10-173242.png',  # URL de imagen de ejemplo
            style={
                'width': '85%',
                'height': '250px',
                'objectFit': 'cover',
                'border': f"2px solid {DARK_THEME['accent']}",
                'boxShadow': '0 4px 15px rgba(255, 215, 0, 0.3)'
            }
        )
    ]),
    
    html.H1("Dashboard de Gimnasios - CDMX", style={
        'textAlign': 'center',
        'marginBottom': '30px',
        'color': DARK_THEME['text'],
        'textShadow': '0 0 10px rgba(255, 215, 0, 0.3)'
    }),

    # Filtros
    html.Div(style={
        'backgroundColor': DARK_THEME['card_background'],
        'padding': '15px',
        'borderRadius': '10px',
        'marginBottom': '20px',
        'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
        'border': f"1px solid {DARK_THEME['border']}"
    }, children=[
        html.Div(style={
            'display': 'flex',
            'flexWrap': 'wrap',
            'justifyContent': 'space-between',
            'gap': '10px'
        }, children=[
            # Filtro de Municipio
            html.Div(style={'flex': '1', 'minWidth': '200px'}, children=[
                html.Label("Municipio:", style={
                    'fontWeight': 'bold',
                    'marginBottom': '5px',
                    'color': DARK_THEME['text']
                }),
                dcc.Dropdown(
                    id='municipio-dropdown',
                    options=get_municipios(),
                    value='all',
                    clearable=False,
                    className='custom-dropdown',
                    style={
                        'width': '100%',
                        'backgroundColor': DARK_THEME['card_background'],
                        'color': DARK_THEME['text'],
                        'border': f"1px solid {DARK_THEME['border']}"
                    }
                )
            ]),
            
            # Filtro de Rating
            html.Div(style={'flex': '1', 'minWidth': '200px'}, children=[
                html.Label("Rango de Rating:", style={
                    'fontWeight': 'bold',
                    'marginBottom': '5px',
                    'color': DARK_THEME['text']
                }),
                dcc.Dropdown(
                    id='rating-dropdown',
                    options=[
                        {'label': 'Todos', 'value': 'all'},
                        {'label': '0-1 ★', 'value': '0-1'},
                        {'label': '1-2 ★', 'value': '1-2'},
                        {'label': '2-3 ★', 'value': '2-3'},
                        {'label': '3-4 ★', 'value': '3-4'},
                        {'label': '4-5 ★', 'value': '4-5'}
                    ],
                    value='all',
                    clearable=False,
                    className='custom-dropdown',
                    style={
                        'width': '100%',
                        'backgroundColor': DARK_THEME['card_background'],
                        'color': DARK_THEME['text'],
                        'border': f"1px solid {DARK_THEME['border']}"
                    }
                )
            ]),
            
            # Filtro de Actividad
            html.Div(style={'flex': '1', 'minWidth': '200px'}, children=[
                html.Label("Filtrar por Actividad:", style={
                    'fontWeight': 'bold',
                    'marginBottom': '5px',
                    'color': DARK_THEME['text']
                }),
                dcc.Dropdown(
                    id='actividad-dropdown',
                    options=[{'label': 'Todas', 'value': 'all'}] + get_unique_activities(),
                    value='all',
                    clearable=False,
                    className='custom-dropdown',
                    style={
                        'width': '100%',
                        'backgroundColor': DARK_THEME['card_background'],
                        'color': DARK_THEME['text'],
                        'border': f"1px solid {DARK_THEME['border']}"
                    }
                )
            ]),
            
            # Filtro de Tipo de Gráfico
            html.Div(style={'flex': '1', 'minWidth': '200px'}, children=[
                html.Label("Tipo de Gráfico:", style={
                    'fontWeight': 'bold',
                    'marginBottom': '5px',
                    'color': DARK_THEME['text']
                }),
                dcc.Dropdown(
                    id='graph-type-dropdown',
                    options=[
                        {'label': 'Actividades', 'value': 'actividades'},
                        {'label': 'Tendencia de Precios', 'value': 'precios'},
                        {'label': 'Cantidad de Gimnasios', 'value': 'gimnasios'},
                        {'label': 'Distribución de Ratings', 'value': 'ratings'}
                    ],
                    value='actividades',
                    clearable=False,
                    className='custom-dropdown',
                    style={
                        'width': '100%',
                        'backgroundColor': DARK_THEME['card_background'],
                        'color': DARK_THEME['text'],
                        'border': f"1px solid {DARK_THEME['border']}"
                    }
                )
            ])
        ])
    ]),
    
    # KPIs (4 indicadores clave)
    html.Div(style={
        'display': 'grid',
        'gridTemplateColumns': 'repeat(auto-fit, minmax(250px, 1fr))',
        'gap': '15px',
        'marginBottom': '30px'
    }, children=[
        # Precio Promedio
        html.Div(style={
            'backgroundColor': DARK_THEME['card_background'],
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
            'border': f"1px solid {DARK_THEME['border']}",
            'textAlign': 'center'
        }, children=[
            html.Div("Precio Promedio", style={
                'fontSize': '16px',
                'fontWeight': 'bold',
                'marginBottom': '10px',
                'color': DARK_THEME['text']
            }),
            html.Div(id='precio-promedio-kpi', style={
                'fontSize': '28px',
                'fontWeight': 'bold',
                'color': DARK_THEME['accent'],
                'textShadow': '0 0 8px rgba(255, 215, 0, 0.3)'
            })
        ]),
        
        # Rating Promedio
        html.Div(style={
            'backgroundColor': DARK_THEME['card_background'],
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
            'border': f"1px solid {DARK_THEME['border']}",
            'textAlign': 'center'
        }, children=[
            html.Div("Rating Promedio", style={
                'fontSize': '16px',
                'fontWeight': 'bold',
                'marginBottom': '10px',
                'color': DARK_THEME['text']
            }),
            html.Div(id='rating-promedio-kpi', style={
                'fontSize': '28px',
                'fontWeight': 'bold',
                'color': DARK_THEME['accent'],
                'textShadow': '0 0 8px rgba(255, 215, 0, 0.3)'
            })
        ]),
        
        # Total Usuarios
        html.Div(style={
            'backgroundColor': DARK_THEME['card_background'],
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
            'border': f"1px solid {DARK_THEME['border']}",
            'textAlign': 'center'
        }, children=[
            html.Div("Total Usuarios", style={
                'fontSize': '16px',
                'fontWeight': 'bold',
                'marginBottom': '10px',
                'color': DARK_THEME['text']
            }),
            html.Div(id='usuarios-kpi', style={
                'fontSize': '28px',
                'fontWeight': 'bold',
                'color': DARK_THEME['accent'],
                'textShadow': '0 0 8px rgba(255, 215, 0, 0.3)'
            })
        ]),
        
        # Total Gimnasios
        html.Div(style={
            'backgroundColor': DARK_THEME['card_background'],
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
            'border': f"1px solid {DARK_THEME['border']}",
            'textAlign': 'center'
        }, children=[
            html.Div("Total Gimnasios", style={
                'fontSize': '16px',
                'fontWeight': 'bold',
                'marginBottom': '10px',
                'color': DARK_THEME['text']
            }),
            html.Div(id='gimnasios-kpi', style={
                'fontSize': '28px',
                'fontWeight': 'bold',
                'color': DARK_THEME['accent'],
                'textShadow': '0 0 8px rgba(255, 215, 0, 0.3)'
            })
        ])
    ]),
    
    # Mapa de gimnasios (nuevo componente)
    html.Div(style={
        'backgroundColor': DARK_THEME['card_background'],
        'padding': '15px',
        'borderRadius': '10px',
        'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
        'marginBottom': '20px',
        'border': f"1px solid {DARK_THEME['border']}"
    }, children=[
        dcc.Graph(id='gym-map')
    ]),
    
    # Gráfico principal
    html.Div(style={
        'backgroundColor': DARK_THEME['card_background'],
        'padding': '15px',
        'borderRadius': '10px',
        'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
        'marginBottom': '20px',
        'border': f"1px solid {DARK_THEME['border']}"
    }, children=[
        dcc.Graph(id='main-graph')
    ]),
    
    # Gráfico secundario (tendencia temporal)
    html.Div(style={
        'backgroundColor': DARK_THEME['card_background'],
        'padding': '15px',
        'borderRadius': '10px',
        'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
        'marginBottom': '20px',
        'border': f"1px solid {DARK_THEME['border']}"
    }, children=[
        dcc.Graph(id='secondary-graph')
    ]),
    
    # Tabla de datos
    html.Div(style={
        'backgroundColor': DARK_THEME['card_background'],
        'padding': '20px',
        'borderRadius': '10px',
        'boxShadow': '0 4px 8px rgba(0,0,0,0.3)',
        'border': f"1px solid {DARK_THEME['border']}"
    }, children=[
        html.H3("Detalle de Gimnasios", style={
            'marginBottom': '20px',
            'color': DARK_THEME['text'],
            'textAlign': 'center'
        }),
        dash_table.DataTable(
            id='data-table',
            columns=[
                {'name': 'Nombre', 'id': 'nombre', 'type': 'text'},
                {'name': 'Precio', 'id': 'precio', 'type': 'text'},
                {'name': 'Rating', 'id': 'averageRating', 'type': 'text'},
                {'name': 'Actividades', 'id': 'actividades', 'type': 'text'}
            ],
            page_size=10,
            filter_action='native',
            sort_action='native',
            style_table={
                'overflowX': 'auto',
                'width': '100%',
                'minWidth': '100%',
                'color': DARK_THEME['text']
            },
            style_header={
                'backgroundColor': DARK_THEME['background'],
                'color': DARK_THEME['accent'],
                'fontWeight': 'bold',
                'textAlign': 'center',
                'border': f"1px solid {DARK_THEME['border']}"
            },
            style_cell={
                'backgroundColor': DARK_THEME['card_background'],
                'color': DARK_THEME['text'],
                'textAlign': 'left',
                'padding': '10px',
                'minWidth': '100px',
                'whiteSpace': 'normal',
                'height': 'auto',
                'border': f"1px solid {DARK_THEME['border']}"
            },
            style_data={
                'border': f"1px solid {DARK_THEME['border']}"
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#2a2a2a'
                },
                {
                    'if': {'state': 'active'},
                    'backgroundColor': 'rgba(255, 215, 0, 0.3)',
                    'border': f"1px solid {DARK_THEME['accent']}"
                }
            ],
            style_filter={
                'backgroundColor': DARK_THEME['card_background'],
                'color': DARK_THEME['text'],
                'border': f"1px solid {DARK_THEME['border']}"
            }
        )
    ])
])


# Callbacks (Para hacer el dashbord interactivo)
@app.callback(
    [Output('precio-promedio-kpi', 'children'),
     Output('rating-promedio-kpi', 'children'),
     Output('usuarios-kpi', 'children'),
     Output('gimnasios-kpi', 'children'),
     Output('main-graph', 'figure'),
     Output('secondary-graph', 'figure'),
     Output('data-table', 'data'),
     Output('gym-map', 'figure')],
    [Input('municipio-dropdown', 'value'),
     Input('rating-dropdown', 'value'),
     Input('actividad-dropdown', 'value'),
     Input('graph-type-dropdown', 'value')]
)
def update_dashboard(selected_municipio, selected_rating, selected_actividad, graph_type):
    # Obtener datos filtrados
    gyms = get_gym_data(selected_municipio)
    df = pd.DataFrame(gyms)
    
    # Si no hay datos, devolver valores por defecto
    if df.empty:
        empty_map = px.scatter_mapbox(
            title=f"No hay gimnasios para mostrar en {selected_municipio if selected_municipio != 'all' else 'CDMX'}",
            height=600
        )
        empty_map.update_layout(
            mapbox_style="open-street-map",
            mapbox_zoom=10,
            mapbox_center={"lat": 19.4326, "lon": -99.1332},
            margin={"r":0,"t":40,"l":0,"b":0}
        )
        
        return (
            "$0.00",
            "0.0 ★",
            "0 usuarios",
            "0 gimnasios",
            px.bar(title="No hay datos disponibles", height=500),
            px.line(title="No hay datos disponibles", height=400),
            [],
            empty_map
        )
    
    # Filtrar por rating 
    if selected_rating != 'all':
        rating_map = {
            '0-1': '0-1 ★',
            '1-2': '1-2 ★',
            '2-3': '2-3 ★',
            '3-4': '3-4 ★',
            '4-5': '4-5 ★'
        }
        df = df[df['rating_group'] == rating_map[selected_rating]]
    
    # Filtrar por actividad 
    if selected_actividad != 'all' and 'actividades' in df.columns:
        df = df[df['actividades'].apply(lambda x: selected_actividad in x if isinstance(x, list) else False)]
    
    # Calcular KPIs
    precio_promedio = f"${df['precio'].mean():.2f}" if 'precio' in df.columns and not df.empty else "$0.00"
    rating_promedio = f"{df['averageRating'].mean():.1f} ★" if 'averageRating' in df.columns and not df.empty else "0.0 ★"
    user_count = f"{get_user_count(selected_municipio)} usuarios"
    gym_count = f"{len(df)} gimnasios"
    
    # Graficas de datos
    main_fig = create_main_figure(df, graph_type, selected_municipio)
    secondary_fig = create_secondary_figure(df, selected_municipio)
    
    # Datos para la tabla
    table_data = prepare_table_data(df)
    
    # Crear el mapa
    gym_map = create_gym_map(df, selected_municipio)
    
    return (
        precio_promedio,
        rating_promedio,
        user_count,
        gym_count,
        main_fig,
        secondary_fig,
        table_data,
        gym_map
    )


# Ejecucion del Dashbord

def find_free_port():
    """Encuentra un puerto disponible automáticamente"""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]

if __name__ == '__main__':
    # port = find_free_port()
    # print(f"\n----------------------------------------")
    # print(f"Dashboard ejecutándose en: http://localhost:{port}")
    # print(f"----------------------------------------\n")
    app.run(debug=False, port=3000, host='0.0.0.0')
