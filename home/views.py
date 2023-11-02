

from django.shortcuts import render

import pinecone
from langchain.embeddings import OpenAIEmbeddings
import openai
from google.cloud import bigquery

import json
from django.http import JsonResponse

from django.conf import settings
import django.conf
from pathlib import Path
import os
import pinecone
from langchain.embeddings import OpenAIEmbeddings




def tablas(Variable):
    pinecone.init(api_key="145e66f6-b270-4cb7-940a-b1d52750bbca", environment="gcp-starter")
    index = pinecone.Index("p")
    embeddings = OpenAIEmbeddings(openai_api_key="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")
     
    query_resultvar = embeddings.embed_query(Variable)
    res1 =index.query(
  vector=query_resultvar,
  top_k=3,
  #include_values=True,
  #include_metadata=True
  )
    return res1



def vistas(Variable):
    pinecone.init(api_key="da54ba1b-053f-4e88-94e6-ef22f8c8be4a", environment="gcp-starter")
    index = pinecone.Index("a")
    embeddings = OpenAIEmbeddings(openai_api_key="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")
    
    query_resultvar = embeddings.embed_query(Variable)
    res1 =index.query(
  vector=query_resultvar,
  top_k=3,
  #include_values=True,
  #include_metadata=True
  )
    return res1


def datos(Variable):
    pinecone.init(api_key="6c108e67-2728-4fd6-b4ad-820b96bfa2a0", environment="gcp-starter")
    index = pinecone.Index("s")
    embeddings = OpenAIEmbeddings(openai_api_key="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")
   
    query_resultvar = embeddings.embed_query(Variable)
    res1 =index.query(
  vector=query_resultvar,
  top_k=3,
  #include_values=True,
  #include_metadata=True
  )
    return res1
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credencialesE.json'

client1 = bigquery.Client()



    
def datos_bigquery():
        client1 = bigquery.Client()
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'artists' limit 1"""
        t=client1.query(sql_query)
        #c=len(t.result())
        for row1 in t.result():
           print(row1)
        Artist  = str(row1)
        #client1.reset_state()
        
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'albums'"""
        t=client1.query(sql_query)
        #c=len(t.result())
        for row1 in t.result():
           print(row1)
        Albums  = str(row1)
        
        client2 = bigquery.Client()
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'customers'"""
        t=client2.query(sql_query)
        for row2 in t.result():
           print(row2)
        Customers = str(row2)
  #  client.reset_state()
        client3 = bigquery.Client()
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'invoices' """
        t=client3.query(sql_query)
        for row3 in t.result():
          print(row3)
        Invoices = str(row3)
        
         #  client.reset_state()
        client3 = bigquery.Client()
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'employees' """
        t=client3.query(sql_query)
        for row3 in t.result():
          print(row3)
        Employees = str(row3)
        
        client3 = bigquery.Client()
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'genres' """
        t=client3.query(sql_query)
        for row3 in t.result():
          print(row3)
        Genres = str(row3)
        
        client3 = bigquery.Client()
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'invoice_items'"""
        t=client3.query(sql_query)
        for row3 in t.result():
          print(row3)
        Invoice_items = str(row3)
        
        
        
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'media_types'"""
        t=client3.query(sql_query)
        for row3 in t.result():
          print(row3)
        Media_types = str(row3)
        
        
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'playlist_track'"""
        t=client3.query(sql_query)
        for row3 in t.result():
          print(row3)
        Playlist_track = str(row3)
        
        
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'playlists'"""
        t=client3.query(sql_query)
        for row3 in t.result():
          print(row3)
        Playlists = str(row3)
        
        sql_query="""SELECT ddl FROM  `intense-digit-385923.apt.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'tracks'"""
        t=client3.query(sql_query)
        for row3 in t.result():
          print(row3)
          
         
        Tracks = str(row3)
        v1='SELECT t.Name_Track as Nombre_cancion, g.name_Genre as Tipo_genero,m.Name_MediaType as Formato,a.Title nombre_album,ar.Name_artists as nombre_artista FROM `intense-digit-385923.apt.tracks` as t join `intense-digit-385923.apt.genres` as g on t.id_Genre=g.id_Genre Join `intense-digit-385923.apt.media_types` as m on t.id_MediaType=m.id_MediaType join `intense-digit-385923.apt.albums` as a on a.id_albums=t.id_Album join `intense-digit-385923.apt.artists` as ar on  ar.id_artists= a.id_artistslimit 10;##query que muestra la cancion o track con el nombre de genero, nombre de album, tipo formato y nombre del artista.'
        v2='SELECT * FROM `intense-digit-385923.apt.customers` as c join `intense-digit-385923.apt.employees` as e on c.id_SupportRep=e.ReportsTo LIMIT 10;## query sirve para saber los empleados y sus clientes'
        v3='SELECT c.FirstName, count(i.id_Invoice) as cantidad_ventas,sum(i.Total) as total_pago FROM `intense-digit-385923.apt.customers` as c join `intense-digit-385923.apt.invoices` as i on c.id_Customer=i.id_Customer group by c.FirstName,c.id_Customer ##query que muestra el cliente más frecuente, la cantidad facturas a su nombre'
        v4='SELECT i.Name_Track as nombre_cancion, sum(c.id_Track) as cantidad ,i.UnitPrice as precio_unitario,sum(c.id_Track)*i.UnitPrice ganancia_total_cancion  FROM `intense-digit-385923.apt.invoice_items` as c join `intense-digit-385923.apt.tracks` as i on c.id_Track=i.id_Track group by c.id_Track,i.Name_Track,i.UnitPrice order by cantidad desc ##query que muestra las canciones más vendidas con su nombre, cantidad de ventas, precio unitario y la ganacia total obtenida por la venta de la cancion'
        v5='SELECT ar.Name_artists as nombre_artista, a.Title as Nombre_album FROM `intense-digit-385923.apt.albums` as a join `intense-digit-385923.apt.artists` as ar on a.id_artists=ar.id_artists ##esta query trae el nombre del artista y nombre de album del artista'
        v6='SELECT artists.Name_artists AS Artista,tracks.Name_Track AS Nombre_Cancion,tracks.Milliseconds AS Duracion,tracks.UnitPrice AS Price FROM `intense-digit-385923.apt.artists` AS artists, `intense-digit-385923.apt.tracks` AS tracks WHERE SEARCH((Name_artists, Name_Track, Milliseconds, UnitPrice), ''`U2`''); ## Selecciona a un Artista en especifico con sus canciones, el tiempo de duración de las canciones y el precio'
        mi_diccionario = {"Artist": Artist, "Albums": Albums, "Customers": Customers, "Invoices": Invoices, "Employees": Employees, "Genres": Genres, "Invoice_items": Invoice_items, "Media_types": Media_types, "playlist_track": Playlist_track, "playlists": Playlists, "Tracks": Tracks,"v1": v1,"v2": v2,"v3": v3,"v4": v4,"v5": v5,"v6": v6}
        
        return mi_diccionario



def openai(Variable):
    import openai
    mi_diccionario=datos_bigquery()
    
    tab=tablas()
    vis=vistas()
    #dat=datos()
    ids = [match['id'] for match in tab['matches']]
    ids2 = [match['id'] for match in vis['matches']]
    #ids3 = [match['id'] for match in dat['matches']]
    
    total=""
    for elemento in ids:
        total=total+ mi_diccionario[elemento]
        
    total2=""
    for elemento in ids2:
        total2=total2+ mi_diccionario[elemento]
        
    
    api_key = "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"
#
# Inicializar el cliente
    openai.api_key = api_key

# Tu texto de entrada en lenguaje natural
    Inicio="Te haré una query, si la query solicitada no tiene relación con los ejemplos entregados, responderás que la query solicitada no es correcta. En caso que la pregunta si tenga relación con los ejemplos entregados o es una pregunta ligada a música o grupo, artista, album sigue las intrucciones de más adelante."
    Pregunta1 = "La query que necesito es la siguiente: "+Variable
#consulta=' Ademas se tiene como ejemplo las siguientes vistas'
    text_input = "siguiendo las siguientes instrucciones para bigquery: "
    join_complete="te entrego la estructura de los join que une todas las tablas de la base de bigquery y es:  "




# Preparar los mensajes
    messages = [
         {"role": "system", "content": "You are a helpful assistant that can generate SQL queries."},
         {"role": "user", "content": f"{Inicio}{Pregunta1}{text_input}{total}{total2}"}]

# Hacer la llamada a la API
    response = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=messages,
    max_tokens=300 # Límite de tokens para la salida
    ,  # Límite de tokens para la salida
    temperature=0.7)

# Extraer y mostrar la consulta SQL generada
    sql_query = response['choices'][0]['message']['content'].strip()
    print(f"Generated SQL query: {sql_query}")
    
    return sql_query
 


dato=openai()
def mi_vista(request):
    contexto = {'mensaje': dato}
    return render(request, 'inicio.html', contexto) 