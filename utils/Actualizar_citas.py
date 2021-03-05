import sqlite3
import sys
from wos import WosClient
import wos.utils
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
import pandas as pd 
from datetime import datetime

def Conexión_api(user, password, query, symbolicTimeSpan):
    with WosClient(user, password) as client:
        if symbolicTimeSpan is not None:
            a = client.search(query=query, symbolicTimeSpan = symbolicTimeSpan)
        else: 
            a = client.search(query=query)
        if symbolicTimeSpan is not None:
                root = ET.fromstring(wos.utils.query(client, query,symbolicTimeSpan, count=a.recordsFound))
        else: 
            root = ET.fromstring(wos.utils.query(client, query, count=a.recordsFound))
        tree = ElementTree(root)
        return tree

def Actualizar_citas(tree, bd):
    cites = []
    df_cols_cites = ["UT", "TC"]
    for item in tree.iterfind('REC'):       
        TC = item.find('.//dynamic_data/citation_related/tc_list/silo_tc').attrib.get('local_count')
        UT = item.find('.//UID').text[4:]
        cites.append({"UT": UT, "TC":TC})
    Cites = pd.DataFrame(cites, columns=df_cols_cites)

    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Cites.itertuples():
        sql = "UPDATE Docs SET TC = ? where UT = ?"
        cursorObj.execute(sql,(row[2], row[1]))
    con.commit()
    print('Actualizando citas....')
    sql ="SELECT UT, DocsCites.TC from DocsCites inner join Docs on DocsCites.idd = Docs.idd where date_to is null"
    cursor = cursorObj.execute(sql)
    filas=cursor.fetchall()
    Result = pd.DataFrame(filas, columns = ["UT", "TC"])
    for row in Cites.itertuples():
        for tup in Result.itertuples():
            if row[1] == tup[1]:
                if row[2] == tup[2]:
                    continue
                else: 
                    sql = "UPDATE DocsCites set date_to = date(?) where UT = ? and date_to is null"
                    cursorObj.execute(sql,(datetime.now(), row[1]))
                    con.commit()
                    sql = "INSERT INTO DocsCites(idd, TC, date_to, date_from) VALUES((select idd from Docs where UT = ?),date(?),?,null)"
                    cursorObj.execute(sql, (row[1], row[2], datetime.now()))
            else:
                continue
    print('Citas actualizadas...')

if len(sys.argv) > 4:
    user = sys.argv[1]
    password = sys.argv[2]
    bd = sys.argv[3]
    if sys.argv[4].endswith( ".txt"):
            f= open(sys.argv[4], 'r')
            query = f.read()
    else:
        query = sys.argv[4]
    tree = Conexión_api(user, password, query, None)
    Actualizar_citas(tree, bd)
    if len(sys.argv)>4:
        if sys.argv[4].endswith( ".txt"):
            query_split = query.split('PY = (')
            year = query_split[1].split(')')
            if int(year[0]) == format(datetime.now().year):
                year = 2010
            else:
                year = int(year[0])+1
            new_query = query_split[0] + "PY = (" + str(year)+ ")"
            f = open(sys.argv[4], "w")
            f.write(new_query)
else:
    print('Faltan argumentos')