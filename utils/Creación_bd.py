import sqlite3
import sys

def Create_BD(bd):
    #Conexión a la base de datos
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    print('Creada la base de datos ' + bd + '...')

    #Creación de la tabla de documentos
    sql = "CREATE TABLE Docs(idd integer PRIMARY KEY, ids integer, UT integer UNIQUE, TI text, PT text, DT text, NR integer, TC integer, PD text, PY integer, VL text, ISS text, abstract_text text, SU text, SI text, MA text, BP text, EP text, PG integer, AR text, DI text, an integer, date_add datetime, FOREIGN KEY(ids) REFERENCES Sources(ids))"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla Docs...')

    #Creación de la tabla Sources
    sql = "CREATE TABLE Sources(ids integer PRIMARY KEY, SO text, SE text,BS text, LA text, PU text, PI text, PA text, SN text UNIQUE, EI text UNIQUE, EIB text UNIQUE, IB text UNIQUE, JI text, J9 text, ST text)"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla Sources...')

    #Creación de la tabla Addresses
    sql = "CREATE TABLE Addresses(idad integer PRIMARY KEY, idd integer, C1 text UNIQUE, OG text, Sub_org text, Country text, FOREIGN KEY(idd) REFERENCES Docs(idd))"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla Addresses...')

    #Creación de la tabla Authors
    sql = "CREATE TABLE Authors(ida integer PRIMARY KEY, AU text, AF text, OI text, DA integer UNIQUE)"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla Authors...')

    #Creación de la tabla Categories
    sql = "CREATE TABLE Categories(idc integer PRIMARY KEY, WC text UNIQUE)"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla Categories...')

    #Creación de la tabla Areas
    sql = "CREATE TABLE Areas(idra integer PRIMARY KEY, SC text UNIQUE)"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla Areas...')

    #Creación de la tabla CatDoc
    sql = "CREATE TABLE CatDoc(idd integer, idc integer, PRIMARY KEY(idd,idc))"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla CatDoc...')

    #Creación de la tabla AreaDoc
    sql = "CREATE TABLE AreaDoc(idd integer, idra integer, PRIMARY KEY(idd, idra))"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla AreaDoc...')

    #Creación de la tabla AddAutDoc
    sql ="CREATE TABLE AddAutDoc(ida integer, idd integer, idad integer, PRIMARY KEY(ida, idd, idad))"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la taba AddAutDoc...')

    #Creación de la tabla GrantDocs
    sql = "CREATE TABLE GrantDocs(idd integer, grant_id text, grant_agency text, PRIMARY KEY(idd, grant_id, grant_agency), FOREIGN KEY(idd) REFERENCES Docs(idd))"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla GrantDocs...')

    #Creación de la tabla Editions
    sql = "CREATE TABLE Editions(ide text, idd integer, PRIMARY KEY(ide, idd), FOREIGN KEY(idd) REFERENCES Docs(idd))"
    cursorObj.execute(sql)
    con.commit() 
    print('Creación de la tabla Editions...')

    #Creación de la tabla JCR
    sql = "CREATE TABLE JCR(year int, issn text, ids int, ISI text, FI float, MC text, PD int, TD int, PC int, TC int, idc int, MD text, PRIMARY KEY(year, ISSN), FOREIGN KEY(idc) REFERENCES Categories(idc), FOREIGN KEY(ids) REFERENCES Sources(ids))"
    cursorObj.execute(sql)
    con.commit()
    print('Creación de la tabla JCR...')

    #Creación de la tabla Actualizations
    sql = "CREATE TABLE Actualizations(idact int, date_add date, time_init time, time_fin time, PRIMARY KEY(idact))"
    cursorObj.execute(sql)
    con.commit() 
    print('Creación de la tabla Actualizations...')

    #Creación de la tabla DocsCites
    sql = "CREATE TABLE DocsCites(idd int, TC int, date_from date, date_to date, PRIMARY KEY(idd, date_from), FOREIGN KEY(idd) REFERENCES Docs(idd))"
    cursorObj.execute(sql)
    con.commit() 
    print('Creación de la tabla DocsCites...')


    print('Todas las tablas creadas!')

if len(sys.argv) > 1:
    bd = sys.argv[1]
    Create_BD(bd)
else:
    print('Es necesario indicar el nombre de la base de datos a crear')