from wos import WosClient
import wos.utils
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree
import pandas as pd 
import re
import sqlite3
import numpy as np
from datetime import datetime
import sys


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

def Init_actualización(bd):
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    sql = "INSERT INTO Actualizations(date_add, time_init) VALUES(date(?),?)"
    cursorObj.execute(sql, (datetime.now(), datetime.now()))
    con.commit()


def Parse_docs(tree, bd):
    rows_docs = []
    df_cols_docs = ["title", "pub_type","doc_type", "NR", "TC", "pub_date", "pub_year", "vol", "issue", "abstract_text", "SU","SI","MA", "pages_begin", "pages_end", "pages_count", "doi", "AR", "UT", "an"]

    print('Recogiendo datos de los documentos...')
    for item in tree.iterfind('REC'):       
        titles = item.find('.//static_data/summary/titles/title[@type="item"]').text
        pub_types = item.find('.//static_data/summary/pub_info').attrib.get('pubtype')
        doc_types = item.find('.//static_data/summary/doctypes/doctype').text
        NR = item.find('.//static_data/fullrecord_metadata/refs').attrib.get('count')
        if (item.find('.//static_data/fullrecord_metadata/abstracts/abstract/abstract_text/p') is not None):
            abstract_text = item.find('.//static_data/fullrecord_metadata/abstracts/abstract/abstract_text/p').text
        else:
            abstract_text = None
        TC = item.find('.//dynamic_data/citation_related/tc_list/silo_tc').attrib.get('local_count')
        pub_year = item.find('.//static_data/summary/pub_info').attrib.get('pubyear')
        pub_month = item.find('.//static_data/summary/pub_info').attrib.get('pubmonth')
        special_issue = item.find('.//static_data/summary/pub_info').attrib.get('special_issue')
        supplement = item.find('.//static_data/summary/pub_info').attrib.get('supplement')
        vol = item.find('.//static_data/summary/pub_info').attrib.get('vol')
        issue = item.find('.//static_data/summary/pub_info').attrib.get('issue')
        pages_begin = item.find('.//static_data/summary/pub_info/page').attrib.get('begin')
        pages_end = item.find('.//static_data/summary/pub_info/page').attrib.get('end')
        pages_count = item.find('.//static_data/summary/pub_info/page').attrib.get('page_count')
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="doi"]') is not None):
            doi = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="doi"]').attrib['value']
        else:
            doi = None
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="art_no"]') is not None):
            AR = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="art_no"]').attrib['value']
        else:
            AR = None
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="meeting_abs"]') is not None):
            MA = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="meeting_abs"]').attrib['value']
        else:
            MA = None
        an = item.find('.//static_data/summary/names').attrib.get('count')
        UT = item.find('.//UID').text[4:] 
        
        rows_docs.append({"title": titles, "pub_type":pub_types, "doc_type": doc_types, "NR": NR, "TC": TC, "pub_date": pub_month, "pub_year": pub_year,
                    "vol": vol, "issue": issue, "abstract_text": abstract_text, "SU": supplement, "SI": special_issue, "MA": MA,"pages_begin": pages_begin, "pages_end": pages_end, 
                    "pages_count": pages_count, "doi": doi, "AR": AR, "UT": UT, "an": an})
        
    Docs = pd.DataFrame(rows_docs, columns = df_cols_docs)
    print('Información de autores recogida, insertando en bd...')

    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Docs.itertuples():
        sql = "INSERT OR IGNORE INTO Docs(UT, TI, PT, DT, NR, TC, PD, PY, VL, ISS, abstract_text, SU,SI,MA,BP, EP, PG, AR, DI, an, date_add) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        cursorObj.execute(sql, (row[19], row[1],row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[18], row[17],row[20],datetime.now()))
    con.commit()
    print('Información de los documentos añadida...')


def Parse_authors(tree,bd):
    rows_authors = []
    df_cols_authors = ["author", "author_full", "daisng_id"]
    print('Recogiendo información de autores...')

    for item in tree.iterfind('REC'):
        for a in item.findall('.//static_data/summary/names/name[@role="author"]'):
            author =  a.find('.//wos_standard').text
            author_full = a.find('.//full_name').text
            daisng_id = a.attrib.get('daisng_id')
            
            rows_authors.append({"author": author, "author_full": author_full, "daisng_id": daisng_id})
            
    Authors = pd.DataFrame(rows_authors, columns = df_cols_authors)
    print('Información de autores recogida, insertando en bd...')

    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Authors.itertuples():
        sql = "INSERT OR IGNORE INTO Authors(AU, AF, OI, DA) VALUES(?,?,?,?)"
        cursorObj.execute(sql, (row[1], row[2], None, row[3]))
    con.commit()

    print('Información de autores añadida...')

def Parse_sources(tree, bd):
    rows_journal = []
    df_cols_journals = ["pub_name","SE", "BS", "language", "publisher", "publisher_city", "publisher_addr", "issn", "eissn", "eisbn","isbn", "journal_iso", "journal_isi", "ST"]

    print('Recogiendo datos de las fuentes...')
    for item in tree.iterfind('REC'):
        ST = item.find('.//static_data/summary/pub_info').attrib.get('pubtype')
        pub_name = item.find('.//static_data/summary/titles/title[@type="source"]').text
        if (item.find('.//static_data/summary/titles/title[@type="series"]') is not None):
            series_title = item.find('.//static_data/summary/titles/title[@type="series"]').text
        else:
            series_title = None
        if (item.find('.//static_data/summary/titles/title[@type="book_series"]') is not None):
            book_series = item.find('.//static_data/summary/titles/title[@type="book_series"]').text
        else:
            book_series = None
        language = item.find('.//static_data/fullrecord_metadata/languages/language').text
        publisher = item.find('.//static_data/summary/publishers/publisher/names/name/full_name').text
        publisher_city = item.find('.//static_data/summary/publishers/publisher/address_spec/city').text
        publisher_addr = item.find('.//static_data/summary/publishers/publisher/address_spec/full_address').text
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="issn"]') is not None):
            issn = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="issn"]').attrib['value']
        else:
            issn = None
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="eissn"]') is not None):
            eissn = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="eissn"]').attrib['value']
        else:
            eissn = None
        if (item.find('.//static_data/summary/titles/title[@type="abbrev_iso"]') is not None):
            journal_iso = item.find('.//static_data/summary/titles/title[@type="abbrev_iso"]').text
        else:
            journal_iso = None
        if (item.find('.//static_data/summary/titles/title[@type="abbrev_29"]') is not None):
            journal_isi = item.find('.//static_data/summary/titles/title[@type="abbrev_29"]').text
        else:
            journal_isi = None
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="eisbn"]') is not None):
            eisbn = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="eisbn"]').attrib['value']
        else:
            eisbn = None
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="isbn"]') is not None):
            isbn = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="isbn"]').attrib['value']
        else:
            isbn = None
            
        rows_journal.append({"pub_name" : pub_name,"SE": series_title, "BS": book_series, "language": language, "publisher": publisher, "publisher_city": publisher_city,
                            "publisher_addr": publisher_addr, "issn": issn, "eissn": eissn, "eisbn": eisbn, "isbn": isbn, "journal_iso": journal_iso, "journal_isi": journal_isi, "ST": ST})
    Journals = pd.DataFrame(rows_journal, columns = df_cols_journals)
    print('Información de las fuentes recogida, añadiendo información a bd...')

    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Journals.itertuples():
        sql = "INSERT OR IGNORE INTO Sources(SO, SE, BS, LA, PU, PI, PA, SN, EI, EIB, IB, JI, J9, ST) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        cursorObj.execute(sql,(row[1],row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14]))
    con.commit()
    print('Información de las fuentes añadida a bd...')


def Parse_Addresses(tree, bd):
    rows_addresses = []
    df_cols_addr = ["Addresses", "Univ", "Sub_org", "Country", "UT"]

    print('Recogiendo información de las direcciones...')
    for item in tree.iterfind('REC'):
        UT = item.find('.//UID').text[4:]
        for a in item.iterfind('.//static_data/fullrecord_metadata/addresses/address_name/address_spec'):
            Addresses = a.find('.//full_address').text
            if (a.find('.//organizations/organization[@pref="Y"]') is not None):
                Univ = a.find('.//organizations/organization[@pref="Y"]').text
            else:
                Univ = None
            if (a.find('.//suborganizations/suborganization') is not None):
                Sub_org = a.find('.//suborganizations/suborganization').text
            else:
                Sub_org = None
            Country = a.find('.//country').text
            
            rows_addresses.append({"Addresses": Addresses, "Univ": Univ,"Sub_org": Sub_org, "Country": Country, "UT": UT})

    Address = pd.DataFrame(rows_addresses, columns=df_cols_addr)
    Address = Address.drop_duplicates()
    print('Información de las direcciones recogida, añadiendo información a bd...')

    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Address.itertuples():
        sql = "INSERT OR IGNORE INTO Addresses( idd, C1, OG, Sub_org, Country) VALUES ((Select idd from Docs where UT = ?),?,?,?,?)"
        cursorObj.execute(sql, (row[5], row[1], row[2], row[3], row[4]))
    con.commit()
    print('Información de las direcciones añadida a bd...')


def Parse_Categories(tree, bd):
    rows_categories = []
    df_cols_categories = ["Category"]
    rows_docs_categories = []
    df_cols_docs_categories = ["UT", "Category"]

    print('Recogiendo información de las categorias...')
    for item in tree.iterfind('REC'):
        if item.find('.//static_data/fullrecord_metadata/category_info/headings/heading') is not None:
            Category = item.find('.//static_data/fullrecord_metadata/category_info/headings/heading').text
        else:
            Category = None
        rows_categories.append({"Category": Category})

    for item in tree.iterfind('REC'):
        UT = item.find('.//UID').text[4:]
        if item.find('.//static_data/fullrecord_metadata/category_info/headings/heading') is not None:
            Category = item.find('.//static_data/fullrecord_metadata/category_info/headings/heading').text
        else:
            Category = None
        rows_docs_categories.append({"UT": UT, "Category": Category}) 
    Categories = pd.DataFrame(rows_categories, columns=df_cols_categories)
    Categories = Categories.drop_duplicates()
    Docs_categories = pd.DataFrame(rows_docs_categories, columns=df_cols_docs_categories)
    print('Información de las categorias recogida, añadiendo información a bd...')

    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Categories.itertuples():
        sql = "INSERT OR IGNORE INTO Categories(WC) VALUES(?)"
        cursorObj.execute(sql,(row[1],))
    con.commit()

    for row in Docs_categories.itertuples():
        sql = "INSERT OR IGNORE INTO CatDoc(idd, idc) VALUES((select idd from Docs where UT = ?), (select idc from Categories where WC=?))"
        cursorObj.execute(sql, (row[1], row[2]))
    con.commit()
    print('Información de las categorias añadida a bd...')

def Parse_areas(tree, bd):

    rows_areas = []
    df_cols_areas = ["Area"]
    rows_docs_areas = []
    df_cols_docs_areas = ["UT", "Area"]

    print('Recogiendo información de las areas...')
    for item in tree.iterfind('REC'):
        for a in item.iterfind('.//static_data/fullrecord_metadata/category_info/subjects/subject[@ascatype="traditional"]'):
            area = a.text
            rows_areas.append({"Area": area})
    Areas = pd.DataFrame(rows_areas, columns=df_cols_areas)
    Areas = Areas.drop_duplicates()

    for item in tree.iterfind('REC'):
        UT = item.find('.//UID').text[4:]
        for a in item.iterfind('.//static_data/fullrecord_metadata/category_info/subjects/subject[@ascatype="traditional"]'):
            area = a.text
            rows_docs_areas.append({"UT": UT, "Area": area})     
    Docs_areas = pd.DataFrame(rows_docs_areas, columns=df_cols_docs_areas)

    print('Información de las areas recogida, añadiendo información a bd...')

    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Areas.itertuples():
        sql = "INSERT OR IGNORE INTO Areas(SC) VALUES(?)"
        cursorObj.execute(sql, (row[1],))
    con.commit()

    for row in Docs_areas.itertuples():
        sql = "INSERT OR IGNORE INTO AreaDoc(idd, idra) VALUES((select idd from Docs where UT=?),(select idra from Areas where SC=?))"
        cursorObj.execute(sql, (row[1], row[2]))
    con.commit()
    print('Información de las areas añadida a bd...')


def Parse_ralation_AutAddDoc(tree,bd):
    rows_authors_docs = []
    rows_address_docs = []
    df_cols_authors_docs = ["UT", "Authors"]
    df_cols_address_docs = ["UT", "Authors", "Address"]

    print('Recogiendo información de las relaciones entre documentos, direcciones y autores..')
    for item in tree.iterfind('REC'):
        UT = item.find('.//UID').text[4:]  
        for a in item.iterfind('.//static_data/summary/names/name[@role="author"]'):
            authors = a.attrib.get('daisng_id')
            rows_authors_docs.append({"UT": UT, "Authors": authors})
        for a in item.iterfind('.//static_data/fullrecord_metadata/addresses/address_name'):
            addr_no = a.find('.//address_spec').attrib.get('addr_no')
            Address = a.find('.//address_spec/full_address').text
            for b in a.iterfind('.//names/name'):
                if b.attrib.get('addr_no') == addr_no:
                    author = b.attrib.get('daisng_id')
                else:
                    author = None
                rows_address_docs.append({"UT": UT, "Authors": author, "Address": Address})
            

    Authors_Docs = pd.DataFrame(rows_authors_docs, columns = df_cols_authors_docs)
    Authors_Address = pd.DataFrame(rows_address_docs, columns = df_cols_address_docs)
    Authors_Docs_Address = pd.merge(left=Authors_Docs, right=Authors_Address, how='outer', left_on=['UT', "Authors"], right_on=["UT", "Authors"]) 
 
    print('Información de las relaciones entre documentos, direcciones y autores recogida, añadiendo información a bd...')
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Authors_Docs_Address.itertuples():
        sql = "INSERT OR IGNORE INTO AddAutDoc(ida, idd, idad) VALUES((select ida from Authors where DA = ?),(select idd from Docs where UT = ?),(select idad from Addresses where C1 = ?))"
        cursorObj.execute(sql, (row[2], row[1], row[3]))
    con.commit()
    print('Información de las relaciones entre documentos, direcciones y autores añadida a bd...')

def Parse_realtion_Sources_Docs(tree,bd):
    rows_docs_journal = []
    df_cols_docs_journal = ["UT", "issn", "eissn", "eisbn", "isbn"]

    print('Recogiendo información de la relación entre fuentes y documentos..')
    for item in tree.iterfind('REC'):
        UT = item.find('.//UID').text[4:]  
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="issn"]') is not None):
            issn = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="issn"]').attrib['value']
        else:
            issn = None
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="eissn"]') is not None):
            eissn = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="eissn"]').attrib['value']
        else:
            eissn = None
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="eisbn"]') is not None):
            eisbn = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="eisbn"]').attrib['value']
        else:
            eisbn = None
        if (item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="isbn"]') is not None):
            isbn = item.find('.//dynamic_data/cluster_related/identifiers/identifier[@type="isbn"]').attrib['value']
        else:
            isbn = None
            
        rows_docs_journal.append({"UT": UT, "issn": issn, "eissn": eissn, "eisbn": eisbn, "isbn": isbn})

    Docs_journal = pd.DataFrame(rows_docs_journal, columns=df_cols_docs_journal)
    
    print('Información de la relación entre fuentes y documentos recogida, añadiendo información a bd...')
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Docs_journal.itertuples():
        sql = "UPDATE Docs set ids = (select ids from Sources where SN = ? or EI = ? or EIB = ? or IB = ?) where UT = ?"
        cursorObj.execute(sql, (row[2], row[3], row[4], row[5], row[1]))
    con.commit()
    print('Información de la relación entre fuentes y documentos añadida a bd...')

def Parse_grant(tree, bd):
    rows_docs_grants = []
    df_cols_docs_grant = ["UT", "Grant_id", "Grant_agency"]

    print('Recogiendo información de las subvenciones...')
    for item in tree.iterfind('REC'):
        UT = item.find('.//UID').text[4:]
        for a in item.iterfind('.//static_data/fullrecord_metadata/fund_ack/grants/grant'):
            grant_agency = a.find('.//grant_agency').text
            if (a.find('.//grant_ids/grant_id') is not None):
                grant_id = a.find('.//grant_ids/grant_id').text
            else:
                grant_id = None
            rows_docs_grants.append({"UT": UT, "Grant_id": grant_id, "Grant_agency": grant_agency})
    Docs_grants = pd.DataFrame(rows_docs_grants, columns=df_cols_docs_grant)
    
    print('Información de las subvenciones recogida, añadiendo información a bd...')
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Docs_grants.itertuples():
        sql = "INSERT OR IGNORE INTO GrantDocs(idd, grant_id, grant_agency) VALUES((select idd from Docs where UT = ?), ?, ?)"
        cursorObj.execute(sql,(row[1], row[2], row[3]))
    con.commit()
    print('Información de las subvenciones añadida a bd...')

def Orcid_author(tree, bd):
    #Relacionar ORCID y autor
    print('Buscando orcids de autores...')
    rows_orcids = []
    df_cols_orcids = ["orcid", "author", "dasing_id"]
    for item in tree.iterfind('REC'):
        for a in item.iterfind('.//static_data/contributors/contributor/name'):
            orcid = a.attrib.get('orcid_id')
            other_id = a.attrib.get('r_id')
            author = a.find('.//display_name').text
            if author == None:
                continue
            stable = re.split(',',a.find('.//display_name').text.upper())
            family_stable = re.split(' |-', stable[0])
            if len(stable)<=1:
                name_stable = ""
            else :
                name_stable = [x[:1] for x in re.split(' |-', stable[1].strip())]
            for b in item.iterfind('.//static_data/summary/names/name[@role="author"]'):
                if b.find('.//display_name').text == author:
                    dasing_id = b.attrib.get('daisng_id')
                else:
                    if b.find('.//data-item-ids/data-item-id[@id-type="PreferredRID"]') is not None:
                        if b.find('.//data-item-ids/data-item-id[@id-type="PreferredRID"]').text == other_id:
                            dasing_id = b.attrib.get('daisng_id')
                        else:
                            sauthor = re.split(',',b.find('.//display_name').text.upper())
                            family_sauthor = re.split(' |-', sauthor[0])
                            res_name = 0
                            res_family = 0
                            if len(sauthor)<=1:
                                name_sauthor = ""
                            else :
                                name_sauthor = [x[:1] for x in re.split(' |-', sauthor[1].strip())]
                            for c in family_sauthor:
                                if c in family_stable:
                                    continue
                                else:
                                    res_family += 1
                            for c in name_sauthor:
                                if c in name_stable:
                                    continue
                                else:
                                    res_name += 1
                            res = 0.9*res_family+0.1*res_name
                            if res < 2:
                                dasing_id = b.attrib.get('daisng_id')
                            else:
                                dasing_id = None 
                    else:
                        dasing_id = None
            rows_orcids.append({"orcid": orcid, "author":author, "dasing_id": dasing_id})

    Orcids = pd.DataFrame(rows_orcids, columns=df_cols_orcids)  

    print('Orcids encontrados, añadiendo información a bd...')
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Orcids.itertuples():
        sql = "UPDATE Authors SET OI = ? where DA = ? and OI is null"
        cursorObj.execute(sql,(row[1], row[3]))
    con.commit()
    print('Información de orcids añadida a bd...')

def Parse_editions(tree, bd):
    editions = []
    df_cols_editions = ["UT", "edition"]

    print('Recogiendo información de las ediciones...')
    for item in tree.iterfind('REC'): 
        UT = item.find('.//UID').text[4:] 
        for a in item.iterfind('.//static_data/summary/EWUID/edition'):
            edition = a.attrib.get('value')[4:]
            editions.append({"UT": UT, "edition": edition})
    Editions = pd.DataFrame(editions, columns=df_cols_editions)

    print('Información de las ediciones recogida, añadiendo información a bd...')
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in Editions.itertuples():
        sql = "INSERT OR IGNORE INTO Editions(ide,idd) VALUES(?, (select idd from Docs where UT = ?))"
        cursorObj.execute(sql,(row[2], row[1]))
    con.commit()
    print('Información de las ediciones añadida a bd...')

##Para actualizar las citas
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

def Actualizar_jcr(bd):
    print("actualizar JCR")
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    sql ="UPDATE JCR as a set ids =(select ids from Sources where SN = a.issn or EI = a.issn or EIB = a.issn or IB = a.issn) where ids is null"
    cursor = cursorObj.execute(sql)
    con.commit()
    sql = "UPDATE JCR as a set idc =(select idc from Categories where WC = a.MD)"
    cursor = cursorObj.execute(sql)
    con.commit()

def Actualizacion_fin(bd):
    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    sql = "UPDATE Actualizations set time_fin = ? where date_add = date(?)"
    cursorObj.execute(sql,(datetime.now(), datetime.now()))
    con.commit()
    print('Actualización terminada...')



if len(sys.argv) > 3:
    user = sys.argv[1]
    password = sys.argv[2]
    bd = sys.argv[3]
    if len(sys.argv)>4:
        if sys.argv[4].endswith( ".txt"):
            f= open(sys.argv[4], 'r')
            query = f.read()
        else:
            query = sys.argv[4]
    else:
        query = ""
    if len(sys.argv)>5:
        symbolicTimeSpan = sys.argv[5]    
    else:
        symbolicTimeSpan = None
    tree = Conexión_api(user, password, query, symbolicTimeSpan)
    Init_actualización(bd)
    Parse_docs(tree,bd)
    Parse_authors(tree,bd)
    Parse_sources(tree, bd)
    Parse_Addresses(tree, bd)
    Parse_Categories(tree, bd)
    Parse_areas(tree, bd)
    Parse_realtion_Sources_Docs(tree, bd)
    Parse_ralation_AutAddDoc(tree, bd)
    Parse_grant(tree, bd)
    Parse_editions(tree, bd)
    Orcid_author(tree, bd)
    Actualizar_citas(tree, bd)
    Actualizar_jcr(bd)
    Actualizacion_fin(bd) 
else:
    print('Faltan argumentos en la llamada')


    




