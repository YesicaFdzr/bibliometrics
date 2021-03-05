import pandas as pd
import sqlite3
import sys

def Import_jcr(bd, jcr):

    JCR = pd.read_excel(jcr)
    Disciplina = JCR['Posición na Discip.'].str.split('º/', expand = True)
    Disciplina.columns = ['PD', 'TD']
    JCR = pd.concat([JCR, Disciplina], axis=1)
    Cuartil = JCR['Posic. no Cuartil'].str.split('º/', expand = True)
    Cuartil.columns = ['PC', 'TC']
    JCR = pd.concat([JCR, Cuartil], axis=1)
    JCR = JCR.drop(['Posición na Discip.', 'Posic. no Cuartil'], axis = 1)
    JCR = JCR[['JCR', 'ISSN', 'Titulo Curto ISI', 'Mellor Cuartil', 'Mellor Disciplina', 'PD', 'TD', 'PC', 'TC']]

    con = sqlite3.connect(bd)
    cursorObj = con.cursor()
    for row in JCR.itertuples():
        sql = "INSERT OR IGNORE INTO JCR(year, ISSN, ids,  ISI, MC, PD, TD, PC, TC, idc, MD) VALUES(?, ?, (select ids from Sources where SN = ? or EI = ? or EIB = ? or IB = ?),?,?,?,?,?,?,(select idc from Categories where upper(WC) = upper(?)),?)"
        cursorObj.execute(sql, (row[1], row[2], row[2], row[2], row[2], row[2], row[3], row[4], row[6], row[7], row[8], row[9], row[5],row[5]))
    con.commit()

if len(sys.argv) > 2:
    bd = sys.argv[1]
    print(bd)
    jcr = sys.argv[2]
    print(jcr)
    Import_jcr(bd, jcr)
else:
    print('Faltan argumentos')