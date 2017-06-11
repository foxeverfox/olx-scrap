from bs4 import BeautifulSoup
import requests
import csv
import pprint
import sys
from lxml import html
import time
import pyodbc
import arrow
import re






#url="http://m.olx.co.id/mobil/bekas/q-avanza-2012/"
def tambahdata(nama_mobil , nharga ,lokasi , tahun ):
    jam = arrow.now().format('YYYY-MM-DD HH:mm')
    strsql = "Insert into hargamobil (keterangan,harga,tanggal_update,lokasi,tahun) values ('{0}',{1},'{2}','{3}','{4}'  )".format(
        nama_mobil.replace("'", ""),
        nharga, jam,lokasi , tahun )
    try:

        cursor.execute(strsql)
    except:
        print(strsql)
    return


def parsing_olx_detail( urldetail):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    r = requests.get(urldetail, headers=headers)
    soup = BeautifulSoup(r.text, "lxml")
    lokasi = soup.find(class_="dt-loc")
    txtLokasi = lokasi.get_text()


    otahun = soup.find_all(class_="dt-desc")[0].find('a',href=re.compile('year'))
    txtTahun= otahun.get('title')
    return txtLokasi , txtTahun

def parsing_olx(soup,cursor):

    nama_mobil = soup.find_all("span", class_="lt-title")
    harga = soup.find_all("span", class_="lt-price")
    listitem= soup.find_all('ul', class_='listing-wrap')

    content = []
    #detail ada di list ke 2
    detaillink =listitem[0]
    orow = detaillink.find_all('li', class_="bg-6")
    for xrow in orow:
        res=xrow.find('a').get('href')
        #tahun , lokasi
        lokasi,tahun = parsing_olx_detail(res)
        info = {
            "lokasi": lokasi,
            "tahun": tahun
              }
        content.append(info)




    detaillink =listitem[2]
    orow = detaillink.find_all('li',class_ = ["bgfff ","bg-6"] )

    for xrow in orow:
        res=xrow.find('a').get('href')
        #tahun , lokasi
        lokasi,tahun = parsing_olx_detail(res)
        info = {
            "lokasi": lokasi,
            "tahun": tahun
              }
        content.append(info)

    print(len(content))
    print(len(nama_mobil))
    for i in range(1, len(nama_mobil)):
        nharga = harga[i].text.replace("Rp", "").replace(".", "")
        tambahdata(nama_mobil[i].text , nharga,content[i]['lokasi'],content[i]['tahun'] )




    nextPage = soup.find("a", class_="next")

    return nextPage






def  gocrawl( urltogo,idxpage,cursor ):
    print(urltogo)

    #if idxpage % 25 == 0 :
    #    time.sleep(10)

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    try:
        r = requests.get(urltogo,headers=headers )
        soup = BeautifulSoup(r.text, "lxml")
        nextPage = parsing_olx(soup,cursor)
        if nextPage is None:
            print("Last Page")
            return
        else:
            idxpage = idxpage + 1
            urltogo2 = nextPage.get('href')
    except:
        urltogo2 = urltogo
        print("wait, Rest for 10 second..Get Tired ...zzzzzzz  "+sys.last_value)
        time.sleep(10)


    gocrawl(urltogo2, idxpage,cursor)


idxpage=1

#setting up database connection
constr= "Driver={SQL Server Native Client 11.0};Server=.\SQLEXPRESS;Database=webScrap;Uid=sa;Pwd=123456"
cnn = pyodbc.connect(constr)
cursor = cnn.cursor()


#start crawling
#url =  "http://m.olx.co.id/mobil/bekas/"
url="http://m.olx.co.id/mobil/bekas/q-avanza-2012/"
#url="http://m.olx.co.id/mobil/bekas/q-avanza-2012/?page=5"

gocrawl( url,1 ,cursor )




cnn.commit()