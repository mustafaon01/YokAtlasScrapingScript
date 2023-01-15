import requests
import psycopg2
import pandas as pd


cities = ["Adana", "Adıyaman", "Afyon", "Ağrı", "Amasya", "Ankara", "Antalya", "Artvin", "Aydın", "Balıkesir", "Bilecik"
    , "Bingöl", "Bitlis", "Bolu", "Burdur", "Bursa", "Çanakkale", "Çankırı", "Çorum", "Denizli", "Diyarbakır", "Edirne",
          "Elazığ", "Erzincan", "Erzurum", "Eskişehir", "Gaziantep", "Giresun", "Gümüşhane", "Hakkari", "Hatay",
          "Isparta", "İçel (Mersin)", "İstanbul", "İzmir", "Kars", "Kastamonu", "Kayseri", "Kırklareli", "Kırşehir",
          "Kocaeli", "Konya", "Kütahya", "Malatya", "Manisa", "Kahramanmaraş", "Mardin", "Muğla", "Muş", "Nevşehir",
          "Niğde", "Ordu", "Rize", "Sakarya", "Samsun", "Siirt", "Sinop", "Sivas", "Tekirdağ", "Tokat", "Trabzon",
          "Tunceli", "Şanlıurfa", "Uşak", "Van", "Yozgat", "Zonguldak", "Aksaray", "Bayburt", "Karaman", "Kırıkkale",
          "Batman", "Şırnak", "Bartın", "Ardahan", "Iğdır", "Yalova", "Karabük", "Kilis", "Osmaniye", "Düzce"]

table_name = 'deneme'
df = pd.read_table("emir.txt", header=None)
hostname = 'localhost'
database = 'yok_atlas'
username = 'postgres'
pwd = 'Ju3Dt8h8'
port_id = 5432
cur = None
conn = None


def create_table(table_name):
    create_table_script = f'''CREATE TABLE IF NOT EXISTS {table_name}(
                                            PROGRAM_KODU VARCHAR(100),
                                            LISE VARCHAR(500),
                                            SEHIR VARCHAR(250),
                                            ILCE VARCHAR(250),
                                            )
        '''
    cur.execute(create_table_script)
    conn.commit()

def insert_table():
    pass


try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()
    create_table(table_name)

except Exception as error:
    print("main error:", error)





