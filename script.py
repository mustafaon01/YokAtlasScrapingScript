import requests
import psycopg2
import pandas as pd

list_try = []
program_number_list = {}
year = 2019
table_name = "generalInformation2"
high_school_table_name = "highSchoolInformation2"
'''table_name = "mark11"
high_school_table_name = "marklise11"'''

df = pd.read_table("emir.txt", header=None)
hostname = 'localhost'
database = 'yok_atlas'
username = 'postgres'
pwd = 'pwd'
port_id = 8080
cur = None
conn = None


def create_table(table_name, high_school_table_name):
    create_high_schools_table_script = f'''CREATE TABLE IF NOT EXISTS {high_school_table_name}(
                                        ID SERIAL,
                                        YIL VARCHAR(100),
                                        PROGRAM_KODU VARCHAR(100),
                                        LISE VARCHAR(500),
                                        SEHIR VARCHAR(250),
                                        ILCE VARCHAR(250),
                                        TOPLAM VARCHAR(100),
                                        YENI_MEZUN VARCHAR(100),
                                        ONCEKI_MEZUN VARCHAR(100),
                                        PRIMARY KEY(ID)
                                        ) 


    '''
    cur.execute(create_high_schools_table_script)
    conn.commit()
    create_script = f''' CREATE TABLE IF NOT EXISTS {table_name} (
                                ID SERIAL,
                                YIL VARCHAR(100),
                                PROGRAM_KODU VARCHAR(100),
                                UNI_TYPE VARCHAR(100),
                                UNIVERSITE_ISMI VARCHAR(500),                      
                                BOLUM_ADI VARCHAR(500),
                                FAKULTE VARCHAR(500),
                                PUAN_TURU VARCHAR(100),
                                BURS_TURU VARCHAR(100),
                                GENEL_KONTENJAN VARCHAR(100),
                                TOPLAM_YERLESEN VARCHAR(100),
                                BOS_KALAN VARCHAR(100),
                                YERLESEN_SON_KISI VARCHAR(100),
                                FOREIGN KEY (ID) REFERENCES {high_school_table_name} (ID)
                                )
        '''
    cur.execute(create_script)
    conn.commit()


def insert_table(table_name, yil, program_code, uni_type, uni_name, major_name, faculty, type_of_score,
                 type_of_scholarship, general_size, total_size, empty_size, last_person):
    try:
        insert_script = f' INSERT INTO {table_name} (YIL, PROGRAM_KODU, UNI_TYPE, UNIVERSITE_ISMI,' \
                        f' BOLUM_ADI,FAKULTE, PUAN_TURU, BURS_TURU, GENEL_KONTENJAN, TOPLAM_YERLESEN, BOS_KALAN,' \
                        f' YERLESEN_SON_KISI) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        insert_values = (str(yil), str(program_code), str(uni_type), str(uni_name), str(major_name), str(faculty),
                         str(type_of_score), str(type_of_scholarship), str(general_size), str(total_size),
                         str(empty_size), str(last_person))

        cur.execute(insert_script, insert_values)
        conn.commit()
    except IndexError as e:
        print("fonksiyon içi", e)


def insert_high_school_table(high_school_table_name, yil, program_code, high_school, city, state,
                             total,
                             new_gard, before_grad):
    insert_high_school_script = f''' INSERT INTO {high_school_table_name} (YIL, PROGRAM_KODU, LISE, SEHIR, ILCE, TOPLAM,
     YENI_MEZUN, ONCEKI_MEZUN) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    '''

    insert_high_school_values = (str(yil), str(program_code), str(high_school),
                                 str(city), str(state), str(total), str(new_gard), str(before_grad))
    cur.execute(insert_high_school_script, insert_high_school_values)
    conn.commit()


def convert_to_html_df(response_of_university, response_of_high_schools, a):
    table_university = pd.read_html(response_of_university.text)
    data_frame1 = table_university[0]
    table_high_schools = pd.read_html(response_of_high_schools.text, header=None)
    YIL = year
    PROGRAM_KODU = data_frame1.iloc[0][1]
    print(PROGRAM_KODU)
    print("liseler yükleniyor")
    print("liselerin sayısı:", len(table_high_schools[0]))
    for i in range(len(table_high_schools[0])):
        LISE_ADI = table_high_schools[0].iloc[i][0]
        indexOfsehir = LISE_ADI.find('(')
        indexOfilce = LISE_ADI.find('-', indexOfsehir)
        SEHIR = LISE_ADI[indexOfsehir + 1:indexOfilce]
        ILCE = LISE_ADI[indexOfilce + 1:-1]
        TOPLAM = str(table_high_schools[0].iloc[i][1])
        YENI_MEZUN = table_high_schools[0].iloc[i][2]
        ONCEKI_MEZUN = table_high_schools[0].iloc[i][3]
        if LISE_ADI == None:
            continue
        else:
            insert_high_school_table(high_school_table_name, YIL, PROGRAM_KODU, LISE_ADI,
                                     SEHIR, ILCE, TOPLAM,
                                     YENI_MEZUN, ONCEKI_MEZUN, )
    print("liseler yüklendi")


'''
    except IndexError as e:
        print("buradasın", e)'''


def convert_universities_table(response_of_university, a):
    table_university = pd.read_html(response_of_university.text)
    data_frame1 = table_university[0]
    data_frame2 = table_university[1]
    data_frame3 = table_university[2]
    try:
        YIL = year
        BOLUM_ADI = data_frame1.columns.values[0]
        PROGRAM_KODU = data_frame1.iloc[0][1]
        print("program kodu:", PROGRAM_KODU)
        UNI_TYPE = data_frame1.iloc[1][1]
        UNIVERSITE_ISMI = data_frame1.iloc[2][1]
        FAKULTE = data_frame1.iloc[3][1]
        PUAN_TURU = data_frame1.iloc[4][1]
        BURS_TURU = data_frame1.iloc[5][1]
        GENEL_KONTENJAN = data_frame2.iloc[0][1]
        TOPLAM_YERLESEN = data_frame2.iloc[5][1]
        BOS_KALAN = data_frame2.iloc[6][1]
        YERLESEN_SON_KISI = data_frame3.iloc[0][1]
        print("genel bilgiler yükleniyor")
        if UNI_TYPE != None:
            insert_table(table_name, YIL, PROGRAM_KODU, UNI_TYPE, UNIVERSITE_ISMI, BOLUM_ADI, FAKULTE,
                         PUAN_TURU, BURS_TURU, GENEL_KONTENJAN, TOPLAM_YERLESEN, BOS_KALAN, YERLESEN_SON_KISI)
            print("genel bilgiler yüklendi")

    except IndexError as e:
        print("convert_universities_table içindesin: ", e)


def get_high_schools(year, university_id):
    URL_HIGH_SCHOOL = f"https://yokatlas.yok.gov.tr/{year}/content/lisans-dynamic/1060.php?y={university_id}"
    response = requests.get(URL_HIGH_SCHOOL)
    return response


def get_universities(year, university_id):
    URL_UNIVERSITIES = f"https://yokatlas.yok.gov.tr/{year}/content/lisans-dynamic/1000_1.php?y={university_id}"
    response = requests.get(URL_UNIVERSITIES)
    return response


try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()

    create_table(table_name, high_school_table_name)
    program_number_list = df.to_dict()

    for a in range(len(program_number_list[0])):
        university_id = program_number_list[0][a]
        convert_to_html_df(get_universities(year, university_id), get_high_schools(year, university_id), a)
        convert_universities_table(get_universities(year, university_id), a)
        print(str(a + 1)+". bölüm verileri çekildi ve yıl:", year)

except Exception as error:
    print("main error:", error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

