import os
import requests
import psycopg2
import pandas as pd
from sqlalchemy import *

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 29)
pd.set_option('display.max_rows', 200)

path = "/Users/mustafaoncu/PycharmProjects/mustafaDeneme"
dirs = os.listdir(path)

hostname = 'localhost'
database = 'yok_atlas'
username = 'postgres'
pwd = 'Ju3Dt8h8.'
port_id = 5433
cur = None
conn = None

table_location = {
        "main_info": "1000_1.php?y=",
        "quota_placement_statistics": "1000_2.php?y=",
        "gender_distribution_of_students": "1010.php?y=",
        "geographic_places_where_students_come_from": "1020ab.php?y=",
        "provinces_of_students": "1020c.php?y=",
        "educational_status_of_students": "1030a.php?y=",
        "high_school_graduation_years_of_students": "1030b.php?y=",
        "high_school_fields_of_students": "1050b.php?y=",
        "high_school_types_of_students": "1050a.php?y=",
        "high_schools_from_which_students_graduated": "1060.php?y=",
        "students_school_firsts": "1030c.php?y=",
        "base_score_and_achievement_statistics": "1000_3.php?y=",
        "last_placed_student_profile": "1070.php?y=",
        "YKS_net_averages_of_students": "1210a.php?y=",
        "students_YKS_scores": "1220.php?y=",
        "YKS_success_order_of_students": "1230.php?y=",
        "preference_statistics_across_the_country": "1080.php?y=",
        "in_which_preferences_students_settled": "1040.php?y=",
        "preference_tendency_general": "1300.php?y=",
        "preference_tendency_university_type": "1310.php?y=",
        "preference_tendency_universities": "1320.php?y=",
        "preference_tendency_provinces": "1330.php?y=",
        "preference_tendency_same_programs": "1340a.php?y=",
        "preference_tendency_programs": "1340b.php?y=",
        "conditions": "1110.php?y="
    }


def get_universities(table_name, pro_code):
    URL_UNIVERSITIES = f"{URL}{table_name}{pro_code}"
    response = requests.get(URL_UNIVERSITIES)
    return response


def universities_table(pro_code,year,uni_name, fakulty, major_name, uni_type, type_of_score, type_of_scholarship):
    universities_table = '''
                        CREATE TABLE IF NOT EXISTS nur51(
                        PRO_CODE INT,
                        YIL INT,
                        UNIVERSITE_ADI VARCHAR(500),
                        FAKULTE VARCHAR(500),
                        BOLUM_ADI VARCHAR(500),
                        UNI_TYPE VARCHAR(100),
                        PUAN_TURU VARCHAR(100),
                        BURS_TURU VARCHAR(100)
     )'''
    cur.execute(universities_table)
    conn.commit()
    universities_table_insert = '''
                        INSERT INTO nur51(
                        PRO_CODE ,
                        YIL ,
                        UNIVERSITE_ADI ,
                        FAKULTE ,
                        BOLUM_ADI,
                        UNI_TYPE ,
                        PUAN_TURU,
                        BURS_TURU ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'''

    insert_values = (str(pro_code), str(year), str(uni_name), str(fakulty), str(major_name), str(uni_type),
                     str(type_of_score), str(type_of_scholarship))
    cur.execute(universities_table_insert, insert_values)
    conn.commit()


conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
cur = conn.cursor()

engine = create_engine(f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}', pool_size=100, max_overflow=50)

for file in dirs:
    try:
        df = pd.read_table(path + "/" + file, header=None)
        program_number_list = df.to_dict()
        if "2021" in file:
            year = 2021
            URL = f"https://yokatlas.yok.gov.tr/{year}/content/lisans-dynamic/"
        elif "2022" in file:
            year = 2022
            URL = f"https://yokatlas.yok.gov.tr/content/lisans-dynamic/"
        elif "2020" in file:
            year = 2020
            URL = f"https://yokatlas.yok.gov.tr/{year}/content/lisans-dynamic/"
        elif "2019" in file:
            year = 2019
            URL = f"https://yokatlas.yok.gov.tr/{year}/content/lisans-dynamic/"

        print(len(program_number_list[0]))
        for a in range(len(program_number_list[0])):
            pro_code = program_number_list[0][a]
            counter = 1
            for key in table_location:
                table = table_location[key]
                text = get_universities(table, pro_code).text
                try:
                    if key == "main_info":
                        table_university = pd.read_html(text)
                        data_frame1 = table_university[0]
                        print(data_frame1)
                        PROGRAM_KODU = data_frame1.iloc[0][1]
                        YIL = year
                        UNIVERSITE_ISMI = data_frame1.iloc[2][1]
                        FAKULTE = data_frame1.iloc[3][1]
                        BOLUM_ADI = data_frame1.columns.values[0]
                        UNI_TYPE = data_frame1.iloc[1][1]
                        PUAN_TURU = data_frame1.iloc[4][1]
                        BURS_TURU = data_frame1.iloc[5][1]
                        universities_table(PROGRAM_KODU, YIL, UNIVERSITE_ISMI, FAKULTE, BOLUM_ADI, UNI_TYPE, PUAN_TURU, BURS_TURU)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        df2 = table_university[1]
                        df2.insert(0, "pro_code", pro_code)
                        df2.insert(1, "yil", year)
                        df2.rename(columns={0: "genel_bilgiler", 1: "genel_bilgiler_değerli"}, inplace=True)
                        print(df2)
                        df2.to_sql(key+"_V2", engine, if_exists='append', index=False)
                        print(key+"_V2", "table has been uploaded")
                        print("###########################################")
                        df3 = table_university[2]
                        df3.insert(0, "pro_code", pro_code)
                        df3.insert(1, "yil", year)
                        df3.rename(columns={0: "genel_bilgiler", 1: "genel_bilgiler_değerli"}, inplace=True)
                        print(df3)
                        df3.to_sql(key+"_V3", engine, if_exists='append', index=False)
                        print(key+"_V3", "table has been uploaded")
                        print("###########################################")
                    elif key == "quota_placement_statistics":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'Yerleşme Oranı %': "yerlesme_oranı", 'Unnamed: 0': "kontenjan_türü"}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "gender_distribution_of_students":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0':'Gender'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "geographic_places_where_students_come_from":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'sehir'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        data_frame2 = table_university[1]
                        data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'bölge'}, inplace=True)
                        data_frame2.insert(0, "pro_code", pro_code)
                        data_frame2.insert(1, "yil", year)
                        print(data_frame2)
                        #create a new table for regions
                        data_frame2.to_sql(key+"_for_regions", engine, if_exists='append', index=False)
                        print(key+"_for_regions", "table has been uploaded")
                        print("###########################################")
                    elif key == "provinces_of_students":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'il'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "educational_status_of_students":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'il'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "high_school_graduation_years_of_students":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "high_school_fields_of_students":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "high_school_types_of_students":
                        table_university = pd.read_html(text, skiprows=1, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key+"_genel_liseler", engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        data_frame2 = table_university[1]
                        data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'bölge'}, inplace=True)
                        data_frame2.insert(0, "pro_code", pro_code)
                        data_frame2.insert(1, "yil", year)
                        print("-------------------------------------")
                        print(data_frame2)
                        # create a new table for meslek liseleri
                        data_frame2.to_sql(key+"_meslek_liseleri", engine, if_exists='append', index=False)
                        print(key+"_meslek_liseleri", "table has been uploaded")
                        print("###########################################")
                    elif key == "high_schools_from_which_students_graduated":
                        table_university = pd.read_html(text, skiprows=1, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "students_school_firsts":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "base_score_and_achievement_statistics":
                        table_university = pd.read_html(text, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        #başarı puanı
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        table_university1 = pd.read_html(text, header=0, skiprows=1)
                        data_frame2 = table_university1[1]
                        data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'bölge'}, inplace=True)
                        data_frame2.insert(0, "pro_code", pro_code)
                        data_frame2.insert(1, "yil", year)
                        print("-------------------------------------")
                        print(data_frame2)
                        # create a new table for son yerleşen sıralama
                        data_frame2.to_sql(key+"_yerlesen_siralama", engine, if_exists='append', index=False)
                        print(key+"_yerlesen_siralama", "table has been uploaded")
                        print("###########################################")
                    elif key == "last_placed_student_profile":
                        table_university = pd.read_html(text)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={0: "genel_bilgi", 1: 'ogrencinin_genel_bilgisi'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "YKS_net_averages_of_students":
                        table_university = pd.read_html(text, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'netler'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "students_YKS_scores":
                        table_university = pd.read_html(text, header=0, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        data_frame2 = table_university[1]
                        data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
                        data_frame2.insert(0, "pro_code", pro_code)
                        data_frame2.insert(1, "yil", year)
                        print("-------------------------------------")
                        print(data_frame2)
                        # create a new table for en düşük ortalama
                        data_frame2.to_sql(key+"_en_düsük", engine, if_exists='append', index=False)
                        print(key+"_en_düsük", "table has been uploaded")
                        print("###########################################")
                    elif key == "YKS_success_order_of_students":
                        table_university = pd.read_html(text, header=0, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        data_frame2 = table_university[1]
                        data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
                        data_frame2.insert(0, "pro_code", pro_code)
                        data_frame2.insert(1, "yil", year)
                        print("-------------------------------------")
                        print(data_frame2)
                        # create a new table for son sıralama
                        data_frame2.to_sql(key+"_son_siralama", engine, if_exists='append', index=False)
                        print(key+"_son_siralama", "table has been uploaded")
                        print("###########################################")
                    elif key == "preference_statistics_across_the_country":
                        table_university = pd.read_html(text, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={0: "genel_bilgiler", 1: 'ogrenci_genel_bilgileri', 2: 'ogrenci_genel_bilgileri_orani'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        data_frame2 = table_university[1]
                        data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
                        data_frame2.insert(0, "pro_code", pro_code)
                        data_frame2.insert(1, "yil", year)
                        data_frame2.drop(columns=data_frame2.columns[-1],  axis=1,  inplace=True)
                        print("-------------------------------------")
                        print(data_frame2)
                        # create a new table for tercih sırası
                        data_frame2.to_sql(key+"_tercih_sırası", engine, if_exists='append', index=False)
                        print(key+"_tercih_sırası", "table has been uploaded")
                        print("###########################################")
                    #TODO: check later!! tercih sırası comes twice.
                    elif key == "in_which_preferences_students_settled":
                        table_university = pd.read_html(text, thousands=None, header=0 )
                        data_frame1 = table_university[0]
                        data_frame1.rename(
                            columns={0: "genel_bilgiler", 1: 'ogrenci_genel_bilgileri', 2: 'ogrenci_genel_bilgileri_orani'},
                            inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        data_frame2 = table_university[1]
                        data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
                        data_frame2.insert(0, "pro_code", pro_code)
                        data_frame2.insert(1, "yil", year)
                        data_frame2.drop(columns=data_frame2.columns[-1], axis=1, inplace=True)
                        print("-------------------------------------")
                        print(data_frame2)
                        # create a new table for son yerleşen sayısı
                        data_frame2.to_sql(key+"_yerlesen_sayisi", engine, if_exists='append', index=False)
                        print(key+"_yerlesen_sayisi", "table has been uploaded")
                        print("###########################################")
                    elif key == "preference_tendency_general":
                        table_university = pd.read_html(text, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={0: "genel_bilgi", 1: 'genel_cevap'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "preference_tendency_university_type":
                        table_university = pd.read_html(text, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={0: "universite_turu", 1: 'sayisi'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "preference_tendency_universities":
                        table_university = pd.read_html(text, thousands=None, skiprows=1, header=0)
                        data_frame1 = table_university[0]
                        data_frame1.rename(
                            columns={0: "genel_bilgiler", 1: 'ogrenci_genel_bilgileri', 2: 'ogrenci_genel_bilgileri_orani'},
                            inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                        data_frame2 = table_university[1]
                        data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
                        data_frame2.insert(0, "pro_code", pro_code)
                        data_frame2.insert(1, "yil", year)
                        print("-------------------------------------")
                        print(data_frame2)
                        # create a new table for devlet üniversiteleri.
                        data_frame2.to_sql(key+"_devlet_üniversiteleri", engine, if_exists='append', index=False)
                        print(key+"_devlet_üniversiteleri", "table has been uploaded")
                        print("###########################################")
                    elif key == "preference_tendency_provinces":
                        table_university = pd.read_html(text, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={0: "universite_turu", 1: 'sayisi'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "preference_tendency_same_programs":
                        table_university = pd.read_html(text, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={0: "universite_turu", 1: 'sayisi'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    elif key == "preference_tendency_programs":
                        table_university = pd.read_html(text, thousands=None)
                        data_frame1 = table_university[0]
                        data_frame1.rename(columns={0: "universite_turu", 1: 'sayisi'}, inplace=True)
                        data_frame1.insert(0, "pro_code", pro_code)
                        data_frame1.insert(1, "yil", year)
                        print(data_frame1)
                        data_frame1.to_sql(key, engine, if_exists='append', index=False)
                        print(key, "table has been uploaded")
                        print("###########################################")
                    counter += 1
                except Exception as e:
                    print(e)
                    pass
    except:
        print("This file is empty or broken.")




