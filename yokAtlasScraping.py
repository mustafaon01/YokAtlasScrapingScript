import requests
import psycopg2
import pandas as pd
from sqlalchemy import *

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 29)
pd.set_option('display.max_rows', 200)

years = [2019, 2020, 2021, 2022]
year = 2020
pro_code = 106510032


hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = 'Ju3Dt8h8'
port_id = 5432
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


def get_universities(table_name):
    URL_UNIVERSITIES = f"{URL}{table_name}{pro_code}"
    response = requests.get(URL_UNIVERSITIES)
    return response


def universities_table(pro_code,year,uni_name, fakulty, major_name):
    universities_table = '''
                        CREATE TABLE IF NOT EXISTS nur5(
                        PRO_CODE INT,
                        YIL INT,
                        UNIVERSITE_ADI VARCHAR(500),
                        FAKULTE VARCHAR(500),
                        BOLUM_ADI VARCHAR(500)
     )'''
    cur.execute(universities_table)
    conn.commit()
    universities_table_insert = '''
                        INSERT INTO nur5(
                        PRO_CODE ,
                        YIL ,
                        UNIVERSITE_ADI ,
                        FAKULTE ,
                        BOLUM_ADI ) VALUES (%s,%s,%s,%s,%s)'''

    insert_values = (str(pro_code), str(year), str(uni_name), str(fakulty), str(major_name))
    cur.execute(universities_table_insert,insert_values)
    conn.commit()


conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
cur = conn.cursor()


engine = create_engine('postgresql://postgres:Ju3Dt8h8@localhost:5432/postgres')


for year in years:
    if year != 2022:
        URL = f"https://yokatlas.yok.gov.tr/{year}/content/lisans-dynamic/"
    else:
        URL = f"https://yokatlas.yok.gov.tr/content/lisans-dynamic/"
    counter = 1
    for key in table_location:
        table = table_location[key]
        text = get_universities(table).text
        if key == "main_info":
            table_university = pd.read_html(text)
            data_frame1 = table_university[0]
            print(data_frame1)
            PROGRAM_KODU = data_frame1.iloc[0][1]
            YIL = year
            UNIVERSITE_ISMI = data_frame1.iloc[2][1]
            FAKULTE = data_frame1.iloc[3][1]
            BOLUM_ADI = data_frame1.columns.values[0]
            universities_table(PROGRAM_KODU, YIL, UNIVERSITE_ISMI, FAKULTE, BOLUM_ADI)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "quota_placement_statistics":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'Yerleşme Oranı %': "yerlesme_oranı"}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur3', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "gender_distribution_of_students":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0':'Gender'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur6', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        #TODO: concat two df
        elif key == "geographic_places_where_students_come_from":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'sehir'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur7', engine, if_exists='replace', index=False)
            data_frame2 = table_university[1]
            data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'bölge'}, inplace=True)
            data_frame2.insert(0, "pro_code", pro_code)
            data_frame2.insert(1, "yil", year)
            print(data_frame2)
            #create a new table for regions
            data_frame2.to_sql('nur8', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "provinces_of_students":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'il'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur9', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "educational_status_of_students":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'il'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur10', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "high_school_graduation_years_of_students":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur11', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "high_school_fields_of_students":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur12', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        #TODO: concat two df
        elif key == "high_school_types_of_students":
            table_university = pd.read_html(text, skiprows=1, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur13', engine, if_exists='replace', index=False)
            data_frame2 = table_university[1]
            data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'bölge'}, inplace=True)
            data_frame2.insert(0, "pro_code", pro_code)
            data_frame2.insert(1, "yil", year)
            print("-------------------------------------")
            print(data_frame2)
            # create a new table for regions
            data_frame2.to_sql('nur14', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "high_schools_from_which_students_graduated":
            table_university = pd.read_html(text, skiprows=1, header=0)
            data_frame1 = table_university[0]
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            data_frame1.to_sql('nur155', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "students_school_firsts":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur15', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "base_score_and_achievement_statistics":
            table_university = pd.read_html(text, header=0)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'mezuniyet_yili'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur16', engine, if_exists='replace', index=False)
            table_university1 = pd.read_html(text, header=0, skiprows=1)
            data_frame2 = table_university1[1]
            data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'bölge'}, inplace=True)
            data_frame2.insert(0, "pro_code", pro_code)
            data_frame2.insert(1, "yil", year)
            print("-------------------------------------")
            print(data_frame2)
            # create a new table for son yerleşen sıralama
            data_frame2.to_sql('nur17', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "last_placed_student_profile":
            table_university = pd.read_html(text)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={0: "genel_bilgi", 1: 'ogrencinin_genel_bilgisi'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur18', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "YKS_net_averages_of_students":
            table_university = pd.read_html(text, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'netler'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur19', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "students_YKS_scores":
            table_university = pd.read_html(text, header=0, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur20', engine, if_exists='replace', index=False)
            data_frame2 = table_university[1]
            data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
            data_frame2.insert(0, "pro_code", pro_code)
            data_frame2.insert(1, "yil", year)
            print("-------------------------------------")
            print(data_frame2)
            # create a new table for son yerleşen sıralama
            data_frame2.to_sql('nur21', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "YKS_success_order_of_students":
            table_university = pd.read_html(text, header=0, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur22', engine, if_exists='replace', index=False)
            data_frame2 = table_university[1]
            data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
            data_frame2.insert(0, "pro_code", pro_code)
            data_frame2.insert(1, "yil", year)
            print("-------------------------------------")
            print(data_frame2)
            # create a new table for son yerleşen sıralama
            data_frame2.to_sql('nur23', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "preference_statistics_across_the_country":
            table_university = pd.read_html(text, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={0: "genel_bilgiler", 1: 'ogrenci_genel_bilgileri', 2: 'ogrenci_genel_bilgileri_orani'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur24', engine, if_exists='replace', index=False)
            data_frame2 = table_university[1]
            data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
            data_frame2.insert(0, "pro_code", pro_code)
            data_frame2.insert(1, "yil", year)
            data_frame2.drop(columns=data_frame2.columns[-1],  axis=1,  inplace=True)
            print("-------------------------------------")
            print(data_frame2)
            # create a new table for son yerleşen sıralama
            data_frame2.to_sql('nur25', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
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
            data_frame1.to_sql('nur26', engine, if_exists='replace', index=False)
            data_frame2 = table_university[1]
            data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
            data_frame2.insert(0, "pro_code", pro_code)
            data_frame2.insert(1, "yil", year)
            data_frame2.drop(columns=data_frame2.columns[-1], axis=1, inplace=True)
            print("-------------------------------------")
            print(data_frame2)
            # create a new table for son yerleşen sıralama
            data_frame2.to_sql('nur27', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "preference_tendency_general":
            table_university = pd.read_html(text, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={0: "genel_bilgi", 1: 'genel_cevap'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur28', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "preference_tendency_university_type":
            table_university = pd.read_html(text, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={0: "universite_turu", 1: 'sayisi'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur29', engine, if_exists='replace', index=False)
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
            data_frame1.to_sql('nur30', engine, if_exists='replace', index=False)
            data_frame2 = table_university[1]
            data_frame2.rename(columns={'% Oran': "oran", 'Unnamed: 0': 'genel_bilgiler'}, inplace=True)
            data_frame2.insert(0, "pro_code", pro_code)
            data_frame2.insert(1, "yil", year)
            #data_frame2.drop(columns=data_frame2.columns[-1], axis=1, inplace=True)
            print("-------------------------------------")
            print(data_frame2)
            # create a new table for devlet üniversiteleri.
            data_frame2.to_sql('nur31', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "preference_tendency_provinces":
            table_university = pd.read_html(text, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={0: "universite_turu", 1: 'sayisi'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur32', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "preference_tendency_same_programs":
            table_university = pd.read_html(text, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={0: "universite_turu", 1: 'sayisi'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur33', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        elif key == "preference_tendency_programs":
            table_university = pd.read_html(text, thousands=None)
            data_frame1 = table_university[0]
            data_frame1.rename(columns={0: "universite_turu", 1: 'sayisi'}, inplace=True)
            data_frame1.insert(0, "pro_code", pro_code)
            data_frame1.insert(1, "yil", year)
            print(data_frame1)
            data_frame1.to_sql('nur34', engine, if_exists='replace', index=False)
            print(key, "table has been uploaded")
            print("###########################################")
        counter += 1






