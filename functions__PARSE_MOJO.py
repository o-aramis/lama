import pandas as pd
import os
import time
import queue
import threading
from bs4 import BeautifulSoup as BS
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromService
import datetime as dt

from variables import MOJO_doc
from variables import PARSE_MOJO__num_threads
from variables import RELAY_dir
from variables import CUPS_dir
from variables import MOJO_parse_check_doc
from variables import LamaReins_doc

params_df = pd.read_excel(LamaReins_doc, sheet_name='params')
loading_time_sec = int(params_df[params_df['par'] == 'PARSE__mojo_loading_time_sec']['val'].tolist()[0])




# Функция для выполнения задач
def dvij_df():
    
    while True:
        link = queue.get()


        # парсинг непосредственно
        driver = webdriver.Chrome(service=ChromService(executable_path=RELAY_dir + 'chromedriver.exe'))
        driver.get(link)
        time.sleep(loading_time_sec)

        html = driver.page_source
        soup = BS(html, "html.parser")
        
        
        dt_ii_head_soup = str(soup.find_all(class_="col_name"))
        dt_ii_head_pre1 = dt_ii_head_soup.replace('<th class="col_name">','')
        dt_ii_head_pre2 = dt_ii_head_pre1.replace('</th>','')
        dt_ii_head_pre3 = dt_ii_head_pre2[1:-1]
        dt_ii_head_probely = dt_ii_head_pre3.split(',')
        
        dt_ii_head_col1 = dt_ii_head_probely[:1]
        dt_ii_head_so_2go =[]
        
        for st_s_probelom in dt_ii_head_probely[1:]:
            st = st_s_probelom[1:]
            dt_ii_head_so_2go.append(st)

        dt_ii_head = dt_ii_head_col1 + dt_ii_head_so_2go


        dt_ii_rows = []
        for i in range(0,100):

            row_name = 'row_' + str(i)
            dt_ii_row_soup = str(soup.find_all(class_="row_with_data",id =row_name))

            dt_ii_row_pre1 = dt_ii_row_soup.replace('<td>','')
            dt_ii_row_pre2 = dt_ii_row_pre1.replace('</tr>','')
            if len(str(i)) == 1:
                k = 0
            elif len(str(i)) == 2:
                k = 1
            elif len(str(i)) == 3:
                k = 2

            nachalo_supa = 38 + k

            dt_ii_row_pre3 = dt_ii_row_pre2[nachalo_supa:-1]
            dt_ii_row_pre4 = dt_ii_row_pre3.split('</td>')
            dt_ii_row = dt_ii_row_pre4[:-1]
            dt_ii_rows.append(dt_ii_row)
            print(dt_ii_row_pre2)
        
        
        # если спарсилось и закрываем драйвер
        if dt_ii_head != ['']:

            # очищаем получивщуюся таблицу
            df = pd.DataFrame(dt_ii_rows, columns = dt_ii_head)
            df['Наименование'] = df['Наименование'].astype('str')
            df_cleaned = df.dropna()
            print(df_cleaned)

            # вставляем в файл MOJO-базы
            #df_cleaned.to_csv(MOJO_doc, mode='a', index= False , header= False)

            # сохраняем результат парса в файл если данные не битые
            try:
                parse_file_doc = CUPS_dir + 'PARSE/parse_docs/' + dt.datetime.now().strftime('%d%H%M%S%f') + '.csv'
                df_cleaned.to_csv(parse_file_doc, index= False)
                crash_test_df = pd.read_csv(MOJO_doc)
                crash_test_df = crash_test_df.drop_duplicates()
                crash_test_df['Время движения'] = pd.to_datetime(crash_test_df['Время движения'], format='%d.%m.%Y %H:%M:%S')
                crash_test_df = crash_test_df.sort_values('Время движения', ascending=False)
            except:
                if os.path.isfile(parse_file_doc) == True:
                    os.remove(parse_file_doc)
            
        
        driver.quit()

        # в любом случае записываем, что парсили + удаляем файл-счётчик + закрываем драйвер
        scanit =  str(link)[str(link).find('barcode&value=') + 14:]
        pd.DataFrame({'col1':['ii'], 'col2':[scanit]}).to_csv(MOJO_parse_check_doc, mode='a', index=False, header=False)
        check_file = CUPS_dir + 'PARSE/counter/' + scanit + '.txt'
        
        try:
            if os.path.isfile(check_file) == True:
                os.remove(check_file)
        except:
            pass
        

        queue.task_done()



# Очередь задач
queue = queue.Queue()


def PARSE_MOJO_GO (urls):
    
    urls = list(urls)
    # Заполнение очереди задач
    for link in urls:
        queue.put(link)

    # Создание потоков и запуск их работы
    for i in range(PARSE_MOJO__num_threads):
        t = threading.Thread(target=dvij_df)
        t.daemon = True
        t.start()

    # Ожидание завершения работы всех потоков
    queue.join()