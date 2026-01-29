from threading import Timer
import datetime as dt
from datetime import datetime
from datetime import timedelta
import pandas as pd
import openpyxl as op
from openpyxl.utils.dataframe import dataframe_to_rows
import os
import time
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromService
from bs4 import BeautifulSoup
import math
import zipfile
import vertica_python
import tkinter as tk
import webbrowser


from variables import LF_dir
from variables import RELAY_dir
from variables import D_dir
from variables import OUT_dir
from variables import MES_dir
from variables import MIS_INID_doc
from variables import CUPS_dir
from variables import QUEUE_dir
from variables import kick_doc
from variables import pins_doc
from variables import LamaReins_doc
from variables import ChromeDrider_doc
from variables import MIS_INID_doc
from variables import MIS_BOOK_doc
from variables import COM_dir
from variables import COMPLAINT__SoupPattern_doc
from variables import COMPLAINT__ManSoupPattern_doc
from variables import COMPLAINT__SoupToGO_doc
from variables import COMPLAINT__Soup_Scanit_doc
from variables import COMPLAINT__Soup_Scanit_Stock_doc
from variables import COMPLAINT__Dbeaver_doc
from variables import MojoSample_doc
from variables import REP_LARGE_SIZE__SSRep_doc
from variables import RELAY_REP_LARGE_SIZE_dir
from variables import REP_LARGE_SIZE__BDUL_Item_doc
from variables import REP_LARGE_SIZE__BDUL_Tag_doc
from variables import TABLO_Registry_doc
from variables import TABLO_Settings_doc
from variables import QCS_doc
from variables import RELAY_TABLO_dir
from variables import START_LAMA_DirCreating_list
from variables import MOJO_doc
from variables import MOJO_parse_check_doc
from variables import PARSE_MOJO__num_threads
from variables import PARSE__mojo_iter_duration_sec
from variables import GUI_TABLO__MisBookPivot_doc
from variables import GUI_TABLO__MisInProgress_doc
from variables import MIS_MF_doc
from variables import QCS_pattern_doc
from variables import MIS_BOOK__duplicate_check_4_key_list_code
from variables import MIS_boost_keys_list_code
from variables import LAMA_archive_cup_dir
from variables import url_posting_max

from functions__PARSE_MOJO import PARSE_MOJO_GO

if 'LAMA' != '':
    def start_lama():
        if 'Я проснулась' != '':
            # сообщаем юзеру, что проснулись
            mes = f'Я проснулась и готова к работе!'
            
            mes_file = MES_dir + '_' + dt.datetime.now().strftime('%Y%m%d%H%M%S%f') + '.txt'
            f = open(mes_file,'w')
            f.write(mes)
            f.close

        if 'УДАЛЯЕМ незавершённые процессы и несегодняшние данные' !='':
            # сносим независимую папку процесса архивирования если она есть
            if os.path.isdir(LAMA_archive_cup_dir) == True:
                shutil.rmtree(LAMA_archive_cup_dir)

            # пеперь обычные процессы
            for di in os.listdir(LF_dir):
                if di not in ['_relay', 'data__' + dt.datetime.now().strftime('%Y.%m.%d')]:
                    print(di)
                    shutil.rmtree(LF_dir + di + '/')

        if 'СОЗДАЁМ ПАПКИ' != '':
            for d in START_LAMA_DirCreating_list:
                if os.path.isdir(d) == False:
                    os.mkdir(d)

        if 'СОЗДАЁМ CSV-файлы':

            if 'MIS_INID' != '':
                if os.path.isfile(MIS_INID_doc) == False:
                    MIS_INID_new_df = pd.DataFrame({'mis_id':[],
                                                'date':[],
                                                'key':[],
                                                'warehouse_id':[],
                                                'dt':[],
                                                'process':[],
                                                'name':[],
                                                'login':[],
                                                'method':[],
                                                'object_id':[],
                                                'place':[],
                                                'pre_com':[],
                                                'comment':[],
                                                'photo':[],
                                                'spec':[]})
                    MIS_INID_new_df.to_csv(MIS_INID_doc, index=False)

            if 'MIS_BOOK' != '':
                if os.path.isfile(MIS_BOOK_doc) == False:
                    MIS_BOOK_new_df = pd.DataFrame({'mis_id':[],
                                                'date':[],
                                                'key':[],
                                                'warehouse_id':[],
                                                'dt':[],
                                                'process':[],
                                                'name':[],
                                                'login':[],
                                                'method':[],
                                                'object_id':[],
                                                'place':[],
                                                'pre_com':[],
                                                'comment':[],
                                                'photo':[],
                                                'spec':[],
                                                'QCS_warehouse_id':[],
                                                'QCS_dt':[],
                                                'QCS_process':[],
                                                'QCS_name':[],
                                                'QCS_login':[],
                                                'QCS_method':[],
                                                'QCS_object_id':[],
                                                'QCS_place':[],
                                                'QCS_comment':[],
                                                'status':[]})
                    MIS_BOOK_new_df.to_csv(MIS_BOOK_doc, index=False)

            if 'pins' != '':
                if os.path.isfile(pins_doc) == False:
                    pins_new_df = pd.DataFrame({'pins':[]})
                    pins_new_df.to_csv(pins_doc, index=False)

            if 'MOJO_parse_check_today' !='':
                if os.path.isfile(MOJO_parse_check_doc) == False:
                    MOJO_parse_check_today_df = pd.DataFrame({'search_type':[],
                                                              'val':[]})
                    MOJO_parse_check_today_df.to_csv(MOJO_parse_check_doc, index=False)

            if 'MOJO_doc' !='':
                if os.path.isfile(MOJO_doc) == False:
                    MOJO_df = pd.read_excel(MojoSample_doc)
                    MOJO_df.to_csv(MOJO_doc, index=False)

        if 'ЗАПУСКАЕМ НЕОБХОДИМАЕ ФУНКЦИИ' !='':
            MIS_BOOK_Pivot()

    def pin_control():
        sdt = dt.datetime.now().strftime('%Y%m%d')
        u = 0
        for s in sdt[:4]:
            u = u + int(s)
        u = u + int(sdt[4:6]) ** 2 + int(sdt[-2:-1])
        x = Enigma('.' + str(math.sqrt(u)))[1][:4]
        #print(x)
        for pin in pd.read_csv(pins_doc)['pins'].tolist():
            if str(pin) ==  x:
                return 'y'
        return 'n'

    def ver_name():
        try:
            f_list = os.listdir('./')
            for f in f_list:
                if 'Lama_' in f:
                    el = Enigma('_' + f)
                    ver = el[1] + '.' + el[2][:-3]

        except: ver = 'Lama (сбой версии)'
        
        return ver
        
    def chat (sender, message):
        sender = str(sender)
        message = str(message)

        mes = f'_____________\n{sender}\n{message}'

        if sender == '-':
            mes = f'_____________\n{message}'

        mes_file = MES_dir + dt.datetime.now().strftime('%Y%m%d%H%M%S%f') + '.txt'
        f = open(mes_file,'w')
        f.write(mes)
        f.close

    def screen_cup():
        res = ''
        cup_list = os.listdir(CUPS_dir)
        cup_list.remove('_queue')

        sn = 'ДОБАВЛЯЮ время к PARSE'
        if sn !='':
            parse_counter_dir = CUPS_dir + 'PARSE/counter/'
            if 'PARSE' in cup_list and os.path.isdir(parse_counter_dir) == True:
                check_files_num = len(os.listdir(parse_counter_dir))
                ind = cup_list.index('PARSE')
                cup_list.pop(ind)
                cup_list.insert(ind, f'PARSE\n(осталость ' + str(check_files_num) + ' стр / ~ ' + str(int((check_files_num / PARSE_MOJO__num_threads * PARSE__mojo_iter_duration_sec / 60) + 1)) + ' мин)')

        if len(cup_list) == 0:
            res = '\n' + 'жду Ваших приказов'
        else:
            for i in cup_list:
                res = res + '\n' + str(i)
    
        return res
    
    def screen_mis_book():
        res = '\n' + open(GUI_TABLO__MisBookPivot_doc).read()
        if os.path.isfile(GUI_TABLO__MisInProgress_doc) == True:
            MisInProgress = open(GUI_TABLO__MisInProgress_doc).read()
            if MisInProgress != '0':
                res = res + '\n' + MisInProgress
            #os.remove(GUI_TABLO__MisInProgress_doc)

        return res

    def def_starter():
        #########################################
        # получаем список чашек
        cup_list = os.listdir(CUPS_dir)
        if len(cup_list) > 0:
            # получаем список чашек с кикок
            def_go_list = []
            for cup in cup_list:
                if os.path.isfile(CUPS_dir + cup + '/' + 'start.txt') == True:
                    def_go_list.append(cup)
            
            # проверяем есть ли что запускать
            if len(def_go_list) > 0:
                # запускаем функции по списку
                for def_name in def_go_list:
                    def_name = str(def_name)
                    def_kick_doc = CUPS_dir + def_name + '/' + 'start.txt' 
                    ###########################################################
                    ###########################################################
                    if def_name == 'NOSAMO':
                        os.remove(def_kick_doc)
                        NOSAMO()

                    elif def_name == 'MIS_MF_LOAD':
                        os.remove(def_kick_doc)
                        MIS_MF_LOAD()

                    elif def_name == 'COMPLAINT_SoupPattern':
                        os.remove(def_kick_doc)
                        COMPLAINT_SoupPattern()

                    elif def_name == 'COMPLAINT_SoupToGo':
                        os.remove(def_kick_doc)
                        COMPLAINT_SoupToGo()

                    elif def_name == 'COMPLAINT_PostingQuery':
                        os.remove(def_kick_doc)
                        COMPLAINT_PostingQuery()

                    elif def_name == 'COMPLAINT_Soup_Scanit':
                        os.remove(def_kick_doc)
                        COMPLAINT_Soup_Scanit()

                    elif def_name == 'COMPLAINT_DBQuery':
                        os.remove(def_kick_doc)
                        COMPLAINT_DBQuery()

                    elif def_name == 'COMPLAINT_Soup_Scanit_Stock':
                        os.remove(def_kick_doc)
                        COMPLAINT_Soup_Scanit_Stock()

                    elif def_name == 'MOJO_single_load':
                        os.remove(def_kick_doc)
                        MOJO_single_load()

                    elif def_name == 'COMPLAINT_BD_data_load':
                        os.remove(def_kick_doc)
                        COMPLAINT_BD_data_load()

                    elif def_name == 'COMPLAINT_ToWork':
                        os.remove(def_kick_doc)
                        COMPLAINT_ToWork()

                    elif def_name == 'REP_LARGE_SIZE__DataIn':
                        os.remove(def_kick_doc)
                        REP_LARGE_SIZE__DataIn()

                    elif def_name == 'REP_LARGE_SIZE__ItemQuery':
                        os.remove(def_kick_doc)
                        REP_LARGE_SIZE__ItemQuery()

                    elif def_name == 'REP_LARGE_SIZE__DBUL_Item':
                        os.remove(def_kick_doc)
                        REP_LARGE_SIZE__DBUL_Item()

                    elif def_name == 'REP_LARGE_SIZE__DBQ_Tag':
                        os.remove(def_kick_doc)
                        REP_LARGE_SIZE__DBQ_Tag()

                    elif def_name == 'REP_LARGE_SIZE__DBUL_Tag':
                        os.remove(def_kick_doc)
                        REP_LARGE_SIZE__DBUL_Tag ()

                    elif def_name == 'REP_LARGE_SIZE__ToWork':
                        os.remove(def_kick_doc)
                        REP_LARGE_SIZE__ToWork ()

                    elif def_name == 'TABLO_Main':
                        os.remove(def_kick_doc)
                        TABLO_Main ()

                    elif def_name == 'TABLO_UpdatingRegistrySettings':
                        os.remove(def_kick_doc)
                        TABLO_UpdatingRegistrySettings ()

                    elif def_name == 'QCS_Updating':
                        os.remove(def_kick_doc)
                        QCS_Updating ()

                    elif def_name == 'MIS_BOOK':
                        os.remove(def_kick_doc)
                        MIS_BOOK ()

                    elif def_name == 'PARSE':
                        os.remove(def_kick_doc)
                        PARSE ()

                    elif def_name == 'MIS_MF_Pattern':
                        os.remove(def_kick_doc)
                        MIS_MF_Pattern ()

                    elif def_name == 'MIS_BOOK_Recalculation':
                        os.remove(def_kick_doc)
                        MIS_BOOK_Recalculation ()

                    elif def_name == 'PARSE_MOJO_GO':
                        os.remove(def_kick_doc)
                        PARSE_MOJO_GO ()

                    elif def_name == 'MIS_BOOK_QCS':
                        os.remove(def_kick_doc)
                        MIS_BOOK_QCS ()

                    elif def_name == 'MIS_BOOK_RecalculationReparse':
                        os.remove(def_kick_doc)
                        MIS_BOOK_RecalculationReparse ()

                    elif def_name == 'MIS_BOOK_ToExcel':
                        os.remove(def_kick_doc)
                        MIS_BOOK_ToExcel ()

                    elif def_name == 'LAMA_Archive':
                        os.remove(def_kick_doc)
                        LAMA_Archive ()

                    elif def_name == 'MIS_BOOK_RecalculationLimit':
                        os.remove(def_kick_doc)
                        MIS_BOOK_RecalculationLimit ()

                    elif def_name == 'MIS_BOOK_OverLimit':
                        os.remove(def_kick_doc)
                        MIS_BOOK_OverLimit ()

                    elif def_name == 'MIS_BOOK_KillDay':
                        os.remove(def_kick_doc)
                        MIS_BOOK_KillDay ()

                    elif def_name == 'COMPLAINT_ToWork2':
                        os.remove(def_kick_doc)
                        COMPLAINT_ToWork2 ()

                    elif def_name == 'COMPLAINT_ManSoupPattern':
                        os.remove(def_kick_doc)
                        COMPLAINT_ManSoupPattern ()

                    elif def_name == 'COMPLAINT_SoupToGoFromMan':
                        os.remove(def_kick_doc)
                        COMPLAINT_SoupToGoFromMan ()

                    elif def_name == 'COMPLAINT_Flow':
                        os.remove(def_kick_doc)
                        COMPLAINT_Flow ()

                        

                    

                    


                        

                     

    def def_initiator (desc, par_list):
        try:
            def_name = 'def_initiator'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            desc = str(desc)
            par_list = list(par_list)

            # определяем название функции по описанию
            if len(comm_df[comm_df['name'] == desc]) == 0 and len(comm_df[comm_df['desc'] == desc]) == 0:
                mes = f'Не могу запустить функцию {desc}'
            else:
                if len(comm_df[comm_df['name'] == desc]) > 0:
                    name = desc
                else:
                    name = comm_df[comm_df['desc'] == desc]['name'].tolist()[0]
                    
                # определяем номер в очереди
                try:
                    go_cup_list = os.listdir(CUPS_dir + '_queue/')
                    num_list = []
                    for cup in go_cup_list:
                        def_name_of_cup = Enigma('#' + cup)[0]
                        if def_name_of_cup == name:
                            num_list.append(int(Enigma('#' + cup)[1]))
                    queue_num = int(max(num_list)) + 1
                except:
                    queue_num = 1

                # СОЗДАЁМ папку-инициатор в очереди
                ini_dir = CUPS_dir + '_queue/' + name + '#' + str(queue_num)
                os.mkdir(ini_dir)
                # сохраняем лист параметров
                df = pd.DataFrame({'par_list': par_list})
                df.to_csv(ini_dir + '/par_list.csv', index=False)

                mes = f'запускаю функцию {desc}'

            
        except:
            mes = f'Не могу запустить {desc}! Что-то пошло не так:('

        chat('-', mes)

    def def_kicker():
            try:
                #########################################
                # проверяем есть очередь
                QUEUE_dir = CUPS_dir + '_queue/'
                go_list = os.listdir(QUEUE_dir)
                if len(go_list) > 0:
                    # ищем первую незапущенную чашку, процесс которой незапущен
                    for go_cup in go_list:
                        def_name = Enigma('#' + go_cup)[0]
                        def_dir = CUPS_dir + def_name + '/'
                        if os.path.isdir(def_dir) == False:
                            # создаём CUP-папку с листом параметров
                            os.mkdir(def_dir)
                            shutil.copy2(QUEUE_dir + go_cup + '/' + 'par_list.csv', def_dir + 'par_list.csv')

                            # сносим папку очереди
                            shutil.rmtree(QUEUE_dir + go_cup + '/')

                            # cоздаём кик для стартера
                            shutil.copy2(kick_doc, def_dir + 'start.txt')
                            
            except:
                
                chat('-', 'Не могу оправить функцию из очереди в исполнение :(')

    def DocOutPrefix():
        return OUT_dir + dt.datetime.now().strftime('%d%H%M%S%f') + '__'

    def LAMA_Archive ():
        def_name = 'LAMA_Archive'
        comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
        mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
        cup_dir = CUPS_dir + def_name + '/'
        
        try:
            sn = 'ПЕРЕМЕННЫЕ'
            if sn != '':
                print(sn)
                dir_for_zip = LAMA_archive_cup_dir + 'dir_for_zip/'

            sn = 'создаю независимую рабочую папку'
            if sn != '':
                print(sn)
                os.mkdir(LAMA_archive_cup_dir)
                os.mkdir(dir_for_zip)

            sn = 'создаю временный архив'
            if sn != '':
                print(sn)
                temp_zip_name = LAMA_archive_cup_dir + 'temp_zip'
                shutil.make_archive(temp_zip_name, 'zip', './')
                temp_zip_file = temp_zip_name + '.zip'

            sn = 'получаю все файлы'
            if sn != '':
                print(sn)
                with zipfile.ZipFile(temp_zip_file, "r") as myzip:
                    myzip.extractall(path=dir_for_zip)

            sn = 'сношу всё лишнее в корневой папке'
            if sn != '':
                print(sn)
                full_file_list = os.listdir(dir_for_zip)
                for i in full_file_list:
                    if '.' not in i and i != LF_dir[2:-1]:
                        shutil.rmtree(dir_for_zip + i + '/')

            sn = 'сношу всё лишнее в Лама-файлес'
            if sn != '':
                print(sn)
                lama_files_dir = dir_for_zip + LF_dir[2:]
                print(lama_files_dir)
                lama_files_list = os.listdir(lama_files_dir)
                print(lama_files_list)
                for i in lama_files_list:
                    print(i)
                    if i != '_relay':
                        shutil.rmtree(lama_files_dir + i + '/')

            sn = 'РЕЗУЛЬТАТ создаю архив'
            if sn != '':
                print(sn)
                res_name = LAMA_archive_cup_dir + 'res'
                shutil.make_archive(res_name, 'zip', dir_for_zip)
                res_file = res_name + '.zip'

            sn = 'ОТДАЮ результат'
            if sn != '':
                print(sn)
                shutil.copy2(res_file, DocOutPrefix() + def_name + '_' + ver_name() + '.zip')


            mes = 'Готово!'

        except:
            mes = 'Ошибка! Что-то пошло не так:('

        # удаляем независимую папку процесса
        if os.path.isdir(LAMA_archive_cup_dir) == True:
            shutil.rmtree(LAMA_archive_cup_dir)
        
        # удаляем папку процесса
        if os.path.isdir(cup_dir) == True:
            shutil.rmtree(cup_dir)
        chat(mes_sender, mes)

if 'ОБЩИЕ' != '': 

    def Repeater(interval, function):
        Timer(interval, Repeater, [interval, function]).start()
        function()

    def Enigma (cypher):
        try:
            cypher = str(cypher)
            EniKey = cypher[0]
            indices = []
            index = -1
            
            while True:
                index = cypher.find(EniKey, index + 1)
                if index == - 1:
                    break
                indices.append(index)
            #print(indices)
            re_indices = list(reversed(indices))
            #print(re_indices)

            Code_list = []
            for i in re_indices:
                link_i = cypher[i+1:]
                cypher = cypher[:i]
                Code_list.append(link_i)
            #print(Code_list)
            val = list(reversed(Code_list))

        except Exception as e:
            val = f'Ошибка функции Enigma: {e}'

        return val

    def ListFracter(ini_list, lim):
        ini_list = list(ini_list)
        lim = int(lim)

        fr_num = int(len(ini_list) / lim) + 1
        index_pair_list = []
        for i in range(1,fr_num + 1):
            if i == 1:
                index_pair_list.append([0,lim])
            else:
                index_pair_list.append([index_pair_list[-1][1], lim * i])

        # собираем результат
        res = []
        for p in index_pair_list:
            p = list(p)
            res.append(ini_list[p[0]:p[1]])

        if res[-1] == []:
            res.pop(-1)

        return res

    def StrDatetimeToExcel (str_datetime):
        try:
            str_datetime = str(str_datetime)
            DateTime = datetime.strptime(str_datetime,'%Y-%m-%d %H:%M:%S')
            temp = dt.datetime(1899,12,30)
            delta = DateTime - temp
            res = float(delta.days) + (float(delta.seconds) / 86400)

        except Exception as e:
            res = f'Ошибка функции DATETIME_TO_EXCEL: {e}'

        return res
    
    def SqlExecuter(qu):
        try:
            def_name = 'SqlExecuter'
            qu = str(qu)

            con_info = {'host': 'vertica-sandbox.s.o3.ru',
                    'port': 5433,
                    'user': 'aramiso',
                    'password': 'Block261!',
                    'database': 'OLAP',
                    'tlsmode': 'disable'}
        
            connection = vertica_python.connect(**con_info)
            cursor = connection.cursor()
            cursor.execute(qu)
            qu_res = cursor.fetchall()
            print(qu_res)
            connection.close()

            df = pd.DataFrame()
            cols_list = [d.name for d in cursor.description]
            for c in cols_list:
                df[c] = []

            for row in qu_res:
                df.loc[len(df)] = row
            
            
            res = df

        except Exception as e:
            res = f'Ошибка функции {def_name}: {e}'

        print(f'Результат функции {def_name}')
        return res
   
if 'ОСНОВНЫЕ' != '':

    def NOSAMO():
        def_name = 'NOSAMO'
        comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
        mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
        cup_dir = CUPS_dir + def_name + '/'
        file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
        
        try:
            # загружаем файлы в датафреймы и определяем функцию для обработки
            bug_files_list = []
            for file in file_list:
                file = str(file)
                file_name = Enigma('/' + file)[-1]
                print(file)
                print(file_name)



                if 'ОПРЕДЕЛЯЕМ МЕТКУ' != '':
                    try:
                        print('----------------------------------ПРОВЕРКА ПО МЕТКЕ ----------------')
                        ##### EXCEL #################
                        if file[-5:] == '.xlsx' or file[-5:] == '.xlsm':
                            # если лист с меткой есть
                            try:
                                df = pd.read_excel(file,sheet_name='LamaMark')
                                mark = df.columns.tolist()[0]
                                print(mark)
                            # если листа с меткой нет
                            except:
                                try:
                                    if 'СКК' != '':
                                        print('проверка на СКК')
                                        try:
                                            df = pd.read_excel(file,sheet_name='Санкт_Петербург_')
                                            mark = 'QCS'
                                        except: mark = '-'
                                        print(mark)

                                    if 'Ж - постинг отчёт' != '':
                                        print('проверка на Ж-постинг отчёт')
                                        if mark == '-':
                                            try:
                                                df = pd.read_excel(file,sheet_name='Выгрузка основной информации по')
                                                mark = 'complaint_posting_rep'
                                            except: mark = '-'
                                            print(mark)

                                    if 'MOJO-файл' != '':
                                        print('проверка на MOJO')
                                        if mark == '-' and pd.read_excel(file).columns.tolist() == pd.read_excel(MojoSample_doc).columns.tolist():
                                            mark = 'mojo'
                                            print(mark)

                                    if 'ВНЕ_ТЯ_SS' != '':
                                        print('проверка на ВНЕ_ТЯ_SS')
                                        if mark == '-' and 'ВНЕ_ТЯ_SS' in file_name:
                                            mark = 'rep_large_size_ss'
                                            print(mark)

                                except: mark = '-'
                        

                        elif file[-4:] == '.csv':
                            df = pd.read_csv(file)
                            if df.columns.tolist()[:3] == ['ArticlePostingID','ArticlePostingType','ArticlePostingName']:
                                mark = 'complaint_posting_rep_csv'
                            else:
                                mark = df.columns.tolist()[-1][9:]

                        else:
                            bug_files_list.append(file_name)
                            mark = '-'

                    except:
                        bug_files_list.append(file_name)
                        mark = '-'

                    print(mark)
                
                
                if 'ЗАПУСКАЕМ ФУНКЦИЮ ПО МЕТКЕ' != '':

                    if mark == 'mf':
                        def_initiator('MIS_MF_LOAD', [file])

                    elif mark == 'complaint_soup':
                        def_initiator('COMPLAINT_SoupToGo', [file])

                    elif mark == 'complaint_posting_rep':
                        def_initiator('COMPLAINT_Soup_Scanit', [file])

                    elif mark == 'complaint_stock':
                        def_initiator('COMPLAINT_Soup_Scanit_Stock', [file])

                    elif mark == 'mojo':
                        def_initiator('MOJO_single_load', [file])

                    elif mark == 'complaint_db_one_query':
                        def_initiator('COMPLAINT_BD_data_load', [file])
                    
                    elif mark == 'rep_large_size_ss':
                        def_initiator('REP_LARGE_SIZE__DataIn', [file])

                    elif mark == 'rep_lirge_size_db_ul_item':
                        def_initiator('REP_LARGE_SIZE__DBUL_Item', [file])

                    elif mark == 'rep_lirge_size_dbul_tag':
                        def_initiator('REP_LARGE_SIZE__DBUL_Tag', [file])

                    elif mark == 'tablo':
                        def_initiator('TABLO_UpdatingRegistrySettings', [file])

                    elif mark == 'QCS':
                        def_initiator('QCS_Updating', [file])

                    elif mark == 'CPL':
                        def_initiator('COMPLAINT_CPLUpdating', [file])

                    elif mark == 'complaint_posting_rep_csv':
                        def_initiator('COMPLAINT_ToWork2', [file])

                    elif mark == 'complaint_man_soup':
                        def_initiator('COMPLAINT_SoupToGoFromMan', [file])

                        


                    


                    




                

            mes_bug_row = ''
            for f in bug_files_list:
                mes_bug_row = f'{mes_bug_row}\n{f}'

            # удаляем папку процесса
            shutil.rmtree(cup_dir)
            

            mes = f'Запущена обработка файлов: {len(file_list) - len(bug_files_list)} из {len(file_list)}\nПроблемные файлы:\n{mes_bug_row}'

        except:
            mes = 'Ошибка! Что-то пошло не так:('

        chat(mes_sender, mes)

    if 'ПАРСИНГ' !='':

        def PARSE__check_files_maker (check_files_list):
            check_files_list = list(check_files_list)
            counter_dir = CUPS_dir + 'PARSE/counter/'
            os.mkdir(counter_dir)
            for el in check_files_list:
                f = open(counter_dir + str(el) + '.txt','w')
                f.close

        def PARSE():
            def_name = 'PARSE'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            par_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            try:
                sn = 'ПЕРЕМЕННЫЕ'
                if sn !='':
                    print(sn)
                    parse_docs_dir = cup_dir + 'parse_docs/'

                sn = 'ОЧИЩАЕМ от повторов полученные элементы для парсинга'
                if sn !='':
                    print(sn)
                    par_list_clean = []
                    for i in par_list:
                        if i not in par_list_clean:
                            par_list_clean.append(i)

                sn = 'СОЗДАЮ чек-файлы для счётчика парсинга и папку для парс-файлов'
                if sn !='':
                    print(sn)
                    PARSE__check_files_maker(par_list_clean)

                    os.mkdir(parse_docs_dir)
                
                sn = 'ПОЛУЧАЮ список ссылок из списка параметров'
                if sn !='':
                    print(sn)

                    now = dt.datetime.now()
                    nazad = timedelta (180)
                    vpered = timedelta (1)
                    date_s_pre = now - nazad
                    date_po_pre = now + vpered
                    date_s = date_s_pre.strftime('%Y-%m-%d')
                    date_po = date_po_pre.strftime('%Y-%m-%d')

                    urls_list = []
                    for x in par_list_clean:
                        url_dt_ii = 'https://wmsa-reports.t.o3.ru/movements_journal/?warehouse=0&date_from=' + date_s + '&date_to=' + date_po + '&search_type=barcode&value=' + x
                        urls_list.append(url_dt_ii)

                sn = 'ИНИЦИИРУЮ ПАРСИН MOJO и сохраняю результат'
                if sn !='':
                    print(sn)

                    PARSE_MOJO_GO(urls_list)

                    # получаем список парс-файлов
                    parse_files_name_list = os.listdir(parse_docs_dir)
                    
                    if len(parse_files_name_list) > 0:
                        
                        files_list = []
                        for i in parse_files_name_list:
                            i = str(i)
                            file_i = parse_docs_dir + i
                            files_list.append(file_i)
                    
                        # создаем df
                        df_list = []
                        for f in files_list:
                            f = str(f)
                            try:
                                df_i = pd.read_csv(f)
                                df_list.append(df_i)
                            except: pass
                       
                        res_df = pd.concat(df_list)
                        res_df = res_df.reset_index()
                        del res_df['index']

                        res_df.to_csv(MOJO_doc, mode='a', index= False , header= False)
                    
            
    

                mes = 'Готово!'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

    if 'СКК' != '':

        def QCS_Updating ():

            def_name = 'QCS_Updating'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            file = str(file_list[0])
            try:
                
                df = pd.read_excel(file, sheet_name='Санкт_Петербург_')
                print(df.head())
                print(df.info())

                # делаем всё текстом
                cols = df.columns.tolist()
                for col in cols:
                    df[col] = df[col].astype(str)
                print(df.info())

                # нормализуем дату фиксации
                def norm_date (row):
                    return row['Дата фиксации ошибки'][:10]
                df['Дата фиксации ошибки'] = df.apply(norm_date, axis=1)
                df['Дата фиксации ошибки'] = pd.to_datetime(df['Дата фиксации ошибки'],format='%d.%m.%Y')

                # добавляем столбец с логином
                def login_col (row):
                    i = row['Нарушитель']
                    login = i[i.find('(') + 1:-1]
                    print(login)
                    return login
                df['login'] = df.apply(login_col, axis=1)
                print(df.head())
                print(df.info())

                # добавляем столбецы сцепки
                print('ДЕЛАЮ сцепку 3')
                df['concat_3'] = df.apply(lambda row: row['Идентификатор объекта нарушения'] + row['Место операции'] + row['login'], axis=1)
                print('ДЕЛАЮ сцепку 4')
                df['concat_4'] = df.apply(lambda row: str(dt.datetime.strptime(str(row['Дата совершения ошибки']) + ':00','%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')) +
                                                    row['Идентификатор объекта нарушения'] + row['Место операции'] + row['login'], axis=1)


                print(df.head())
                print(df.info())
                
                # удаляем старую базу, если есть
                if os.path.isfile(QCS_doc) == True:
                    os.remove(QCS_doc)

                # сохраняем датасет
                df.to_csv(QCS_doc, index= False)
                print(df)

                # характеристики
                col_vals = df['Метод начисления ошибки'].unique().tolist()
                if len(col_vals) == 1:
                    methods = col_vals[0]
                else:
                    methods = ''
                    for i in col_vals:
                        if col_vals.index(i) == 0:
                            methods = methods + i
                        else:
                            methods = methods + ', ' + i
                

                mes = f'''база СКК обновлена:\nстроки: {len(df)}\nдаты: {df['Дата фиксации ошибки'].min().strftime('%d.%m.%Y')} - {df['Дата фиксации ошибки'].max().strftime('%d.%m.%Y')}\nметоды: {methods}'''


            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

    if 'MOJO-база' != '':
        
        def MOJO_single_load():
            def_name = 'MOJO_single_load'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            file = str(file_list[0])
            try:
                file_name = Enigma('/' + file)[-1]
                scanit = Enigma('_'+ file_name)[0]
                print(scanit)
                df = pd.read_excel(file)
                df.to_csv(MOJO_doc, mode='a', index= False , header= False)

                pd.DataFrame({'col1':['ii'], 'col2':[scanit]}).to_csv(MOJO_parse_check_doc, mode='a', index=False, header=False)

                mes = f'Загрузила файл {file_name}'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)
        
        def MOJO_data_frame ():
            MOJO_df = pd.read_csv(MOJO_doc)
            MOJO_df = MOJO_df.drop_duplicates()
            MOJO_df['Время движения'] = pd.to_datetime(MOJO_df['Время движения'], format='%d.%m.%Y %H:%M:%S')
            MOJO_df = MOJO_df.sort_values('Время движения', ascending=False)
            #print(MOJO_df)
            return MOJO_df

    if 'Книга ошибок' != '':
    
        def MIS_MF_Pattern ():
            def_name = 'MIS_MF_Pattern'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                shutil.copy2(MIS_MF_doc, OUT_dir + dt.datetime.now().strftime('%d%H%M%S%f' + ' - Lama_MF_' + ver_name() + '.xlsx'))
                mes = 'Кинула суп-шаблон для жалоб в аут'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def MIS_MF_LOAD ():
            def_name = 'MIS_MF_LOAD'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            try:
                file = str(file_list[0])
                now = dt.datetime.now()
                MF_in_df = pd.read_excel(file, sheet_name='ошибки')
                print(MF_in_df)
                print(MF_in_df.info())
                #MF_in_df['Дата и время совершения ошибки'] = MF_in_df['Дата и время совершения ошибки'].dt.strftime('%d.%m.%Y %H:%M:%S', errors='ignore')

                print(MF_in_df.head(15))
                print(MF_in_df.info())

                # некорректные строки МФ-ки
                error_df = MF_in_df[MF_in_df['проверка'] == 0]
                if len(error_df) > 0:
                    mes = f'В MF-файле есть некорректные строки: {len(error_df)} шт\nТакой файл я забирать не буду, переделывай!'
                else:
                    # определяем INID_df    
                    INID_df = pd.read_csv(MIS_INID_doc).sort_values(by='mis_id')
                    print(INID_df)

                    sn = 'ПОЛУЧАЕМ MF данные с преобразованием по смарт ключам'
                    if sn != '':
                        # фильтруем
                        df = MF_in_df[MF_in_df['проверка'] == 1].iloc[:,:13]
                        print(df.head(15))
                        print(df.info())
                        print('------------------------------------------------Т У Т ------------------')

                        # проверяем буст ключи
                        boost_key_list = []
                        for str_k in Enigma(MIS_boost_keys_list_code):
                            boost_key_list.append(int(str_k))

                        print(boost_key_list)
                        
                        df_norm = df[~df['ключ'].isin(boost_key_list)]
                        df_boost = df[df['ключ'].isin(boost_key_list)]
                        print(df_norm)
                        print(df_boost)
                        if len(df_boost) > 0: 
                            if len(df_norm) > 0:
                                df_list = [df_norm]
                            else: 
                                df_list = []

                            def boost_to_norm (row):
                                print(row)
                                try:
                                    RES_list = []
                                    x = int(Enigma('#' + str(row['Фото']))[0])
                                    print(x)
                                    if x > 59:
                                        x = 59

                                    for col in df_boost.columns.tolist():
                                        tab_col_list = [col]
                                        
                                        if col == 'Дата и время совершения ошибки':
                                            ini_str_dt = str(row[col])
                                            print(ini_str_dt)
                                            boost_dt_col = []
                                            for s in range(1,x+1):
                                                if s < 10:
                                                    s = '0' + str(s)
                                                else:
                                                    s = str(s)

                                                # вставляем вместо минут
                                                boost_dt_col.append(ini_str_dt[:-5] + s + ':00')

                                            tab_col_list = tab_col_list + boost_dt_col


                                        elif col == 'Фото':
                                            ini_com_code = Enigma('#' + str(row[col]))
                                            if len(ini_com_code) == 1:
                                                tab_col_list = tab_col_list + [' '] * x
                                            else:
                                                tab_col_list = tab_col_list + [ini_com_code[1]] * x

                                        else:
                                            tab_col_list = tab_col_list + [row[col]] * x



                                        RES_list.append(tab_col_list)
                                    
                                    print(RES_list)
                                    res_dic = {}
                                    for res_list in RES_list:
                                        res_dic[res_list[0]] = res_list[1:]

                                    print(pd.DataFrame(res_dic))
                                    

                                    df_list.append(pd.DataFrame(res_dic))

                                except:
                                    pass

                            df_boost.apply(boost_to_norm, axis=1)
                            df = pd.concat(df_list)
                            print(df)

                            
                    if 'добавляем столбцы и приводим к виду INID' != '':
                        # создаём столбец дата
                        date_col = [now.strftime('%Y-%m-%d')] * len(df)
                        print(date_col)

                        # создаём столбец дата
                        # определяем номер этой МФ-ки за сегодня
                        INID_mis_is_list = INID_df[INID_df['date'] == now.strftime('%Y-%m-%d')]['mis_id'].tolist()
                        if len(INID_mis_is_list) == 0:
                            MF_today_num = 1
                        else:
                            MF_today_num = int(INID_mis_is_list[-1][15:18]) + 1

                        print(MF_today_num)


                        mis_id_col = []
                        for i in range(1, len(df)+1):
                            mis_id_col.append(now.strftime('%Y%m%d%H%M%S') + '_'+ str(1000 + MF_today_num)[1:] + str(1000000 + i)[1:])
                        
                        # слепляем таблицы и меняем столбцы
                        df.insert(0,'date', date_col)
                        df.insert(0,'mis_id', mis_id_col)
                        df.columns = INID_df.columns.tolist()
                        print(df)
                        df = df.fillna('-')
                        print(df)
                        
                    if 'обрабатываем столбец dt' !='':
                        # в столбце разные типы данных, если строка оставляем строкой, если датетайм делаем строкой
                        dt_new_col_list = []
                        for i in df['dt'].tolist():
                            if isinstance(i, datetime):
                                i_str = i.strftime('%Y-%m-%d %H:%M:%S')
                            else: i_str = str(i)

                            dt_new_col_list.append(i_str)

                        df['dt'] = dt_new_col_list

                        #print(df)
                        print(df.info())

                    if 'вставляем' != '':
                        df.to_csv(MIS_INID_doc, mode='a', index= False , header= False )
                        I_df = pd.read_csv(MIS_INID_doc)
                        print(I_df)
                        print(I_df.info())


                    mes = f'добавила MF-строки: {len(df)}'
            except:
                mes = 'ОШИБКА: неудалось добавить MF-строки :('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def MIS_GO ():
            def_name = 'MIS_GO'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            try:
                # условие запуска
                if os.path.isdir(CUPS_dir + 'PARSE/') == False and os.path.isdir(CUPS_dir + 'MIS_BOOK/') == False and os.path.isdir(CUPS_dir + 'MIS_BOOK_RecalculationLimit/') == False:
                    
                    if 'ПЕРЕМЕННЫЕ' != '':
                        INID_df = pd.read_csv(MIS_INID_doc)
                        #print(INID_df.info())
                        MIBO_df = pd.read_csv(MIS_BOOK_doc)
                        RUCU_df = pd.read_excel(LamaReins_doc, sheet_name='RUCU')
                        MOJO_pach_df = pd.read_csv(MOJO_parse_check_doc)

                    if 'определение GO-логов ' != '':
                        # учитываем только сегодняшние логи
                        INID_df = INID_df[INID_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')]
                        MIS_GO_log_list = INID_df[~INID_df['mis_id'].isin(MIBO_df['mis_id'].tolist())]['mis_id'].tolist()
                        
                        if len(MIS_GO_log_list) == 0:
                            trigger_condition = 'n'
                        else:
                            trigger_condition = 'y'
                            # записываем кол-во ошибок в работе для табло
                            f = open(GUI_TABLO__MisInProgress_doc, 'w')
                            f.write('-------------------------\nошибок в работе: ' + str(len(MIS_GO_log_list)))
                            f.close

                    if 'GO-логи есть!' != '' and trigger_condition == 'y':
                        MIGO_full_df = INID_df[INID_df['mis_id'].isin(MIS_GO_log_list)]
                        print('MIGO всяяяяяяяяяяяяяяяяяяяяя')
                        print(MIGO_full_df)
                        MIGO_rep_df = MIGO_full_df[MIGO_full_df['key'].isin(RUCU_df[RUCU_df['type'] == 'rep']['key'].tolist())]
                        print(MIGO_rep_df)
                        MIGO_parse_df = MIGO_full_df[MIGO_full_df['key'].isin(RUCU_df[RUCU_df['type'] != 'rep']['key'].tolist())]
                        MIGO_parse_done_df = MIGO_parse_df[MIGO_parse_df['object_id'].isin(MOJO_pach_df[MOJO_pach_df['search_type'] == 'ii']['val'].tolist())]
                        print(MIGO_parse_done_df)

                        
                        #MIGO_complete_data_log_list = pd.concat([MIGO_rep_df, MIGO_parse_done_df])['mis_id'].tolist()
                        MIGO_complete_data_log_list = MIGO_rep_df['mis_id'].tolist() + MIGO_parse_done_df['mis_id'].tolist()
                        print(MIGO_complete_data_log_list)
                        MIGO_parse_need_df = MIGO_parse_df[~MIGO_parse_df['mis_id'].isin(MIGO_complete_data_log_list)]
                        print('ТАБЛИЦА которую надо качать')

                        # ЗАПУСКАЕМ просчёт ошибок, по которым есть данные
                        if len(MIGO_complete_data_log_list) > 0:
                            def_initiator('MIS_BOOK', MIGO_complete_data_log_list)
                        
                        # Если есть что парсить запускаем
                        elif len(MIGO_parse_need_df) > 0:
                            def_initiator('PARSE', MIGO_parse_need_df['object_id'].tolist())

                
            except Exception as e:
                mes = 'Ошибка! Что-то пошло не так:('
                chat(mes_sender, mes)
                print(e)

        def MIS_BOOK():
            def_name = 'MIS_BOOK'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            log_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            try:
            
                if 'ПЕРЕМЕННЫЕ' != '':
                    print('ПЕРЕМЕННЫЕЕЕЕЕЕЕЕЕЕ')
                    dupl_check_concat4_str_key_list = Enigma(MIS_BOOK__duplicate_check_4_key_list_code)
                    INID_df = pd.read_csv(MIS_INID_doc)
                    MIGO_df = INID_df[INID_df['mis_id'].isin(log_list)]
                    print(MIGO_df)
                    BOOK_df = pd.read_csv(MIS_BOOK_doc)
                    print('МИС-БУУУУУК в порядкеееееееееееееееееееееееееееееееееееееееееееее')
                    MOJO_df = MOJO_data_frame()
                    print(MOJO_df)
                    print('смотрим MOJOOOO')
                    RUCU_df = pd.read_excel(LamaReins_doc, sheet_name='RUCU')
                    
                    # для повторов в СКК
                    print('для повторов в СКК')
                    if os.path.isfile(QCS_doc) == True:
                        QCS_df = pd.read_csv(QCS_doc)
                        QCS_ConCat3_list = QCS_df['concat_3'].tolist()
                        QCS_ConCat4_list = QCS_df['concat_4'].tolist()
                        QCS_Identifier_list = QCS_df['Идентификатор объекта нарушения'].tolist()
                    else:
                        QCS_ConCat3_list = QCS_ConCat4_list = []
                        QCS_Identifier_list = []

                    # для повторов в Ламе за сегодня
                    BOOK_today_df = BOOK_df[BOOK_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')]
                    BOOK_ConCat3_list = list(BOOK_today_df.apply(lambda row:
                                                            str(row['QCS_object_id']) + 
                                                            str(row['QCS_place']) +
                                                            str(row['QCS_login']),
                                                            axis=1))
                    
                    BOOK_ConCat4_list = list(BOOK_today_df.apply(lambda row:
                                                            str(row['QCS_dt']) + 
                                                            str(row['QCS_object_id']) + 
                                                            str(row['QCS_place']) +
                                                            str(row['QCS_login']),
                                                            axis=1))
                    BOOK_Identifier_list = BOOK_today_df['QCS_object_id'].tolist()

                if 'расчёт СКК-СТОЛБЦОВ' != '':
                    print('пошёл расчёт столбцов!!!')
                    if 'столбцы' !='':
                        QCS_warehouse_id_list = ['QCS_warehouse_id']
                        QCS_dt_list = ['QCS_dt']
                        QCS_process_list = ['QCS_process']
                        QCS_name_list = ['QCS_name']
                        QCS_login_list = ['QCS_login']
                        QCS_method_list = ['QCS_method']
                        QCS_object_id_list = ['QCS_object_id']
                        QCS_place_list = ['QCS_place']
                        QCS_comment_list = ['QCS_comment']

                        COLS_list = [QCS_warehouse_id_list,
                                    QCS_dt_list,
                                    QCS_process_list,
                                    QCS_name_list,
                                    QCS_login_list,
                                    QCS_method_list,
                                    QCS_object_id_list,
                                    QCS_place_list,
                                    QCS_comment_list]
                
                    # функция расчёта
                    def col_culc(row):
                        print('-------------------------РАСЧЁТ СТОЛБЦОВ----------------')

                        sn = 'Определяем MOJO TARO, если требуется'
                        if sn !='':
                            print(sn)
                            MOJO_TARO_code = str(RUCU_df[RUCU_df['key'] == row['key']]['MOJO_TARO_CODE'].tolist()[0])
                            print(MOJO_TARO_code)
                            if MOJO_TARO_code != '-':
                                print('MOJO TARO нужна, определяем')
                                
                                sn = 'определяем время фильтрации'
                                if sn !='':
                                    print(sn)
                                    time_code = Enigma(Enigma(MOJO_TARO_code)[0])[1][1:]
                                    print(time_code)
                                    print('значение ВРЕМЕНИИИИИИ :' + str(row['dt']))
                                    if time_code == 'ALL':
                                        dt_filt = datetime.strptime('2099-12-31 23:59:59', '%Y-%m-%d %H:%M:%S')
                                    elif time_code == 'EXACT':
                                        dt_filt = datetime.strptime(str(row['dt']), '%Y-%m-%d %H:%M:%S')
                                    elif time_code == 'DAY':
                                        dt_filt = datetime.strptime(str(row['dt'])[:10] + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
                                    print(dt_filt)
                                
                                sn = 'отфильтровываем по времени и сканиту'
                                if sn !='':
                                    print(sn)
                                    MJTR_df = pd.DataFrame(MOJO_df[(MOJO_df['Время движения'] <= dt_filt) & (MOJO_df['Экземпляр'] == str(row['object_id']))])
                                    print(MJTR_df)

                                sn = 'отфильтровываем по всем условиям'
                                if sn !='':
                                    print(sn)
                                    for link in Enigma(MOJO_TARO_code)[1:]:
                                        e = Enigma(link)
                                        print(e)
                                        col = e[0]
                                        print(col)
                                        val_code = e[1]
                                        print(val_code)
                                        MJTR_df = MJTR_df[MJTR_df[col].isin(Enigma(val_code))]
                                    print(MJTR_df)
                                

                        for cl in COLS_list:
                            print(f'столбец: {cl}')
                            # ячейка в кубике-рубике
                            rc_code = RUCU_df[RUCU_df['key'] == int(row['key'])][cl[0][4:]].tolist()[0]
                            print(rc_code)

                            # список звеньев
                            link_list = Enigma(rc_code)
                            print(link_list)

                            # заменяем каждое звено на значение и соединяем
                            cell_val = ''
                            for link in link_list:
                                link = str(link)
                                source = link[:4]
                                print(source)
                                entity = link[5:]
                                print(entity)

                                try:
                                    # описываем вырианты источников
                                    if source == 'RUCU':
                                        cell_val_part = entity

                                    elif source == 'MIGO':
                                        cell_val_part = str(row[entity])

                                    elif source == 'MJTR':
                                        cell_val_part = str(MJTR_df[entity].tolist()[0])

                                    print(cell_val_part)

                                    # добавляем в значение ячейки часть
                                    cell_val = cell_val + cell_val_part

                                except:
                                    if len(MJTR_df) == 0:
                                        cell_val = 'нет движения сканита'
                                    else: cell_val = 'ОШИБКА ключа'

                                print(cell_val)

                            
                            # добавляем полученное значение ячейки в столбец
                            cl.append(cell_val)

                    MIGO_df.apply(col_culc, axis=1)
                    df = MIGO_df.copy()
                    print(df)
                    for col in COLS_list:
                        df[col[0]] = col[1:]

                    print(df)
                    print(df.info())

                if 'расчёт СТАТУСА' != '':
                    print('-------------------------------РАСЧЁТ СТАТУСА-------------------------------')
                    # сцепки этой итерации
                    ConCat3_list = []
                    ConCat4_list = []
            
                    def status_culc(row):
                        
                        concat3_i = str(row['QCS_object_id']) + str(row['QCS_place']) + str(row['QCS_login'])
                        print(concat3_i)

                        concat4_i = str(row['QCS_dt']) + str(row['QCS_object_id']) + str(row['QCS_place']) + str(row['QCS_login'])
                        print(concat4_i)

                        if row['key'] == 1000:
                            status =  'база сканитов'
                        
                        elif row['QCS_warehouse_id'][:6] == 'ОШИБКА':
                            status = 'ОШИБКА ключа'

                        elif row['QCS_dt'][0] != '2':
                            status = row['QCS_dt']

                        elif str(row['key']) not in dupl_check_concat4_str_key_list and (concat3_i in ConCat3_list or concat3_i in BOOK_ConCat3_list):
                            status = 'дубль'
                        
                        elif str(row['key']) not in dupl_check_concat4_str_key_list and concat3_i in QCS_ConCat3_list:
                            status =   'дубль по СКК'

                        elif str(row['key']) in dupl_check_concat4_str_key_list and (concat4_i in ConCat4_list or concat4_i in BOOK_ConCat4_list):
                            status = 'дубль'
                        
                        elif str(row['key']) in dupl_check_concat4_str_key_list and concat4_i in QCS_ConCat4_list:
                            status =   'дубль по СКК'

                        elif str(row['QCS_comment']).lower() == 'т кро' and row['QCS_object_id'] in QCS_object_id_list or str(row['QCS_comment']).lower() == 'т кро' and row['QCS_object_id'] in BOOK_Identifier_list:
                            status =   'дубль КРО'

                        elif str(row['QCS_comment']).lower() == 'т кро' and row['QCS_object_id'] in QCS_Identifier_list:
                            status =   'дубль КРО по СКК'

                        elif str(row['key'])[0] == '2' and row['key'] != 2124:
                            print('определение статуса размещения')
                            fix_date_str = str(row['date'])
                            print(fix_date_str)
                            mis_date_str = str(row['QCS_dt'])[:10]
                            print(mis_date_str)
                            if (datetime.strptime(fix_date_str, '%Y-%m-%d') - datetime.strptime(mis_date_str, '%Y-%m-%d')).days > 14:
                                status = 'просрок'
                            else: status = 'к выставлению'

                        else: status = 'к выставлению'

                        ConCat3_list.append(concat3_i)
                        ConCat4_list.append(concat4_i)
                        return status


                    df['status'] = df.apply(status_culc,axis=1)
                    print(df)

                if 'добавляем в КО в док для табло' != '':

                    df.to_csv(MIS_BOOK_doc, mode='a', index= False , header= False)
                    MIS_BOOK_Pivot()

            
                mes = 'Готово!'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            print(mes)

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def MIS_BOOK_Recalculation ():
            def_name = 'MIS_BOOK_Recalculation'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                BOOK_df = pd.read_csv(MIS_BOOK_doc)
                mis_today_list = BOOK_df[BOOK_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')]['mis_id'].tolist()
                #mis_today_list = BOOK_df[BOOK_df['mis_id'][:8] == dt.datetime.now().strftime('%Y%m%d')]['mis_id'].tolist() # берем сегодняшние ошибки по логам
                df = BOOK_df[~BOOK_df['mis_id'].isin(mis_today_list)]
                df.to_csv(MIS_BOOK_doc, index= False)

                # удаляем индикацию ошибок в КО
                MIS_BOOK_Pivot()

                mes = 'КНИГА ОШИБОК за сегодня очищена, скоро начнётся пересчёт'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def MIS_BOOK_RecalculationReparse ():
            def_name = 'MIS_BOOK_RecalculationReparse'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                if 'ПЕРЕМЕННЫЕ' != '':
                    
                    BOOK_df = pd.read_csv(MIS_BOOK_doc)
                    BOOK_df = BOOK_df[BOOK_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')]
                    MOJO_parse_check_df = pd.read_csv(MOJO_parse_check_doc)

                if 'ОПРЕДЕЛЯЕМ то, что не спарсилось и переписываем чек-файл' !='':
                    BOOK_df['filt'] = BOOK_df.apply(lambda row: 1 if row['status'][:3] == 'нет' else 0, axis=1)
                    fail_parse_list = BOOK_df[BOOK_df['filt'] == 1]['object_id'].tolist()
                    print(fail_parse_list)
                    MOJO_parse_check_df = MOJO_parse_check_df[~MOJO_parse_check_df['val'].isin(fail_parse_list)]
                    MOJO_parse_check_df.to_csv(MOJO_parse_check_doc, index= False)

                MIS_BOOK_Recalculation()


                mes = 'Репарс определёт, запускаю пересчёт ошибок'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def MIS_BOOK_OverLimit ():
            def_name = 'MIS_BOOK_OverLimit'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            par_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            par = str(par_list[0])
            try:
                sn = 'ПЕРЕМЕННЫЕ'
                if sn != '':
                    print(sn)
                    BOOK_df = pd.read_csv(MIS_BOOK_doc)
                    INID_df = pd.read_csv(MIS_INID_doc)

                sn = 'ЕСЛИ НЕКОРРЕКТНЫЙ параметр'
                if sn != '':
                    print(sn)
                    if par.lower() != 'on' and par.lower() != 'off':
                        mes = 'Некорректный параметр! Не понимаю, что от меня требуется...'

                sn = 'ON если'
                if sn != '':
                    print(sn)
                    if par.lower() == 'on':
                        
                        sn = 'находим фрагмент КО не за сегодня сверх лимита'
                        if sn !='':
                            print(sn)
                            on_df = BOOK_df.copy()
                            on_df = on_df[(on_df['date'] != dt.datetime.now().strftime('%Y-%m-%d')) &
                                          (on_df['status'] == 'сверх лимита')]
                            print(on_df)

                            if len(on_df) == 0:
                                mes = 'Нечего добовлять'

                            else:
                                sn = 'оставляем только ini столбцы без mis_id и даты, удаляем дубли'
                                if sn !='':
                                    print(sn)
                                    on_df = on_df.iloc[:,2:-10]
                                    on_df = on_df.drop_duplicates()
                                
                                sn = 'добавляем столбец даты сегодняшней и mis_id с префиксом 999'
                                if sn !='':
                                    print(sn)
                                    on_df.insert(0, 'date', [dt.datetime.now().strftime('%Y-%m-%d')] * len(on_df))

                                    mis_id_prefix = dt.datetime.now().strftime('%Y%m%d%H%M%S_')
                                    mis_id_col_list = []
                                    for i in range(1,len(on_df) + 1):
                                        mis_id_col_list.append(mis_id_prefix + str(999000000 + i))

                                    on_df.insert(0, 'mis_id', mis_id_col_list)

                                sn = 'ВСТАВЛЯЕМ и запускаем пересчёт'
                                if sn !='':
                                    print(sn)
                                    on_df.to_csv(MIS_INID_doc, mode='a', index= False , header= False )
                                    def_initiator('MIS_BOOK_Recalculation', ['-'])
                            
                                mes = 'Запускаю пересчёт с новыми-старыми строками'
                
                sn = 'OFF если'
                if sn != '':
                    print(sn)
                    if par.lower() == 'off':
                        
                        sn = 'перезаписываем INID без сегодняшних старых вставок'
                        if sn !='':
                            print(sn)
                            #off_mis_id_list = INID_df[(INID_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')) &
                            #                          (INID_df['mis_id'][-9:-6] == '999')]['mis_id'].tolist()

                            off_mis_id_list = []
                            for mis_id in INID_df[INID_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')]['mis_id'].tolist():
                                mis_id = str(mis_id)
                                if mis_id[-9:-6] == '999':
                                    off_mis_id_list.append(mis_id)

                            if len(off_mis_id_list) == 0:
                                mes = 'Нечего убирать, сегодня такое не добавляли'

                            else:
                                off_df = INID_df[~INID_df['mis_id'].isin(off_mis_id_list)]
                                print(off_df)
                                off_df.to_csv(MIS_INID_doc, index= False)

                                def_initiator('MIS_BOOK_Recalculation', ['-'])
                            
                                mes = 'Запускаю пересчёт только сегодняшних строк'



            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def MIS_BOOK_RecalculationLimit ():
            def_name = 'MIS_BOOK_RecalculationLimit'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            par_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            par = str(par_list[0])
            try:
                sn = 'ПЕРЕМЕННЫЕ'
                if sn != '':
                    print(sn)
                    BOOK_df = pd.read_csv(MIS_BOOK_doc)

                sn = 'ПРОВЕРКА параметра'
                if sn != '':
                    print(sn)
                    try:
                        par = int(par)
                        check_par = 1

                    except:
                        check_par = 0
                        

                if check_par == 0:
                    mes = 'Некорректный параметр!'
                    
                else:

                    sn = 'Получаем список mis_id которые нужно обработать'
                    if sn != '':
                        print(sn)
                        mis_id_list = BOOK_df[(BOOK_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')) &
                                              (BOOK_df['status'] == 'к выставлению') |
                                              (BOOK_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')) &
                                              (BOOK_df['status'] == 'сверх лимита')]['mis_id'].tolist()
                        print(mis_id_list)

                    sn = 'получаем ДФ-ку без статусов, которую надо обработать'
                    if sn !='':
                        print(sn)
                        df = BOOK_df.copy()
                        df = df[df['mis_id'].isin(mis_id_list)].iloc[:,:-1]
                        print(df)
                    
                    sn = 'ПОЛУЧАЕМ НОВЫЕ СТАТУСЫ'
                    if sn !='':
                        print(sn)
                        
                        login_check = []
                        def lim_status_def (row):
                            login_check.append(row['QCS_login'])
                            if login_check.count(row['QCS_login']) > par:
                                return 'сверх лимита'
                            else:
                                return 'к выставлению'
                            
                        df['status'] = df.apply(lim_status_def, axis=1)

                    sn = 'сносим строки, вносим но-новой'
                    if sn !='':
                        print(sn)
                        res_df = BOOK_df[~BOOK_df['mis_id'].isin(mis_id_list)]
                        res_df.to_csv(MIS_BOOK_doc, index= False)
                        df.to_csv(MIS_BOOK_doc, mode='a', index= False , header= False)
                        MIS_BOOK_Pivot()

                    mes = 'Готово!'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def MIS_BOOK_Pivot():
            MIBO_df = pd.read_csv(MIS_BOOK_doc)
            df = MIBO_df[MIBO_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')]
            
            # если ошибок нет
            if len(df) == 0:
                res = 'за сегодня ошибок нет'
            
            # если ошибки есть
            else:
                sn = 'определяем столбец статусов'
                if sn !='':
                    print(sn)
                    status_col_list = list(df['status'].unique())
                    main_status = 'к выставлению'
                    if main_status in status_col_list:
                        status_col_list.remove(main_status)
                    status_col_list.insert(0,main_status)
                    status_col_list.insert(0,'статусы')
                    print(status_col_list)

                sn = 'определяем столбцы результатов и создаём общий ЛИСТ листов'
                if sn !='':
                    print(sn)
                    # определим спецов
                    spec_list = list(df['spec'].unique())
                    #print(spec_list)

                    VAL_COLS_list = []
                    for spec in spec_list:
                        spec= str(spec)
                        i_list = []
                        i_list.append(spec)
                        for status in status_col_list[1:]:
                            status = str(status)
                            df_i = df[(df['spec'] == spec) & (df['status'] == status)]
                            val = str(len(df_i))

                            if status == 'сверх лимита' and val !='0':
                                over_piv = df_i.groupby('QCS_login')['mis_id'].count().reset_index()
                                num = len(over_piv)
                                min_n = over_piv['mis_id'].min()
                                max_n = over_piv['mis_id'].max()
                                med = int(over_piv['mis_id'].median())

                                val = val + ' (' + str(num) + '/' + str(min_n) + '/' + str(max_n) + '/' + str(med) + ')'

                            i_list.append(val)
                        VAL_COLS_list.append(i_list)

                    VAL_COLS_list.insert(0,status_col_list)

                sn = 'собираем сводную таблицу'
                if sn !='':
                    print(sn)
                    # собираем словарь
                    piv_dict = {}
                    for i in VAL_COLS_list:
                        piv_dict[i[0]] = i[1:]

                    piv_df = pd.DataFrame(piv_dict)
                    print(piv_df)

                sn = 'подготавливем данные RES-ЛИСТ листов и размеры ячеек'
                if sn !='':
                    print(sn)
                    columns = piv_df.columns.tolist()

                    data = []
                    def tab_to_lists (row):
                        row_list = []
                        for col in columns:
                            row_list.append(str(row[col]))
                        data.append(row_list)

                    piv_df.apply(tab_to_lists, axis=1)

                    #print(data)

                    # максимальные длины столбцов
                    RES_list = [columns] + data
                    col_lens = []
                    for val_list in [columns] + data:
                        for i in range (len(val_list)):
                            if  len(col_lens) < len(val_list):
                                col_lens.insert(i,len(val_list[i]))
                            else:
                                if len(val_list[i]) > col_lens[i]:
                                    col_lens[i] = len(val_list[i])

                    print(RES_list)
                    print(col_lens)

                sn = 'СОБИРАЕМ РЕЗУЛЬТАТ RES-строку'
                if sn !='':
                    print(sn)
                    res = ''
                    for row in RES_list:
                        for i in range(len(row)):
                            cell = row[i]
                            len_cell = int(col_lens[i])
                            space_num = len_cell  - len(cell)
                            print(space_num)
                            res = res + ' ' * space_num + cell + ' | '
                        res = res + '\n'
                        if RES_list.index(row) == 0:
                            res = res + '=' * (sum(col_lens) + len(col_lens)*3) + '\n'

                    
                    res = res + '\n#########################\n' + 'ВСЕГО К ВЫСТАВЛЕНИЮ - ' + str(len(df[df['status'] == main_status]))
                    print(res)

                sn = 'СНОСИМ индикацию сколько в работе'
                if sn !='':
                    print(sn) 
                    if os.path.isfile(GUI_TABLO__MisInProgress_doc) == True:
                        os.remove(GUI_TABLO__MisInProgress_doc)

            sn = 'ЗАПИСЫВАЕМ результат'
            if sn !='':
                print(sn)
                f = open(GUI_TABLO__MisBookPivot_doc, 'w')
                f.write(res)
                f.close

        def MIS_BOOK_QCS ():
            def_name = 'MIS_BOOK_QCS'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                BOOK_df = pd.read_csv(MIS_BOOK_doc)
                # берём сегодня + к выставлению
                df = BOOK_df[(BOOK_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')) & (BOOK_df['status'] == 'к выставлению')]

                if len(df) == 0:
                    mes = 'за сегодня в КО нет ошибок к выствлению'
                else:
                    if 'ДАННЫЕ ПО СПЕЦАМ' != '':
                        # забираем только нужные слолбцы
                        df = df.iloc[:,14:]
                        print(df)
                        print(df.info())

                        # конвертируем столбец даты времени
                        df['QCS_dt'] = df.apply(lambda row: StrDatetimeToExcel(row['QCS_dt']), axis=1)
                        print(df)
                        print(df.info())

                        # получаем список целевых дф-ок по спецам вида [ спец, целевая df]
                        spec_list = list(df['spec'].unique())
                        print(spec_list)

                        spec_df_list = []
                        for spec in spec_list:
                            spec = str(spec)
                            df_i = df[df['spec'] == spec].iloc[:,1:-1]
                            spec_df_list.append([spec, df_i])

                    if 'РЕЗУЛЬТАТ' !='':
                        # создаём папку результатов
                        res_dir = cup_dir + 'res/'
                        os.mkdir(res_dir)

                        for spec_df in spec_df_list:
                            spec_df = list(spec_df)

                            spec_i = str(spec_df[0])
                            df_i = pd.DataFrame(spec_df[1])
                            print(df_i)

                            # создаём файл
                            file = res_dir + 'СКК - ' + spec_i + '.xlsx'
                            print(file)
                            shutil.copy2(QCS_pattern_doc, file)
                            print(os.listdir(res_dir))

                            # вставляем данные
                            wb = op.load_workbook(file)
                            ws = wb['Штрафы']

                            for r in dataframe_to_rows (df_i, index=False, header=False):
                                ws.append(r)

                            wb.save(file)
                            wb.close()

                    if 'ОТДАЮ' != '':
                        for rf in os.listdir(res_dir):
                            shutil.copy2(res_dir + rf, DocOutPrefix() + rf)


                mes = 'Готово!'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)
        
        def MIS_BOOK_ToExcel ():
            def_name = 'MIS_BOOK_ToExcel'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            par_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            par = str(par_list[0])
            try:
                sn = 'ПЕРЕМЕННЫЕ'
                if sn !='':
                    print(sn)
                    
                    MIBO_df = pd.read_csv(MIS_BOOK_doc)
                    print(par)
                    if par.lower() != 'f': 
                        df = MIBO_df[MIBO_df['date'] == dt.datetime.now().strftime('%Y-%m-%d')]
                    else: df = MIBO_df

                sn = 'РЕЗУЛЬТАТ'
                if sn !='':
                    print(sn)
                    # создаём папку результатов
                    res_dir = cup_dir + 'res/'
                    os.mkdir(res_dir)

                    if len(df) == 0:
                        mes = 'Нет строк по Вашему запросу, отдавать нечего...'
                    else:
                        res_file_body_name = 'ЛАМА__Книга_ошибок.xlsx'
                        df.to_excel(res_dir + res_file_body_name, index=False)

                        # ОТДАЮ
                        for rf in os.listdir(res_dir):
                            shutil.copy2(res_dir + rf, DocOutPrefix() + rf)

                        mes = f'Кинула файл, там {len(df)} строк'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def MIS_BOOK_KillDay ():
            def_name = 'MIS_BOOK_KillDay'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                if 1 == 1:
                    ErrorCheck = 0
                    step = 0
                    step_num = 2

                    def tech_mes(t):
                        print(t)

                sn = 'ПЕРЕМЕННЫЕ'
                if sn != '':
                    try:
                        print(sn)
                        INID_df = pd.read_csv(MIS_INID_doc)
                        BOOK_df = pd.read_csv(MIS_BOOK_doc)
                        today_str = dt.datetime.now().strftime('%Y-%m-%d')

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'

                sn = 'НУЛИМ INID за сегодня'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        #####
                        INID_df = INID_df[INID_df['date'] != today_str]
                        INID_df.to_csv(MIS_INID_doc, index=False)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'

                sn = 'НУЛИМ BOOK за сегодня'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        #####
                        BOOK_df = BOOK_df[BOOK_df['date'] != today_str]
                        BOOK_df.to_csv(MIS_BOOK_doc, index=False)
                        mes = 'Готово!'

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

    if 'Жалобы' != '':
        
        def COMPLAINT_SHARED_SoupToGo (soup_file):
            # суповой список
            soup_list = pd.read_excel(soup_file, sheet_name='суп')['суп'].tolist()
            #print(soup_list)

            # чистый список
            clean_list = []
            for s in soup_list:
                s = str(s)
                if s[:6] in ['Задача', 'Жалоба']:
                    clean_list.append(s)

            # словарь месяцев
            mon_df = pd.read_excel(LamaReins_doc, sheet_name='wms_enums')

            print(clean_list)
            
            # res-таблица
            complaint_id_list = []
            date_list = []
            details_list = []
            posting_list = []
            for i in range(len(clean_list)):
                if clean_list[i][:6] == 'Задача':
                    s1 = str(clean_list[i])
                    if s1.count('№') == 2:
                        index_sym_sec = s1.rfind('№')
                        s1 = s1[:index_sym_sec] + s1[index_sym_sec + 2:]
                    print(s1)
                    s2 = str(clean_list[i+1])
                    print(s2)
                    # id жалобы
                    complaint_id_list.append(s2[9:s2.index('Изменена')])

                    # дата
                    date_soup_str = s2[s2.index('Изменена') + 9:s2.index('в ') - 1]
                    print(date_soup_str)
                    date_list.append(date_soup_str[-4:] + '-' + mon_df[mon_df['eng_name'] == date_soup_str[3:-5]]['rus_name'].tolist()[0][1:] + '-' + date_soup_str[:2])
                    print(date_list[-1])
                    
                    # детализация
                    detail = ''
                    s1_tail = s1[s1.index('ID')+3:]
                    for n in range(len(s1_tail)):
                        if s1_tail[:n].istitle() == True or s1_tail[:n] == ' ':
                            detail = detail + s1_tail[n-1:]
                            break
                    details_list.append(detail)
                    print(details_list[-1])

                    # постинг
                    posting_list.append(s1[s1.index('Постинг')+8:s1.index('Товар ID')])
                    print(posting_list[-1])

            df = pd.DataFrame({'id': complaint_id_list,
                                'dt': date_list,
                                'detail': details_list,
                                'posting': posting_list})
            
            #print(df)
            #print(df.info())

            return df

        def COMPLAINT_scanit_move_analysis (SoupScanit_df: pd.DataFrame, MOVE_df: pd.DataFrame):
            sn = 'проверка таблицы двидений'
            if sn != '':
                try:
                    print(sn)
                    if len(MOVE_df) == 0: # or MOVE_df['ts'].isna().sum() > 0:
                        parse_check_list = pd.read_csv(MOJO_parse_check_doc)['val'].tolist()
                        scanit_list = SoupScanit_df[~SoupScanit_df['scanit'].isin(parse_check_list)]['scanit'].tolist()
                        if os.path.isdir(CUPS_dir + 'PARSE/') == False and os.path.isdir(CUPS_dir + 'MIS_BOOK/') == False and os.path.isdir(CUPS_dir + 'MIS_BOOK_RecalculationLimit/') == False:
                            if len(scanit_list) > 0:
                                def_initiator('PARSE', scanit_list)
                                time.sleep(20)
                                while os.path.isdir(CUPS_dir + 'PARSE/') == True:
                                    time.sleep(10)

                            MOVE_df = MOJO_data_frame()
                            MOVE_df = MOVE_df[['Время движения',
                                               'Экземпляр',
                                               'ItemID',
                                               'Поставка',
                                               'ID склада',
                                               'Тип движения',
                                               'Откуда']]
                            
                            MOVE_df.columns = ['ts',
                                               'scanit',
                                               'item_id',
                                               'supply_id',
                                               'warehouse_id',
                                               'mov',
                                               'cell']
                            
                            
                            for col in ['item_id','supply_id']:
                                MOVE_df[col] = MOVE_df[col].replace('-', '0')
                                MOVE_df[col] = MOVE_df[col].astype('int')

                except Exception as e:
                    ErrorCheck = 1
                    res = f'Ошибка пункта {sn}: {e}'
            
            sn = 'переменные'
            if sn != '':
                try:
                    print(sn)
                    ErrorCheck = 0

                    SoupScanit_df['dt'] = pd.to_datetime(SoupScanit_df['dt'])

                    MOVE_df['ts'] = pd.to_datetime(MOVE_df['ts'], format='%d.%m.%Y %H:%M:%S')
                    MOVE_df = MOVE_df.sort_values('ts', ascending=False)
                    MOVE_df = MOVE_df.reset_index()
                    del MOVE_df['index']

                    type_of_mov_df = pd.read_excel(LamaReins_doc, sheet_name='types_of_mov')
                    return_flow_mov = type_of_mov_df[type_of_mov_df['point'].isin(['возврат БО','возврат WMS'])]['MOJO'].tolist()
                    direct_flow_mov = type_of_mov_df[type_of_mov_df['point'].isin(['приемка','приеморазмещение'])]['MOJO'].tolist()

                    enums_df = pd.read_excel(LamaReins_doc, sheet_name='wms_enums')
                    enums_df = enums_df[(enums_df['service'] == 'wms_storage') & (enums_df['alias'] == 'reason')]

                except Exception as e:
                    ErrorCheck = 1
                    res = f'Ошибка пункта {sn}: {e}'

            sn = 'расшифровка ризона'
            if ErrorCheck == 0:
                try:
                    print(sn)
                    def mov_col_culc(row):
                        enum_id = int(row['reason'])
                        print(enum_id)
                        #time.sleep(10)
                        try:
                            rus_name = enums_df[enums_df['enum_id'] == enum_id]['rus_name'].tolist()[0]
                            eng_name = enums_df[enums_df['enum_id'] == enum_id]['rus_name'].tolist()[0]
                            if rus_name is not None:
                                print(rus_name) 
                                return rus_name
                            else:
                                print(eng_name)
                                return eng_name
                        except:
                            print('-')
                            return '-'
                    
                    #print(MOVE_df.columns.tolist())
                    #print('mov' in MOVE_df.columns.tolist())
                    #time.sleep(120)
                    if 1 == 1: #'mov' in MOVE_df.columns.tolist() == False:
                        print('Расшифровываем ризон')
                        #time.sleep(60)
                        MOVE_df.insert(5,'mov', MOVE_df.apply(mov_col_culc, axis=1))
                        del MOVE_df['reason']
                    else: print('cтолбец mov не нужен')

                    print(MOVE_df)
                    #time.sleep(120)

                except Exception as e:
                    ErrorCheck = 1
                    res = f'Ошибка пункта {sn}: {e}'

            sn = 'анализ движений'
            if ErrorCheck == 0:
                try:
                    print(sn)
                    MOVE_res_df = pd.DataFrame({'scanit':[],
                                                'warehouse_id':[],
                                                'flow':[],
                                                'item':[],
                                                'supply':[],
                                                'cell':[]})
                    def col_culc(row):
                        scanit = str(row['scanit'])
                        print(scanit)
                        try:
                            if 'вводные данные' != '':        
                                dt_str_i = row['dt'] + timedelta(hours=23, minutes=59, seconds=59)
                                print(dt_str_i)
                                df = MOVE_df[(MOVE_df['scanit'] == scanit) & (MOVE_df['ts'] <= dt_str_i)]
                                print(df)
                                df = pd.DataFrame(df)

                                # находим наш подбор
                                selects = df[df['mov'] == 'Подбор']
                                print(selects)
                                select_index = int(min(list(selects.index)))

                                # все движения до нашего подбора
                                movs_pre_sel = df.loc[select_index:, 'mov'].tolist()
                                print(movs_pre_sel)

                            if 'поток' != '':
                                # перебираем и определяем поток
                                for mov in movs_pre_sel:
                                    mov = str(mov)
                                    if mov in return_flow_mov:
                                        flow = 'ВП'
                                        break
                                    elif mov in direct_flow_mov:
                                        flow = 'ПП'
                                        break
                                    else: flow = '-'

                            if 'товар' != '':
                                item = df.loc[select_index, 'item_id']
                                
                                
                            if 'поставка' != '':
                                supply = df.loc[select_index, 'supply_id']
                                

                            if 'склад' != '':
                                warehouse_id = df.loc[select_index, 'warehouse_id']
                                

                            if 'ячейка подбора' != '':
                                cell = df.loc[select_index, 'cell']
                                print(cell)
                        
                        except:
                            flow = item = supply = warehouse_id = cell = None

                        print(item)
                        print(supply)
                        print(warehouse_id)
                        print(cell)


                        # добавляем в таблицу
                        MOVE_res_df.loc[len(MOVE_res_df)] = [scanit, warehouse_id, flow, item, supply, cell]

                    SoupScanit_df.apply(col_culc, axis=1)
                    print(MOVE_res_df)


                except Exception as e:
                    ErrorCheck = 1
                    res = f'Ошибка пункта {sn}: {e}'

            sn = 'РЕЗУЛЬТАТ анализа двидений'
            if ErrorCheck == 0:
                try:
                    print(sn)
                    RES_df = pd.merge(SoupScanit_df, MOVE_res_df, on='scanit', how='left')
                    res = RES_df
                    

                except Exception as e:
                    ErrorCheck = 1
                    res = f'Ошибка пункта {sn}: {e}'

            return res

        def COMPLAINT_SoupPattern ():
            def_name = 'COMPLAINT_SoupPattern'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                shutil.copy2(COMPLAINT__SoupPattern_doc, OUT_dir + dt.datetime.now().strftime('%d%H%M%S%f' + ' - Ж__суп-шаблон.xlsx'))
                mes = 'Кинула суп-шаблон для жалоб в аут'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_ManSoupPattern ():
            def_name = 'COMPLAINT_ManSoupPattern'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                shutil.copy2(COMPLAINT__ManSoupPattern_doc, OUT_dir + dt.datetime.now().strftime('%d%H%M%S%f' + ' - Ж__ручной суп-шаблон.xlsx'))
                mes = 'Кинула ручной суп-шаблон для жалоб в аут'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_SoupToGo ():
            def_name = 'COMPLAINT_SoupToGo'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            file = str(file_list[0])
            
            try:
                df = COMPLAINT_SHARED_SoupToGo(file)

                if 'Удаляем предыдущие результаты операции' != '':
                    del_doc_list = [COMPLAINT__Soup_Scanit_doc, COMPLAINT__Soup_Scanit_Stock_doc]
                    for dd in del_doc_list:
                        if os.path.isfile(dd) == True:
                            os.remove(dd)
                
                df.to_csv(COMPLAINT__SoupToGO_doc, index=False)


                def_initiator('COMPLAINT_PostingQuery', ['-'])
                mes = 'Суп сохранила. Запускаю генерацию постинг-запроса'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_PostingQuery ():
            def_name = 'COMPLAINT_PostingQuery'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                if os.path.isfile(COMPLAINT__SoupToGO_doc) == False:
                    mes = 'Сначала мне нужен жалобный суп'
                else:    
                    posting_list = pd.read_csv(COMPLAINT__SoupToGO_doc)['posting'].tolist()
                    query = ''
                    for p in posting_list:
                        query = query + str(p) + ','
                    f = open(DocOutPrefix() + 'Ж__постинг_запрос.txt','w')
                    f.write(query[:-1])
                    f.close

                    webbrowser.open(url_posting_max, new=2)

                    mes = 'Лови!'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_Soup_Scanit ():
            def_name = 'COMPLAINT_Soup_Scanit'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                if os.path.isfile(COMPLAINT__SoupToGO_doc) == False:
                    mes = 'Нет файла Суп с собой'
                else:
                    SoupToGO_df = pd.read_csv(COMPLAINT__SoupToGO_doc)

                    file = str(pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()[0])
                    df = pd.read_excel(file)
                    print(df)
                    df = df[['Наименование постинга','ШК экземпляра']]
                    df = df.iloc[:-1,:]
                    df.columns = ['posting', 'scanit']
                    df = df.drop_duplicates(subset='posting')
                    df['posting'] = df['posting'].fillna('-')
                    print(df)

                    res_df = SoupToGO_df.merge(df, how='left', on='posting')
                    print(res_df)

                    res_df.to_csv(COMPLAINT__Soup_Scanit_doc, index=False)

                    def_initiator('COMPLAINT_DBQuery', ['-'])
                    mes = 'Сканиты притянула. Запустила генерацию бобр-запроса'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_DBQuery ():
            def_name = 'COMPLAINT_DBQuery'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                if os.path.isfile(COMPLAINT__Soup_Scanit_doc) == False:
                    mes = 'Нет файла Суп-Сканит'
                else:
                    scanit_list = pd.read_csv(COMPLAINT__Soup_Scanit_doc)['scanit'].tolist()
                    q_body = ''
                    for sc in scanit_list:
                        q_body = q_body + "'" + str(sc) + "',"
                    q_body = q_body[:-1]

                    q_head = open(COM_dir + 'SQL___One_Query_1.txt').read()
                    q_tail = open(COM_dir + 'SQL___One_Query_2.txt').read()

                    query = q_head + q_body + q_tail

                    f = open(DocOutPrefix() + ' - Ж бобёр запрос.txt','w')
                    f.write(query)
                    f.close

                    

                    mes = 'Лови! Бобру привет передавай'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_Soup_Scanit_Stock ():
            def_name = 'COMPLAINT_Soup_Scanit_Stock'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                if os.path.isfile(COMPLAINT__Soup_Scanit_doc) == False:
                    mes = 'Нет файла Суп-сканит'
                else:
                    SS_df = pd.read_csv(COMPLAINT__Soup_Scanit_doc)

                    file = str(pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()[0])
                    df = pd.read_csv(file)[['scanit','ean','stock']]
                    print(df)

                    res_df = SS_df.merge(df, how='left', on='scanit')
                    res_df['stock'] = res_df['stock'].fillna('-')
                    res_df['ean'] = res_df['ean'].fillna('-')
                    print(res_df)

                    res_df.to_csv(COMPLAINT__Soup_Scanit_Stock_doc, index=False)
                    res_df[['scanit']].to_excel(DocOutPrefix() + 'сканиты для жалоб.xlsx', index=False)
                
                    mes = 'Готово! Для следующего шага должна быть донолнена MOJO-база'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_BD_data_load ():
            def_name = 'COMPLAINT_BD_data_load'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                file = str(pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()[0])
                shutil.copy2(file, COMPLAINT__Dbeaver_doc)
                def_initiator('COMPLAINT_ToWork', ['-'])
                mes = 'Данные по жалобам от бобра сохнанила, запускаю генерацию файла Ж в работу'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_ToWork():
            def_name = 'COMPLAINT_ToWork'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            try:
                print('##############################################################################')
                print('################################  ЖАЛОБЫ В РАБОТУ  ###########################')
                print('##############################################################################')
                # условие запуска наличие суп-сканит файла  
                print('проверка условий запуска')  
                if os.path.isfile(COMPLAINT__Soup_Scanit_doc) == False:
                    mes = 'Скачала надо сформировать Суп-сканит'
                else:

                    sn = 'ПЕРЕМЕННЫЕ'
                    if sn != '':
                        print(sn)
                        C_df = pd.read_csv(COMPLAINT__Soup_Scanit_doc)
                        C_df['dt'] = pd.to_datetime(C_df['dt'], format='%Y-%m-%d')
                        print(C_df)
                        print(C_df.info())

                    sn = 'ОПРЕДЕЛЯЕМ БОБРОВУЮ df'
                    if sn != '':
                        print(sn)
                        # исходная таблица
                        if os.path.isfile(COMPLAINT__Dbeaver_doc) == False:
                            DB_df = pd.DataFrame({})
                        else:
                            ini_df = pd.read_csv(COMPLAINT__Dbeaver_doc)
                            print(ini_df)

                            if 'определяем DB_df движений' != '':
                                mov_df = ini_df.iloc[:,:-3]
                                # удаляем пустые строки
                                mov_df = mov_df.dropna(how='all')
                                # заменяем пропуски в стоке на 0
                                mov_df['stock'] = mov_df['stock'].fillna(0)
                                print(mov_df)
                                
                                #расшифровываем ризон
                                enums_df = pd.read_excel(LamaReins_doc, sheet_name='wms_enums')
                                enums_df = enums_df[(enums_df['service'] == 'wms_storage') & (enums_df['alias'] == 'reason')]
                                #print(enums_df)

                                def mov_col_culc(row):
                                    enum_id = int(row['reason'])
                                    print(enum_id)
                                    try:
                                        rus_name = enums_df[enums_df['enum_id'] == enum_id]['rus_name'].tolist()[0]
                                        eng_name = enums_df[enums_df['enum_id'] == enum_id]['rus_name'].tolist()[0]
                                        #print(rus_name)
                                        #print(eng_name)
                                        if rus_name is not None:
                                            return rus_name
                                        else: return eng_name
                                    except:
                                        return '-'

                                mov_df.insert(4,'mov', mov_df.apply(mov_col_culc, axis=1))
                                del mov_df['reason']
                                print(mov_df)

                            if 'определяем таблицу с EANами' != '':
                                ean_df = ini_df.iloc[:,-3:-1]
                                ean_df = ean_df.dropna(how='all')
                                
                                def concat_ean_col_culc(row):
                                    item_ean = int(row['item_ean'])
                                    ean_list = ean_df[ean_df['item_ean'] == item_ean]['ean'].tolist()
                                    ean_full = ''
                                    for e in ean_list:
                                        e = str(e)
                                        if len(ean_full) > 0:
                                            ean_full = ean_full + ' | ' + e
                                        else:
                                            ean_full = ean_full + e

                                    return ean_full
                                
                                ean_df['ean'] = ean_df.apply(concat_ean_col_culc,axis=1)
                                ean_df = ean_df.drop_duplicates()
                                ean_df.columns = ['item_id','ean']
                                
                            DB_df = mov_df.merge(ean_df, on='item_id')
                            DB_df['ts'] = pd.to_datetime(DB_df['ts'], format='%d.%m.%Y %H:%M:%S')
                            DB_df = DB_df.sort_values('ts', ascending=False)
                            DB_df = DB_df.reset_index()
                            del DB_df['index']
                                

                            print(DB_df)
                            print(DB_df.info())

                    if 'определяем будующие столбцы' != '':
                        # если выгрузки бобра
                        if len(DB_df) == 0:
                            flow_list = ['return'] + ['-'] * len(C_df)
                            item_list = ['item'] + ['-'] * len(C_df)
                            supply_list = ['supply'] + ['-'] * len(C_df)
                            cell_list = ['cell'] + ['-'] * len(C_df)
                            ean_list = ['ean'] + ['-'] * len(C_df)
                            stock_list = ['stock'] + ['-'] * len(C_df)
                            category_list = ['category'] + ['B'] * len(C_df)
                            link_list = ['link'] + ['-'] * len(C_df)

                        # если есть, то считаем
                        else:
                            flow_list = ['flow']
                            item_list = ['item']
                            supply_list = ['supply']
                            cell_list = ['cell']
                            ean_list = ['ean']
                            stock_list = ['stock']
                            category_list = ['category']
                            link_list = ['link']


                            if 'РАСЧЁТ СТОЛБЦОВ' != '':
                                type_of_mov_df = pd.read_excel(LamaReins_doc, sheet_name='types_of_mov')
                                return_flow_mov = type_of_mov_df[type_of_mov_df['point'].isin(['возврат БО','возврат WMS'])]['MOJO'].tolist()
                                direct_flow_mov = type_of_mov_df[type_of_mov_df['point'].isin(['приемка','приеморазмещение'])]['MOJO'].tolist()
                                
                                def col_culc(row):
                                    try:
                                        if 'вводные данные' != '':
                                            scanit = str(row['scanit'])
                                            print(scanit)
                                            dt_str_i = row['dt'] + timedelta(hours=23, minutes=59, seconds=59)
                                            df = DB_df[(DB_df['scanit'] == scanit) & (DB_df['ts'] <= dt_str_i)]
                                            print(df)
                                            df = pd.DataFrame(df)

                                            # находим наш подбор
                                            selects = df[df['mov'] == 'Подбор']
                                            print(selects)
                                            select_index = int(min(list(selects.index)))

                                            # все движения до нашего подбора
                                            movs_pre_sel = df.loc[select_index:, 'mov'].tolist()
                                            print(movs_pre_sel)

                                        if 'поток' != '':
                                            # перебираем и определяем поток
                                            for mov in movs_pre_sel:
                                                mov = str(mov)
                                                if mov in return_flow_mov:
                                                    flow = 'ВП'
                                                    break
                                                elif mov in direct_flow_mov:
                                                    flow = 'ПП'
                                                    break
                                                else: flow = '-'

                                        if 'товар' != '':
                                            item = df.loc[select_index, 'item_id']
                                            
                                        if 'поставка' != '':
                                            supply = df.loc[select_index, 'supply_id']
                                            
                                        if 'ячейка подбора' != '':
                                            cell = df.loc[select_index, 'cell']

                                        if 'EAN' != '':
                                            ean = df.loc[select_index, 'ean']

                                        if 'сток' != '':
                                            stock = df.loc[select_index, 'stock']
                                            
                                        if 'категория' != '':
                                            try:
                                                if flow == '-':
                                                    cat = 'B'

                                                elif flow == 'ВП':
                                                    cat = 'C'

                                                elif flow == 'ПП':
                                                    if str(stock) == '0.0':
                                                        cat = 'D'
                                                    else: cat = 'A'
                                                
                                                else: cat = 'error'

                                            except:
                                                cat = 'B'

                                        if 'ссылка' != '':
                                            link = 'https://crm.o3team.ru/complaints/complaint/' + str(row['id']) + '/tickets'

                                    except:
                                        flow = item = supply = cell = ean = stock = '-'
                                        link = 'https://crm.o3team.ru/complaints/complaint/' + str(row['id']) + '/tickets'
                                        cat = 'B'

                                    flow_list.append(flow)
                                    print(flow)
                                    item_list.append(item)
                                    print(item)
                                    supply_list.append(supply)
                                    print(supply)
                                    cell_list.append(cell)
                                    print(cell)
                                    ean_list.append(ean)
                                    print(ean)
                                    stock_list.append(stock)
                                    print(stock)
                                    category_list.append(cat)
                                    print(cat)
                                    link_list.append(link)
                                    print(link)


                                C_df.apply(col_culc, axis=1)

                        if 'собираем результат' != '':
                            COLS_list = [flow_list,
                                        item_list,
                                        supply_list,
                                        cell_list,
                                        ean_list,
                                        stock_list,
                                        category_list,
                                        link_list]

                            for col in COLS_list:
                                C_df[col[0]] = col[1:]

                            print(C_df)

                    if 'РЕЗУЛЬТАТ' != '':
                        C_df.to_excel(DocOutPrefix() + 'Ж в работу.xlsx', index=False)

                    mes = 'Лови!'

            except Exception as e:
                print(e)
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_ToWork2 ():
            def_name = 'COMPLAINT_ToWork2'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            file = str(file_list[0])
            try:
                
                if 1 == 1:
                    ErrorCheck = 0
                    step = 0
                    step_num = 11

                    def tech_mes(t):
                        print(t)

                sn = 'ПЕРЕМЕННЫЕ'
                if sn != '':
                    try:
                        print(sn)
                        FULL_df = pd.read_csv(COMPLAINT__SoupToGO_doc)
                        posting_rep_df = pd.read_csv(file)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'

                sn = 'СКАНИТЫ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        posting_rep_df = posting_rep_df[['ArticlePostingName', 'Barcode']].copy()
                        posting_rep_df.columns = ['posting', 'scanit']
                        print(posting_rep_df)
                        posting_rep_df['dupl'] = posting_rep_df.apply(lambda row: posting_rep_df['posting'].tolist().count(row['posting']), axis=1)
                        print(posting_rep_df)
                        posting_rep_df = posting_rep_df[posting_rep_df['dupl'] == 1][['posting','scanit']]

                        FULL_df = pd.merge(FULL_df, posting_rep_df, on='posting', how='left')
                        FULL_df['scanit'] = FULL_df['scanit'].fillna('-')
                        
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'ДВИЖЕНИЯ СКАНИТОВ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        scanit_list = FULL_df['scanit'].tolist()
                        q_body = ''
                        for sc in scanit_list:
                            q_body = q_body + "'" + str(sc) + "',"
                        q_body = q_body[:-1]

                        q_head = open(COM_dir + 'SQL_Query___ScanitMove_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___ScanitMove_2.txt').read()

                        query = q_head + q_body + q_tail
                        if  os.path.isfile(D_dir + 'ScanitMove.csv') == False:
                            SqlExecuter(query).to_csv(D_dir + 'ScanitMove.csv', index=False)
                        
                        MOVE_df = pd.read_csv(D_dir + 'ScanitMove.csv')

                        print(MOVE_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'АНАЛИЗ ДВИЖЕНИЙ СКАНИТОВ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        FULL_df = COMPLAINT_scanit_move_analysis(FULL_df, MOVE_df)
                        print('Ответ функции получен')
                        print(FULL_df)
                        print(FULL_df.info())

                        FULL_val_rows_df = FULL_df[(FULL_df['item'] > 0) &
                                                   (FULL_df['supply'] > 0) &
                                                   (FULL_df['warehouse_id'] > 0)]

                        #FULL_val_rows_df = FULL_df.copy()
                        #FULL_val_rows_df = FULL_val_rows_df.dropna()
                        
                        #for col in ['item','supply','warehouse_id']:
                        #    FULL_val_rows_df = FULL_val_rows_df[FULL_val_rows_df[col] != '-']
                        #    print('замена прочерка')
                        #    FULL_val_rows_df[col] = FULL_val_rows_df[col].astype('int')
                        #    print('смена формата')
                        
                        #FULL_val_rows_df = FULL_val_rows_df[(FULL_val_rows_df['item'] > 0) &
                        #                                    (FULL_val_rows_df['supply'] > 0) &
                        #                                    (FULL_val_rows_df['warehouse_id'] > 0)]
                        
                        print(FULL_df)
                        print(FULL_df.info())
                        print(FULL_val_rows_df)
                        print(FULL_val_rows_df.info())

                        where_list = []
                        def stock_query(row):
                            where_list.append('(ItemId = ' + str(int(row['item'])) + ' AND SupplyId = ' + str(int(row['supply'])) + ' AND WarehouseId = ' + str(int(row['warehouse_id'])) + ') OR')
                        
                        FULL_val_rows_df.apply(stock_query, axis=1)
                        q_body = ''
                        for i in where_list:
                            i = str(i)
                            q_body = q_body + i + '\n'
                        
                        q_body = q_body[:-3]
                        print(q_body)

                        q_head = open(COM_dir + 'SQL_Query___AllStock_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___AllStock_2.txt').read()
                        query = q_head + q_body + q_tail
                        print(query)
                        f = open('1.txt', 'w')
                        f.write(query)
                        f.close()
                        

                        if  os.path.isfile(D_dir + 'AllStock.csv') == False:
                            print('Запускаю запрос')
                            SqlExecuter(query).to_csv(D_dir + 'AllStock.csv', index=False)
                        
                        stock_df = pd.read_csv(D_dir + 'AllStock.csv')
                        print(stock_df)
                        print(stock_df.info())

                        FULL_df = pd.merge(FULL_df, stock_df, on=['item', 'supply', 'warehouse_id'], how='left')
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'КАТЕГОРИЯ ЖАЛОБЫ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        FULL_df['stock'] = FULL_df['stock'].fillna(0)
                        def comp_cat(row):
                            try:
                                if row['flow'] == '-' or row['flow'] is None:
                                    return 'B'
                                elif row['flow'] == 'ВП':
                                    return 'C'

                                elif row['flow'] == 'ПП':
                                    if row['stock'] == 0:
                                        return 'D'
                                    else: return 'A'
                                                                        
                                else: return 'error'

                            except:
                                return 'B'
                            
                        FULL_df['category'] = FULL_df.apply(comp_cat, axis=1)
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'ОБРАБОТКА ОСТАТКОВ категории В'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        FULL_df['stock'] = FULL_df.apply(lambda row: '-' if row['category'] == 'B' else row['stock'], axis=1)
                        #FULL_df = FULL_df.fillna('-')

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'ССЫЛКА'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        FULL_df['link'] = FULL_df.apply(lambda row: 'https://crm.o3team.ru/complaints/complaint/' + str(row['id']) + '/tickets', axis=1)
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'НАЗВАНИЯ СКЛАДОВ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        warehouse_id_list = FULL_df[FULL_df['warehouse_id'] > 0]['warehouse_id'].tolist()
                        q_body = str(warehouse_id_list)[1:-1]

                        q_head = open(COM_dir + 'SQL_Query___Warehouse_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___Warehouse_2.txt').read()

                        query = q_head + q_body + q_tail
                        warehouse_df = SqlExecuter(query)
                        print(warehouse_df)
                        print(warehouse_df.info())
                        print(FULL_df)
                        print(FULL_df.info())
                        #FULL_df['warehouse_id'] = FULL_df['warehouse_id'].astype('int')
                        FULL_df = pd.merge(FULL_df, warehouse_df, on='warehouse_id', how='left')
                        FULL_df['warehouse'] = FULL_df['warehouse'].fillna('-')
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'EAN'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        item_list = FULL_df[FULL_df['item'] > 0]['item'].astype('int').tolist()
                        q_body = str(item_list)[1:-1]

                        q_head = open(COM_dir + 'SQL_Query___Ean_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___Ean_2.txt').read()

                        query = q_head + q_body + q_tail
                        print(query)
                        ean_df = SqlExecuter(query)
                        print(ean_df)

                        def concat_ean_col_culc(row):
                            item = int(row['item'])
                            ean_list = ean_df[ean_df['item'] == item]['ean'].tolist()
                            ean_full = ''
                            for e in ean_list:
                                e = str(e)
                                if len(ean_full) > 0:
                                    ean_full = ean_full + ' | ' + e
                                else:
                                    ean_full = ean_full + e

                            return ean_full
                        
                        ean_df['ean'] = ean_df.apply(concat_ean_col_culc,axis=1)
                        ean_df = ean_df.drop_duplicates()

                        print(ean_df)
                        print(ean_df.info())

                        FULL_df = pd.merge(FULL_df, ean_df, on='item', how='left')
                        FULL_df['ean'] = FULL_df['ean'].fillna('-')
                        print(FULL_df)
                        print(FULL_df.info())

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'КАТЕГОРИИ ТОВАРОВ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        q_head = open(COM_dir + 'SQL_Query___ItemCategory_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___ItemCategory_2.txt').read()

                        query = q_head + q_body + q_tail
                        desc_df = SqlExecuter(query)
                        print(desc_df)

                        FULL_df = pd.merge(FULL_df, desc_df, on='item', how='left')

                        for i in range(1,5):
                            col = 'desc' + str(i)
                            FULL_df[col] = FULL_df[col].fillna('-')

                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'КОММЕНТАРИЙ ВОЗВРАТА'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        posting_list = FULL_df['posting'].tolist()
                        q_body = str(posting_list)[1:-1]

                        q_head = open(COM_dir + 'SQL_Query___ReturnComment_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___ReturnComment_2.txt').read()

                        query = q_head + q_body + q_tail
                        return_comment_df = SqlExecuter(query)
                        print(return_comment_df)

                        FULL_df = pd.merge(FULL_df, return_comment_df, on='posting', how='left')
                        FULL_df['comment'] = FULL_df['comment'].fillna('-')
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'ФАЙЛ В РАБОТУ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        RES_df = FULL_df[['dt',
                                          'id',
                                          'warehouse_id',
                                          'warehouse',
                                          'detail',
                                          'posting',
                                          'scanit',
                                          'item',
                                          'supply',
                                          'ean',
                                          'desc1',
                                          'desc2',
                                          'desc3',
                                          'desc4',
                                          'cell',
                                          'flow',
                                          'stock',
                                          'category',
                                          'link',
                                          'comment']]

                        RES_df.to_excel(DocOutPrefix() + 'Ж в работу.xlsx', index=False)
                        mes = 'Лови!'

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)
                

            except:
                mes = 'Ошибка! Что-то пошло не так:('


            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_SoupToGoFromMan ():
            def_name = 'COMPLAINT_SoupToGoFromMan'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            file = str(file_list[0])
            try:
                df = pd.read_excel(file, sheet_name='жалобы')

                if 'Удаляем предыдущие результаты операции' != '':
                    del_doc_list = [COMPLAINT__Soup_Scanit_doc, COMPLAINT__Soup_Scanit_Stock_doc]
                    for dd in del_doc_list:
                        if os.path.isfile(dd) == True:
                            os.remove(dd)
                
                df.to_csv(COMPLAINT__SoupToGO_doc, index=False)


                def_initiator('COMPLAINT_PostingQuery', ['-'])
                mes = 'Суп сохранила. Запускаю генерацию постинг-запроса'

            except:
                mes = 'Ошибка! Что-то пошло не так:('

            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

        def COMPLAINT_Flow ():
            def_name = 'COMPLAINT_Flow'
            comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
            cup_dir = CUPS_dir + def_name + '/'
            par_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
            par = str(par_list[0])
            try:
                
                if 1 == 1:
                    ErrorCheck = 0
                    step = 0
                    step_num = 11

                    def tech_mes(t):
                        print(t)

                sn = 'ПЕРЕМЕННЫЕ'
                if sn != '':
                    try:
                        print(sn)
                        now = dt.datetime.now()
                        mon_df = pd.read_excel(LamaReins_doc, sheet_name='wms_enums')
                        par_list = Enigma('#' + par)
                        mon_str = mon_df[mon_df['eng_name'] == Enigma(' ' + par_list[0])[1]] \
                                                               ['rus_name'].tolist()[0][1:]
                        RES_dict = {}
                        MOVE_doc = D_dir + 'ScanitMove.csv'

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'

                sn = 'ТС - ПОСТИНГ - СКАНИТ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        RES_dict['id'] = now.strftime('%Y%m%d_%H%M%S')
                        RES_dict['date'] = now.strftime('%Y-%m-%d')
                        RES_dict['dt'] = par_list[0][-4:] + '-' + mon_str + '-' + par_list[0][:2]
                        RES_dict['posting'] = par_list[1]
                        RES_dict['scanit'] = par_list[2]

                        print(RES_dict)
                        print(RES_dict['scanit'])
                        mes = f'Пункт {sn} выполнен'

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'ДВИЖЕНИЯ СКАНИТОВ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        # получаем движения сканита
                        '''
                        q_body = ''
                        for sc in scanit_list:
                            q_body = q_body + "'" + str(sc) + "',"
                        q_body = q_body[:-1]
                        '''
                        q_body = "'" + RES_dict['scanit'] + "'"

                        q_head = open(COM_dir + 'SQL_Query___ScanitMove_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___ScanitMove_2.txt').read()

                        query = q_head + q_body + q_tail



                        r = SqlExecuter(query)
                        print(r)

                        #f = open('sc.txt', 'w')
                        #f.write(query)
                        #f.close()
                        query_res_df = SqlExecuter(query)

                        
                        if  os.path.isfile(MOVE_doc) == False:
                            query_res_df.to_csv(MOVE_doc, index=False)
                        else:
                            query_res_df.to_csv(MOVE_doc, mode='a', index= False , header= False)
                        
                        MOVE_df = pd.read_csv(MOVE_doc)
                        MOVE_df = MOVE_df.drop_duplicates()
                        MOVE_df.to_csv(MOVE_doc, index=False)

                        print(MOVE_df)
                        

                        mes = f'Пункт {sn} выполнен'

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                
                sn = 'АНАЛИЗ ДВИЖЕНИЙ СКАНИТОВ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        FULL_df = COMPLAINT_scanit_move_analysis(pd.DataFrame(RES_dict), MOVE_df)
                        print('Ответ функции получен')
                        print(FULL_df)
                        print(FULL_df.info())

                        FULL_val_rows_df = FULL_df[(FULL_df['item'] > 0) &
                                                    (FULL_df['supply'] > 0) &
                                                    (FULL_df['warehouse_id'] > 0)]

                        #FULL_val_rows_df = FULL_df.copy()
                        #FULL_val_rows_df = FULL_val_rows_df.dropna()
                        
                        #for col in ['item','supply','warehouse_id']:
                        #    FULL_val_rows_df = FULL_val_rows_df[FULL_val_rows_df[col] != '-']
                        #    print('замена прочерка')
                        #    FULL_val_rows_df[col] = FULL_val_rows_df[col].astype('int')
                        #    print('смена формата')
                        
                        #FULL_val_rows_df = FULL_val_rows_df[(FULL_val_rows_df['item'] > 0) &
                        #                                    (FULL_val_rows_df['supply'] > 0) &
                        #                                    (FULL_val_rows_df['warehouse_id'] > 0)]
                        
                        print(FULL_df)
                        print(FULL_df.info())
                        print(FULL_val_rows_df)
                        print(FULL_val_rows_df.info())

                        where_list = []
                        def stock_query(row):
                            where_list.append('(ItemId = ' + str(int(row['item'])) + ' AND SupplyId = ' + str(int(row['supply'])) + ' AND WarehouseId = ' + str(int(row['warehouse_id'])) + ') OR')
                        
                        FULL_val_rows_df.apply(stock_query, axis=1)
                        q_body = ''
                        for i in where_list:
                            i = str(i)
                            q_body = q_body + i + '\n'
                        
                        q_body = q_body[:-3]
                        print(q_body)

                        q_head = open(COM_dir + 'SQL_Query___AllStock_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___AllStock_2.txt').read()
                        query = q_head + q_body + q_tail
                        print(query)
                        f = open('1.txt', 'w')
                        f.write(query)
                        f.close()
                        

                        if  os.path.isfile(D_dir + 'AllStock.csv') == False:
                            print('Запускаю запрос')
                            SqlExecuter(query).to_csv(D_dir + 'AllStock.csv', index=False)
                        
                        stock_df = pd.read_csv(D_dir + 'AllStock.csv')
                        print(stock_df)
                        print(stock_df.info())

                        FULL_df = pd.merge(FULL_df, stock_df, on=['item', 'supply', 'warehouse_id'], how='left')
                        print(FULL_df)
                        
                        ######################################
                        mes = f'Пункт {sn} выполнен'

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                '''
                sn = 'КАТЕГОРИЯ ЖАЛОБЫ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        FULL_df['stock'] = FULL_df['stock'].fillna(0)
                        def comp_cat(row):
                            try:
                                if row['flow'] == '-' or row['flow'] is None:
                                    return 'B'
                                elif row['flow'] == 'ВП':
                                    return 'C'

                                elif row['flow'] == 'ПП':
                                    if row['stock'] == 0:
                                        return 'D'
                                    else: return 'A'
                                                                        
                                else: return 'error'

                            except:
                                return 'B'
                            
                        FULL_df['category'] = FULL_df.apply(comp_cat, axis=1)
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'ОБРАБОТКА ОСТАТКОВ категории В'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        FULL_df['stock'] = FULL_df.apply(lambda row: '-' if row['category'] == 'B' else row['stock'], axis=1)
                        #FULL_df = FULL_df.fillna('-')

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'ССЫЛКА'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        FULL_df['link'] = FULL_df.apply(lambda row: 'https://crm.o3team.ru/complaints/complaint/' + str(row['id']) + '/tickets', axis=1)
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'НАЗВАНИЯ СКЛАДОВ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        warehouse_id_list = FULL_df[FULL_df['warehouse_id'] > 0]['warehouse_id'].tolist()
                        q_body = str(warehouse_id_list)[1:-1]

                        q_head = open(COM_dir + 'SQL_Query___Warehouse_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___Warehouse_2.txt').read()

                        query = q_head + q_body + q_tail
                        warehouse_df = SqlExecuter(query)
                        print(warehouse_df)
                        print(warehouse_df.info())
                        print(FULL_df)
                        print(FULL_df.info())
                        #FULL_df['warehouse_id'] = FULL_df['warehouse_id'].astype('int')
                        FULL_df = pd.merge(FULL_df, warehouse_df, on='warehouse_id', how='left')
                        FULL_df['warehouse'] = FULL_df['warehouse'].fillna('-')
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'EAN'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        item_list = FULL_df[FULL_df['item'] > 0]['item'].astype('int').tolist()
                        q_body = str(item_list)[1:-1]

                        q_head = open(COM_dir + 'SQL_Query___Ean_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___Ean_2.txt').read()

                        query = q_head + q_body + q_tail
                        print(query)
                        ean_df = SqlExecuter(query)
                        print(ean_df)

                        def concat_ean_col_culc(row):
                            item = int(row['item'])
                            ean_list = ean_df[ean_df['item'] == item]['ean'].tolist()
                            ean_full = ''
                            for e in ean_list:
                                e = str(e)
                                if len(ean_full) > 0:
                                    ean_full = ean_full + ' | ' + e
                                else:
                                    ean_full = ean_full + e

                            return ean_full
                        
                        ean_df['ean'] = ean_df.apply(concat_ean_col_culc,axis=1)
                        ean_df = ean_df.drop_duplicates()

                        print(ean_df)
                        print(ean_df.info())

                        FULL_df = pd.merge(FULL_df, ean_df, on='item', how='left')
                        FULL_df['ean'] = FULL_df['ean'].fillna('-')
                        print(FULL_df)
                        print(FULL_df.info())

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'КАТЕГОРИИ ТОВАРОВ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        q_head = open(COM_dir + 'SQL_Query___ItemCategory_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___ItemCategory_2.txt').read()

                        query = q_head + q_body + q_tail
                        desc_df = SqlExecuter(query)
                        print(desc_df)

                        FULL_df = pd.merge(FULL_df, desc_df, on='item', how='left')

                        for i in range(1,5):
                            col = 'desc' + str(i)
                            FULL_df[col] = FULL_df[col].fillna('-')

                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                sn = 'КОММЕНТАРИЙ ВОЗВРАТА'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        posting_list = FULL_df['posting'].tolist()
                        q_body = str(posting_list)[1:-1]

                        q_head = open(COM_dir + 'SQL_Query___ReturnComment_1.txt').read()
                        q_tail = open(COM_dir + 'SQL_Query___ReturnComment_2.txt').read()

                        query = q_head + q_body + q_tail
                        return_comment_df = SqlExecuter(query)
                        print(return_comment_df)

                        FULL_df = pd.merge(FULL_df, return_comment_df, on='posting', how='left')
                        FULL_df['comment'] = FULL_df['comment'].fillna('-')
                        print(FULL_df)

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)

                
                sn = 'ФАЙЛ В РАБОТУ'
                if ErrorCheck == 0:
                    try:
                        step = step + 1
                        tech_mes(f'{step} из {step_num}: {sn}')
                        ######################################
                        RES_df = FULL_df[['dt',
                                            'id',
                                            'warehouse_id',
                                            'warehouse',
                                            'detail',
                                            'posting',
                                            'scanit',
                                            'item',
                                            'supply',
                                            'ean',
                                            'desc1',
                                            'desc2',
                                            'desc3',
                                            'desc4',
                                            'cell',
                                            'flow',
                                            'stock',
                                            'category',
                                            'link',
                                            'comment']]

                        RES_df.to_excel(DocOutPrefix() + 'Ж в работу.xlsx', index=False)
                        mes = 'Лови!'

                    except Exception as e:
                        ErrorCheck = 1
                        mes = f'Ошибка пункта {sn}: {e}'
                        print(mes)
                '''

            except:
                mes = 'Ошибка! Что-то пошло не так:('


            # удаляем папку процесса
            if os.path.isdir(cup_dir) == True:
                shutil.rmtree(cup_dir)
            chat(mes_sender, mes)

    if 'ОТЧЁТЫ' != '':

        if 'ВНЕ ТЯ' != '':

            def REP_LARGE_SIZE__DataIn ():
                def_name = 'REP_LARGE_SIZE__DataIn'
                comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
                mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
                cup_dir = CUPS_dir + def_name + '/'
                file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
                file = str(file_list[0])
                try:
                    # сохнаняем SS-отчёт
                    df = pd.read_excel(file)
                    df = df[df['Тип предмета'] == 'EXEMPLAR']
                    print(df)
                    print(df.info())

                    df.to_csv(REP_LARGE_SIZE__SSRep_doc, index=False)
                    
                    def_initiator('REP_LARGE_SIZE__ItemQuery',['-'])
                    mes = 'SS-отчёт сохранила, инициирую генерацию бобр-запроса на item'

                except:
                    mes = 'Ошибка! Что-то пошло не так:('

                # удаляем папку процесса
                if os.path.isdir(cup_dir) == True:
                    shutil.rmtree(cup_dir)
                chat(mes_sender, mes)

            def REP_LARGE_SIZE__ItemQuery ():
                def_name = 'REP_LARGE_SIZE__ItemQuery'
                comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
                mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
                cup_dir = CUPS_dir + def_name + '/'
                try:
                    # получаем спмсок инстансов
                    df = pd.read_csv(REP_LARGE_SIZE__SSRep_doc)
                    df['ID Предмета'] = pd.to_numeric(df['ID Предмета'], errors='coerce', downcast='integer')
                    instance_list = df['ID Предмета'].tolist()
                    print(instance_list[:10])

                    query_bodys_list = ListFracter(instance_list,20000)
                    #print(query_bodys_list[:10])
                    q_head = open(RELAY_REP_LARGE_SIZE_dir + 'SQL_Query___Item_1.txt').read()
                    q_tail = open(RELAY_REP_LARGE_SIZE_dir + 'SQL_Query___Item_2.txt').read()
                    print(q_head)

                    for body in query_bodys_list:
                        q_body = str(body)[1:-1]
                        query = q_head + q_body + q_tail

                        f = open(DocOutPrefix() + ' - ВНЕ_ТЯ - бобёр запрос item_id.txt','w')
                        f.write(query)
                        f.close

                    mes = 'Кинула бобр-запросы на айтемы'

                except:
                    mes = 'Ошибка! Что-то пошло не так:('

                # удаляем папку процесса
                if os.path.isdir(cup_dir) == True:
                    shutil.rmtree(cup_dir)
                chat(mes_sender, mes)

            def REP_LARGE_SIZE__DBUL_Item ():
                def_name = 'REP_LARGE_SIZE__DBUL_Item'
                comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
                mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
                cup_dir = CUPS_dir + def_name + '/'
                file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
                file = str(file_list[0])
                try:
                    # если таких выгрузок ещё нет - копируем, есть - соединяем
                    if os.path.isfile(REP_LARGE_SIZE__BDUL_Item_doc) == False:
                        shutil.copy2(file, REP_LARGE_SIZE__BDUL_Item_doc)
                    else:
                        df = pd.read_csv(file)
                        df.to_csv(REP_LARGE_SIZE__BDUL_Item_doc, mode='a', index= False , header= False)

                    
                    mes = 'Cохранила'

                except:
                    mes = 'Ошибка! Что-то пошло не так:('

                # удаляем папку процесса
                if os.path.isdir(cup_dir) == True:
                    shutil.rmtree(cup_dir)
                chat(mes_sender, mes)

            def REP_LARGE_SIZE__DBQ_Tag ():
                def_name = 'REP_LARGE_SIZE__DBQ_Tag'
                comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
                mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
                cup_dir = CUPS_dir + def_name + '/'
                try:
                    # получаем спмсок инстансов
                    df = pd.read_csv(REP_LARGE_SIZE__BDUL_Item_doc)
                    df = df.drop_duplicates(subset='item_id')
                    item_list = df['item_id'].tolist()
                    print(item_list[:10])

                    query_bodys_list = ListFracter(item_list,20000)
                    q_head = open(RELAY_REP_LARGE_SIZE_dir + 'SQL_Query___Tags_1.txt').read()
                    q_tail = open(RELAY_REP_LARGE_SIZE_dir + 'SQL_Query___Tags_2.txt').read()
                    print(q_head)

                    for body in query_bodys_list:
                        q_body = str(body)[1:-1]
                        query = q_head + q_body + q_tail

                        f = open(DocOutPrefix() + ' - ВНЕ_ТЯ - бобёр запрос тэги.txt','w')
                        f.write(query)
                        f.close

                    mes = 'Кинула бобр-запросы на тэги'

                except:
                    mes = 'Ошибка! Что-то пошло не так:('

                # удаляем папку процесса
                if os.path.isdir(cup_dir) == True:
                    shutil.rmtree(cup_dir)
                chat(mes_sender, mes)

            def REP_LARGE_SIZE__DBUL_Tag ():
                def_name = 'REP_LARGE_SIZE__DBUL_Tag'
                comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
                mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
                cup_dir = CUPS_dir + def_name + '/'
                file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
                file = str(file_list[0])
                try:
                    # если таких выгрузок ещё нет - копируем, есть - соединяем
                    if os.path.isfile(REP_LARGE_SIZE__BDUL_Tag_doc) == False:
                        shutil.copy2(file, REP_LARGE_SIZE__BDUL_Tag_doc)
                    else:
                        df = pd.read_csv(file)
                        df.to_csv(REP_LARGE_SIZE__BDUL_Tag_doc, mode='a', index= False , header= False)

                    
                    mes = 'Cохранила'

                except:
                    mes = 'Ошибка! Что-то пошло не так:('

                # удаляем папку процесса
                if os.path.isdir(cup_dir) == True:
                    shutil.rmtree(cup_dir)
                chat(mes_sender, mes)

            def REP_LARGE_SIZE__ToWork ():
                def_name = 'REP_LARGE_SIZE__ToWork'
                comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
                mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
                cup_dir = CUPS_dir + def_name + '/'
                try:
                    if 'определяем все df-ки' != '':
                        SS_df = pd.read_csv(REP_LARGE_SIZE__SSRep_doc)
                        print(SS_df)
                        print(SS_df.info())

                        ITEM_df = pd.read_csv(REP_LARGE_SIZE__BDUL_Item_doc).iloc[:,:-1]
                        ITEM_df = ITEM_df.drop_duplicates()
                        print(ITEM_df)
                        print(ITEM_df.info())

                        TAG_df = pd.read_csv(REP_LARGE_SIZE__BDUL_Tag_doc).iloc[:,:-1]
                        TAG_df = TAG_df.drop_duplicates()
                        print(TAG_df)
                        print(TAG_df.info())

                    if 'собираем тэги' != '':
                        item_list = TAG_df.drop_duplicates(subset='item_id')['item_id'].tolist()
                        tags_list = []
                        for i in item_list:
                            i = int(i)
                            tag_list_i = TAG_df[TAG_df['item_id'] == i]['tag_name'].tolist()
                            tag_i = ''
                            for t in tag_list_i:
                                if len(tag_i) == 0:
                                    tag_i = tag_i + t
                                else:
                                    tag_i = tag_i + ',' + t

                            tags_list.append(tag_i)
                            print(tag_i)

                        res_tag_df = pd.DataFrame({'item_id': item_list, 'tags': tags_list})
                        print(res_tag_df)


                    if 'собираем РЕЗУЛЬТАТ' != '':
                        print('ииииииииииииииииииииииииии')
                        inst_item_tag_df = ITEM_df.merge(res_tag_df, how='left', on='item_id')
                        print(inst_item_tag_df)
                        RES_df = SS_df.merge(inst_item_tag_df, how='left', left_on='ID Предмета', right_on='instance_id')
                        RES_df['instance_id'] = RES_df['instance_id'].fillna(0)
                        RES_df['item_id'] = RES_df['item_id'].fillna(0)
                        RES_df['tags'] = RES_df['tags'].fillna('-')
                        print(RES_df)
                        
                        # сохраняем результат в рабочей папке
                        res_dir = cup_dir + 'res/'
                        os.mkdir(res_dir)
                        RES_df.to_excel(res_dir + 'ВНЕ ТЯ - в работу.xlsx', index=False)

                    if 'ОТДАЮ' != '':
                        for rf in os.listdir(res_dir):
                            print(rf)
                            shutil.copy2(res_dir + rf, DocOutPrefix() + rf)
                        

                    
                    
                    
                    mes = 'Готово!'

                except:
                    mes = 'Ошибка! Что-то пошло не так:('

                # удаляем папку процесса
                if os.path.isdir(cup_dir) == True:
                    shutil.rmtree(cup_dir)
                chat(mes_sender, mes)

        if 'ТАБЛО' != '':
            
            def TABLO_UpdatingRegistrySettings ():
                def_name = 'TABLO_UpdatingRegistrySettings'
                comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
                mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
                cup_dir = CUPS_dir + def_name + '/'
                file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
                file = str(file_list[0])
                try:

                    if 'Обновление настроек' != '':
                    
                        df_set = pd.read_excel(file, sheet_name='настройки')

                        # удаляем старую базу, если есть
                        if os.path.isfile(TABLO_Settings_doc) == True:
                            os.remove(TABLO_Settings_doc)

                        # сохраняем новую
                        df_set.to_csv(TABLO_Settings_doc, index= False)
                        print(df_set)
                        print(df_set.info())


                    if 'Обновление реестра' != '':
                        
                        if 'подготовка исходных данных' !='':

                            now = dt.datetime.now()
                            df = pd.read_excel(file, sheet_name='реестр')

                            # ставим заглушку на пропуски
                            cols_all = df.columns.tolist()
                            for col in cols_all:
                                df[col] = df[col].fillna('-')
                
                            # фильтруем месяцы
                            mon_cur = now.strftime('%m.%Y')
                            if now.month == 1:
                                mon_last = '12.' + str(now.year - 1)
                            else:
                                mon_last_num = now.month - 1
                                if mon_last_num >=10:
                                    mon_last = str(mon_last_num) + '.' + str(now.year)
                                else:
                                    mon_last = '0' + str(mon_last_num) + '.' + str(now.year)

                            df = df[df['дата'].dt.strftime('%m.%Y').isin([mon_cur, mon_last])]
                            
                            # сортируем и сбрасываем индекс
                            df = df.sort_values(by='дата')
                            
                            # переименовываем столбцы
                            df.columns = ['date', 'shift', 'staff', 'report_mis_plan','ss_release_sum','ss_mis', 'complaint_backlog','complaint_backlog_last_mon']

                            # добавляем столбец с номером дня
                            df.insert(1,'day', df['date'].dt.day.tolist())
                            print(df)
                            print(df.info())

                        if 'расчёт допстолбцов' != '':

                            staff_shift_out = []
                            staff_shift_up = []
                            report_mis_plan_sum = []
                            release_per_day_mon_last = []
                            release_per_day_mon_cur = []
                            release_sum_mon_last = []
                            release_sum_mon_cur = []

                            def registry_tab(row):
                            
                                mon_str = str(row['date'].strftime('%m.%Y'))
                                df_def = df[df['date'].dt.strftime('%m.%Y') == mon_str]
                                row_num = int(row['day'])
                                print(row_num)
                                # челосмен всего
                                staff_shift_all =  df_def[df_def['shift'] == str(row['shift'])]['staff'].sum()

                                #отработано челосмен
                                staff_shift_out_i = df_def[(df_def['shift'] == str(row['shift'])) & (df_def['day'] < row_num)]['staff'].sum()
                                staff_shift_out.append(staff_shift_out_i)
                                print(staff_shift_out_i)
                                
                                # осталось челосмен
                                staff_shift_up_i = staff_shift_all - staff_shift_out_i
                                staff_shift_up.append(staff_shift_up_i)
                                print(staff_shift_up_i)

                                # план отчётных сумма
                                report_mis_plan_sum.append(int(sum(df_def['report_mis_plan'].tolist()[:row_num])))

                                # прогноз выхода
                                try:
                                    int(row['ss_release_sum'])
                                    if row_num == 1:
                                        release_per_day_i = int(row['ss_release_sum'])
                                        print('первое значение')
                                    else:
                                        release_per_day_i = int(row['ss_release_sum'] - df_def['ss_release_sum'].tolist()[row_num - 2])
                                        print(f'результат {release_per_day_i}')
                                    
                                except:
                                    if row_num in [1,2]:
                                        release_per_day_i = int(df_set[df_set['параметр'] == 'постинги в день']['значение'].tolist()[0])
                                    else:
                                        if mon_str == mon_last:
                                            release_per_day_i = int(sum(release_per_day_mon_last)/len(release_per_day_mon_last))
                                        else:
                                            release_per_day_i = int(sum(release_per_day_mon_cur)/len(release_per_day_mon_cur))
                                
                                

                                # прогноз выхода сумма
                                if mon_str == mon_last:
                                    release_per_day_mon_last.append(release_per_day_i)   
                                    release_sum_mon_last.append(sum(release_per_day_mon_last))
                                else:
                                    release_per_day_mon_cur.append(release_per_day_i)
                                    release_sum_mon_cur.append(sum(release_per_day_mon_cur))


                            
                            df[df['date'].dt.strftime('%m.%Y') == mon_last].apply(registry_tab, axis=1)
                            df[df['date'].dt.strftime('%m.%Y') == mon_cur].apply(registry_tab, axis=1)
                            df.insert(4,'staff_shift_out',staff_shift_out)
                            df.insert(5,'staff_shift_up',staff_shift_up)
                            df.insert(7,'report_mis_plan_sum',report_mis_plan_sum)

                            release_per_day = release_per_day_mon_last + release_per_day_mon_cur
                            df.insert(10,'release_per_day',release_per_day)

                            release_sum = release_sum_mon_last + release_sum_mon_cur
                            df.insert(11,'release_sum',release_sum)

                            df_res = df.reset_index()
                            del df_res['index']

                        if 'обработка результата' != '':

                            # удаляем старую базу, если есть
                            if os.path.isfile(TABLO_Registry_doc) == True:
                                os.remove(TABLO_Registry_doc)
                            
                            # сохраняем датасет
                            df_res.to_excel('1.xlsx', index= False)
                            df_res.to_csv(TABLO_Registry_doc, index= False)
                            print(df_res)

                    

                    mes = 'Реестр и настройки ТАБЛО обновлёны'

                except:
                    mes = 'Ошибка! Что-то пошло не так:('

                # удаляем папку процесса
                if os.path.isdir(cup_dir) == True:
                    shutil.rmtree(cup_dir)
                chat(mes_sender, mes)

            def TABLO_ComplaintData (TR_df, TS_df, CPL_df, mode):
                try:
                    # определяем вводные
                    TR_df = pd.DataFrame(TR_df)
                    TS_df = pd.DataFrame(TS_df)
                    CPL_df = pd.DataFrame(CPL_df)
                    mode = str(mode)

                    # переменные
                    now = dt.datetime.now()
                    FBR_par = float(TS_df[TS_df['параметр'] == 'FBR']['значение'].tolist()[0])
                    C_target_par = float(TS_df[TS_df['параметр'] == 'цель по жалобам']['значение'].tolist()[0])
                    CR_to_cancel_par = float(TS_df[TS_df['параметр'] == 'конверсия жалоб в списание']['значение'].tolist()[0])

                    print(FBR_par)

                    
                    print('\n\n######################################################################################################')
                    print('Определяем основной датафрейм')
                    print('######################################################################################################')
                    del_col_list = ['shift_out','shift_up','report_mis_plan','report_mis_plan_sum','complaint_backlog_last_mon']
                    if mode == 'curr':
                        month_num = now.month
                        df = TR_df[(TR_df['date'].dt.month == month_num) & (TR_df['date'] <= now)]
                        
                    elif mode == 'last':
                        month_num = now.month - 1
                        df = TR_df[(TR_df['date'].dt.month.isin([month_num, month_num + 1])) & (TR_df['date'] <= now)]
                        print(df)
                        print('расчитываем столбецы выхода беклога с учётом данных прошлого месяца')
                        day_list = []
                        release_list = []
                        backlog_list = []
                        def col_release_backlog (row):
                            date_row = int(row['date'].month)
                            print(date_row)
                            if date_row == month_num:
                                day_list.append(row['day'])
                                release_list.append(row['release_sum'])
                                backlog_list.append(row['complaint_backlog'])
                            elif date_row == month_num + 1:
                                day_list.append(df[df['date'].dt.month == month_num]['day'].tolist()[-1] + row['day'])
                                release_list.append(df[df['date'].dt.month == month_num]['release_sum'].tolist()[-1])
                                backlog_list.append(row['complaint_backlog_last_mon'])
                        df.apply(col_release_backlog, axis=1)
                        df['day'] = day_list
                        df['release_sum'] = release_list
                        df['complaint_backlog'] = backlog_list
                    print('Удаляем ненужные столбцы')
                    for col in del_col_list:
                        del df[col]
                    print(month_num)
                    print(df)
                    
                    
                    # добавляем в таблицу реестра все необходимые столбцы
                    backlog_tomorrow_list = ['backlog_tomorrow']
                    conditional_release_sum_list = ['conditional_release_sum']
                    allowed_mis_list = ['allowed_mis']
                    reasonable_complaint_list = ['reasonable_complaint']
                    reasonable_complaint_sum_list = ['reasonable_complaint_sum']
                    remaining_reasonable_complaint_list = ['remaining_reasonable_complaint']
                    cancellation_target_list = ['cancellation_target']
                    cancellation_complaint_list = ['cancellation_complaint']
                    done_list = ['done']
                    DR_SS_list = ['DR_SS']
                    DR_culc_list = ['DR_culc']
                    cancellation_complaint_target_rate_list = ['cancellation_complaint_target_rate']

                    COLS_list = [backlog_tomorrow_list,   # беклог на завтра
                                    conditional_release_sum_list,   # условный выход на сегодня
                                    allowed_mis_list,   # допустимые ошибки
                                    reasonable_complaint_list,   # обоснованные жалобы
                                    reasonable_complaint_sum_list,   # обоснованные жалобы сумма
                                    remaining_reasonable_complaint_list,   # остаток обоснованных жалоб
                                    cancellation_target_list,   # цель но списанию
                                    cancellation_complaint_list,   # к списанию
                                    done_list,   # сделано
                                    DR_SS_list,   # DR по SS
                                    DR_culc_list,   # DR расчётный
                                    cancellation_complaint_target_rate_list   # процент достижения цели по списанию
                                    ]
                    

                    def culc_cols(row):
                        # беклог на завтра = беклог + (выход за вчера * FBR)
                        if row['day'] == 1:
                            backlog_tomorrow_i = float(row['complaint_backlog']) + (float(row['release_per_day']) * FBR_par)
                            backlog_tomorrow_i = int(backlog_tomorrow_i)
                            print(backlog_tomorrow_i)
                        else:
                            print('туууттт')
                            last_day_num = int(row['day']) - 1
                            print(last_day_num)
                            #release_per_day_tomm = RELA(df_reg,'day', last_day_num, 'release_per_day')
                            release_per_day_tomm = df[df['day'] == last_day_num]['release_per_day'].tolist()[0]
                            print(release_per_day_tomm)
                            comp_tomm = release_per_day_tomm * FBR_par
                            print(comp_tomm)
                            complaint_backlog = float(row['complaint_backlog'])
                            print(complaint_backlog)
                            backlog_tomorrow_i = complaint_backlog + comp_tomm
                        backlog_tomorrow_i = int(backlog_tomorrow_i)
                        backlog_tomorrow_list.append(backlog_tomorrow_i)
                        
                        # выход на сегодня условно(-2 дня)
                        if row['day'] <= 2:
                            conditional_release_sum_i = int(df['release_sum'].tolist()[0])
                            print(conditional_release_sum_i)
                        else:
                            day_minus_2 = int(row['day'] - 2)
                            print(day_minus_2)
                            conditional_release_sum_i = int(df[df['day'] == day_minus_2]['release_sum'].tolist()[0])
                        conditional_release_sum_list.append(conditional_release_sum_i)
                        print(conditional_release_sum_i)

                        # допустимые ошибки = показатель * выход на сегодня условно(-2 дня)/100
                        allowed_mis_i = int(C_target_par * conditional_release_sum_i / 100)
                        allowed_mis_list.append(allowed_mis_i)
                        print(allowed_mis_i)

                        # обоснованные жалобы
                        reasonable_complaint_i = len(CPL_df[(CPL_df['date_ts'].dt.month == month_num) & (CPL_df['Дата обработки'] == row['date']) & (CPL_df['Резолюция'] == 'обоснована')])
                        reasonable_complaint_list.append(reasonable_complaint_i)
                        print(reasonable_complaint_i)

                        # к списанию
                        cancellation_complaint_i = len(CPL_df[(CPL_df['date_ts'].dt.month == month_num) & (CPL_df['Дата обработки'] == row['date']) & (CPL_df['Резолюция'].isin(['не обоснована', 'чужая вина']))])
                        cancellation_complaint_list.append(cancellation_complaint_i)
                        print(cancellation_complaint_i)

                        # сделано
                        done_i = reasonable_complaint_i + cancellation_complaint_i
                        done_list.append(done_i)

                        # обоснованные жалобы сумма
                        reasonable_complaint_sum_i = int(sum(reasonable_complaint_list[1:]))
                        reasonable_complaint_sum_list.append(reasonable_complaint_sum_i)
                        print(reasonable_complaint_sum_i)

                        # остаток допустимых ошибок = допустимые ошибоки - обоснованные сумма на дату + обоснованные за сегодня
                        remaining_reasonable_complaint_i = allowed_mis_i - reasonable_complaint_sum_i + reasonable_complaint_i
                        remaining_reasonable_complaint_list.append(remaining_reasonable_complaint_i)
                        print(remaining_reasonable_complaint_i)

                        # цель по списанию = беклог на завтра - остаток допустимых ошибок
                        cancellation_target_i = backlog_tomorrow_i - remaining_reasonable_complaint_i
                        if cancellation_target_i >= row['complaint_backlog']:
                            cancellation_target_i = int(row['complaint_backlog'] * CR_to_cancel_par)
                        cancellation_target_list.append(cancellation_target_i)

                        # DR по SS = ошибки * 100 / выход
                        print('Расчитываем DR SS - но новому')
                        
                        if int(row['date'].month) == month_num and row['ss_release_sum'] != '-' and row['ss_mis'] != '-':
                            DR_SS_i = round(float(row['ss_mis']) * 100 / float(row['ss_release_sum']),3)
                        else:
                            try:
                                if mode == 'curr':
                                    DR_SS_i_df = df[(df['ss_release_sum'] != '-') & (df['ss_mis'] != '-')]
                                elif mode == 'last':
                                    DR_SS_i_df = df[(df['ss_release_sum'] != '-') & (df['ss_mis'] != '-') & (df['date'].dt.month == month_num)]
                                SS_mis_i = float(DR_SS_i_df['ss_mis'].tolist()[-1])
                                DR_SS_i = round(SS_mis_i * 100 / float(DR_SS_i_df['ss_release_sum'].tolist()[-1]),3)
                            except:
                                DR_SS_i = '-' 

                        print(DR_SS_i)
                        DR_SS_list.append(DR_SS_i)

                        # показатель = (обоснованные + бэклог) * 100 / выход условно сегодня
                        print('Расчитываем DR расчётный')
                        DR_culc_mis_sum_i = reasonable_complaint_sum_i + float(row['complaint_backlog']) - done_i
                        print(DR_culc_mis_sum_i)
                        DR_culc_i = round(DR_culc_mis_sum_i * 100 / conditional_release_sum_i,3)
                        print(DR_culc_i)
                        DR_culc_list.append(DR_culc_i)

                        # процент достяжения цели по списанию на день
                        try:
                            cancellation_complaint_target_rate_i = round(float(cancellation_complaint_i)/cancellation_target_i,2)
                        except:
                            cancellation_complaint_target_rate_i = '-'
                        cancellation_complaint_target_rate_list.append(cancellation_complaint_target_rate_i)


                    #------------------------------------------------------------------------------------------
                    #-------РАСЧИТЫВАЕМ СТОЛБЦЫ
                    df.apply(culc_cols,axis=1)
                    for col in COLS_list:
                        df[col[0]] = col[1:]
                    print(df)
                    df.to_excel(CUPS_dir + 'TABLO_Main/' + 'TABLO_DATA_COMPLAINT_main.xlsx')

                    otvet = df

                except Exception as e:
                    otvet = f'TABLO_DATA_COMPLAINT_DF: {e}'

                return otvet

            def TABLO_MisData (TR_df, TS_df, QCS_df):
                try:
                    
                    if 'ПЕРЕМЕННЫЕ' != '':
                        # определяем вводные данные
                        TR_df = pd.DataFrame(TR_df)
                        TS_df = pd.DataFrame(TS_df)
                        QCS_df = pd.DataFrame(QCS_df)

                        # переменные
                        now = dt.datetime.now()
                        #now = datetime.strptime('2025-03-31', "%Y-%m-%d").date()
                        MIS_TARGET_par = float(TS_df[TS_df['параметр'] == 'показатель по ошибкам']['значение'].tolist()[0])
                        WORK_RATIO_par = float(TS_df[TS_df['параметр'] == 'рабочий коэффициент к цели']['значение'].tolist()[0])
                        TS_los_rate = float(TS_df[TS_df['параметр'] == 'доля ООП от цели']['значение'].tolist()[0]) # доля ООП от выхода
                        month_num = now.month
                        print(month_num)

                    if 'определяем ГЛАВНУЮ ТАБЛИЦУ' != '':
                    
                        df = TR_df[TR_df['date'].dt.month == month_num][['date',
                                                                        'day',
                                                                        'shift',
                                                                        'staff',
                                                                        'staff_shift_out',
                                                                        'staff_shift_up',
                                                                        'report_mis_plan',
                                                                        'report_mis_plan_sum',
                                                                        'release_per_day',
                                                                        'release_sum']]
                        print('контрольня точка --------------------')
                        print(df)
                    
                    if 'РАСЧЁТ СТОЛБЦОВ' != '':
                    
                        if 'определяем столбцы' != '':
                            
                            target_total_list = ['target_total']
                            target_list = ['target']
                            fact_sum_list = ['fact_sum']
                            rate_list = ['rate']

                            work_target_list = ['work_target']
                            work_rate_list = ['work_rate']
                            

                            rep_plan_list = ['rep_plan']
                            rep_plan_sum_list = ['rep_plan_sum']
                            rep_plan_total_list = ['rep_plan_total']
                            rep_fact_list = ['rep_fact']
                            rep_fact_sum_list = ['rep_fact_sum']
                            rep_rate_list = ['rep_rate']
                            rep_shortage_list = ['rep_shortage']

                            man_target_total_list = ['man_target_total']
                            man_target_list = ['man_target']
                            man_fact_sum_list = ['man_fact_sum']
                            man_rate_list = ['man_rate']

                            man_shift_target_total_list = ['man_shift_target_total']
                            man_shift_target_list = ['man_shift_target']
                            man_shift_day_plan_list = ['man_shift_day_plan']
                            man_shift_fact_list = ['man_shift_fact']
                            man_shift_fact_sum_list = ['man_shift_fact_sum']
                            man_shift_day_plan_rate_list = ['man_shift_day_plan_rate']
                            
                            

                            COLS_list = [target_total_list,
                                         target_list,
                                         fact_sum_list,
                                         rate_list,

                                         rep_plan_total_list,
                                         rep_plan_list,
                                         rep_plan_sum_list,
                                         rep_fact_list,
                                         rep_fact_sum_list,
                                         rep_rate_list,
                                         rep_shortage_list,

                                         work_target_list,
                                         work_rate_list,
                                         
                                         man_target_total_list,
                                         man_target_list,
                                         man_fact_sum_list,
                                         man_rate_list,
                                         man_shift_target_total_list,
                                         man_shift_target_list,
                                         man_shift_day_plan_list,
                                         man_shift_fact_list,
                                         man_shift_fact_sum_list,
                                         man_shift_day_plan_rate_list]

                        if 'считаем' != '':

                            def col_culc(row):

                                if 'переменные функции' != '':
                                    # кол-во челосмен в месяце
                                    staff_shift_total = int(df['staff'].sum())
                                    #staff_shift_sum = int(df[df['day'] <= int(row['day'])]['staff'].sum())

                                    qcs_day_df = pd.DataFrame(QCS_df[(QCS_df['Дата фиксации ошибки'] == row['date']) &
                                                                     (QCS_df['Статус ошибки'] == 'Утверждена') &
                                                                     (QCS_df['Метод начисления ошибки'] == 'Ручной')]
                                                             )

                                    print('-----------------------------------------------------------------------------------------------------------')
                                    print(f'расчёт дня {row['day']}')
                                    print('-----------------------------------------------------------------------------------------------------------')

                                if 'ПОКАЗАТЕЛЬ' != '':

                                    sn = 'ЦЕЛЬ всего'
                                    if sn != '':
                                        print(sn)
                                        target_total = int(df['release_sum'].tolist()[-1] * MIS_TARGET_par / 100)
                                        target_total_list.append(target_total)
                                        print(target_total)

                                    sn = 'ЦЕЛЬ'
                                    if sn != '':
                                        print(sn)
                                        target = int(row['release_sum'] * MIS_TARGET_par / 100)
                                        target_list.append(target)
                                        print(target)

                                    sn = 'ФАКТ сумма'
                                    if sn != '':
                                        print(sn)
                                        fact_sum = len(pd.DataFrame(QCS_df[(QCS_df['Дата фиксации ошибки'].dt.month == now.month) &
                                                                           (QCS_df['Дата фиксации ошибки'] <= row['date']) &
                                                                           (QCS_df['Статус ошибки'] == 'Утверждена') &
                                                                           (QCS_df['Метод начисления ошибки'] == 'Ручной')]
                                                                    )
                                                                )
                                        
                                        fact_sum_list.append(fact_sum)
                                        print(fact_sum)

                                    sn = 'ДОЛЯ ЦЕЛИ'
                                    if sn != '':
                                        print(sn)
                                        rate = round(fact_sum / target, 2)
                                        rate_list.append(rate)
                                        print(rate)

                                if 'ОТЧЁТНЫЕ' != '':
                                    
                                    sn = 'ОТЧЁТНЫЕ план'
                                    if sn != '':
                                        print(sn)
                                        rep_plan = row['report_mis_plan']
                                        rep_plan_list.append(rep_plan)
                                        print(rep_plan)

                                    sn = 'ОТЧЁТНЫЕ план сумма'
                                    if sn != '':
                                        print(sn)
                                        rep_plan_sum = row['report_mis_plan_sum']
                                        rep_plan_sum_list.append(rep_plan_sum)
                                        print(rep_plan_sum)

                                    sn = 'ОТЧЁТНЫЕ план всего'
                                    if sn != '':
                                        print(sn)
                                        rep_plan_total = df['report_mis_plan_sum'].tolist()[-1]
                                        rep_plan_total_list.append(rep_plan_total)
                                        print(rep_plan_total)

                                    sn = 'ОТЧЁТНЫЕ факт'
                                    if sn != '':
                                        print(sn)
                                        rep_fact = sum(qcs_day_df.apply(lambda lrow: 1 if str(lrow['Комментарий']).lower()[:5] in ['отчет', 'отчёт'] else 0, axis=1))
                                        rep_fact_list.append(rep_fact)
                                        print(rep_fact)
                                    
                                    sn = 'ОТЧЁТНЫЕ факт сумма'
                                    if sn != '':
                                        print(sn)
                                        rep_fact_sum = int(sum(rep_fact_list[1:]))
                                        rep_fact_sum_list.append(rep_fact_sum)
                                        print(rep_fact_sum)

                                    sn = 'ДОЛЯ достижения плана по ОТЧЁТНЫМ'
                                    if sn != '':
                                        print(sn)
                                        if rep_plan_sum == 0:
                                            rep_rate = 1
                                        elif rep_fact_sum > 0 and rep_plan_sum > 0:
                                            rep_rate = round(float(rep_fact_sum) / rep_plan_sum,2)
                                        else: rep_rate = 0
                                        rep_rate_list.append(rep_rate)
                                        print(rep_rate)

                                    sn = 'НЕДОБОР по отчётным'
                                    if sn != '':
                                        print(sn)
                                        rep_shortage = rep_plan_sum - rep_fact_sum
                                        rep_shortage_list.append(rep_shortage)
                                        print(rep_shortage)

                                if 'ЦЕЛЬ' != '':
                                    
                                    sn = 'РАБОЧАЯ ЦЕЛЬ'
                                    if sn != '':
                                        print(sn)
                                        ####### РАБОЧАЯ ЦЕЛЬ = план по отчётным + план по ручным 

                                        ##### план по ручным = план по ручным всего / все челосмены * (отработанные челосмены + сегодняшние челосмены)
                                        ### план по ручным всего = ПОКАЗАТЕЛЬ всего - ОТЧЁТНЫЕ план всего

                                        man_plan_total = target_total - rep_plan_total
                                        man_plan = int(float(man_plan_total) / staff_shift_total * df[df['day'] <= int(row['day'])]['staff'].sum())
                                        
                                        work_target = rep_plan_sum + man_plan
                                        work_target_list.append(work_target)
                                        print(work_target)

                                    sn = 'ДОЛЯ РАБОЧЕЙ ЦЕЛИ'
                                    if sn != '':
                                        print(sn)
                                        work_rate = round(fact_sum / work_target, 2)
                                        work_rate_list.append(work_rate)
                                        print(work_rate)

                                if 'РУЧНЫЕ' != '':

                                    sn = 'РУЧНЫЕ цель всего'
                                    if sn != '':
                                        print(sn)
                                        # ЦЕЛЬ всего - общий план по отчётным + недобор по отчётным
                                        man_target_total = target_total - rep_plan_total + rep_shortage
                                        man_target_total_list.append(man_target_total)
                                        print(man_target_total)

                                    sn = 'РУЧНЫЕ цель'
                                    if sn != '':
                                        print(sn)
                                        man_target = int(float(man_target_total) / staff_shift_total * df[df['day'] <= int(row['day'])]['staff'].sum())
                                        man_target_list.append(man_target)
                                        print(man_target)

                                    sn = 'РУЧНЫЕ ФАКТ сумма'
                                    if sn != '':
                                        print(sn)
                                        man_fact_sum = sum(pd.DataFrame(QCS_df[(QCS_df['Дата фиксации ошибки'].dt.month == now.month) &
                                                                               (QCS_df['Дата фиксации ошибки'] <= row['date']) &
                                                                               (QCS_df['Статус ошибки'] == 'Утверждена') &
                                                                               (QCS_df['Метод начисления ошибки'] == 'Ручной')]
                                                                        ).apply(lambda lrow: 1 if str(lrow['Комментарий']).lower()[:5] not in ['отчет', 'отчёт'] else 0, axis=1)
                                                        )
                                        
                                        man_fact_sum_list.append(man_fact_sum)
                                        print(man_fact_sum)

                                    sn = 'ДОЛЯ РУЧНОЙ ЦЕЛИ'
                                    if sn != '':
                                        print(sn)
                                        man_rate = round(man_fact_sum / man_target, 2)
                                        man_rate_list.append(man_rate)
                                        print(man_rate)

                                if 'РУЧНЫЕ СМЕНЫ' != '':

                                    sn = 'РУЧНЫЕ СМЕНЫ всего'
                                    if sn != '':
                                        print(sn)
                                        # ручная цель всего/все челчасы * все челчасы смены
                                        man_shift_target_total = int(man_target_total / staff_shift_total * df[df['shift'] == str(row['shift'])]['staff_shift_up'].tolist()[0])
                                        man_shift_target_total_list.append(man_shift_target_total)
                                        print(man_shift_target_total)

                                    sn = 'РУЧНЫЕ СМЕНЫ'
                                    if sn != '':
                                        print(sn)
                                        # ручная цель смены всего/все челчасы смены * отработанные челочасы смены + сегодня
                                        man_shift_target = int(man_shift_target_total / float(df[df['shift'] == str(row['shift'])]['staff'].sum()) * int(row['staff_shift_out'] + row['staff']))
                                        man_shift_target_list.append(man_shift_target)
                                        print(man_shift_target)

                                    sn = 'РУЧНЫЕ СМЕНЫ план на день'
                                    if sn != '':
                                        print(sn)
                                        shift_dates_list = df[df['shift'] == str(row['shift'])]['date'].tolist()
                                        # (цель смены всего - все выставленные до сегодня ручные сменой) / остаток челосмен этой смены * челосмены сегодня
                                        sub_qcs_df = pd.DataFrame(QCS_df[(QCS_df['Дата фиксации ошибки'] < row['date']) &
                                                                (QCS_df['Дата фиксации ошибки'].isin(shift_dates_list)) &
                                                                (QCS_df['Статус ошибки'] == 'Утверждена') &
                                                                (QCS_df['Метод начисления ошибки'] == 'Ручной')]
                                                                )
                                        #print(sub_qcs_df)
                                        if len(sub_qcs_df) == 0:
                                            man_shift_done = 0
                                        else:
                                            man_shift_done = sum(sub_qcs_df.apply(lambda lrow: 1 if str(lrow['Комментарий']).lower()[:5] not in ['отчет', 'отчёт'] else 0, axis=1))
        
                                        print(man_shift_done)
                                        
                                        man_shift_day_plan = int((man_shift_target_total - man_shift_done) / row['staff_shift_up'] * row['staff'])
                                        man_shift_day_plan_list.append(man_shift_day_plan)
                                        print(man_shift_day_plan)

                                    sn = 'РУЧНЫЕ СМЕНЫ факт'
                                    if sn != '':
                                        print(sn)
                                        man_shift_fact = sum(qcs_day_df.apply(lambda lrow: 1 if str(lrow['Комментарий']).lower()[:5] not in ['отчет', 'отчёт'] else 0, axis=1))
                                        man_shift_fact_list.append(man_shift_fact)
                                        print(man_shift_fact)

                                    sn = 'РУЧНЫЕ СМЕНЫ факт сумма'
                                    if sn != '':
                                        print(sn)
                                        shift_dates_list = df[df['shift'] == str(row['shift'])]['date'].tolist()
                                        man_shift_fact_sum = sum(pd.DataFrame(QCS_df[(QCS_df['Дата фиксации ошибки'] <= row['date']) &
                                                                                (QCS_df['Дата фиксации ошибки'].isin(shift_dates_list)) &
                                                                                (QCS_df['Статус ошибки'] == 'Утверждена') &
                                                                                (QCS_df['Метод начисления ошибки'] == 'Ручной')]
                                                                        ).apply(lambda lrow: 1 if str(lrow['Комментарий']).lower()[:5] not in ['отчет', 'отчёт'] else 0, axis=1)
                                                                )
                                        
                                        man_shift_fact_sum_list.append(man_shift_fact_sum)
                                        print(man_shift_fact_sum)

                                    sn = 'РУЧНЫЕ СМЕНЫ процент достяжения дневного плана'
                                    if sn != '':
                                        print(sn)

                                        if man_shift_day_plan == 0:
                                            man_shift_day_plan_rate = 0
                                        else:
                                            man_shift_day_plan_rate = round(man_shift_fact / man_shift_day_plan,2)
                                        man_shift_day_plan_rate_list.append(man_shift_day_plan_rate)
                                        
                                        print(man_shift_day_plan_rate)


                            #------------------------------------------------------------------------------------------
                            #-------РАСЧИТЫВАЕМ СТОЛБЦЫ по ошибкам
                            df.apply(col_culc,axis=1)
                            for col in COLS_list:
                                df[col[0]] = col[1:]
                                print(col[0])
                                print(col[1:])

                            # добавляем столбец 100%
                            #df['100_percent'] = [1] * len(df)
                        
                    if 'СОХРАНЯЕМ' != '':
                        df.to_excel(CUPS_dir + 'TABLO_Main/' + 'TABLO_DATA_MIS.xlsx', index=False)
                        #df.to_excel('1.xlsx')
                        print(df)

                except Exception as e:
                    zero_dict = {}
                    df = pd.DataFrame(zero_dict)

                return df

            def TABLO_Main ():
                def_name = 'TABLO_Main'
                comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
                mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
                cup_dir = CUPS_dir + def_name + '/'
                try:
                    if 'УСЛОВИЯ ЗАПУСКА' != '':

                        if os.path.isfile(TABLO_Registry_doc) == False:
                            mes = 'Необходимо обновить реест Табло'
                            start_conditions = 0
                        
                        elif os.path.isfile(TABLO_Settings_doc) == False:
                            mes = 'Необходимо обновить настройки Табло'
                            start_conditions = 0

                        elif os.path.isfile(QCS_doc) == False:
                            mes = 'Необходимо обновить СКК'
                            start_conditions = 0

                        else:
                            start_conditions = 1

                    if 'ТЕЛО' != '' and start_conditions == 1:

                        if 'ПЕРЕМЕННЫЕ ' != '':
                                
                            now = dt.datetime.now()

                            TR_df = pd.read_csv(TABLO_Registry_doc)
                            TR_df['date'] = TR_df['date'].astype('datetime64[ns]')
                            TR_df['complaint_backlog'] = pd.to_numeric(TR_df['complaint_backlog'], errors='coerce')
                            TR_df['complaint_backlog_last_mon'] = pd.to_numeric(TR_df['complaint_backlog_last_mon'], errors='coerce')
                            print(TR_df)
                            print(TR_df.info())

                            TS_df = pd.read_csv(TABLO_Settings_doc)
                            print(TS_df)
                            print(TS_df.info())

                            QCS_df = pd.read_csv(QCS_doc)
                            QCS_df['Дата фиксации ошибки'] = QCS_df['Дата фиксации ошибки'].astype('datetime64[ns]')
                            print(QCS_df)
                            print(QCS_df.info())

                            # создаём финальный лист листов для записи результатов
                            TOTAL_RES_list = []

                        if 'DF для диаграмм' != '':
                            MIS_df = TABLO_MisData (TR_df, TS_df, QCS_df)

                            sn = 'M1 - индикаторы'
                            if sn != '':
                                print(sn)
                                cols_list = ['shift', 'rate','work_rate','rep_rate','man_rate']
                                try:
                                    
                                    M1_df = MIS_df[MIS_df['day'] == now.day][cols_list]
                                    print('df получена')
                    
                                except:
                                    res_dict = {}
                                    for col in cols_list:
                                        res_dict[col] = [0]
                                    M1_df = pd.DataFrame(res_dict)
                                    print('проблема с получением df, отдаю пустую')

                                print(M1_df)
                                TOTAL_RES_list.append(['M1', M1_df])

                            sn = 'M2 - ОБЩАЯ ДИНАМИКА план/факт'
                            if sn != '':
                                print(sn)
                                cols_list = ['day',
                                                 'target_total',
                                                 'target',
                                                 'fact_sum',
                                                 'rep_plan_sum',
                                                 'rep_fact_sum',
                                                 'work_target',
                                                 'man_target',
                                                 'man_fact_sum']
                                try:
                                    
                                    M2_df = MIS_df[MIS_df['day'] <= now.day].iloc[-10:,:][cols_list]
                                    print('df получена')
                    
                                except:
                                    res_dict = {}
                                    for col in cols_list:
                                        res_dict[col] = [0]
                                    M2_df = pd.DataFrame(res_dict)
                                    print('проблема с получением df, отдаю пустую')

                                print(M2_df)
                                TOTAL_RES_list.append(['M2', M2_df])

                            sn = 'M3 - ПЛАН на смену'
                            if sn != '':
                                print(sn)
                                cols_list = ['shift',
                                                 'man_shift_day_plan',
                                                 'man_shift_fact',
                                                 'man_shift_day_plan_rate']
                                try:
                                    M3_df = MIS_df[MIS_df['day'] == now.day][cols_list]
                                    print('df получена')
                    
                                except:
                                    res_dict = {}
                                    for col in cols_list:
                                        res_dict[col] = [0]
                                    M3_df = pd.DataFrame(res_dict)
                                    print('проблема с получением df, отдаю пустую')

                                print(M3_df)
                                TOTAL_RES_list.append(['M3', M3_df])

                            sn = 'M4 - ДИНАМИКА выполнения плана на смену'
                            if sn != '':
                                print(sn)
                                cols_list = ['day','shift','man_shift_day_plan_rate','100%']
                                try:
                                    M4_df = MIS_df[MIS_df['day'] <= now.day][cols_list[:-1]]
                                    M4_df[cols_list[-1]] = [1] * len(M4_df)
                                    print('df получена')

                                except:
                                    res_dict = {}
                                    for col in cols_list:
                                        res_dict[col] = [0]
                                    M4_df = pd.DataFrame(res_dict)
                                    print('проблема с получением df, отдаю пустую')

                                print(M4_df)
                                TOTAL_RES_list.append(['M4', M4_df])

                            sn = 'M5 - СОСТОЯНИЕ ЦЕЛИ смены на месяц'
                            if sn != '':
                                print(sn)
                                cols_list = ['day','shift','man_shift_target_total','man_shift_target','man_shift_fact_sum']
                                try:
                                    M5_df_pre = MIS_df[MIS_df['day'] <= now.day][cols_list]
                                    df_list = []
                                    for shift in MIS_df['shift'].unique():
                                        df_i = M5_df_pre[M5_df_pre['shift'] == str(shift)].iloc[-1:,:]
                                        df_list.append(df_i)

                                    M5_df = pd.concat(df_list)
                                    
                                    print('df получена')

                                except:
                                    res_dict = {}
                                    for col in cols_list:
                                        res_dict[col] = [0]
                                    M5_df = pd.DataFrame(res_dict)
                                    print('проблема с получением df, отдаю пустую')

                                print(M5_df)
                                TOTAL_RES_list.append(['M5', M5_df])

                            sn = 'M6 - РУЧНЫЕ БЕЗ ЧАТОВ по спецам'
                            if sn != '':
                                print(sn)
                                cols_list = ['spec','all' ,'val' ,'cancel','rate']
                                try:
                                    def spec_man_not_chat_mis_detailing ():
                                        df = QCS_df[QCS_df['Дата фиксации ошибки'].dt.month == dt.datetime.now().month]
                                        print(df)

                                        df['spec'] = df.apply(lambda row: str(row['Контроллер'])[str(row['Контроллер']).index('(')+1:-1], axis=1)
                                        print(df)

                                        df['target_mis'] = df.apply(lambda row: 1 if str(row['Комментарий']).lower()[:5] not in ['отчёт', 'отчет'] and
                                                                                    str(row['Комментарий']).lower()[:3] != 'чат'
                                                                                    else 0, axis=1
                                                                    )
                                        df = df[(df['target_mis'] == 1) & (df['Статус ошибки'].isin(['Утверждена', 'Резолюция снята']))]
                                        df['all'] = [1] * len(df)
                                        df['val'] = df.apply(lambda row: 1 if row['Статус ошибки'] == 'Утверждена' else 0, axis=1)
                                        df['cancel'] = df.apply(lambda row: 1 if row['val'] == 0 else 0, axis=1)
                                        print(df)

                                        df_res = df.groupby('spec').agg({'all':'sum', 'val': 'sum', 'cancel':'sum'}).reset_index()
                                        df_res['rate'] = round(df_res['cancel'] / df_res['all'],2)
                                        df_res = df_res.sort_values(by='val', ascending=False)
                                        print(df_res)

                                        return df_res

                                    M6_df = spec_man_not_chat_mis_detailing ()
                                    
                                    print('df получена')

                                except:
                                    res_dict = {}
                                    for col in cols_list:
                                        res_dict[col] = [0]
                                    M5_df = pd.DataFrame(res_dict)
                                    print('проблема с получением df, отдаю пустую')

                                print(M6_df)
                                TOTAL_RES_list.append(['M6', M6_df])



                        # -------------------------------------------------------------------------------------------
                        sn = 'CОБИРАЮ RES-файл'
                        if sn != '':
                            print(sn)

                            res_doc_pre = cup_dir + 'LamaTD.xlsx'
                            shutil.copy2(RELAY_TABLO_dir + 'DataPattern.xlsx', res_doc_pre)

                            wb = op.load_workbook(res_doc_pre)
                            for chart_stack in TOTAL_RES_list:
                                ws = wb[chart_stack[0]]
                                # очищаем старое
                                #ws.delete_rows(1,1000)
                                # вставляем данные
                                for r in dataframe_to_rows (chart_stack[1], index=False, header=True):
                                    ws.append(r)

                            wb.save(res_doc_pre)
                            wb.close()


                            res_doc_file_name = OUT_dir + 'Lama_tablo_data'
                            shutil.make_archive(res_doc_file_name, 'zip', cup_dir)

                        mes = 'Обновляй табло, архив в ауте'

                except:
                    mes = 'Ошибка! Что-то пошло не так:('

                # удаляем папку процесса
                if os.path.isdir(cup_dir) == True:
                    shutil.rmtree(cup_dir)
                chat(mes_sender, mes)

if 'тестовые' != '':

    def new_def ():
        def_name = 'new_def'
        comm_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
        mes_sender = comm_df[comm_df['name'] == def_name]['desc'].tolist()[0]
        cup_dir = CUPS_dir + def_name + '/'
        #file_list = pd.read_csv(cup_dir + 'par_list.csv')['par_list'].tolist()
        #file = str(file_list[0])
        try:
            
            mes = 'Готово!'

        except:
            mes = 'Ошибка! Что-то пошло не так:('

        # удаляем папку процесса
        if os.path.isdir(cup_dir) == True:
            shutil.rmtree(cup_dir)
        chat(mes_sender, mes)


    if 1 == 2:
        df = pd.read_csv(MIS_BOOK_doc)
        df = df[df['date'] == '2025-09-15']
        print(df)

        #res_df = df.groupby('status')['spec'].count()
        #g2 = df.groupby(['spec','status'])['mis_id'].count()
        #print(res_df)
        #print(g2)

        g3 = df.pivot_table(index='status', columns='spec', values='mis_id', aggfunc='count').reset_index()
        print(g3)

        columns = g3.columns.tolist()
        print(columns)

        data = []
        def tab_to_lists (row):
            row_list = []
            for col in columns:
                row_list.append(str(row[col]))
            data.append(row_list)

        g3.apply(tab_to_lists, axis=1)

        print(data)
        # расчёт максимальной длинны колонок 
        max_columns = [] # список максимальной длинны колонок 
        for col in zip(*data): 
            len_el = [] 
            [len_el.append(len(el)) for el in col]   
            max_columns.append(max(len_el))

        # вывод таблицы с колонками максимальной длинны строки каждого столбца 

        # печать шапки таблицы 
        for n, column in enumerate(columns): 
            print(f'{column:{max_columns[n]+1}}', end='') 
        print() 
 
        # печать разделителя шапки '=' 
        r = f'{"="*sum(max_columns)+"="*5}' 
        print(r[:-1]) 
        
        # печать тела таблицы 
        for el in data: 
            for n, col in enumerate(el): 
                print(f'{col:{max_columns[n]+1}}', end='') # выравнвание по правому краю > 
            print()

    if 1 == 2:
        con_info = {'host': 'vertica-sandbox.s.o3.ru',
                    'port': 5433,
                    'user': 'aramiso',
                    'password': 'Block254!',
                    'database': 'OLAP'}
        
        connection = vertica_python.connect(**con_info)
        cursor = connection.cursor()
        #cursor.execute("""SELECT  CPAN.SourceKey AS posting,
        #                            IFNULL(AGCRE.Comment,'-') AS comment
        #                            FROM    dwh_data.Anc_ClientPosting CPAN
        #                            LEFT JOIN dwh_data.Bridge_ClientReturn_Item_Exemplar_InventoryExemplar_AccountExemplar_ClientPosting_ClientReturnReason BRCRE USING(ClientPostingId)
        #                            LEFT JOIN dwh_data.AtrGrp_ClientReturn_Item_Exemplar_InventoryExemplar_AccountExemplar_ClientPosting_ClientReturnReason_Attributes AGCRE USING(ClientReturn_Item_Exemplar_InventoryExemplar_AccountExemplar_ClientPosting_ClientReturnReasonId)
        #
        #                            WHERE   CPAN.SourceKey IN 
        #                                                            (
        #                            ---------------------------------
        #                            '49261999-0097-1',
        #                            '52130733-0147-9',
        #                            '29726726-0147-2'
        #                                                            
        #                            ---------------------------------
        #                                                            ))"""
        #                )
        #cursor.execute("SELECT  TO_CHAR (at::timestamptz at TIME zone 'MSK', 'DD.MM.YYYY HH:MI:SS') AS ts, instance_id, item_id, supply_id FROM	wms_csharp_service_storage_all_new.WmsStorageMovementLogsAdded_MovementLog MOLO WHERE	warehouse_id = 18044249781000 AND instance_id = 22858980965 ORDER BY ts DESC")
        cursor.execute(open('1.txt').read())
        res = cursor.fetchall()
        print(res)
        connection.close()

        df = pd.DataFrame()
        cols_list = [d.name for d in cursor.description]
        for c in cols_list:
            df[c] = []

        for row in res:
            df.loc[len(df)] = row
        
        print(df)

    if 1 == 2:
        SoupScanit_df = pd.read_csv('SoupScanit.csv')
        MOVE_df = pd.read_csv('move.csv')

        res = COMPLAINT_scanit_move_analysis(SoupScanit_df, MOVE_df)

        print(res)

    if 1 == 2:
        print(int('01'))
        print(int('1 '))
        print(1 + \
              2 + \
              5)
