import os
import shutil
from tkinter import *
from tkinter import scrolledtext
from tkinter import filedialog
from tkinter import Menu
from tkinter.ttk import Combobox
import pandas as pd
import datetime as dt

from functions_SET import start_lama
from functions_SET import pin_control
from functions_SET import ver_name
from functions_SET import chat
from functions_SET import screen_cup
from functions_SET import screen_mis_book
from functions_SET import Repeater
from functions_SET import def_initiator
from functions_SET import def_kicker
from functions_SET import def_starter
from functions_SET import MIS_GO


from variables import MES_dir
from variables import MES_Send_dir
from variables import LamaReins_doc
from variables import pins_doc


start_lama()
#---------------------------------------------------------------------------------
if 'ПЕРЕМЕННЫЕ' != '':
    window = Tk()

if pin_control() == 'n':
    window.title('Lama ' + ver_name())
    window.geometry('300x100')
    lbl_activate = Label(window, text='Введите пин-код и перезапустите приложение')
    lbl_activate.grid(column=0, row=0)
    ent_activate_par = Entry(window, width=20)
    ent_activate_par.grid(column=0, row=1)
    def btn_activate_send_click():
        pd.DataFrame({'pin':[ent_activate_par.get()]}).to_csv(pins_doc, mode='a', index= False , header= False)
    btn_activate_send = Button(window, text='отправить пин', bg='black', fg='white', command=btn_activate_send_click)
    btn_activate_send.grid(column=0, row=2)

    window.mainloop()

else:
    #---------------------------------------------------------------------------------
    if 'ИНТЕРФЕЙС' != '':
        window.title('Lama ' + ver_name())
        window.geometry('1050x550')

        if 'лейбл полоска' != '':
            lbl_stripe = Label(window, text='  ')
            lbl_stripe.grid(column=0, row=0)

        if 'лейбл чата' != '':
            lbl_chat = Label(window, text='         Ч А Т         ', font = ('Arial Bold', 10, 'bold'), bg='black', fg='white')
            lbl_chat.grid(column=1, row=0)

        if 'ЧАТ' != '':
            MessageFrame = scrolledtext.ScrolledText(window, width=45, height=20, font = ('Arial', 9))
            MessageFrame.grid(column=1, row=1)
            
            def CHAT_update_messege():
                new_mes_list = os.listdir(MES_dir)
                new_mes_list.remove('send')
                if len(new_mes_list) > 0:
                    new_mes_file = MES_dir + new_mes_list[0]
                    new_mes_txt = open(new_mes_file).read()
                    mes_feed_today_doc = MES_Send_dir + 'LamaChat_' + dt.datetime.now().strftime('%Y.%m.%d') + '.txt'
                    if os.path.isfile(mes_feed_today_doc) == True:
                        mes_feed_today_txt = open(mes_feed_today_doc).read()
                    else: mes_feed_today_txt = ''
                    
                    mes_feed = f'{mes_feed_today_txt}\n\n\n{new_mes_txt}'

                    f = open(mes_feed_today_doc,'w')
                    f.write(mes_feed)
                    f.close

                    if os.path.isfile(new_mes_file) == True:
                        os.remove(new_mes_file)

                    MessageFrame.delete(1.0, END)
                    MessageFrame.insert(INSERT, mes_feed)
                    MessageFrame.see('end')

            CHAT_update_messege()
            
        if 'лейбл команды' != '':
            lbl_commangs = Label(window, text=f'\nкоманда')
            lbl_commangs.grid(column=1, row=2)

        if 'КОМАНДЫ' != '':
            combo_commands = Combobox(window, width=45)
            LR_commands_df = pd.read_excel(LamaReins_doc, sheet_name='commands')
            combo_commands['values'] = tuple(LR_commands_df[LR_commands_df['gui'] == 'combo_commands']['desc'].tolist())
            combo_commands.current(0)  # установите вариант по умолчанию
            combo_commands.grid(column=1, row=3)

        if 'лейбл параметра' != '':
            lbl_com_par = Label(window, text='параметр')
            lbl_com_par.grid(column=1, row=4)

        if 'ПОЛЕ ВВОДА параметра' !='':
            ent_com_par = Entry(window, width=45)
            ent_com_par.grid(column=1, row=5)

        if 'лейбл кнопки го' != '':
            lbl_go = Label(window, text='')
            lbl_go.grid(column=1, row=6)

        if 'GO!' != '':
            def go_click ():
                def_desc = combo_commands.get()
                def_par = ent_com_par.get()
                def_initiator(def_desc,[def_par])
                print(def_desc)
                print(def_par)
                # очищаем поле
                ent_com_par.delete(0,'end')
                combo_commands.delete(0,'end')


            btn_GO = Button(window, text='GO!', bg='black', fg='white', command=go_click)
            btn_GO.grid(column=1, row=7)

        if 'лейбл кнопки LOAD' != '':
            lbl_load = Label(window, text='')
            lbl_load.grid(column=1, row=8)

        if 'LOAD' != '':
            def files_for_LOAD():
                files = filedialog.askopenfilenames(title='выбери файлы, с которыми Lama занет, что делать')
                file_list = list(files)
                def_initiator('NOSAMO', file_list)

            btn_LOAD = Button(window, text='   LOAD   ', bg='grey', fg='black', command=files_for_LOAD)
            btn_LOAD.grid(column=1, row=9)    

        # - - - - - - - 
        if 'лейбл ТЕКУЩИЕ ПРОЦЕССЫ' != '':
            lbl_cup = Label(window, width=25, text='  ТЕКУЩИЕ ПРОЦЕССЫ  ', font = ('Arial Bold', 10, 'bold'), bg='grey', fg='black')
            lbl_cup.grid(column=2, row=0)

        if 'ТЕКУЩИЕ ПРОЦЕССЫ' != '':
            def screen_cup_update():
                screen_cup_content = screen_cup()
                lbl_cup = Label(window, text=screen_cup_content, width=25, height=20, anchor='n', justify='center')
                lbl_cup.grid(column=2, row=1)

        if 'лейбл КНИГА ОШИБОК' != '':
            lbl_mis_book = Label(window, width=65, text='  КНИГА ОШИБОК  ', font = ('Arial Bold', 10, 'bold'), bg='black', fg='red')
            lbl_mis_book.grid(column=3, row=0)

        if 'КНИГА ОШИБОК' != '':
            def screen_mis_book_update():
                screen_mis_book_content = screen_mis_book()
                lbl_mis_book = Label(window, width=65, height=20, text=screen_mis_book_content, font = ('Consolas', 10), anchor='n', justify='left')
                lbl_mis_book.grid(column=3, row=1)

    #---------------------------------------------------------------------------------
    if 'ДВИЖОК' != '':
        Repeater(2, CHAT_update_messege)
        Repeater(1, screen_cup_update)
        Repeater(3, screen_mis_book_update)
        Repeater(1, def_kicker)
        Repeater(1, def_starter)
        Repeater(3, MIS_GO)
        #MIS_GO()
        window.mainloop() 