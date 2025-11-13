import datetime as dt
import pandas as pd



LF_dir = './_LamaFiles/'
OUT_dir = './OUT/'



if 'ЭСТАФЕТА' != '':
    RELAY_dir = LF_dir + '_relay/'

    if 'настройки' != '':
        SET_dir = RELAY_dir + 'settings/'
        kick_doc = SET_dir + 'starter_kick_plug.txt'
        LamaReins_doc = SET_dir + 'LamaReins.xlsx'
        ChromeDrider_doc = SET_dir + 'chromedriver.exe'
        MojoSample_doc = SET_dir + 'MojoSample.xlsx'
        pins_doc = SET_dir + 'pins.csv'


    if 'сообщения' != '':
        MES_dir = RELAY_dir + 'messages/'
        MES_Send_dir = MES_dir + 'send/'

    if 'ошибки' != '':
        MIS_dir = RELAY_dir + 'MIS/'
        MIS_INID_doc = MIS_dir + 'Lama_MIS_Initial_data.csv'
        MIS_BOOK_doc = MIS_dir + 'Lama_MIS_Book.csv'
        MIS_MF_doc = MIS_dir + 'Lama_MF.xlsx'
        QCS_pattern_doc = MIS_dir + 'QCS_Pattern.xlsx'

    if 'жалобы' != '':
        COM_dir = RELAY_dir + 'COMPLAINT/'
        COMPLAINT__SoupPattern_doc = COM_dir + 'COMPLAINT__SoupPattern.xlsx'

    if 'ОТЧЁТЫ' !='':
        RELAY_REP_LARGE_SIZE_dir = RELAY_dir + 'REP_LARGE_SIZE/'
        RELAY_TABLO_dir = RELAY_dir + 'TABLO/'
        
        



if 'ДАННЫЕ' != '':
    D_dir = LF_dir + 'data__' + dt.datetime.now().strftime('%Y.%m.%d') + '/'

    if 'ИНТЕРФЕЙС' != '':
        GUI_TABLO__MisBookPivot_doc = D_dir + 'GUI_TABLO__MisBookPivot.txt'
        GUI_TABLO__MisInProgress_doc = D_dir + 'GUI_TABLO__MisInProgress.txt'

    if 'Жалобы' != '':
        COMPLAINT__SoupToGO_doc = D_dir + 'COMPLAINT__SoupToGo.csv'
        COMPLAINT__Soup_Scanit_doc = D_dir + 'COMPLAINT__Soup_Scanit.csv'
        COMPLAINT__Soup_Scanit_Stock_doc = D_dir + 'COMPLAINT__Soup_Scanit_Stock.csv'
        COMPLAINT__Dbeaver_doc = D_dir + 'COMPLAINT__Dbeaver.csv'

    if 'ВНЕ ТЯ' != '':
        REP_LARGE_SIZE__SSRep_doc = D_dir + 'REP_LARGE_SIZE__SSRep.csv'
        REP_LARGE_SIZE__BDUL_Item_doc = D_dir + 'REP_LARGE_SIZE__BDUL_Item.csv'
        REP_LARGE_SIZE__BDUL_Tag_doc = D_dir + 'REP_LARGE_SIZE__BDUL_Tag.csv'

    if 'ТАБЛО' != '':
        TABLO_Registry_doc = D_dir + 'TABLO__Registry.csv'
        TABLO_Settings_doc = D_dir + 'TABLO__Sittings.csv'

    if 'MOJO' !='':
        MOJO_parse_check_doc = D_dir + 'MOJO__parse_check.csv'
        MOJO_doc = D_dir + 'MOJO__base.csv'



    QCS_doc = D_dir + 'QCS.csv'

if 'CUPS' != '':
    CUPS_dir = LF_dir + 'cups/'
    QUEUE_dir = CUPS_dir + '_queue/'

if 'данные для Lama Start' !='':
    LAMA_archive_cup_dir = 'C:/LAMA_archive_cup/'


    # папки на создание
    START_LAMA_DirCreating_list = [CUPS_dir,
                                    QUEUE_dir,
                                    OUT_dir,
                                    D_dir]
    
if 'ПАРАМЕТРЫ'!='':
    df_par = pd.read_excel(LamaReins_doc, sheet_name='params')

    PARSE_MOJO__num_threads = int(df_par[df_par['par'] == 'PARSE__mojo_num_threads']['val'].tolist()[0])
    PARSE__mojo_iter_duration_sec = int(df_par[df_par['par'] == 'PARSE__mojo_iter_duration_sec']['val'].tolist()[0])

    MIS_BOOK__duplicate_check_4_key_list_code = df_par[df_par['par'] == 'MIS_BOOK__duplicate_check_4_key_list_code']['val'].tolist()[0]
    MIS_boost_keys_list_code = str(df_par[df_par['par'] == 'MIS_boost_keys_list_code']['val'].tolist()[0])