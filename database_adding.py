import sqlite3
import pandas as pd

df_class_text_ner_rus = pd.read_json("df_class_text_ner_rus.json")
df_class_text_ner_eng = pd.read_json("df_class_text_ner_eng.json")

# global lst_Countries_Cities_States
# global lst_Geographic_objects
# global lst_Date
# global lst_People
# global lst_Organizations
# global lst_events

def many_to_one_list(lst):
    lst_new = []
    for lst_ in lst:
        for elem in lst_:
            lst_new.append(elem)
    return lst_new


def lists_extraction(df_class_text_ner):

    lst_Countries_Cities_States = list(set(many_to_one_list(df_class_text_ner['Countries, Cities, States'])))
    print(lst_Countries_Cities_States)
    # Провести еще дополнительно лемматизацию элементов
    # lst_Geographic_objects = list(set(many_to_one_list(df_class_text_ner['Geographic objects'])))
    lst_Geographic_objects = many_to_one_list(df_class_text_ner['Geographic objects'])
    print(lst_Geographic_objects)
    # lst_Date = list(set(many_to_one_list(df_class_text_ner['Date'])))
    lst_Date = many_to_one_list(df_class_text_ner['Date'])
    print(lst_Date)
    # lst_People = list(set(many_to_one_list(df_class_text_ner['People'])))
    lst_People = many_to_one_list(df_class_text_ner['People'])
    print(lst_People)
    # lst_Organizations = list(set(many_to_one_list(df_class_text_ner['Organizations'])))
    lst_Organizations = many_to_one_list(df_class_text_ner['Organizations'])
    print(lst_Organizations)
    # lst_events = list(set(many_to_one_list(df_class_text_ner['Events'])))
    lst_events = many_to_one_list(df_class_text_ner['Events'])
    print(lst_events)
    return lst_Countries_Cities_States, lst_Geographic_objects, lst_Date, lst_People, lst_Organizations, lst_events


# lst_Countries_Cities_States, lst_Geographic_objects, lst_Date, lst_People, lst_Organizations, lst_events = lists_extraction(df_class_text_ner_rus)
lst_Countries_Cities_States, lst_Geographic_objects, lst_Date, lst_People, lst_Organizations, lst_events = lists_extraction(df_class_text_ner_eng)
# Создание подключения к базе данных
# conn = sqlite3.connect('database_rus_2.db')
conn = sqlite3.connect('database_eng_2.db')
# Создание курсора для выполнения SQL-запросов
cursor = conn.cursor()

def extract_ids(df_class_text_ner, col, row_idx):
    ID_lst = []
    print(df_class_text_ner[col].iloc[row_idx])
    if col == 'Countries, Cities, States':
        table_name = 'CountryCityState'
        ID_name = 'CCS_ID'
        Name = 'CCS_Name'
    elif col == 'Geographic objects':
        table_name = 'GeographicalObjects'
        ID_name = 'GeoID'
        Name = 'GeographicalObjectName'
    elif col == 'People':
        table_name = 'People'
        ID_name = 'PersonID'
        Name = 'PersonName'
    elif col == 'Organizations':
        table_name = 'Organizations'
        ID_name = 'OrgID'
        Name = 'OrgName'
    elif col == 'Events':
        table_name = 'Events'
        ID_name = 'EventID'
        Name = 'EventName'

    for name in df_class_text_ner[col].iloc[row_idx]:
        print(name)
        query = f"SELECT {ID_name} ID FROM {table_name} WHERE {Name} = ?"
        print(query)
        cursor.execute(f"SELECT {ID_name} ID FROM {table_name} WHERE {Name} = ?", (name,))
        result = cursor.fetchone()
        print(result)
        ID = result[0]
        ID_lst.append(str(ID))
    ID_lst_str = ",".join(set(ID_lst))  # Преобразование списка в строку
    print(ID_lst_str)
    return ID_lst_str

# Функция для добавления данных в таблицы
def insert_data(lst_Countries_Cities_States, lst_Geographic_objects, lst_Date,
                lst_People, lst_Organizations, lst_events, df_class_text_ner):

    # # Вставка данных в таблицу Class
    cursor.execute("INSERT INTO Class (ClassName) VALUES (?)", ("Природные ЧС",))
    cursor.execute("INSERT INTO Class (ClassName) VALUES (?)", ("Теракты/криминал",))
    cursor.execute("INSERT INTO Class (ClassName) VALUES (?)", ("Митинги/протесты",))
    cursor.execute("INSERT INTO Class (ClassName) VALUES (?)", ("Угроза",))

    # Вставка данных в таблицу CountryCityState
    for elem in lst_Countries_Cities_States:
        cursor.execute("INSERT INTO CountryCityState (CCS_Name) VALUES (?)", (elem, ))

    # Вставка данных в таблицу GeographicalObjects
    for elem in lst_Geographic_objects:
        cursor.execute("INSERT INTO GeographicalObjects (GeographicalObjectName) VALUES (?)", (elem,))

    #date

    # Вставка данных в таблицу People
    for elem in lst_People:
        cursor.execute("INSERT INTO People (PersonName) VALUES (?)", (elem,))

    # Вставка данных в таблицу Organizations
    for elem in lst_Organizations:
        cursor.execute("INSERT INTO Organizations (OrgName) VALUES (?)", (elem,))

    # Вставка данных в таблицу Events
    for elem in lst_events:
        cursor.execute("INSERT INTO Events (EventName) VALUES (?)", (elem,))

    conn.commit()

    # Вставка данных в таблицу Text
    for row_idx in range(df_class_text_ner.shape[0]):
        CCS_ID_lst_str = extract_ids(df_class_text_ner,"Countries, Cities, States", row_idx)
        GeoID_lst_str = extract_ids(df_class_text_ner, "Geographic objects", row_idx)
        PersonID_lst_str = extract_ids(df_class_text_ner, "People", row_idx)
        OrgID_lst_str = extract_ids(df_class_text_ner, "Organizations", row_idx)
        EventID_lst_str = extract_ids(df_class_text_ner, "Events", row_idx)
        if len(list(df_class_text_ner["Date"].iloc[row_idx])) == 0:
            Date = "None"
        else:
            Date = list(df_class_text_ner["Date"].iloc[row_idx])[0]
        print(df_class_text_ner["Text"].iloc[row_idx])
        print(df_class_text_ner["Class"].iloc[row_idx])
        print("CCS_ID_lst_str", CCS_ID_lst_str)
        print("GeoID_lst_str", GeoID_lst_str)
        print(df_class_text_ner["Date"].iloc[row_idx])
        print("PersonID_lst_str", PersonID_lst_str)
        print("OrgID_lst_str", OrgID_lst_str)
        print("EventID_lst_str", EventID_lst_str)
        # print("CCS_ID_lst_str", GeoID_lst_str, type(GeoID_lst_str))
        # CCS_ID_lst = []
        # for CCS_Name in df_class_text_ner["Countries, Cities, States"].iloc[row_idx]:
        #     cursor.execute("SELECT CCS_ID FROM CountryCityState WHERE CCS_Name = ?", (CCS_Name,))
        #     result = cursor.fetchone()
        #     CCS_ID = result[0]
        #     CCS_ID_lst.append(CCS_ID)
        # list_data = ["item1", "item2", "item3"]
        # CCS_ID_lst_str = ",".join(CCS_ID_lst)  # Преобразование списка в строку

        cursor.execute('''
            INSERT INTO Text (Text, ClassID, CCS_ID, GeoID, Date, PersonID, OrgID, EventID)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            df_class_text_ner["Text"].iloc[row_idx],
            int(df_class_text_ner["Class"].iloc[row_idx]),
            CCS_ID_lst_str,
            GeoID_lst_str,
            # переделать, потому что дат может быть много, а здесь только первая берётся !!!
            Date,
            PersonID_lst_str,
            OrgID_lst_str,
            EventID_lst_str
        ))
        #
        # cursor.execute('''
        #     INSERT INTO Text (Text, ClassID, CCS_ID, GeoID, Date, PersonID, OrgID, EventID)
        #     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        # ''', (
        #     "Sample text for document 2",
        #     2,  # ClassID for "Class B"
        #     2,  # CCS_ID for "Canada"
        #     2,  # GeoID for "Toronto"
        #     "2024-12-27",
        #     2,  # PersonID for "Jane Smith"
        #     2,  # OrgID for "Organization B"
        #     2   # EventID for "Event B"
        # ))

# def insert_data_in_text(df_class_text_ner):
#     # Вставка данных в таблицу Text
#     for row_idx in range(df_class_text_ner.shape[0]):
#         CCS_ID_lst_str = extract_ids(df_class_text_ner["Countries, Cities, States"], row_idx)
#         GeoID_lst_str = extract_ids(df_class_text_ner["Geographic objects"], row_idx)
#         PersonID_lst_str = extract_ids(df_class_text_ner["People"], row_idx)
#         OrgID_lst_str = extract_ids(df_class_text_ner["Organizations"], row_idx)
#         EventID_lst_str = extract_ids(df_class_text_ner["Events"], row_idx)
#         # CCS_ID_lst = []
#         # for CCS_Name in df_class_text_ner["Countries, Cities, States"].iloc[row_idx]:
#         #     cursor.execute("SELECT CCS_ID FROM CountryCityState WHERE CCS_Name = ?", (CCS_Name,))
#         #     result = cursor.fetchone()
#         #     CCS_ID = result[0]
#         #     CCS_ID_lst.append(CCS_ID)
#         # list_data = ["item1", "item2", "item3"]
#         # CCS_ID_lst_str = ",".join(CCS_ID_lst)  # Преобразование списка в строку
#
#         cursor.execute('''
#                 INSERT INTO Text (Text, ClassID, CCS_ID, GeoID, Date, PersonID, OrgID, EventID)
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#             ''', (
#             df_class_text_ner["Text"].iloc[row_idx],
#             df_class_text_ner["Class"].iloc[row_idx],
#             CCS_ID_lst_str,
#             GeoID_lst_str,
#             df_class_text_ner["Date"].iloc[row_idx],
#             PersonID_lst_str,
#             OrgID_lst_str,
#             EventID_lst_str
#         ))
#         #
#         # cursor.execute('''
#         #     INSERT INTO Text (Text, ClassID, CCS_ID, GeoID, Date, PersonID, OrgID, EventID)
#         #     VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#         # ''', (
#         #     "Sample text for document 2",
#         #     2,  # ClassID for "Class B"
#         #     2,  # CCS_ID for "Canada"
#         #     2,  # GeoID for "Toronto"
#         #     "2024-12-27",
#         #     2,  # PersonID for "Jane Smith"
#         #     2,  # OrgID for "Organization B"
#         #     2   # EventID for "Event B"
#         # ))

# Вызов функции для вставки данных
# insert_data(lst_Countries_Cities_States, lst_Geographic_objects, lst_Date,
#                 lst_People, lst_Organizations, lst_events, df_class_text_ner_rus)


insert_data(lst_Countries_Cities_States, lst_Geographic_objects, lst_Date,
                lst_People, lst_Organizations, lst_events, df_class_text_ner_eng)

# Сохранение изменений и закрытие подключения
conn.commit()
conn.close()
