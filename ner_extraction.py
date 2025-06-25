import pandas as pd
# import locationtagger
import spacy
from date_spacy import find_dates
import json

df_class_text_rus = pd.read_json("class_text_df_rus.json")
df_class_text_eng = pd.read_json("class_text_df_eng.json")

# df_class_text_ner_rus = pd.DataFrame(data = {'Text': [], 'Class': [], 'Countries, Cities, States': [],
#      'Geographic objects': [], 'Date': [], 'People': [], 'Organizations': [],
#      'Names of natural disasters': []})
df_class_text_ner_rus = pd.DataFrame(columns = ["Text", "Class", "Countries, Cities, States", "Geographic objects", "Date",
                                                "People", "Organizations", "Events"])

# df_class_text_ner_eng = pd.DataFrame(data = {'Text': [], 'Class': [], 'Countries, Cities, States': [],
#      'Geographic objects': [], 'Date': [], 'People': [], 'Organizations': [],
#      'Names of natural disasters': []})
df_class_text_ner_eng = pd.DataFrame(columns = ["Text", "Class", "Countries, Cities, States", "Geographic objects", "Date",
                                                "People", "Organizations", "Events"])

print(df_class_text_rus)
print(df_class_text_rus.shape)
# print(df_class_text_ner)

def ner_extraction(df_class_text, df_class_text_ner, lang):
    # Загрузка модели spaCy
    # nlp = spacy.blank(lang)
    if lang == 'rus':
        nlp = spacy.load("ru_core_news_sm")
    elif lang == 'eng':
        nlp = spacy.load("en_core_web_sm")

    # Добавление компонента для выделения дат в конвейер
    # dates = nlp.add_pipe('find_dates')


    for i in range(df_class_text.shape[0]):
        for j in range(df_class_text.shape[1]):
            text = df_class_text.iloc[i, j]
            # print(text)
            if text != "None" and type(text) != float:
                doc = nlp(text)
                print(doc.ents)
                lst_Countries_Cities_States = []
                lst_Geographic_objects = []
                lst_Date = []
                lst_People = []
                lst_Organizations = []
                lst_events = []
                for ent in doc.ents:
                    if ent.label_ == "GPE":  # Локации (города, страны)
                        lst_Countries_Cities_States.append(ent.text)
                    elif ent.label_ == "LOC":  # Геополитические единицы
                        lst_Geographic_objects.append(ent.text)
                    elif ent.label_ == "DATE":  # Даты
                        lst_Date.append(ent.text)
                    elif ent.label_ == "PER":  # Персоны
                        lst_People.append(ent.text)
                    elif ent.label_ == "ORG":  # Организации
                        lst_Organizations.append(ent.text)
                    elif ent.label_ == "EVENT":  # События (можно использовать для стихийных бедствий)
                        lst_events.append(ent.text)
                # print(lst_Countries_Cities_States)
                index = df_class_text_ner.shape[0]
                df_class_text_ner.at[index, 'Text'] = text
                df_class_text_ner.at[index, 'Class'] = j
                df_class_text_ner.at[index, 'Countries, Cities, States'] = lst_Countries_Cities_States
                df_class_text_ner.at[index, 'Geographic objects'] = lst_Geographic_objects
                df_class_text_ner.at[index, 'Date'] = lst_Date
                df_class_text_ner.at[index, 'People'] = lst_People
                df_class_text_ner.at[index, 'Organizations'] = lst_Organizations
                df_class_text_ner.at[index, 'Events'] = lst_events

                # df_class_text_ner['Countries, Cities, States'].loc[df_class_text_ner.shape[0]] = lst_Countries_Cities_States
                # df_class_text_ner['Geographic objects'].loc[df_class_text_ner.shape[0]] = lst_Geographic_objects
                # df_class_text_ner['Date'].loc[df_class_text_ner.shape[0]] = lst_Date
                # df_class_text_ner['People'].loc[df_class_text_ner.shape[0]] = lst_People
                # df_class_text_ner['Organizations'].loc[df_class_text_ner.shape[0]] = lst_Organizations
                # df_class_text_ner['Names of natural disasters'].loc[df_class_text_ner.shape[0]] = lst_Names_of_natural_disasters
                print(df_class_text_ner)

                # Применение функции к каждому тексту в DataFrame
                # df_class_text['entities'] = extract_entities(nlp, text)

                # df_class_text_ner['Text'].loc[i] = text
                # # print(df_class_text_rus.iloc[i][j])
                # df_class_text_ner['Class'].loc[i] = j
                #
                # # Обработка текста с помощью конвейера для выделения сущностей
                # doc = nlp('Эрдоган принял помощь российских спасателей 21.05.2006')
                # print(doc.ents)
                #
                # # Выделение дат
                # # Итерация по объектам в документе и доступ к специальному расширению даты
                # for ent in doc.ents:
                #     print("ent", ent)
                #     # Выделение стран и городов
                #     if (ent.label_ == 'GPE'):
                #         df_class_text_ner['Countries, Cities, States'].loc[i] = ent.text
                #     elif (ent.label_ == 'LOC'):
                #         df_class_text_ner['Geographic objects'].loc[i] = ent.text
                #
                #     print("dates", dates(doc))
                #     if ent.label_ == "DATE":
                #         df_class_text_ner['Date'].loc[i] = ent._.date
                #         # print(f"Text: {ent.text} -> Parsed Date: {ent._.date}")
                #
                # # Выделение личностей
                # df_class_text_ner['People'].loc[i] = 'None'
                # # Выделение организаций
                # df_class_text_ner['Organizations'].loc[i] = 'None'
                # # Выделение названий стихийных бедствий
                # df_class_text_ner['Names of natural disasters'].loc[i] = 'None'

    df_class_text_ner = df_class_text_ner.to_dict()
    #
    with open(f'df_class_text_ner_{lang}.json', 'w', encoding='utf-8') as outfile:
        json.dump(df_class_text_ner, outfile, ensure_ascii=False, indent=4)

ner_extraction(df_class_text_rus, df_class_text_ner_rus, 'rus')
ner_extraction(df_class_text_eng, df_class_text_ner_eng, 'eng')

exit()

# Выделение стран и городов
list_of_countries = list(pd.read_csv("Countries.csv", on_bad_lines='skip')["Countries"])
# print(list_of_countries)

list_of_cities_ru = list(pd.read_csv("Cities.csv", on_bad_lines='skip')["Города"])
print(list_of_cities_ru)
list_of_cities_en = list(pd.read_csv("Cities.csv", on_bad_lines='skip')["Cities"]) + list(pd.read_csv("Chinese_cities.csv", on_bad_lines='skip')["Chinese_cities"])
print(list_of_cities_en)

for text in df_class_text_eng["Природные ЧС"]:
    if text != "None":
        # print("text\n", text)
        # extracting entities
        place_entity = locationtagger.find_locations(text = text)

        # getting all countries
        print("The countries in text : ")
        list_countries_in_text = place_entity.countries
        list_countries_in_text_filtered = []
        for i in list_countries_in_text:
            if i in list_of_countries:
                list_countries_in_text_filtered.append(i)
        print("list_countries_in_text", list_countries_in_text)
        print("list_countries_in_text_filtered", list_countries_in_text_filtered)

        # # getting all states
        # print("The states in text : ")
        # print(place_entity.regions)
        #
        # getting all cities
        print("The cities in text : ")
        print(place_entity.cities)
        list_cities_in_text = place_entity.cities
        list_cities_in_text_filtered = []
        for i in list_cities_in_text:
            if i in list_of_cities_en:
                list_cities_in_text_filtered.append(i)
        print("list_countries_in_text", list_cities_in_text)
        print("list_countries_in_text_filtered", list_cities_in_text_filtered)

# Выделение дат

# Загрузка модели spaCy
nlp = spacy.blank('en')

# Добавление компонента в конвейер
nlp.add_pipe('find_dates')



for text in df_class_text_eng["Природные ЧС"]:
    if text != "None":
        # Обработка текста с помощью конвейера
        doc = nlp(f"""{text}""")

        # Итерация по объектам в документе и доступ к специальному расширению даты
        for ent in doc.ents:
            if ent.label_ == "DATE":
                print(f"Text: {ent.text} -> Parsed Date: {ent._.date}")
