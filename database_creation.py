import sqlite3

# Создание подключения к базе данных
# conn = sqlite3.connect('database_rus_2.db')
conn = sqlite3.connect('database_eng_2.db')
# Создание курсора для выполнения SQL-запросов
cursor = conn.cursor()

# Создание таблицы Text
cursor.execute('''
CREATE TABLE IF NOT EXISTS Text (
    TextID INTEGER PRIMARY KEY AUTOINCREMENT,
    Text TEXT,
    ClassID INTEGER,
    CCS_ID TEXT,
    GeoID TEXT,    
    Date TEXT,
    PersonID TEXT,
    OrgID TEXT,
    EventID TEXT,
    FOREIGN KEY (ClassID) REFERENCES Class (ClassID),
    FOREIGN KEY (CCS_ID) REFERENCES CountryCityState (CCS_ID),
    FOREIGN KEY (GeoID) REFERENCES GeographicalObjects (GeoID),
    FOREIGN KEY (PersonID) REFERENCES People (PersonID),
    FOREIGN KEY (OrgID) REFERENCES Organizations (OrgID),
    FOREIGN KEY (EventID) REFERENCES Events (EventID)
);
''')

# Создание таблицы Class
cursor.execute('''
CREATE TABLE IF NOT EXISTS Class (
    ClassID INTEGER PRIMARY KEY AUTOINCREMENT,
    ClassName TEXT
);
''')

# Создание таблицы Country, city, state
cursor.execute('''
CREATE TABLE IF NOT EXISTS CountryCityState (
    CCS_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    CCS_Name TEXT
);
''')
# Создание таблицы GeographicalObjects
cursor.execute('''
CREATE TABLE IF NOT EXISTS GeographicalObjects (
    GeoID INTEGER PRIMARY KEY AUTOINCREMENT,
    GeographicalObjectName TEXT
);
''')

# Создание таблицы People
cursor.execute('''
CREATE TABLE IF NOT EXISTS People (
    PersonID INTEGER PRIMARY KEY AUTOINCREMENT,
    PersonName TEXT
);
''')

# Создание таблицы Date
cursor.execute('''
CREATE TABLE IF NOT EXISTS Date (
    Date TEXT,
    TextID,
    FOREIGN KEY (TextID) REFERENCES Text (TextID)
);
''')

# Создание таблицы Organizations
cursor.execute('''
CREATE TABLE IF NOT EXISTS Organizations (
    OrgID INTEGER PRIMARY KEY AUTOINCREMENT,
    OrgName TEXT
);
''')

# Создание таблицы Events
cursor.execute('''
CREATE TABLE IF NOT EXISTS Events (
    EventID INTEGER PRIMARY KEY AUTOINCREMENT,
    EventName TEXT
);
''')

# Сохранение изменений и закрытие подключения
conn.commit()
conn.close()
