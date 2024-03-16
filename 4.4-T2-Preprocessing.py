import pandas as pd
from datetime import datetime as dt

path_original = './csv/'
path_changed = './csv_changed/'
file_customers = 'customers.csv'
file_customers_changed = 'customers_ch.csv'
file_organizations = 'organizations.csv'
file_organizations_changed = 'organizations_ch.csv'
file_people = 'people.csv'
file_people_changed = 'people_ch.csv'
group_quantity = 10

# Читаем CSV-файлы
df_customers = pd.read_csv(path_original+file_customers, sep=',')
df_organizations = pd.read_csv(path_original+file_organizations, sep=',')
df_people = pd.read_csv(path_original+file_people, sep=',')

# Функция для создания столбца с номерами групп для датафрэйма
def group_splitting(df):
    df['Group'] = [i // (int(len(df)) // group_quantity) + 1 for i in range(int(len(df)))]
    return df

# Функция для добавления столбца с годом подписки
def year_insert(df):
    subscription_year = []
    for date in df['Subscription Date']:
          subscription_year.append(dt.strptime(date, '%Y-%m-%d').year)
    df['Subscription Year'] = subscription_year
    return df

# Добавление столбца с номерами групп и столбца с годом подписки в файл customers.csv
year_insert(group_splitting(df_customers)).to_csv(path_changed+file_customers_changed, index=False)
# Добавление столбца с номерами групп в файл organizations.csv
group_splitting(df_organizations).to_csv(path_changed+file_organizations_changed, index=False)
# Добавление столбца с номерами групп в файл people.csv
group_splitting(df_people).to_csv(path_changed+file_people_changed, index=False)
