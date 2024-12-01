import requests
import pandas as pd
import sqlite3

api_key = '82e2bcfeb2aa8ead419ae527e10ee2f3'
cities = ['Cairo', 'London', 'Beirut', 'Amman', 'Luxor']

conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        temperature REAL,
        humidity REAL,
        pressure REAL,
        weather TEXT
    )
''')

weather_data_list = []

for city in cities:
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        weather_data = {
            'city': data['name'],
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'weather': data['weather'][0]['description'],
        }

        weather_data_list.append(weather_data)  
        print(weather_data)  
    else:
        print(f"Failed to retrieve data for {city}: {response.status_code}")

df = pd.DataFrame(weather_data_list)

df = df.fillna({'temperature': 0, 'humidity': 0, 'pressure': 0, 'weather': 'Unknown'})

df['temperature'] = df['temperature'].astype(float)
df['humidity'] = df['humidity'].astype(float)
df['pressure'] = df['pressure'].astype(float)

print("Cleaned Data: ")
print(df)

df.to_csv("weather_data.csv", index=False)

df.to_sql('weather', conn, if_exists='append', index=False)

conn.commit()

cursor.execute('SELECT * FROM weather')
rows = cursor.fetchall()

for row in rows:
    print(row)

conn.close()

print("Data has been successfully inserted into the database.")