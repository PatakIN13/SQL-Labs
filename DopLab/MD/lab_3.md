# Дополнительная лабораторная работа No3
## Задача:
- имеются часть данных геномов 2 животных (какие животные - я озвучу чуть позже после вашего исследования) в текстовых файлах (2 файла с данными приложены к заданию)
- необходимо провести расчеты по сравнению “близости” двух геномов, используя последовательность символов-шинглов (k=2, 5, 9). Например, для последовательности аминокислот TCAGACTT при k=2 у нас получается набор элементов: {TC, CA, AG, GA, AC, CT, TT}
- для этого создайте модель данных в базе данных для хранения последовательностей для k=2, 5, 9.
- загрузите в ваши таблицы последовательности символов для k=2, 5, 9, используя знакомые вам механизмы парсинга текста (bash, Java, Python, SQL, Excel, AI ...)
- напишите один SQL запрос, который будет использовать формулу Жаккара для подсчета расстояния отдельно для последовательностей символов k=2, 5, 9.  
`𝐽 = (|𝐴 ⋂ 𝐵|)/(|𝐴 ⋃ 𝐵|)`

| Значение k | Значение J |
| ---------- | ---------- |
| 2 | ? |
| 5 | ? |
| 9 | ? |

## Создаем подключение к БД


```python
import psycopg2
from tabulate import tabulate

### Подключение к PostgreSQL
connection = psycopg2.connect(
    host="localhost",
    port="5432",
    database="",
    user="",
    password="",
)
### Создание курсора
cursor = connection.cursor()

print("Подключение к PostgreSQL успешно.")
```

    Подключение к PostgreSQL успешно.
    

## Создание схемы


```python
cursor.execute("CREATE SCHEMA IF NOT EXISTS ivan_patakin;")
connection.commit()

print("Схема 'ivan_patakin' создана.")
```

    Схема 'ivan_patakin' создана.
    

## Подготовка данных

```python
with open('../Data/Lab3/Genome_1-1.txt') as f:
    genome1 = f.read().strip()
with open('../Data/Lab3/Genome_2-1.txt') as f:
    genome2 = f.read().strip()
```


```python
def get_shingles(seq, k):
    return {seq[i:i+k] for i in range(len(seq)-k+1)}

shingles = {}
for k in (2, 5, 9):
    s1 = get_shingles(genome1, k)
    s2 = get_shingles(genome2, k)
    shingles[k] = (s1, s2)
```

## Создание таблиц


```python
for k in (2, 5, 9):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS ivan_patakin.shingles_k{k} (
            shingle TEXT NOT NULL,
            source INTEGER NOT NULL
        );
    """)

connection.commit()
```

## Заполнение таблиц


```python
for k in (2, 5, 9):
    data = [(s, 1) for s in shingles[k][0]] + [(s, 2) for s in shingles[k][1]]
    cursor.executemany(
        f"INSERT INTO ivan_patakin.shingles_k{k} (shingle, source) VALUES (%s, %s)", data
    )
connection.commit()
```

## Создание запросов


```python
query = """
WITH j2 AS (
  SELECT 
    (SELECT COUNT(DISTINCT a.shingle) FROM ivan_patakin.shingles_k2 a JOIN ivan_patakin.shingles_k2 b
     ON a.shingle=b.shingle WHERE a.source=1 AND b.source=2) AS inters,
    (SELECT COUNT(DISTINCT shingle) FROM (
         SELECT shingle FROM ivan_patakin.shingles_k2 WHERE source=1
         UNION
         SELECT shingle FROM ivan_patakin.shingles_k2 WHERE source=2
     )) AS uni
),
j5 AS (
  SELECT 
    (SELECT COUNT(DISTINCT a.shingle) FROM ivan_patakin.shingles_k5 a JOIN ivan_patakin.shingles_k5 b
     ON a.shingle=b.shingle WHERE a.source=1 AND b.source=2) AS inters,
    (SELECT COUNT(DISTINCT shingle) FROM (
         SELECT shingle FROM ivan_patakin.shingles_k5 WHERE source=1
         UNION
         SELECT shingle FROM ivan_patakin.shingles_k5 WHERE source=2
     )) AS uni
),
j9 AS (
  SELECT 
    (SELECT COUNT(DISTINCT a.shingle) FROM ivan_patakin.shingles_k9 a JOIN ivan_patakin.shingles_k9 b
     ON a.shingle=b.shingle WHERE a.source=1 AND b.source=2) AS inters,
    (SELECT COUNT(DISTINCT shingle) FROM (
         SELECT shingle FROM ivan_patakin.shingles_k9 WHERE source=1
         UNION
         SELECT shingle FROM ivan_patakin.shingles_k9 WHERE source=2
     )) AS uni
)
SELECT 2 as k, ROUND(1.0*j2.inters/j2.uni,6) as J FROM j2 UNION ALL
SELECT 5 as k, ROUND(1.0*j5.inters/j5.uni,6) FROM j5 UNION ALL
SELECT 9 as k, ROUND(1.0*j9.inters/j9.uni,6) FROM j9;
"""
```


```python
cursor.execute(query)
rows = cursor.fetchall()

headers = ["K", "Jaccard"]
print(tabulate(rows, headers=headers))
```

      K    Jaccard
    ---  ---------
      2   1
      5   1
      9   0.398709
    


```python
cursor.execute("""
DROP TABLE IF EXISTS ivan_patakin.shingles_k2;
DROP TABLE IF EXISTS ivan_patakin.shingles_k5;
DROP TABLE IF EXISTS ivan_patakin.shingles_k9;
""")

connection.commit()
print("Таблицы удалены.")
```

    Таблицы удалены.
    


```python
cursor.close()
connection.close()
```
