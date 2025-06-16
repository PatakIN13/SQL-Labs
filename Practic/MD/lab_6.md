# Лабораторная работа No6
## Задача:
Имеется модель данных для управления библиотекой.
1. Самостоятельно сгенерируйте около 5,000 записей в каждую таблицу для тестирования производительности. (данные предполагаются генеративными и случайными)
2. Необходимо проанализировать три медленных SQL запроса ниже и оптимизировать их путем создания соответствующих индексов (B-tree, функциональных , индексов с включенными столбцами, ...).

### Артефакт для проверки.
- Код генерации данных для каждой таблицы
- Код создания индексов
- EXPLAIN ANALYZE “до индексов”
- EXPLAIN ANALYZE “после индексов”

### Описание таблиц
#### Таблица “authors” (Авторы)
Хранит информацию об авторах книг.

| Столбец    | Описание                                        |
|------------|-------------------------------------------------|
| author_id  | Первичный ключ, уникальный идентификатор автора |
| first_name | Имя автора (обязательное поле)                  |
| last_name  | Фамилия автора (обязательное поле)              |
| birth_date | Дата рождения автора (может быть NULL)          |
| country    | Страна происхождения автора (может быть NULL)   |

#### Таблица “books” (Книги)
Содержит данные о книгах в библиотеке.

| Столбец          | Описание                                                     |
|------------------|--------------------------------------------------------------|
| book_id          | Первичный ключ, уникальный идентификатор книги               |
| title            | Название книги (обязательное поле)                           |
| author_id        | Внешний ключ, ссылается на `authors.author_id`               |
| publication_year | Год публикации книги                                         |
| isbn             | Уникальный ISBN книги                                        |
| genre            | Жанр книги (например, "Фэнтези", "Научная литература")       |
| page_count       | Количество страниц в книге                                   |
| available_copies | Количество доступных для выдачи экземпляров (по умолчанию 1) |

#### Таблица “borrowers” (Читатели)
Хранит информацию о зарегистрированных читателях библиотеки.

| Столбец           | Описание                                                                          |
|-------------------|-----------------------------------------------------------------------------------|
| borrower_id       | Первичный ключ, уникальный идентификатор читателя                                 |
| first_name        | Имя читателя (обязательное поле)                                                  |
| last_name         | Фамилия читателя (обязательное поле)                                              |
| email             | Электронная почта                                                                 |
| registration_date | Дата регистрации читателя                                                         |
| membership_status | Статус читателя (например, "active", "expired", "banned"). По умолчанию "active". |

#### Таблица “loans” (Выдачи книг)
Фиксирует информацию о выданных книгах.

| Столбец     | Описание                                                             |
|-------------|----------------------------------------------------------------------|
| loan_id     | Первичный ключ, уникальный идентификатор                             |
| book_id     | Внешний ключ, ссылается на `books.book_id`                           |
| borrower_id | Внешний ключ, ссылается на `borrowers.borrower_id`                   |
| loan_date   | Дата выдачи книги                                                    |
| due_date    | Дата, когда книга должна быть возвращена                             |
| return_date | Фактическая дата возврата книги (NULL, если книга еще не возвращена) |
| fine_amount | Размер штрафа за просрочку (по умолчанию 0)                          |

#### sql для таблиц
```sql
CREATE TABLE authors (
author_id SERIAL PRIMARY KEY,
first_name VARCHAR(100) NOT NULL,
last_name VARCHAR(100) NOT NULL,
birth_date DATE,
country VARCHAR(100)
);

CREATE TABLE books (
book_id SERIAL PRIMARY KEY,
title VARCHAR(255) NOT NULL,
author_id INTEGER REFERENCES authors(author_id),
publication_year INTEGER,
isbn VARCHAR(20) UNIQUE,
genre VARCHAR(100),
page_count INTEGER,
available_copies INTEGER DEFAULT 1
);

CREATE TABLE borrowers (
borrower_id SERIAL PRIMARY KEY,
first_name VARCHAR(100) NOT NULL,
last_name VARCHAR(100) NOT NULL,
email VARCHAR(255) UNIQUE,
registration_date DATE DEFAULT CURRENT_DATE,
membership_status VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE loans (
loan_id SERIAL PRIMARY KEY,
book_id INTEGER REFERENCES books(book_id),
borrower_id INTEGER REFERENCES borrowers(borrower_id),
loan_date DATE DEFAULT CURRENT_DATE,
due_date DATE,
return_date DATE,
fine_amount DECIMAL(10, 2) DEFAULT 0
);
```

### Задание
Для каждого SQL запроса:
- Проанализируйте план выполнения (EXPLAIN ANALYZE)
- Определите, какие индексы (B-tree, функциональные или included) улучшат производительность
- Создайте индексы
- Проверьте улучшение производительности, используя EXPLAIN ANALYZE после создания индекса (ов)

#### Запрос No1
```sql
SELECT b.title, a.first_name, a.last_name, b.publication_year
FROM books b
JOIN authors a ON b.author_id = a.author_id
WHERE b.title LIKE '%Гарри%'
AND b.genre = 'Фэнтези'
ORDER BY b.publication_year DESC;
```
#### Запрос No2
```sql
SELECT br.first_name, br.last_name, b.title, l.loan_date, l.due_date,
(CURRENT_DATE - l.due_date) AS days_overdue,
(CURRENT_DATE - l.due_date) * 10 AS fine
FROM loans l
JOIN borrowers br ON l.borrower_id = br.borrower_id
JOIN books b ON l.book_id = b.book_id
WHERE l.return_date IS NULL
AND l.due_date < CURRENT_DATE
AND br.membership_status = 'active'
ORDER BY days_overdue DESC;
```
#### Запрос No3
```sql
SELECT a.first_name, a.last_name,
COUNT(l.loan_id) AS loans_count
FROM authors a
JOIN books b ON a.author_id = b.author_id
JOIN loans l ON b.book_id = l.book_id
WHERE b.publication_year BETWEEN 2000 AND 2020
GROUP BY a.author_id
HAVING COUNT(l.loan_id) > 10
ORDER BY loans_count DESC;
```

## Создаем подключение к БД


```python
import psycopg2
import random
from faker import Faker

fake = Faker('ru_RU')
data_count = 15000  # Количество записей в каждой таблице

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
cursor.execute("SET search_path TO ivan_patakin;")

print("Схема 'ivan_patakin' создана. Используется в текущем сеансе.")
```

    Схема 'ivan_patakin' создана. Используется в текущем сеансе.
    

## Создаем таблицы


```python
tables = [
    """
    CREATE TABLE IF NOT EXISTS authors (
        author_id SERIAL PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        birth_date DATE,
        country VARCHAR(100)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS books (
        book_id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        author_id INTEGER REFERENCES authors(author_id),
        publication_year INTEGER,
        isbn VARCHAR(20) UNIQUE,
        genre VARCHAR(100),
        page_count INTEGER,
        available_copies INTEGER DEFAULT 1
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS borrowers (
        borrower_id SERIAL PRIMARY KEY,
        first_name VARCHAR(100) NOT NULL,
        last_name VARCHAR(100) NOT NULL,
        email VARCHAR(255) UNIQUE,
        registration_date DATE DEFAULT CURRENT_DATE,
        membership_status VARCHAR(20) DEFAULT 'active'
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS loans (
        loan_id SERIAL PRIMARY KEY,
        book_id INTEGER REFERENCES books(book_id),
        borrower_id INTEGER REFERENCES borrowers(borrower_id),
        loan_date DATE DEFAULT CURRENT_DATE,
        due_date DATE,
        return_date DATE,
        fine_amount DECIMAL(10, 2) DEFAULT 0
    );
    """
]

for table in tables:
    cursor.execute(table)
connection.commit()

print("Таблицы успешно созданы.")
```

    Таблицы успешно созданы.
    

## Генерация данных для таблиц

#### Генерация авторов


```python
authors_data = []
countries = ["Россия", "США", "Великобритания", "Франция", "Германия", "Япония", "Китай", "Индия", "Канада", "Австралия"]

for _ in range(data_count):
    first_name = fake.first_name()
    last_name = fake.last_name()
    birth_date = fake.date_of_birth(minimum_age=20, maximum_age=100)
    country = random.choice(countries)
    authors_data.append((first_name, last_name, birth_date, country))

cursor.executemany(
    "INSERT INTO authors (first_name, last_name, birth_date, country) VALUES (%s, %s, %s, %s)",
    authors_data
)
connection.commit()
print("Авторы: данные сгенерированы и загружены.")
```

    Авторы: данные сгенерированы и загружены.
    

#### Генерация книг


```python
books_data = []
genres = ["Фэнтези", "Детектив", "Роман", "Научная литература", "История", "Приключения", "Фантастика", "Поэзия", "Детская литература", "Биография"]

for i in range(data_count):
    title = fake.catch_phrase()
    # Добавляем слово "Гарри" примерно в 5% названий для запроса №1
    if i % 20 == 0:
        title = f"Гарри {title}"
    author_id = random.randint(1, data_count)
    publication_year = random.randint(1950, 2023)
    isbn = f"978-{random.randint(0, 9)}-{random.randint(10000, 99999)}-{random.randint(100, 999)}-{random.randint(0, 9)}"
    genre = random.choice(genres)
    page_count = random.randint(50, 1500)
    available_copies = random.randint(0, 10)
    books_data.append((title, author_id, publication_year, isbn, genre, page_count, available_copies))

cursor.executemany(
    "INSERT INTO books (title, author_id, publication_year, isbn, genre, page_count, available_copies) VALUES (%s, %s, %s, %s, %s, %s, %s)",
    books_data
)

connection.commit()
print("Книги: данные сгенерированы и загружены.")
```

    Книги: данные сгенерированы и загружены.
    

#### Генерация читателей


```python
borrowers_data = []
statuses = ["active", "expired", "banned"]
status_weights = [0.8, 0.15, 0.05]  # 80% активных, 15% с истёкшим сроком, 5% заблокированных
used_emails = set()  # Множество для отслеживания уже использованных email

for _ in range(data_count):
    first_name = fake.first_name()
    last_name = fake.last_name()

    email = fake.email()
    while email in used_emails:
        username, domain = email.split('@')
        email = f"{username}{random.randint(1, 9999)}@{domain}"
    
    used_emails.add(email)
    
    registration_date = fake.date_between(start_date='-5y', end_date='today')
    membership_status = random.choices(statuses, weights=status_weights, k=1)[0]
    borrowers_data.append((first_name, last_name, email, registration_date, membership_status))

cursor.executemany(
    "INSERT INTO borrowers (first_name, last_name, email, registration_date, membership_status) VALUES (%s, %s, %s, %s, %s)",
    borrowers_data
)
connection.commit()
print("Читатели: данные сгенерированы и загружены.")
```

    Читатели: данные сгенерированы и загружены.
    

#### Генерация выдач книг
Используем скрип в PostgreSQL, а не только Python.


```python
cursor.execute(f"""
DO $$
DECLARE
    book_ids INTEGER[] := ARRAY(SELECT book_id FROM books);
    borrower_ids INTEGER[] := ARRAY(SELECT borrower_id FROM borrowers);
    today DATE := CURRENT_DATE;
    i INT;
    book_id INT;
    borrower_id INT;
    loan_date DATE;
    due_date DATE;
    return_date DATE;
    fine_amount NUMERIC;
BEGIN
    FOR i IN 1..{data_count} LOOP
        book_id := book_ids[trunc(random() * array_length(book_ids, 1) + 1)::INT];
        borrower_id := borrower_ids[trunc(random() * array_length(borrower_ids, 1) + 1)::INT];
        
        loan_date := today - (random() * 365)::INT;
        due_date := loan_date + (14 + random() * 16)::INT;
        
        IF random() < 0.8 THEN
            return_date := loan_date + (random() * (due_date - loan_date + 15))::INT;
            IF return_date > due_date THEN
                fine_amount := (return_date - due_date) * 10;
            ELSE
                fine_amount := 0;
            END IF;
        ELSE
            return_date := NULL;
            IF due_date < today THEN
                fine_amount := (today - due_date) * 10;
            ELSE
                fine_amount := 0;
            END IF;
        END IF;

        INSERT INTO loans (book_id, borrower_id, loan_date, due_date, return_date, fine_amount)
        VALUES (book_id, borrower_id, loan_date, due_date, return_date, fine_amount);
    END LOOP;
END $$;
""")
connection.commit()
print("Выдачи книг: данные сгенерированы и загружены.")
```

    Выдачи книг: данные сгенерированы и загружены.
    

### Вывод итогового количества записей в таблицах


```python

cursor.execute("SELECT COUNT(*) FROM authors")
authors_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM books")
books_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM borrowers")
borrowers_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM loans")
loans_count = cursor.fetchone()[0]

print("\n=== Итоговое количество записей в таблицах ===")
print(f"Авторы: {authors_count}")
print(f"Книги: {books_count}")
print(f"Читатели: {borrowers_count}")
print(f"Выдачи: {loans_count}")
```

    
    === Итоговое количество записей в таблицах ===
    Авторы: 15000
    Книги: 15000
    Читатели: 15000
    Выдачи: 15000
    

### Анализ и оптимизация запросов


```python
# Функция для выполнения запроса с EXPLAIN ANALYZE и вывода результатов
def analyze_query(query, title=""):
    print(f"\n=== {title} ===")
    cursor.execute(f"EXPLAIN ANALYZE {query}")
    plan = cursor.fetchall()
    for line in plan:
        print(line[0])

```

#### Запрос №1:


```python
query1 = """
SELECT b.title, a.first_name, a.last_name, b.publication_year
FROM books b
JOIN authors a ON b.author_id = a.author_id
WHERE b.title LIKE '%Гарри%'
AND b.genre = 'Фэнтези'
ORDER BY b.publication_year DESC;
"""

# Анализ запроса до создания индексов
analyze_query(query1, "Запрос №1 до создания индексов")

# Создание индексов для оптимизации запроса №1
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_books_title_lower ON books (LOWER(title));
CREATE INDEX IF NOT EXISTS idx_books_genre ON books (genre);
CREATE INDEX IF NOT EXISTS idx_books_genre_title_pub_year 
    ON books (genre, publication_year DESC) INCLUDE (title);
""")
connection.commit()
print("\nИндексы для запроса №1 созданы")

# Обновление запроса для использования LOWER(title)
query1 = """
SELECT b.title, a.first_name, a.last_name, b.publication_year
FROM books b
JOIN authors a ON b.author_id = a.author_id
WHERE LOWER(b.title) LIKE '%гарри%'
AND b.genre = 'Фэнтези'
ORDER BY b.publication_year DESC;
"""

# Анализ запроса после создания индексов
analyze_query(query1, "Запрос №1 после создания индексов")
```

    
    === Запрос №1 до создания индексов ===
    Sort  (cost=368.11..368.11 rows=1 width=956) (actual time=7.506..7.513 rows=80 loops=1)
      Sort Key: b.publication_year DESC
      Sort Method: quicksort  Memory: 36kB
      ->  Nested Loop  (cost=0.28..368.10 rows=1 width=956) (actual time=0.142..7.442 rows=80 loops=1)
            ->  Seq Scan on books b  (cost=0.00..359.80 rows=1 width=524) (actual time=0.123..6.868 rows=80 loops=1)
                  Filter: (((title)::text ~~ '%Гарри%'::text) AND ((genre)::text = 'Фэнтези'::text))
                  Rows Removed by Filter: 14920
            ->  Index Scan using authors_pkey on authors a  (cost=0.28..8.29 rows=1 width=440) (actual time=0.006..0.006 rows=1 loops=80)
                  Index Cond: (author_id = b.author_id)
    Planning Time: 0.232 ms
    Execution Time: 7.563 ms
    
    Индексы для запроса №1 созданы
    
    === Запрос №1 после создания индексов ===
    Sort  (cost=191.99..192.00 rows=1 width=956) (actual time=3.748..3.754 rows=80 loops=1)
      Sort Key: b.publication_year DESC
      Sort Method: quicksort  Memory: 36kB
      ->  Nested Loop  (cost=5.13..191.98 rows=1 width=956) (actual time=0.395..3.702 rows=80 loops=1)
            ->  Bitmap Heap Scan on books b  (cost=4.85..183.68 rows=1 width=524) (actual time=0.324..3.081 rows=80 loops=1)
                  Recheck Cond: ((genre)::text = 'Фэнтези'::text)
                  Filter: (lower((title)::text) ~~ '%гарри%'::text)
                  Rows Removed by Filter: 1421
                  Heap Blocks: exact=316
                  ->  Bitmap Index Scan on idx_books_genre  (cost=0.00..4.85 rows=75 width=0) (actual time=0.104..0.104 rows=1501 loops=1)
                        Index Cond: ((genre)::text = 'Фэнтези'::text)
            ->  Index Scan using authors_pkey on authors a  (cost=0.28..8.29 rows=1 width=440) (actual time=0.007..0.007 rows=1 loops=80)
                  Index Cond: (author_id = b.author_id)
    Planning Time: 0.611 ms
    Execution Time: 3.954 ms
    


```python
cursor.execute("""
DROP INDEX IF EXISTS idx_books_title_lower;
DROP INDEX IF EXISTS idx_books_genre;
DROP INDEX IF EXISTS idx_books_genre_title_pub_year;
""")
connection.commit()
print("Индексы для запроса №1 удалены")
```

    Индексы для запроса №1 удалены
    

#### Запрос №2:


```python
# Запрос №2
query2 = """
SELECT br.first_name, br.last_name, b.title, l.loan_date, l.due_date,
(CURRENT_DATE - l.due_date) AS days_overdue,
(CURRENT_DATE - l.due_date) * 10 AS fine
FROM loans l
JOIN borrowers br ON l.borrower_id = br.borrower_id
JOIN books b ON l.book_id = b.book_id
WHERE l.return_date IS NULL
AND l.due_date < CURRENT_DATE
AND br.membership_status = 'active'
ORDER BY days_overdue DESC;
"""

# Анализ запроса до создания индексов
analyze_query(query2, "Запрос №2 до создания индексов")

# Создание индексов для оптимизации запроса №2
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_loans_return_date_due_date ON loans (return_date, due_date);
CREATE INDEX IF NOT EXISTS idx_borrowers_membership_status ON borrowers (membership_status);
CREATE INDEX IF NOT EXISTS idx_loans_borrower_id ON loans (borrower_id);
CREATE INDEX IF NOT EXISTS idx_loans_book_id ON loans (book_id);
""")
connection.commit()
print("\nИндексы для запроса №2 созданы")

# Анализ запроса после создания индексов
analyze_query(query2, "Запрос №2 после создания индексов")
```

    
    === Запрос №2 до создания индексов ===
    Sort  (cost=482.72..482.73 rows=1 width=968) (actual time=16.695..17.568 rows=2248 loops=1)
      Sort Key: ((CURRENT_DATE - l.due_date)) DESC
      Sort Method: quicksort  Memory: 413kB
      ->  Nested Loop  (cost=0.56..482.71 rows=1 width=968) (actual time=0.070..15.210 rows=2248 loops=1)
            ->  Nested Loop  (cost=0.28..475.12 rows=1 width=448) (actual time=0.054..9.575 rows=2248 loops=1)
                  ->  Seq Scan on loans l  (cost=0.00..310.80 rows=22 width=16) (actual time=0.032..1.758 rows=2806 loops=1)
                        Filter: ((return_date IS NULL) AND (due_date < CURRENT_DATE))
                        Rows Removed by Filter: 12194
                  ->  Index Scan using borrowers_pkey on borrowers br  (cost=0.28..7.39 rows=1 width=440) (actual time=0.002..0.002 rows=1 loops=2806)
                        Index Cond: (borrower_id = l.borrower_id)
                        Filter: ((membership_status)::text = 'active'::text)
                        Rows Removed by Filter: 0
            ->  Index Scan using books_pkey on books b  (cost=0.29..7.58 rows=1 width=520) (actual time=0.002..0.002 rows=1 loops=2248)
                  Index Cond: (book_id = l.book_id)
    Planning Time: 0.501 ms
    Execution Time: 17.904 ms
    
    Индексы для запроса №2 созданы
    
    === Запрос №2 после создания индексов ===
    Sort  (cost=222.42..222.42 rows=1 width=968) (actual time=8.723..8.946 rows=2248 loops=1)
      Sort Key: ((CURRENT_DATE - l.due_date)) DESC
      Sort Method: quicksort  Memory: 413kB
      ->  Nested Loop  (cost=70.97..222.41 rows=1 width=968) (actual time=1.491..7.700 rows=2248 loops=1)
            ->  Hash Join  (cost=70.69..214.89 rows=1 width=448) (actual time=1.468..4.036 rows=2248 loops=1)
                  Hash Cond: (br.borrower_id = l.borrower_id)
                  ->  Bitmap Heap Scan on borrowers br  (cost=4.87..148.68 rows=75 width=440) (actual time=0.300..1.353 rows=11932 loops=1)
                        Recheck Cond: ((membership_status)::text = 'active'::text)
                        Heap Blocks: exact=189
                        ->  Bitmap Index Scan on idx_borrowers_membership_status  (cost=0.00..4.85 rows=75 width=0) (actual time=0.275..0.276 rows=11932 loops=1)
                              Index Cond: ((membership_status)::text = 'active'::text)
                  ->  Hash  (cost=65.51..65.51 rows=25 width=16) (actual time=1.154..1.155 rows=2806 loops=1)
                        Buckets: 4096 (originally 1024)  Batches: 1 (originally 1)  Memory Usage: 164kB
                        ->  Bitmap Heap Scan on loans l  (cost=4.54..65.51 rows=25 width=16) (actual time=0.139..0.655 rows=2806 loops=1)
                              Recheck Cond: ((return_date IS NULL) AND (due_date < CURRENT_DATE))
                              Heap Blocks: exact=111
                              ->  Bitmap Index Scan on idx_loans_return_date_due_date  (cost=0.00..4.54 rows=25 width=0) (actual time=0.124..0.124 rows=2806 loops=1)
                                    Index Cond: ((return_date IS NULL) AND (due_date < CURRENT_DATE))
            ->  Index Scan using books_pkey on books b  (cost=0.29..7.50 rows=1 width=520) (actual time=0.001..0.001 rows=1 loops=2248)
                  Index Cond: (book_id = l.book_id)
    Planning Time: 1.559 ms
    Execution Time: 9.199 ms
    


```python
cursor.execute("""
DROP INDEX IF EXISTS idx_loans_return_date_due_date;
DROP INDEX IF EXISTS idx_borrowers_membership_status;
DROP INDEX IF EXISTS idx_loans_borrower_id;
DROP INDEX IF EXISTS idx_loans_book_id;
""")
connection.commit()
print("Индексы для запроса №2 удалены")
```

    Индексы для запроса №2 удалены
    

#### Запрос №3


```python
# Запрос №3
query3 = """
SELECT a.first_name, a.last_name,
COUNT(l.loan_id) AS loans_count
FROM authors a
JOIN books b ON a.author_id = b.author_id
JOIN loans l ON b.book_id = l.book_id
WHERE b.publication_year BETWEEN 2000 AND 2020
GROUP BY a.author_id
HAVING COUNT(l.loan_id) > 10
ORDER BY loans_count DESC;
"""

# Анализ запроса до создания индексов
analyze_query(query3, "Запрос №3 до создания индексов")

# Создание индексов для оптимизации запроса №3
cursor.execute("""
CREATE INDEX IF NOT EXISTS idx_books_pub_year ON books (publication_year);
CREATE INDEX IF NOT EXISTS idx_books_author_id_pub_year_compound ON books (author_id, publication_year);
CREATE INDEX IF NOT EXISTS idx_loans_book_id_loan_id ON loans (book_id, loan_id);
CREATE INDEX IF NOT EXISTS idx_loans_book_id ON loans (book_id);
""")
connection.commit()
print("\nИндексы для запроса №3 созданы")

# Анализ запроса после создания индексов
analyze_query(query3, "Запрос №3 после создания индексов")
```

    
    === Запрос №3 до создания индексов ===
    Sort  (cost=1046.29..1046.35 rows=25 width=448) (actual time=40.176..40.190 rows=0 loops=1)
      Sort Key: (count(l.loan_id)) DESC
      Sort Method: quicksort  Memory: 25kB
      ->  GroupAggregate  (cost=1044.21..1045.71 rows=25 width=448) (actual time=40.084..40.096 rows=0 loops=1)
            Group Key: a.author_id
            Filter: (count(l.loan_id) > 10)
            Rows Removed by Filter: 2499
            ->  Sort  (cost=1044.21..1044.40 rows=75 width=444) (actual time=37.966..38.449 rows=4265 loops=1)
                  Sort Key: a.author_id
                  Sort Method: quicksort  Memory: 430kB
                  ->  Hash Join  (cost=741.29..1041.87 rows=75 width=444) (actual time=21.349..34.712 rows=4265 loops=1)
                        Hash Cond: (b.author_id = a.author_id)
                        ->  Hash Join  (cost=542.94..843.32 rows=75 width=8) (actual time=8.880..16.774 rows=4265 loops=1)
                              Hash Cond: (l.book_id = b.book_id)
                              ->  Seq Scan on loans l  (cost=0.00..261.00 rows=15000 width=8) (actual time=0.282..2.694 rows=15000 loops=1)
                              ->  Hash  (cost=542.00..542.00 rows=75 width=8) (actual time=5.758..5.761 rows=4341 loops=1)
                                    Buckets: 8192 (originally 1024)  Batches: 1 (originally 1)  Memory Usage: 234kB
                                    ->  Seq Scan on books b  (cost=0.00..542.00 rows=75 width=8) (actual time=0.111..4.220 rows=4341 loops=1)
                                          Filter: ((publication_year >= 2000) AND (publication_year <= 2020))
                                          Rows Removed by Filter: 10659
                        ->  Hash  (cost=176.49..176.49 rows=1749 width=440) (actual time=12.411..12.412 rows=15000 loops=1)
                              Buckets: 16384 (originally 2048)  Batches: 1 (originally 1)  Memory Usage: 1108kB
                              ->  Seq Scan on authors a  (cost=0.00..176.49 rows=1749 width=440) (actual time=0.032..3.117 rows=15000 loops=1)
    Planning Time: 0.986 ms
    Execution Time: 40.824 ms
    
    Индексы для запроса №3 созданы
    
    === Запрос №3 после создания индексов ===
    Sort  (cost=687.98..688.05 rows=25 width=448) (actual time=24.997..25.004 rows=0 loops=1)
      Sort Key: (count(l.loan_id)) DESC
      Sort Method: quicksort  Memory: 25kB
      ->  GroupAggregate  (cost=685.90..687.40 rows=25 width=448) (actual time=24.974..24.981 rows=0 loops=1)
            Group Key: a.author_id
            Filter: (count(l.loan_id) > 10)
            Rows Removed by Filter: 2499
            ->  Sort  (cost=685.90..686.09 rows=75 width=444) (actual time=23.332..23.657 rows=4265 loops=1)
                  Sort Key: a.author_id
                  Sort Method: quicksort  Memory: 430kB
                  ->  Hash Join  (cost=382.99..683.57 rows=75 width=444) (actual time=12.303..20.838 rows=4265 loops=1)
                        Hash Cond: (b.author_id = a.author_id)
                        ->  Hash Join  (cost=184.63..485.02 rows=75 width=8) (actual time=4.148..9.174 rows=4265 loops=1)
                              Hash Cond: (l.book_id = b.book_id)
                              ->  Seq Scan on loans l  (cost=0.00..261.00 rows=15000 width=8) (actual time=0.025..1.952 rows=15000 loops=1)
                              ->  Hash  (cost=183.70..183.70 rows=75 width=8) (actual time=4.079..4.082 rows=4341 loops=1)
                                    Buckets: 8192 (originally 1024)  Batches: 1 (originally 1)  Memory Usage: 234kB
                                    ->  Bitmap Heap Scan on books b  (cost=5.05..183.70 rows=75 width=8) (actual time=0.345..2.582 rows=4341 loops=1)
                                          Recheck Cond: ((publication_year >= 2000) AND (publication_year <= 2020))
                                          Heap Blocks: exact=317
                                          ->  Bitmap Index Scan on idx_books_pub_year  (cost=0.00..5.04 rows=75 width=0) (actual time=0.293..0.294 rows=4341 loops=1)
                                                Index Cond: ((publication_year >= 2000) AND (publication_year <= 2020))
                        ->  Hash  (cost=176.49..176.49 rows=1749 width=440) (actual time=8.120..8.121 rows=15000 loops=1)
                              Buckets: 16384 (originally 2048)  Batches: 1 (originally 1)  Memory Usage: 1108kB
                              ->  Seq Scan on authors a  (cost=0.00..176.49 rows=1749 width=440) (actual time=0.035..2.551 rows=15000 loops=1)
    Planning Time: 1.417 ms
    Execution Time: 25.598 ms
    


```python
cursor.execute("""
DROP INDEX IF EXISTS idx_books_pub_year;
DROP INDEX IF EXISTS idx_books_author_id_pub_year_compound;
DROP INDEX IF EXISTS idx_loans_book_id_loan_id;
DROP INDEX IF EXISTS idx_loans_book_id;
""")
connection.commit()
print("Индексы для запроса №3 удалены")
```

    Индексы для запроса №3 удалены
    

## Удаление таблиц


```python
tables_to_drop = [
    "loans",
    "books",
    "borrowers",
    "authors"
]
for table in tables_to_drop:
    cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
    
connection.commit()
print("Таблицы успешно удалены.")
```

    Таблицы успешно удалены.
    


```python
cursor.close()
connection.close()

print("Соединение с PostgreSQL закрыто.")
```

    Соединение с PostgreSQL закрыто.
    
