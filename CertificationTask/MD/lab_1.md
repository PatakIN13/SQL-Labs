# Аттестационное задание No1
## Задача:  
Друзья, решили организовать свой собственный технологический  
стартап. Ребята выстроили между собой следующие взаимоотношения в  
рамках проектной деятельности.  
- Сергей Петров - лидер проекта  
- Ильяс Мухаметшин - тимлид команды разработчиков  
- Иван Иванов - frontend разработчик  
- Екатерина Андреева - backend разработчик  
- Анна Потапова - тимлид команды специалистов по тестированию  
  ![Взаимоотношения](media/lab1_1.png)

Создайте модель данных для хранения этих зависимостей между  
коллегами. Для этого:  
1. Добавьте отдельную таблицу с зависимостями между участниками  
стартапа.  
- Укажите внешние ключи необходимые для поддержания  
консистентности данных.  
- Заполните таблицу соответствующими  
записями по зависимостям.  
2. Добавьте таблицу-справочник, хранящую роли (должности) участников  
проекта и использующуюся как источник.   
- Заполните таблицу соответствующими записями.  
  
Напишите SQL-функцию **(ВНИМАНИЕ! не pl/pgsql-функцию !)** с именем  
`ваша_схема.getHierarchy` c входящей переменной, которая указывает,  
начиная с какого ID менеджера выстраивать иерархию вывода (не обязательно  
с роли “Лидера проекта”).     
Например, вызов функции с указанием менеджера Ильяса выглядит вот так:  
```sql
SELECT * FROM getHierarchy(p_manager_id => 50);   
```  

| студент            | роль                         | уровень | путь от лидера                         |
| ------------------ | ---------------------------- | ------- | -------------------------------------- |
| Ильяс Мухаметшин   | тимлид команды разработчиков | 1       | Ильяс Мухаметшин                       |
| Екатерина Андреева | backend разработчик          | 2       | Ильяс Мухаметшин -> Екатерина Андреева |
| Иван Иванов        | frontend разработчик         | 2       | Ильяс Мухаметшин -> Иван Иванов        |
  
Если значение переменной не указано (принимается значение null), то  
необходимо выводить данные с самого начала иерархии. Пример вывода без  
указания переменной `p_manager_id`.  
```sql  
SELECT * FROM getHierarchy(p_manager_id => null);  
SELECT * FROM getHierarchy();  
```  

| студент                 | роль                                                            | уровень | путь от лидера                                                   |
| ----------------------- | --------------------------------------------------------------- | ------- | ---------------------------------------------------------------- |
| Сергей Петров           | лидер проекта                                                   | 1       | Сергей Петров                                                    |
| Ильяс  <br>Мухаметшин   | тимлид  <br>команды  <br>разработчиков                          | 2       | Сергей Петров ->  <br>Ильяс Мухаметшин                           |
| Анна Потапова           | тимлид  <br>команды  <br>специалистов  <br>по  <br>тестированию | 2       | Сергей Петров ->Анна  <br>Потапова                               |
| Екатерина  <br>Андреева | backend  <br>разработчик                                        | 3       | Сергей Петров ->  <br>Ильяс Мухаметшин  <br>->Екатерина Андреева |
| Иван Иванов             | frontend  <br>разработчик                                       | 3       | Сергей Петров ->  <br>Ильяс Мухаметшин ->  <br>Иван Иванов       |

Пожалуйста, предоставьте результаты выполнения 3 вызовов функции  
(xxx - это существующий ID менеджера в вашей модели)  
```sql  
SELECT * FROM getHierarchy(p_manager_id := xxx);  
SELECT * FROM getHierarchy(p_manager_id := null);  
SELECT * FROM getHierarchy();  
```

## Создаем подключение к БД


```python
import psycopg2
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
    

## Создание таблиц
### Создадим справочники для хранения ролей и таблицу сотрудников


```python
cursor.execute("""
    CREATE TABLE ivan_patakin.role (
    id SERIAL PRIMARY KEY,
    role_name VARCHAR(255) NOT NULL
    );
""")
connection.commit()

print("Таблица 'role' создана.")
```

    Таблица 'role' создана.
    


```python
cursor.execute("""
    CREATE TABLE ivan_patakin.staff (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    role_id INTEGER NOT NULL REFERENCES ivan_patakin.role(id)
    );
""")
connection.commit()

print("Таблица 'staff' создана.")
```

    Таблица 'staff' создана.
    

### Создадим cправочник для хранения взаимоотношений между сотрудниками


```python
cursor.execute("""
    CREATE TABLE ivan_patakin.dependency (
    id SERIAL PRIMARY KEY,
    staff_id INTEGER NOT NULL REFERENCES ivan_patakin.staff(id),
    dependency_id INTEGER NOT NULL REFERENCES ivan_patakin.staff(id),
    UNIQUE (staff_id, dependency_id)
    );
""")
connection.commit()

print("Таблица 'dependency' создана.")
```

    Таблица 'dependency' создана.
    

## Заполнение таблиц данными


```python
cursor.execute("""
    INSERT INTO ivan_patakin.role (role_name) VALUES
    ('лидер проекта'),
    ('тимлид команды разработчиков'),
    ('frontend разработчик'),
    ('backend разработчик'),
    ('тимлид команды специалистов по тестированию');
""")
connection.commit()

print("Таблица 'role' заполнена данными.")
```

    Таблица 'role' заполнена данными.
    


```python
cursor.execute("""
    INSERT INTO ivan_patakin.staff (name, role_id) VALUES
    ('Сергей Петров', 1),
    ('Ильяс Мухаметшин', 2),
    ('Иван Иванов', 3),
    ('Екатерина Андреева', 4),
    ('Анна Потапова', 5);
""")
connection.commit()

print("Таблица 'staff' заполнена данными.")
```

    Таблица 'staff' заполнена данными.
    


```python
cursor.execute("""
    INSERT INTO ivan_patakin.dependency (dependency_id, staff_id) VALUES
    (1, 2), -- Сергей Петров -> Ильяс Мухаметшин
    (2, 3), -- Ильяс Мухаметшин -> Иван Иванов
    (2, 4), -- Ильяс Мухаметшин -> Екатерина Андреева
    (1, 5); -- Сергей Петров -> Анна Потапова
""")
connection.commit()

print("Таблица 'dependency' заполнена данными.")
```

    Таблица 'dependency' заполнена данными.
    

## Создание функции для получения всех пользователей по взаимоотношениям

Функция `getHierarchy` строит иерархию сотрудников на основе их взаимоотношений. Она показывает, кто кому подчиняется, начиная с указанного менеджера (или с самого верхнего уровня, если менеджер не указан).  
Результат функции — таблица, где указаны:  
- Имя сотрудника.
- Его роль.
- Уровень в иерархии (чем меньше число, тем выше уровень).
- Полный путь от руководителя до этого сотрудника.


```python
cursor.execute("""
CREATE FUNCTION ivan_patakin.getHierarchy(p_manager_id INT DEFAULT NULL)
RETURNS TABLE (
    staff VARCHAR,
    role VARCHAR,
    level INT,
    path TEXT
) AS $$
WITH RECURSIVE hierarchy AS (
    SELECT
        s.id,
        s.name AS staff,
        r.role_name AS role,
        1 AS level,
        s.name::TEXT AS path
    FROM ivan_patakin.staff s
    JOIN ivan_patakin.role r ON s.role_id = r.id
    WHERE s.id = COALESCE(
        p_manager_id,
        (SELECT s.id FROM ivan_patakin.staff s
         LEFT JOIN ivan_patakin.dependency d ON s.id = d.staff_id
         WHERE d.staff_id IS NULL LIMIT 1)
    )
    UNION ALL
    SELECT
        s.id,
        s.name,
        r.role_name,
        h.level + 1,
        h.path || ' -> ' || s.name
    FROM ivan_patakin.staff s
    JOIN ivan_patakin.role r ON s.role_id = r.id
    JOIN ivan_patakin.dependency d ON s.id = d.staff_id
    JOIN hierarchy h ON d.dependency_id = h.id
)
SELECT staff, role, level, path FROM hierarchy;
$$ LANGUAGE sql;
""")
connection.commit()

print("Функция 'getHierarchy' создана.")
```

    Функция 'getHierarchy' создана.
    


```python
cursor.execute(""" SELECT * FROM ivan_patakin.getHierarchy(p_manager_id := 2); """)
rows = cursor.fetchall()
for row in rows:
    print(row)
    
```

    ('Ильяс Мухаметшин', 'тимлид команды разработчиков', 1, 'Ильяс Мухаметшин')
    ('Иван Иванов', 'frontend разработчик', 2, 'Ильяс Мухаметшин -> Иван Иванов')
    ('Екатерина Андреева', 'backend разработчик', 2, 'Ильяс Мухаметшин -> Екатерина Андреева')
    


```python
cursor.execute(""" SELECT * FROM ivan_patakin.getHierarchy(p_manager_id := null); """)
rows = cursor.fetchall()
for row in rows:
    print(row)
```

    ('Сергей Петров', 'лидер проекта', 1, 'Сергей Петров')
    ('Ильяс Мухаметшин', 'тимлид команды разработчиков', 2, 'Сергей Петров -> Ильяс Мухаметшин')
    ('Анна Потапова', 'тимлид команды специалистов по тестированию', 2, 'Сергей Петров -> Анна Потапова')
    ('Иван Иванов', 'frontend разработчик', 3, 'Сергей Петров -> Ильяс Мухаметшин -> Иван Иванов')
    ('Екатерина Андреева', 'backend разработчик', 3, 'Сергей Петров -> Ильяс Мухаметшин -> Екатерина Андреева')
    


```python
cursor.execute(""" SELECT * FROM ivan_patakin.getHierarchy(); """)
rows = cursor.fetchall()
for row in rows:
    print(row)
```

    ('Сергей Петров', 'лидер проекта', 1, 'Сергей Петров')
    ('Ильяс Мухаметшин', 'тимлид команды разработчиков', 2, 'Сергей Петров -> Ильяс Мухаметшин')
    ('Анна Потапова', 'тимлид команды специалистов по тестированию', 2, 'Сергей Петров -> Анна Потапова')
    ('Иван Иванов', 'frontend разработчик', 3, 'Сергей Петров -> Ильяс Мухаметшин -> Иван Иванов')
    ('Екатерина Андреева', 'backend разработчик', 3, 'Сергей Петров -> Ильяс Мухаметшин -> Екатерина Андреева')
    


```python
cursor.execute("""
DROP FUNCTION IF EXISTS ivan_patakin.getHierarchy;
DROP TABLE IF EXISTS ivan_patakin.dependency;
DROP TABLE IF EXISTS ivan_patakin.staff;
DROP TABLE IF EXISTS ivan_patakin.role;
""")

connection.commit()
print("Таблицы удалены.")
```

    Таблицы удалены.
    


```python
cursor.close()
connection.close()
```
