# Дополнительная лабораторная работа No2
## Задача:
Создать наследованную (inhereted table) таблицу
`table_inhereted_master` с атрибутами (id serial, created_at timestamp with
timezone).  
Для данной таблицы создать триггер с именем `trg_inhereted_master` и
типом `FOR EACH ROW BEFORE INSERT` и соответствующую триггерную
функцию с именем `fnc_inhereted_master`.  
Логика данного триггера должна определять автоматическое создание
таблицы наследуемой от `table_inhereted_master` на основании значения
`NEW.created_at` (потомок должен создаваться на каждый месяц).
- Имя таблицы наследуемого потомка должно определять динамически
(например как `table_child_032021`)
- Используйте команду `EXECUTE` для выполнения динамического SQL на
уровне триггера

**ВНИМАНИЕ! Не старайтесь создавать таблицы потомки самостоятельно
заранее, они должны создаваться автоматически при каждом INSERT
выражении**

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
### Создадим таблицу `table_inhereted_master`


```python
cursor.execute("""
    CREATE TABLE ivan_patakin.table_inhereted_master (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
    );
""")
connection.commit()

print("Таблица 'table_inhereted_master' создана.")
```

    Таблица 'table_inhereted_master' создана.
    

### Создадим триггерную функцию `fnc_inhereted_master`


```python
cursor.execute("""
    CREATE FUNCTION ivan_patakin.fnc_inhereted_master()
    RETURNS TRIGGER AS $$
    DECLARE
        child_table_name TEXT;
        start_of_month TIMESTAMP WITH TIME ZONE;
        end_of_month TIMESTAMP WITH TIME ZONE;
    BEGIN
        child_table_name := 'ivan_patakin.table_child_' || TO_CHAR(NEW.created_at, 'MMYYYY');
        
        start_of_month := DATE_TRUNC('month', NEW.created_at);
        end_of_month := start_of_month + INTERVAL '1 month';
    
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I.%I (
                CHECK (created_at >= %L AND created_at < %L)
            ) INHERITS (ivan_patakin.table_inhereted_master);',
            'ivan_patakin', 'table_child_' || TO_CHAR(NEW.created_at, 'MMYYYY'), start_of_month, end_of_month);
    
        EXECUTE format('INSERT INTO %I.%I (id, created_at) VALUES ($1, $2)',
            'ivan_patakin', 'table_child_' || TO_CHAR(NEW.created_at, 'MMYYYY'))
        USING NEW.id, NEW.created_at;
    
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
""")
connection.commit()

print("Триггерная функция 'fnc_inhereted_master' создана.")
```

    Триггерная функция 'fnc_inhereted_master' создана.
    

Функция `fnc_inhereted_master` автоматически создаёт таблицы для каждого месяца на основе даты вставляемой записи. Если таблицы для нужного месяца ещё нет, она создаётся с ограничением, чтобы в неё попадали только записи с датами этого месяца. Затем данные из вставляемой строки перенаправляются в соответствующую таблицу.

### Создание триггера `trg_inhereted_master`


```python
cursor.execute("""
    CREATE TRIGGER trg_inhereted_master
    BEFORE INSERT ON ivan_patakin.table_inhereted_master
    FOR EACH ROW
    EXECUTE FUNCTION ivan_patakin.fnc_inhereted_master();
""")
connection.commit()

print("Триггер 'trg_inhereted_master' создан.")
```

    Триггер 'trg_inhereted_master' создан.
    

## Тестирование триггера
### Вставляем данные в родительскую таблицу


```python
cursor.execute("""
    INSERT INTO ivan_patakin.table_inhereted_master (created_at)
    VALUES ('2023-10-01 12:00:00+00');
""")
connection.commit()

print("Данные вставлены в table_inhereted_master.")
```

    Данные вставлены в table_inhereted_master.
    


```python
cursor.execute("""
    INSERT INTO ivan_patakin.table_inhereted_master (created_at)
    VALUES ('2024-10-01 12:00:00+00');
""")
connection.commit()

cursor.execute("""
    INSERT INTO ivan_patakin.table_inhereted_master (created_at)
    VALUES ('2024-10-01 13:00:00+00');
""")
connection.commit()

print("Данные вставлены в table_inhereted_master.")
```

    Данные вставлены в table_inhereted_master.
    

### Проверяем созданные таблицы-потомки


```python
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name = 'table_child_102023'
    AND table_schema = 'ivan_patakin';
""")

child_table = cursor.fetchone()
if child_table:
    print(f"Таблица-потомок '{child_table[0]}' успешно создана.")
else:
    print("Таблица-потомок не создана.")
```

    Таблица-потомок 'table_child_102023' успешно создана.
    


```python
cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_name = 'table_child_102024'
    AND table_schema = 'ivan_patakin';
""")

child_table = cursor.fetchone()
if child_table:
    print(f"Таблица-потомок '{child_table[0]}' успешно создана.")
else:
    print("Таблица-потомок не создана.")
```

    Таблица-потомок 'table_child_102024' успешно создана.
    

### Проверяем данные в таблице-потомке


```python
cursor.execute("""
    SELECT * FROM ivan_patakin.table_inhereted_master;
""")
rows = cursor.fetchall()
print("Данные в родительской таблице:")
for row in rows:
    print(row)
```

    Данные в родительской таблице:
    (1, datetime.datetime(2023, 10, 1, 12, 0, tzinfo=datetime.timezone.utc))
    (2, datetime.datetime(2024, 10, 1, 12, 0, tzinfo=datetime.timezone.utc))
    (3, datetime.datetime(2024, 10, 1, 13, 0, tzinfo=datetime.timezone.utc))
    


```python
cursor.execute("""
    SELECT * FROM ivan_patakin.table_child_102023;
""")
rows = cursor.fetchall()
print("Данные в таблице-потомке:")
for row in rows:
    print(row)
```

    Данные в таблице-потомке:
    (1, datetime.datetime(2023, 10, 1, 12, 0, tzinfo=datetime.timezone.utc))
    


```python
cursor.execute("""
    SELECT * FROM ivan_patakin.table_child_102024;
""")
rows = cursor.fetchall()
print("Данные в таблице-потомке:")
for row in rows:
    print(row)
```

    Данные в таблице-потомке:
    (2, datetime.datetime(2024, 10, 1, 12, 0, tzinfo=datetime.timezone.utc))
    (3, datetime.datetime(2024, 10, 1, 13, 0, tzinfo=datetime.timezone.utc))
    

## Удаляем созданные таблицы и триггер


```python
cursor.execute("""
    DROP TRIGGER IF EXISTS trg_inhereted_master ON ivan_patakin.table_inhereted_master;
    DROP FUNCTION IF EXISTS ivan_patakin.fnc_inhereted_master();
    DROP TABLE IF EXISTS ivan_patakin.table_inhereted_master CASCADE;
""")
connection.commit()

print("Триггер, функция и таблица удалены.")
```

    Триггер, функция и таблица удалены.
    

## Закрываем соединение с БД


```python
cursor.close()
connection.close()
```
