import sqlite3
from faker import Faker
import random
import os
import timeit

fake = Faker()

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.create_schema()

    def create_schema(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT,
                birth_date TEXT,
                group_number TEXT
            );
            ''')
            conn.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER,
                address TEXT,
                phone_number TEXT,
                FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
            );
            ''')
            conn.execute('''
            CREATE TABLE IF NOT EXISTS education (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER,
                course INTEGER,
                qualification TEXT,
                FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
            );
            ''')

    def generate_single_student(self):
        full_name = fake.name()
        birth_date = fake.date_of_birth(minimum_age=17, maximum_age=30).strftime("%Y-%m-%d")
        group_number = f"09-{random.randint(1, 999):03d}"
        return (full_name, birth_date, group_number)

    def generate_single_contact(self, student_id):
        address = fake.address()
        phone_number = fake.phone_number()
        return (student_id, address, phone_number)

    def generate_single_education(self, student_id):
        course = random.randint(1, 6)
        qualification = 'Бакалавр' if course <= 4 else 'Магистр'
        return (student_id, course, qualification)

    def generate_data(self, table, n):
        with sqlite3.connect(self.db_path) as conn:
            if table == 'students':
                for _ in range(n):
                    student_data = self.generate_single_student()
                    conn.execute('''
                    INSERT INTO students (full_name, birth_date, group_number)
                    VALUES (?, ?, ?);
                    ''', student_data)
            elif table == 'contacts':
                student_ids = [row[0] for row in conn.execute('SELECT id FROM students').fetchall()]
                if not student_ids:
                    raise ValueError("Студенты не найдены.")
                for _ in range(n):
                    student_id = random.choice(student_ids)
                    contact_data = self.generate_single_contact(student_id)
                    conn.execute('''
                    INSERT INTO contacts (student_id, address, phone_number)
                    VALUES (?, ?, ?);
                    ''', contact_data)
            elif table == 'education':
                student_ids = [row[0] for row in conn.execute('SELECT id FROM students').fetchall()]
                if not student_ids:
                    raise ValueError("Студенты не найдены.")
                for _ in range(n):
                    student_id = random.choice(student_ids)
                    education_data = self.generate_single_education(student_id)
                    conn.execute('''
                    INSERT INTO education (student_id, course, qualification)
                    VALUES (?, ?, ?);
                    ''', education_data)
            else:
                raise ValueError(f"Неизвестная таблица: {table}")

    def clear_data(self, table):
        with sqlite3.connect(self.db_path) as conn:
            if table == 'students':
                conn.execute('DELETE FROM students;')
            elif table == 'contacts':
                conn.execute('DELETE FROM contacts;')
            elif table == 'education':
                conn.execute('DELETE FROM education;')
            else:
                raise ValueError(f"Неизвестная таблица: {table}")

    def replace_data(self, table, n):
        self.clear_data(table)
        self.generate_data(table, n)

    def copy_schema(self, source_conn, dest_conn):
        cursor = source_conn.cursor()
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        for table_name, table_sql in tables:
            if table_name != 'sqlite_sequence':
                dest_conn.execute(table_sql)

    def copy_data(self, source_conn, dest_conn):
        cursor = source_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        for (table_name,) in tables:
            if table_name != 'sqlite_sequence':
                rows = cursor.execute(f"SELECT * FROM {table_name}").fetchall()
                column_count = len(cursor.execute(f"PRAGMA table_info({table_name});").fetchall())
                placeholders = ', '.join(['?'] * column_count)
                dest_conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders});", rows)

    def create_sandbox(self, sandbox_db_path):
        if os.path.exists(sandbox_db_path):
            os.remove(sandbox_db_path)

        with sqlite3.connect(self.db_path) as source_conn, sqlite3.connect(sandbox_db_path) as sandbox_conn:
            self.copy_schema(source_conn, sandbox_conn)
            self.copy_data(source_conn, sandbox_conn)

        return sandbox_db_path

def execute_query(db_path, query, params=None):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = cursor.fetchall()
    return results

def time_query_execution(db_path, query, params=None, number=100):
    def wrapper():
        execute_query(db_path, query, params)

    execution_time = timeit.timeit(wrapper, number=number)
    average_time = execution_time / number
    return average_time

db_path = 'main.db'
sandbox_db_path = 'sandbox.db'

manager = DatabaseManager(db_path)

sandbox_db_path = manager.create_sandbox(sandbox_db_path)

manager.generate_data('students', 10)  # Генерация 10 студентов
manager.generate_data('contacts', 10)  # Генерация 10 контактов
manager.generate_data('education', 10)  # Генерация 10 записей об образовании

manager.replace_data('contacts', 5)

def fetch_data(db_path):
    with sqlite3.connect(db_path) as conn:
        students = conn.execute('SELECT * FROM students').fetchall()
        contacts = conn.execute('SELECT * FROM contacts').fetchall()
        education = conn.execute('SELECT * FROM education').fetchall()
    return students, contacts, education

students, contacts, education = fetch_data(db_path)
print("Students:")
for student in students:
    print(student)
print("\nContacts:")
for contact in contacts:
    print(contact)
print("\nEducation:")
for edu in education:
    print(edu)

manager.clear_data('education')

students, contacts, education = fetch_data(db_path)
print("\nПосле очистки данных:")
print("Students:", students)
print("Contacts:", contacts)
print("Education:", education)

query = 'SELECT * FROM students WHERE group_number = ?'
params = ('09-634',)

average_exec_time = time_query_execution(db_path, query, params, number=100)

print(f"Среднее время выполнения запроса: {average_exec_time:.6f} секунд")