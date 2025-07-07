from faker import Faker
import random

class dataGenTable:
    def __init__(self):
        self.fake = Faker()

    def generate_single(self):

        full_name = self.fake.name()
        group_number = f"{random.randint(9, 9):02d}-{random.randint(1, 999):03d}"
        course = random.randint(1, 6)
        if course <= 4:
            qualification = 'Бакалавр'
        else:
            qualification = 'Магистр'
        gender = random.choice(['М', 'Ж'])
        birth_date = self.fake.date_of_birth(minimum_age=17, maximum_age=30)
        formatted_birth_date = birth_date.strftime("%Y-%m-%d")
        address = self.fake.address()
        phone_number = self.fake.phone_number()

        return {'ФИО': full_name, 'Номер группы': group_number, 'Курс': course, 'Квалификация': qualification, 'Пол': gender,
                'Дата рождения': formatted_birth_date, 'Адрес проживания': address, 'Телефон': phone_number}

    def generate_multiple(self, n):

        data = []

        for _ in range(n):
            str = self.generate_single()

            if self.check_data(str):
                data.append(str)

        return data

    def check_data(self, str):
        return True

generate = dataGenTable()

str1 = generate.generate_single()
print(str1)

print()

n = 5
str_n = generate.generate_multiple(n)
for str in str_n:
    print(str)
