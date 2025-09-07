from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import re

from PyPDF2 import PdfReader


def extract_pdf(headings, subheadings):
    pdf_path = '/files/hackathon/hackathon_file.pdf'
    
    # Открываем PDF
    reader = PdfReader(pdf_path)
    
    # Извлекаем весь текст
    full_text = ""
    for page in reader.pages:
        extracted = page.extract_text()
        if extracted:
            full_text += extracted + "\n"

    # Очистка общего текста
    text = re.sub(r'^\s*Рис\.?\s*\d+[.\d]*\s*[^\n]*(?:\n|$)', '', full_text, flags=re.MULTILINE)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\sа-яА-ЯёЁ.,!?;:()\-–]', '', text)
   

    # Собираем маркеры для фрагментов
    markers = []
    for h in headings:
        idx = text.find(h)
        if idx != -1:
            markers.append((idx, 'heading', h))

    for sh in subheadings:
        idx = text.find(sh)
        if idx != -1:
            markers.append((idx, 'subheading', sh))

    all_headings = headings + subheadings
    num_page = {}

    for page_num in range(len(reader.pages)):
        page = reader.pages[page_num]
        page_text = page.extract_text()
        if not page_text:  # Пропускаем пустые страницы
            continue

        # Очистка текста страницы
        page_text = re.sub(r'\s+', ' ', page_text)
        page_text = re.sub(r'Рис\.?\s*\d+\.?\d*\s*[^.!?]*[.!?]', '', page_text)

        # Проверяем каждый заголовок
        for heading in all_headings:
            if heading not in num_page and heading in page_text:
                num_page[heading] = page_num + 1  # Нумерация с 1

    # Сортируем маркеры по позиции в тексте
    markers = sorted(markers, key=lambda x: x[0])

    fragments = []

    for i in range(len(markers)):
        curr_pos, curr_h, curr_text = markers[i]
        if curr_h == 'heading':
            head = curr_text

        if i < len(markers) - 1:
            next_pos = markers[i + 1][0]
            frag = text[curr_pos:next_pos].replace(curr_text, "", 1)  # Удаляем только первое вхождение
            if frag.strip():  # Если фрагмент не пустой
                if curr_h == 'heading':
                    page_info = num_page.get(curr_text, "N/A")
                    fragments.append((
                        curr_text,
                        frag.strip(),
                        f'Раздел: {curr_text}, Страница: {page_info}'
                    ))
                else:
                    page_info = num_page.get(curr_text, "N/A")
                    fragments.append((
                        f"{head}.{curr_text}",
                        frag.strip(),
                        f'Раздел: {head}, Подраздел: {curr_text}, Страница: {page_info}'
                    ))
        else:
            # Последний фрагмент
            frag = text[curr_pos:].replace(curr_text, "", 1)
            if frag.strip():
                if curr_h == 'heading':
                    page_info = num_page.get(curr_text, "N/A")
                    fragments.append((
                        curr_text,
                        frag.strip(),
                        f'Раздел: {curr_text}, Страница: {page_info}'
                    ))
                else:
                    page_info = num_page.get(curr_text, "N/A")
                    fragments.append((
                        f"{head}.{curr_text}",
                        frag.strip(),
                        f'Раздел: {head}, Подраздел: {curr_text}, Страница: {page_info}'
                    ))

    # Формируем DataFrame
    data = {
        'text': [],
        'source_url': [],
        'course_name': [],
        'section': [],
        'created_at': []
    }

    for section, content, url in fragments:
        data['text'].append(content)
        data['source_url'].append(url)
        data['course_name'].append("Формализация систем")
        data['section'].append(section)
        data['created_at'].append(datetime.now())

    df = pd.DataFrame(data)
    
    #загрузка csv в docker
    output_path = '/opt/airflow/dags/output.csv'
    df.to_csv(output_path, index=False, encoding='utf-8')
    


# процедура счичывания данных из csv
def insert_data(table_name):
    
    #считывание файла
    df = pd.read_csv(f"/opt/airflow/dags/{table_name}.csv", delimiter=",", encoding='utf-8')
    df = df.drop_duplicates()

    #подключение и преобразование датафрейма в sql
    postgres_hook = PostgresHook("pg-studymate")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage", if_exists="append", index=False)

#функция с дагом
def dag_func(default_args, file_name, course_name, description_course, description_file, url, headings, subheadings):
    with DAG(
        "insert_in_test",
        default_args=default_args,
        description="Загрузка данных в Stage",
        catchup=False,
        schedule="0 0 * * *",
    ) as macdag:
        #задача парсинга файла
        parsing_task = PythonOperator(
            task_id='parsing_task',
            python_callable=extract_pdf,
            op_kwargs = {'headings' : headings, 'subheadings' : subheadings}
        )

        #задача очистки таблицы для загрузки грязных данных
        del_task = PostgresOperator(
            task_id='del_task',
            postgres_conn_id='pg-studymate',
            sql='DROP TABLE IF EXISTS stage.output',
            autocommit=True
        )

        #задача вставки данных в таблицу output в схеме stage
        insert_task_output = PythonOperator(
            task_id='insert_task_output',
            python_callable=insert_data,
            op_kwargs={'table_name': 'output'}
        )

        #задача загрузки данных в схему dm для чистых данных
        insert_task_dm = PostgresOperator(
            task_id='insert_task_dm',
            postgres_conn_id='pg-studymate',
            sql='CALL dm.load_from_stage(%s, %s, %s, %s, %s)',
            parameters=[course_name, file_name, description_course, description_file, url],
            autocommit=True
        )

        #граф задач
        parsing_task >> del_task >> insert_task_output >> insert_task_dm


def main():
    default_args = {
        "owner": "gavrilov",
        "start_date": datetime(2025, 9, 6),
        "retries": 2
    }

    #передаваемые списки заголовков
    headings = [
        "Введение",
        "1. Задачи и сущность процесса моделирования",
        "2. Концептуальная модель системы",
    ]
    subheadings = [
        "Система как объект исследования",
        "Характерные особенности системы:",
        "Функциональные свойства объекта моделирования",
        "Внешние функции системы.",
        "Внутренние свойства системы.",
        "Информационные свойства объекта моделирования.",
        "Структурные свойства.",
        "Формализация систем",
        "Классификация моделей",
        "Математическое описание моделей",
        "Схема формирования математической модели",
        "Характеристика системы."
    ]


    file_name = 'Информатика и ИКТ'
    course_name = 'Формализация систем'
    description_course = 'Очень крутой курс, всем советую!'
    description_file = 'Очень крутая лекция, всем советую!'
    url = ''

    #такой вызов функции с переменными для возможного  расширения бота + для лучшей тестируемости + для лучшей гибкости и возможности ввода новых файлов
    dag_func(default_args, file_name, course_name, description_course, description_file, url, headings, subheadings)


main()
