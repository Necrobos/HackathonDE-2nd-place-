--DROP SCHEMA dm CASCADE;
--DROP TABLE IF EXISTS dm.courses, dm.downloaded_files, dm.chunks CASCADE;

create schema if not exists stage;
CREATE SCHEMA IF NOT EXISTS dm;
--drop TABLE dm.chunks CASCADE; 
-- Курсы — каждый курс соответствует одному PDF-файлу
CREATE TABLE dm.courses (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,   -- название курса 
    description TEXT, --краткое описание
	url TEXT
);


CREATE TABLE dm.downloaded_files(
	id SERIAL PRIMARY KEY, 
	course_id INTEGER NOT NULL REFERENCES dm.courses(id) ON DELETE CASCADE, --ссылка на курс, к которому относится файл
	name VARCHAR(255) NOT NULL UNIQUE, --название курса
	description TEXT --краткое описание файла
);


CREATE TABLE dm.chunks (
    id SERIAL PRIMARY KEY,
	id_file INTEGER NOT NULL,
    title_name VARCHAR(255) NOT NULL, -- название заголовка
	source_url VARCHAR(255) DEFAULT NULL, --крч здесь текстом указана страница и тд в файле, вытащить изображение не нада!
    content TEXT NOT NULL,             --текст чанка
	created_at TIMESTAMP WITH TIME ZONE --время создания 
);




CREATE TABLE dm.chunks_vectors(
	chunk_id INTEGER REFERENCES dm.chunks(id) ON DELETE CASCADE, --ссылка на айди чанка, они уникальные
	embedding VECTOR(256) NOT NULL UNIQUE, -- ВЕКТОР
	PRIMARY KEY(chunk_id)
);

--drop table if exists dm.chunks_vectors;

-- Теги для категоризации
CREATE TABLE dm.tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE  -- например: "функции", "циклы"
);

-- Связь между чанками и тегами (многие-ко-многим)
CREATE TABLE dm.chunk_tags (
    chunk_id INTEGER NOT NULL REFERENCES dm.chunks(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES dm.tags(id) ON DELETE CASCADE,
    PRIMARY KEY (chunk_id, tag_id)
);


CREATE SCHEMA logs;
CREATE TABLE IF NOT EXISTS logs.bd_logs (
    id SERIAL PRIMARY KEY,
    operation_name TEXT NOT NULL,          -- название операции, например: 'load_chunks_from_stage'
    start_time TIMESTAMPTZ NOT NULL,       -- время начала
    end_time TIMESTAMPTZ,                  -- время окончания (может быть NULL, если ещё не завершено)
    row_count INT,                         -- сколько строк обработано
    status TEXT NOT NULL DEFAULT 'STARTED',-- 'STARTED', 'SUCCESS', 'ERROR'
    error_message TEXT                     -- если ошибка — сообщение
);




CREATE OR REPLACE PROCEDURE dm.load_from_stage(
    IN v_name_course VARCHAR(255),
    IN v_name_file VARCHAR(255),
    IN v_description_course TEXT,
    IN v_description_file TEXT,
    IN v_url TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_main_log_id BIGINT;       -- ID основного лога
    v_log_id BIGINT;            -- ID текущей операции
    v_row_count INT;            -- счётчик строк
    v_start_time TIMESTAMPTZ;   -- время начала основной операции
    v_id_file INT;
    v_id_course INT;
    v_flg CHAR(1);
BEGIN
    -- основной лог: начало всей процедуры
    v_start_time := NOW();
    INSERT INTO logs.bd_logs (operation_name, start_time, status)
    VALUES ('load_from_stage: PROCEDURE START', v_start_time, 'STARTED')
    RETURNING id INTO v_main_log_id;

    RAISE NOTICE 'Начало процедуры load_from_stage. Лог ID: %', v_main_log_id;

    -- Поиск или вставка курса
    INSERT INTO logs.bd_logs (operation_name, start_time, status)
    VALUES ('load_from_stage: CHECK/INSERT COURSE', NOW(), 'STARTED')
    RETURNING id INTO v_log_id;

    WITH cte_max AS (
        SELECT COALESCE(MAX(id), 0) AS max_id
        FROM dm.courses
    )
    SELECT 
        COALESCE(id, max_id + 1), 
        CASE WHEN id IS NOT NULL THEN 'Y' ELSE 'N' END
    INTO v_id_course, v_flg
    FROM cte_max
    LEFT JOIN dm.courses ON LOWER(v_name_course) = LOWER(name);

    IF v_flg = 'N' THEN
        INSERT INTO dm.courses (name, description, url)
        VALUES (v_name_course, v_description_course, v_url)
        RETURNING id INTO v_id_course;
        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        RAISE NOTICE 'Курс "%s" создан. ID: %', v_name_course, v_id_course;
    ELSE
        v_row_count := 0;
        RAISE NOTICE 'Курс "%s" уже существует. ID: %', v_name_course, v_id_course;
    END IF;

    UPDATE logs.bd_logs
    SET 
        end_time = NOW(),
        row_count = v_row_count,
        status = 'SUCCESS'
    WHERE id = v_log_id;

    -- Поиск или вставка файла
    INSERT INTO logs.bd_logs (operation_name, start_time, status)
    VALUES ('load_from_stage: CHECK/INSERT FILE', NOW(), 'STARTED')
    RETURNING id INTO v_log_id;


    WITH cte_max AS (
        SELECT COALESCE(MAX(id), 0) AS max_id
        FROM dm.downloaded_files
    )
    SELECT 
        COALESCE(id, max_id + 1), 
        CASE WHEN id IS NOT NULL THEN 'Y' ELSE 'N' END
    INTO v_id_file, v_flg
    FROM cte_max
    LEFT JOIN dm.downloaded_files ON LOWER(v_name_file) = LOWER(name);

    IF v_flg = 'N' THEN
        INSERT INTO dm.downloaded_files (course_id, name, description)
        VALUES (v_id_course, v_name_file, v_description_file)
        RETURNING id INTO v_id_file;
        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        RAISE NOTICE 'Файл "%s" создан. ID: %', v_name_file, v_id_file;
    ELSE
        v_row_count := 0;
        RAISE NOTICE 'Файл "%s" уже существует. ID: %', v_name_file, v_id_file;
    END IF;

    UPDATE logs.bd_logs
    SET 
        end_time = NOW(),
        row_count = v_row_count,
        status = 'SUCCESS'
    WHERE id = v_log_id;

    --удаление старых чанков 
    INSERT INTO logs.bd_logs (operation_name, start_time, status)
    VALUES ('load_from_stage: DELETE OLD CHUNKS', NOW(), 'STARTED')
    RETURNING id INTO v_log_id;

    DELETE FROM dm.chunks 
    WHERE id_file = v_id_file;
    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    UPDATE logs.bd_logs
    SET 
        end_time = NOW(),
        row_count = v_row_count,
        status = 'SUCCESS'
    WHERE id = v_log_id;

    RAISE NOTICE '🗑Удалено % старых чанков для файла ID %', v_row_count, v_id_file;

    -- Вставка новых чанков — логируем
    INSERT INTO logs.bd_logs (operation_name, start_time, status)
    VALUES ('load_from_stage: INSERT NEW CHUNKS', NOW(), 'STARTED')
    RETURNING id INTO v_log_id;

    INSERT INTO dm.chunks (id_file, title_name, source_url, content, created_at)
    SELECT 
        v_id_file,
        section,
        source_url, 
        text, 
        created_at::TIMESTAMP WITH TIME ZONE
    FROM stage.output;
    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    UPDATE logs.bd_logs
    SET 
        end_time = NOW(),
        row_count = v_row_count,
        status = 'SUCCESS'
    WHERE id = v_log_id;

    RAISE NOTICE 'Вставлено % новых чанков для файла ID %', v_row_count, v_id_file;

    --обновляем основной лог
    UPDATE logs.bd_logs
    SET 
        end_time = NOW(),
        row_count = v_row_count, 
        status = 'SUCCESS'
    WHERE id = v_main_log_id;

    RAISE NOTICE 'Процедура load_from_stage завершена. Лог ID: %', v_main_log_id;

EXCEPTION
    WHEN OTHERS THEN
        -- обновляем основной лог при ошибке
        UPDATE logs.bd_logs
        SET 
            end_time = NOW(),
            status = 'ERROR',
            error_message = SQLERRM
        WHERE id = v_main_log_id;

        RAISE NOTICE 'Ошибка в процедуре: %', SQLERRM;
        RAISE; 
END;
$$;


select * from dm.chunks c;
select * from dm.downloaded_files df ;
select * from dm.courses c ;
