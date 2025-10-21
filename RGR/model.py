import os
import psycopg
from psycopg.rows import dict_row

DB = {
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "1111"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

ALLOWED_TABLES = ["parents", "student", "teacher", "subject", "journal"]
ATTENDANCE_STATUSES = ('present', 'absent', 'late')

# Виняток (є дочірні записи)
class ChildRowsExistError(Exception):
    def __init__(self, counts):
        super().__init__("Child rows exist")
        self.counts = counts

# Виняток (помилка валідації)
class ValidationError(Exception):
    pass

class Model:
    PK_MAP = {
        "parents": "parents_id",
        "teacher": "teacher_id",
        "subject": "subject_id",
        "student": "student_id",
        "journal": "journal_id",
    }

    # Ініціалізація з'єднання
    def __init__(self):
        self.conn = psycopg.connect(**DB)
        self.conn.row_factory = dict_row
        self.conn.autocommit = False

    # Закрити з'єднання
    def close(self):
        if self.conn:
            self.conn.close()

    # Перевірка допустимої таблиці
    def _validate_table(self, table):
        if table not in ALLOWED_TABLES:
            raise ValueError("Невідома таблиця")

    # Повернути список стовпців
    def _get_columns_list(self, table):
        q = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (table,))
            return [r["column_name"] for r in cur.fetchall()]

    # Повернути список таблиць
    def get_tables(self):
        return ALLOWED_TABLES.copy()

    # Повернути інформацію про стовпці таблиці
    def get_columns(self, table):
        self._validate_table(table)
        q = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (table,))
            return cur.fetchall()

    # Повернути рядки таблиці
    def list_table(self, table, limit=200):
        self._validate_table(table)
        q = f'SELECT * FROM "{table}" ORDER BY 1 LIMIT %s'
        with self.conn.cursor() as cur:
            cur.execute(q, (limit,))
            return cur.fetchall()

    # Перевірити наявність рядка за PK
    def row_exists(self, table, pk_col, value):
        self._validate_table(table)
        q = f'SELECT 1 FROM "{table}" WHERE "{pk_col}" = %s LIMIT 1'
        with self.conn.cursor() as cur:
            cur.execute(q, (value,))
            return cur.fetchone() is not None

    # Вставка батьків
    def insert_parent(self, parents_id, first_name, last_name, phone, email):
        if parents_id is None:
            raise ValidationError("parents_id обов'язковий для вставки (не можна автогенерувати).")
        with self.conn.cursor() as cur:
            cur.execute(
                'INSERT INTO "parents"(parents_id, first_name, last_name, phone, email) '
                'VALUES (%s,%s,%s,%s,%s) RETURNING parents_id;',
                (parents_id, first_name, last_name, phone, email)
            )
            row = cur.fetchone()
            self.conn.commit()
            return row["parents_id"] if isinstance(row, dict) else row[0]

    # Вставка студентів
    def insert_student(self, student_id, parents_id, first_name, last_name, birth_date, class_, email):
        if student_id is None:
            raise ValidationError("student_id обов'язковий для вставки (не можна автогенерувати).")
        if parents_id is not None and not self.row_exists("parents", "parents_id", parents_id):
            raise ValidationError("Parent with given parents_id not found.")
        with self.conn.cursor() as cur:
            q = 'INSERT INTO "student"(student_id, parents_id, first_name, last_name, birth_date, class, email) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING student_id;'
            cur.execute(q, (student_id, parents_id, first_name, last_name, birth_date, class_, email))
            sid = cur.fetchone()["student_id"]
            self.conn.commit()
            return sid

    # Вставка вчителя
    def insert_teacher(self, teacher_id, first_name, last_name, email):
        if teacher_id is None:
            raise ValidationError("teacher_id обов'язковий для вставки (не можна автогенерувати).")
        with self.conn.cursor() as cur:
            q = 'INSERT INTO "teacher"(teacher_id, first_name, last_name, email) VALUES (%s,%s,%s,%s) RETURNING teacher_id;'
            cur.execute(q, (teacher_id, first_name, last_name, email))
            tid = cur.fetchone()["teacher_id"]
            self.conn.commit()
            return tid

    # Вставка предмета
    def insert_subject(self, subject_id, name):
        if subject_id is None:
            raise ValidationError("subject_id обов'язковий для вставки (не можна автогенерувати).")
        with self.conn.cursor() as cur:
            q = 'INSERT INTO "subject"(subject_id, name) VALUES (%s,%s) RETURNING subject_id;'
            cur.execute(q, (subject_id, name))
            sid = cur.fetchone()["subject_id"]
            self.conn.commit()
            return sid

    # Вставка журналу
    def insert_journal(self, journal_id, student_id, teacher_id, subject_id, entry_date, grade, attendance_status):
        if journal_id is None:
            raise ValidationError("journal_id обов'язковий для вставки (не можна автогенерувати).")
        if not self.row_exists("student", "student_id", student_id):
            raise ValidationError("Student not found.")
        if teacher_id is not None and not self.row_exists("teacher", "teacher_id", teacher_id):
            raise ValidationError("Teacher not found.")
        if subject_id is not None and not self.row_exists("subject", "subject_id", subject_id):
            raise ValidationError("Subject not found.")

        if attendance_status is not None and attendance_status not in ATTENDANCE_STATUSES:
            raise ValidationError("Invalid attendance_status.")

        if attendance_status == 'absent':
            if grade is not None:
                raise ValidationError("If attendance is 'absent', grade must be NULL / not provided.")
        else:
            if grade is None:
                raise ValidationError("For present/late attendance grade must be provided (1..12).")
            try:
                g = int(grade)
            except Exception:
                raise ValidationError("Grade must be integer.")
            if g < 1 or g > 12:
                raise ValidationError("Grade out of allowed range (1-12).")

        with self.conn.cursor() as cur:
            q = """INSERT INTO "journal"(journal_id, student_id, teacher_id, subject_id, entry_date, grade, attendance_status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING journal_id;"""
            cur.execute(q, (journal_id, student_id, teacher_id, subject_id, entry_date, grade, attendance_status))
            jid = cur.fetchone()["journal_id"]
            self.conn.commit()
            return jid

    # Повернути всі рядки за PK
    def select_by_pk(self, table, pk_col, pk_val):
        self._validate_table(table)
        if pk_val is None:
            q = f'SELECT * FROM "{table}" ORDER BY 1 LIMIT 10;'
            with self.conn.cursor() as cur:
                cur.execute(q)
                return cur.fetchall()
        q = f'SELECT * FROM "{table}" WHERE "{pk_col}" = %s;'
        with self.conn.cursor() as cur:
            cur.execute(q, (pk_val,))
            return cur.fetchall()

    # Допоміжний метод — приклади дочірніх рядків, які посилаються на батьківські PK
    def select_child_examples(self, child_table, child_col, parent_table, limit=10):
        self._validate_table(child_table)
        self._validate_table(parent_table)
        parent_pk = self.PK_MAP.get(parent_table)
        if not parent_pk:
            return []

        q = f'''
            SELECT c.*
            FROM "{child_table}" c
            JOIN "{parent_table}" p ON c."{child_col}" = p."{parent_pk}"
            LIMIT %s;
        '''
        with self.conn.cursor() as cur:
            cur.execute(q, (limit,))
            return cur.fetchall()

    # Оновити рядок по PK
    def update_by_pk(self, table, pk_col, pk_val, updates: dict):
        self._validate_table(table)
        if not updates:
            return None
        cols = self._get_columns_list(table)
        set_parts = []
        params = []
        for k, v in updates.items():
            if k not in cols:
                raise ValueError(f"Невідомий стовпець: {k}")
            set_parts.append(f'"{k}" = %s')
            params.append(v)
        params.append(pk_val)
        set_clause = ", ".join(set_parts)
        q = f'UPDATE "{table}" SET {set_clause} WHERE "{pk_col}" = %s RETURNING *;'
        with self.conn.cursor() as cur:
            cur.execute(q, tuple(params))
            row = cur.fetchone()
            self.conn.commit()
            return row

    # Порахувати дітей
    def count_children(self, child_table, fk_column, value):
        self._validate_table(child_table)
        q = f'SELECT COUNT(*) AS cnt FROM "{child_table}" WHERE "{fk_column}" = %s'
        with self.conn.cursor() as cur:
            cur.execute(q, (value,))
            return cur.fetchone()["cnt"]

    # Видалити всі рядки в таблиці
    def delete_all(self, table):
        self._validate_table(table)
        fks = self.get_referencing_fks(table)
        child_counts = {}
        with self.conn.cursor() as cur:
            for fk in fks:
                child = fk['child_table']; child_col = fk['child_column']
                q = f'''
                    SELECT COUNT(*) AS cnt
                    FROM "{child}"
                    WHERE "{child_col}" IS NOT NULL
                      AND "{child_col}" IN (SELECT "{self.PK_MAP[table]}" FROM "{table}");
                '''
                cur.execute(q)
                rr = cur.fetchone()
                cnt = rr["cnt"] if isinstance(rr, dict) else rr[0]
                if cnt > 0:
                    child_counts[child] = cnt

            if child_counts:
                raise ChildRowsExistError(child_counts)

            cur.execute(f'DELETE FROM "{table}";')
            deleted = cur.rowcount
            self.conn.commit()
            return deleted

    # Порахувати рядки в таблиці
    def count_rows(self, table):
        self._validate_table(table)
        q = f'SELECT COUNT(*) AS cnt FROM "{table}";'
        with self.conn.cursor() as cur:
            cur.execute(q)
            return cur.fetchone()["cnt"]

    # Отримати FK, які посилаються на цю таблицю
    def get_referencing_fks(self, table):
        q = """
        SELECT
          kcu.table_name AS child_table,
          kcu.column_name AS child_column
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND ccu.table_name = %s
          AND ccu.table_schema = 'public';
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (table,))
            rows = cur.fetchall()
            res = []
            for r in rows:
                if isinstance(r, dict):
                    res.append({'child_table': r['child_table'], 'child_column': r['child_column']})
                else:
                    res.append({'child_table': r[0], 'child_column': r[1]})
            return res

    # Попередній підрахунок дочірніх записів (для видалення по PK)
    def preview_child_counts(self, table, pk_col, pk_val):
        self._validate_table(table)
        counts = {}
        with self.conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) AS cnt FROM "{table}" WHERE "{pk_col}" = %s;', (pk_val,))
            r = cur.fetchone()
            parent_exists = (r["cnt"] if isinstance(r, dict) else r[0]) > 0
            counts[table] = 1 if parent_exists else 0
            fks = self.get_referencing_fks(table)
            for fk in fks:
                child = fk['child_table']; child_col = fk['child_column']
                cur.execute(f'SELECT COUNT(*) AS cnt FROM "{child}" WHERE "{child_col}" = %s;', (pk_val,))
                rr = cur.fetchone()
                cnt = rr["cnt"] if isinstance(rr, dict) else rr[0]
                counts[child] = cnt
        return counts

    # Видалення по PK з забороною при наявності дочірніх записів
    def delete_by_pk(self, table, pk_col, pk_val):
        self._validate_table(table)
        counts = self.preview_child_counts(table, pk_col, pk_val)
        if counts.get(table, 0) == 0:
            return None, {}
        child_totals = {t: c for t, c in counts.items() if t != table and c > 0}
        if child_totals:
            raise ChildRowsExistError(child_totals)
        with self.conn.cursor() as cur:
            cur.execute(f'DELETE FROM "{table}" WHERE "{pk_col}" = %s RETURNING *;', (pk_val,))
            row = cur.fetchone()
            self.conn.commit()
            deleted_counts = {table: (1 if row else 0)}
            return row, deleted_counts

    # Генерація батьків
    def generate_parents(self, n):
        q = """
        WITH maxv AS (
          SELECT COALESCE(MAX(parents_id), 0) AS m FROM "parents"
        ), gens AS (
          SELECT m + row_number() OVER () AS new_id,
                 left(md5(random()::text),8) AS fn,
                 left(md5(random()::text),8) AS ln,
                 ('+380' || (100000000 + floor(random()*900000000)::bigint)::text) AS phone,
                 lower(left(md5(random()::text),8) || '@example.com') AS em
          FROM maxv, generate_series(1, %s)
        )
        INSERT INTO "parents"(parents_id, first_name, last_name, phone, email)
        SELECT new_id, fn, ln, phone, em FROM gens;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (n,))
            self.conn.commit()

    # Генерація вчителів
    def generate_teachers(self, n):
        q = """
        WITH maxv AS (
          SELECT COALESCE(MAX(teacher_id), 0) AS m FROM "teacher"
        ), gens AS (
          SELECT m + row_number() OVER () AS new_id,
                 left(md5(random()::text),8) AS fn,
                 left(md5(random()::text),8) AS ln,
                 lower(left(md5(random()::text),8) || '@example.com') AS em
          FROM maxv, generate_series(1, %s)
        )
        INSERT INTO "teacher"(teacher_id, first_name, last_name, email)
        SELECT new_id, fn, ln, em FROM gens;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (n,))
            self.conn.commit()

    # Генерація предметів
    def generate_subjects(self, n):
        q = """
        WITH maxv AS (
          SELECT COALESCE(MAX(subject_id), 0) AS m FROM "subject"
        ), gens AS (
          SELECT m + row_number() OVER () AS new_id,
                 left(md5(random()::text),10) AS nm
          FROM maxv, generate_series(1, %s)
        )
        INSERT INTO "subject"(subject_id, name)
        SELECT new_id, nm FROM gens;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (n,))
            self.conn.commit()

    # Генерація студентів
    def generate_students(self, n):
        q = """
        WITH maxv AS (
          SELECT COALESCE(MAX(student_id), 0) AS m FROM "student"
        ), pids AS (
          SELECT row_number() OVER (ORDER BY parents_id) AS idx, parents_id
          FROM "parents"
        ), gens AS (
          SELECT m + row_number() OVER () AS new_id,
                 (floor(random() * (SELECT count(*) FROM "parents"))::int + 1) AS pidx,
                 left(md5(random()::text),6) AS fn,
                 left(md5(random()::text),6) AS ln,
                 (date '2005-01-01' + (trunc(random()*4000)::int))::date AS bd,
                 (floor(1 + random()*11)::int)::text
                    || (CASE WHEN random() < 0.25 THEN chr((65 + floor(random()*2))::int) ELSE '' END) AS cls,
                 lower(left(md5(random()::text),6) || '@example.com') AS em
          FROM maxv, generate_series(1, %s)
        )
        INSERT INTO "student"(student_id, parents_id, first_name, last_name, birth_date, class, email)
        SELECT g.new_id, p.parents_id, g.fn, g.ln, g.bd, g.cls, g.em
        FROM gens g
        JOIN pids p ON p.idx = g.pidx;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (n,))
            self.conn.commit()

    # Генерація журналу
    def generate_journal(self, n):
        if n <= 0:
            return

        insert_q = """
        WITH maxv AS (
          SELECT COALESCE(MAX(journal_id), 0) AS m FROM "journal"
        ), counts AS (
          SELECT (SELECT count(*) FROM "student") AS students_count,
                 (SELECT count(*) FROM "teacher") AS teachers_count,
                 (SELECT count(*) FROM "subject") AS subjects_count
        ), gens AS (
          SELECT
            m + row_number() OVER () AS new_id,
            (floor(random() * (SELECT students_count FROM counts))::int + 1) AS s_idx,
            (floor(random() * (SELECT teachers_count FROM counts))::int + 1) AS t_idx,
            (floor(random() * (SELECT subjects_count FROM counts))::int + 1) AS sb_idx,
            (date '2020-01-01' + (trunc(random()*2000)::int))::date AS ed,
            (floor(random()*12)::int + 1) AS gr_rand
          FROM maxv, counts, generate_series(1, %s)
        )
        INSERT INTO "journal"(journal_id, student_id, teacher_id, subject_id, entry_date, grade, attendance_status)
        SELECT
          g.new_id,
          s.student_id,
          t.teacher_id,
          sb.subject_id,
          g.ed,
          g.gr_rand,
          'present'
        FROM gens g
        JOIN (SELECT row_number() OVER (ORDER BY student_id) AS idx, student_id FROM "student") s ON s.idx = g.s_idx
        JOIN (SELECT row_number() OVER (ORDER BY teacher_id) AS idx, teacher_id FROM "teacher") t ON t.idx = g.t_idx
        JOIN (SELECT row_number() OVER (ORDER BY subject_id) AS idx, subject_id FROM "subject") sb ON sb.idx = g.sb_idx;
        """

        with self.conn.cursor() as cur:
            try:
                cur.execute('SELECT COALESCE(MAX(journal_id), 0) AS m FROM "journal";')
                old_m_row = cur.fetchone()
                old_max = old_m_row["m"] if isinstance(old_m_row, dict) else old_m_row[0]

                cur.execute(insert_q, (n,))
                self.conn.commit()

                update_q = """
                WITH new_rows AS (
                  SELECT journal_id, (floor(random()*3)::int) AS r
                  FROM "journal"
                  WHERE journal_id > %s
                )
                UPDATE "journal" j
                SET attendance_status = CASE new_rows.r WHEN 0 THEN 'present' WHEN 1 THEN 'absent' ELSE 'late' END,
                    grade = CASE WHEN new_rows.r = 1 THEN NULL ELSE j.grade END
                FROM new_rows
                WHERE j.journal_id = new_rows.journal_id;
                """
                cur.execute(update_q, (old_max,))
                self.conn.commit()
            except Exception:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                raise

    # Складні запити — середній бал по предметах для класу
    def complex_query_1(self, class_value):
        q = """
        SELECT sb.name AS subject, COUNT(j.journal_id) AS marks_count, AVG(j.grade) AS avg_grade
        FROM "journal" j
        JOIN "subject" sb ON j.subject_id = sb.subject_id
        JOIN "student" s ON j.student_id = s.student_id
        WHERE s.class = %s
        GROUP BY sb.name
        ORDER BY avg_grade DESC;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (class_value,))
            return cur.fetchall()

    # Складні запити — кількість оцінок по вчителях за період
    def complex_query_2(self, date_from, date_to):
        q = """
        SELECT t.first_name || ' ' || t.last_name AS teacher, COUNT(j.journal_id) AS marks_count
        FROM "journal" j
        JOIN "teacher" t ON j.teacher_id = t.teacher_id
        WHERE j.entry_date BETWEEN %s AND %s
        GROUP BY teacher ORDER BY marks_count DESC LIMIT 50;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (date_from, date_to))
            return cur.fetchall()

    # Складні запити — розподіл відвідуваності по класам для предмета
    def complex_query_3(self, subject_name):
        q = """
        SELECT s.class, j.attendance_status, COUNT(*) AS cnt
        FROM "journal" j
        JOIN "student" s ON j.student_id = s.student_id
        JOIN "subject" sb ON j.subject_id = sb.subject_id
        WHERE sb.name = %s
        GROUP BY s.class, j.attendance_status
        ORDER BY s.class;
        """
        with self.conn.cursor() as cur:
            cur.execute(q, (subject_name,))
            return cur.fetchall()