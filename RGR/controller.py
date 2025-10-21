import re
import time
from datetime import datetime
import psycopg

from model import Model, ChildRowsExistError, ValidationError
from view import View

EMAIL_RE = re.compile(r'^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$', re.I)
PHONE_RE = re.compile(r'^\+380[0-9]{9}$')
CLASS_RE = re.compile(r'^[0-9]{1,2}[A-Za-z\-]?$')
GRADE_MIN, GRADE_MAX = 1, 12
ATTENDANCE_STATUSES = ('present', 'absent', 'late')

class Controller:
    # Ініціалізація контролера
    def __init__(self):
        self.model = Model()
        self.view = View()

    # Головний цикл меню
    def run(self):
        while True:
            cmd = self.view.show_menu()
            if cmd == "1":
                self.view.show_rows([{"table": t} for t in self.model.get_tables()])
            elif cmd == "2":
                table = self.view.choose_table(self.model.get_tables())
                if table:
                    cols = self.model.get_columns(table)
                    self.view.show_rows(cols)
            elif cmd == "3":
                table = self.view.choose_table(self.model.get_tables())
                if table:
                    cols = self.model.get_columns(table)
                    names = [{"column_name": c["column_name"]} for c in cols]
                    self.view.show_rows(names)
            elif cmd == "4":
                table = self.view.choose_table(self.model.get_tables())
                if table:
                    rows = self.model.list_table(table, limit=200)
                    self.view.show_rows(rows)
            elif cmd == "5":
                table = self.view.choose_table(self.model.get_tables())
                if table:
                    fks = self.model.get_referencing_fks(table)
                    if not fks:
                        self.view.show_message("Зовнішніх ключів не знайдено.")
                    else:
                        self.view.show_rows(fks)
            elif cmd == "6":
                self.handle_generate()
            elif cmd == "7":
                self.handle_insert()
            elif cmd == "8":
                self.handle_update()
            elif cmd == "9":
                self.handle_delete()
            elif cmd == "10":
                self.handle_delete_all()
            elif cmd == "11":
                self.handle_complex_queries()
            elif cmd == "12":
                self.view.show_message("Вихід..."); self.model.close(); break
            else:
                self.view.show_message("Невідома команда.")

    # Обробка вставки записів
    def handle_insert(self):
        table = self.view.choose_table(self.model.get_tables())
        if not table:
            return
        try:
            pk_name = self.model.PK_MAP.get(table)
            while True:
                pk_raw = input(f"{pk_name}: ").strip()
                if pk_raw == "":
                    print("Порожнє значення недопустиме.")
                    continue
                if not pk_raw.isdigit():
                    print("Потрібно ввести ціле число.")
                    continue
                pk_val = int(pk_raw)
                break

            if table == "parents":
                fn = self._read_nonempty("First name: ")
                ln = self._read_nonempty("Last name: ")
                phone = self._read_phone("Phone (+380XXXXXXXXX): ")
                email = self._read_email("Email: ")
                try:
                    pid = self.model.insert_parent(pk_val, fn, ln, phone, email)
                    self.view.show_message(f"Inserted parents_id={pid}")
                except ValidationError as e:
                    self.view.show_message(f"Помилка валідації: {e}")
                except psycopg.errors.UniqueViolation:
                    self.view.show_message("Помилка: parents_id вже існує (duplicate key).")
                except Exception as e:
                    self.view.show_message(f"Помилка при вставці: {e}")

            elif table == "student":
                while True:
                    parents_raw = input("parents_id: ").strip()
                    if not parents_raw.isdigit():
                        print("parents_id має бути цілим числом.")
                        continue
                    parents_val = int(parents_raw)
                    if not self.model.row_exists("parents", "parents_id", parents_val):
                        print("Parent з таким ID не знайдений. Введіть існуючий parents_id.")
                        continue
                    break
                fn = self._read_nonempty("First name: ")
                ln = self._read_nonempty("Last name: ")
                birth = self._read_date("Birth date (YYYY-MM-DD): ")
                class_ = self._read_class("Class (наприклад 10A): ")
                email = self._read_email("Email: ")
                try:
                    sid = self.model.insert_student(pk_val, parents_val, fn, ln, birth, class_, email)
                    self.view.show_message(f"Inserted student_id={sid}")
                except ValidationError as e:
                    self.view.show_message(f"Помилка валідації: {e}")
                except psycopg.errors.ForeignKeyViolation:
                    self.view.show_message("Помилка: батько з таким ID не знайдений (FK).")
                except Exception as e:
                    self.view.show_message(f"Помилка при вставці: {e}")

            elif table == "teacher":
                fn = self._read_nonempty("First name: ")
                ln = self._read_nonempty("Last name: ")
                email = self._read_email("Email: ")
                try:
                    tid = self.model.insert_teacher(pk_val, fn, ln, email)
                    self.view.show_message(f"Inserted teacher_id={tid}")
                except ValidationError as e:
                    self.view.show_message(f"Помилка валідації: {e}")
                except Exception as e:
                    self.view.show_message(f"Помилка при вставці: {e}")

            elif table == "subject":
                name = self._read_nonempty("Subject name: ")
                try:
                    suid = self.model.insert_subject(pk_val, name)
                    self.view.show_message(f"Inserted subject_id={suid}")
                except ValidationError as e:
                    self.view.show_message(f"Помилка валідації: {e}")
                except Exception as e:
                    self.view.show_message(f"Помилка при вставці: {e}")

            elif table == "journal":
                while True:
                    sid_raw = input("student_id: ").strip()
                    if not sid_raw.isdigit():
                        print("student_id має бути цілим числом.")
                        continue
                    sid = int(sid_raw)
                    if not self.model.row_exists("student", "student_id", sid):
                        print("Student не знайдений. Введіть існуючий student_id.")
                        continue
                    break

                while True:
                    tid_raw = input("teacher_id: ").strip()
                    if not tid_raw.isdigit():
                        print("teacher_id має бути цілим числом.")
                        continue
                    tid = int(tid_raw)
                    if not self.model.row_exists("teacher", "teacher_id", tid):
                        print("Teacher не знайдений. Введіть існуючий teacher_id.")
                        continue
                    break

                while True:
                    subid_raw = input("subject_id: ").strip()
                    if not subid_raw.isdigit():
                        print("subject_id має бути цілим числом.")
                        continue
                    subid = int(subid_raw)
                    if not self.model.row_exists("subject", "subject_id", subid):
                        print("Subject не знайдений. Введіть існуючий subject_id.")
                        continue
                    break

                entry = self._read_date("entry_date (YYYY-MM-DD): ")

                while True:
                    att = input("attendance (present/absent/late): ").strip()
                    if att not in ATTENDANCE_STATUSES:
                        print("Недопустимий attendance_status. Введіть present/absent/late.")
                        continue
                    break

                if att == 'absent':
                    grade = None
                else:
                    while True:
                        grade_raw = input("grade (1-12): ").strip()
                        if grade_raw == "":
                            print("Оцінка обов'язкова для present/late.")
                            continue
                        if not grade_raw.isdigit():
                            print("Оцінка має бути цілим числом.")
                            continue
                        grade = int(grade_raw)
                        if grade < GRADE_MIN or grade > GRADE_MAX:
                            print("Оцінка поза діапазоном 1..12.")
                            continue
                        break

                try:
                    jid = self.model.insert_journal(pk_val, sid, tid, subid, entry, grade, att)
                    self.view.show_message(f"Inserted journal_id={jid}")
                except ValidationError as e:
                    self.view.show_message(f"Помилка валідації: {e}")
                except psycopg.errors.ForeignKeyViolation:
                    self.view.show_message("Помилка: FK violation при вставці journal")
                except Exception as e:
                    self.view.show_message(f"Помилка при вставці: {e}")

        except Exception as e:
            self.view.show_message(f"Помилка при вставці: {e}")

    # Оновлення записів
    def handle_update(self):
        table = self.view.choose_table(self.model.get_tables())
        if not table: return
        pk = input("PK column name (наприклад student_id): ").strip()
        pkv = input("PK value: ").strip()
        if pkv.isdigit(): pkv = int(pkv)
        updates = {}
        self.view.show_message("Вводьте col=val. Порожній рядок для завершення.")
        while True:
            s = input("col=val: ").strip()
            if not s: break
            if "=" not in s:
                print("Формат col=val")
                continue
            col, val = s.split("=",1)
            col=col.strip(); val=val.strip() or None
            updates[col]=val
        try:
            row = self.model.update_by_pk(table, pk, pkv, updates)
            if row:
                self.view.show_message("Оновлено: " + str(row))
            else:
                self.view.show_message("Немає такого рядка.")
        except Exception as e:
            self.view.show_message(f"Помилка при оновленні: {e}")

    # Видалення по PK
    def handle_delete(self):
        table = self.view.choose_table(self.model.get_tables())
        if not table:
            return
        pk_col = self.model.PK_MAP.get(table)
        if not pk_col:
            self.view.show_message("PK не визначено для цієї таблиці.")
            return

        cols_info = self.model.get_columns(table)
        pk_type = None
        for c in cols_info:
            if c["column_name"] == pk_col:
                pk_type = c["data_type"]
                break

        print("Доступні стовпці для видалення по PK:", pk_col)
        pkv_raw = input(f"PK value ({pk_col}): ").strip()
        if pk_type and any(x in pk_type for x in ("integer","bigint","smallint","serial","bigserial")):
            try:
                pkv = int(pkv_raw)
            except:
                self.view.show_message("PK має бути цілим числом.")
                return
        else:
            pkv = pkv_raw

        try:
            preview = self.model.preview_child_counts(table, pk_col, pkv)
            if preview.get(table, 0) == 0:
                self.view.show_message("Рядок з таким PK не знайдено.")
                return
            self.view.show_message("Попередній підрахунок (рядок батька та дочірні записи):")
            for t, c in preview.items():
                self.view.show_message(f"  {t}: {c}")

            try:
                row, deleted_counts = self.model.delete_by_pk(table, pk_col, pkv)
                if row:
                    self.view.show_message(f"Видалено батьківський рядок: {row}")
                    if deleted_counts:
                        self.view.show_message("Фактично видалено:")
                        for tt, cc in deleted_counts.items():
                            self.view.show_message(f"  {tt}: {cc}")
                else:
                    self.view.show_message("Нічого не видалено.")
            except ChildRowsExistError as e:
                self.view.show_message("Неможливо видалити — знайдені залежні (дочірні) записи:")
                for t, c in e.counts.items():
                    self.view.show_message(f"  {t}: {c}")

                self.view.show_message("Приклади дочірніх рядків (перші 10) по кожній дочірній таблиці:")
                fks = self.model.get_referencing_fks(table)
                for child_table, _ in e.counts.items():
                    fk_cols = [fk['child_column'] for fk in fks if fk['child_table'] == child_table]
                    if not fk_cols:
                        continue
                    fk_col = fk_cols[0]
                    rows = self.model.select_by_pk(child_table, fk_col, pkv)
                    self.view.show_message(f"--- {child_table} where {fk_col} = {pkv} ---")
                    self.view.show_rows(rows[:10])
                self.view.show_message("Причина: існують рядки в дочірніх таблицях, які посилаються на цей батьківський PK (референційна цілісність).")
        except Exception as e:
            self.view.show_message(f"Помилка при видаленні: {e}")

    # Видалити всі записи таблиці
    def handle_delete_all(self):
        table = self.view.choose_table(self.model.get_tables())
        if not table:
            return
        cnt_preview = self.model.count_rows(table)
        self.view.show_message(f"В таблиці {table} зараз {cnt_preview} рядків.")
        confirm = input("Ви впевнені, що хочете видалити ВСЕ? Це дія незворотна. (y/n): ").strip().lower()
        if confirm not in ("y", "yes"):
            self.view.show_message("Операція скасована.")
            return
        try:
            deleted = self.model.delete_all(table)
            self.view.show_message(f"Видалено рядків у {table}: {deleted}")
        except ChildRowsExistError as e:
            self.view.show_message("Неможливо видалити — знайдені залежні (дочірні) записи:")
            for t, c in e.counts.items():
                self.view.show_message(f"  {t}: {c}")

            fks = self.model.get_referencing_fks(table)
            self.view.show_message("Приклади дочірніх рядків (перші 10) по кожній дочірній таблиці:")
            for child_table, _ in e.counts.items():
                fk_cols = [fk['child_column'] for fk in fks if fk['child_table'] == child_table]
                if not fk_cols:
                    continue
                fk_col = fk_cols[0]
                rows = self.model.select_child_examples(child_table, fk_col, table, limit=10)
                self.view.show_message(f"--- {child_table} (приклади записів що посилаються на {table}) ---")
                self.view.show_rows(rows[:10])
            self.view.show_message("Видалення скасовано. Щоб видалити — спочатку видаліть дочірні записи або змініть їх FK.")
        except Exception as e:
            self.view.show_message(f"Помилка при видаленні всіх рядків: {e}")

    # Генерація тестових даних
    def handle_generate(self):
        tbls = ["parents","teacher","subject","student","journal"]
        table = self.view.choose_table(tbls)
        if not table: return
        n_raw = input("Введіть кількість записів для генерації (наприклад 100000): ").strip()
        try:
            n = int(n_raw)
        except:
            self.view.show_message("Невірне число.")
            return
        try:
            if table == "parents":
                self.model.generate_parents(n)
            elif table == "teacher":
                self.model.generate_teachers(n)
            elif table == "subject":
                self.model.generate_subjects(n)
            elif table == "student":
                if not self.model.list_table("parents", limit=1):
                    self.view.show_message("Спочатку згенеруйте parents.")
                    return
                self.model.generate_students(n)
            elif table == "journal":
                if not (self.model.list_table("student", limit=1) and self.model.list_table("teacher", limit=1) and self.model.list_table("subject", limit=1)):
                    self.view.show_message("Спочатку згенеруйте student/teacher/subject.")
                    return
                self.model.generate_journal(n)
            self.view.show_message("Генерація завершена")
        except Exception as e:
            self.view.show_message(f"Помилка генерації: {e}")

    # Складні запити
    def handle_complex_queries(self):
        print("1) Середній бал по предметах для класу")
        print("2) Кількість оцінок по вчителях за період")
        print("3) Розподіл відвідуваності по класам для предмета")
        ch = input("Виберіть запит: ").strip()
        try:
            if ch == "1":
                cls = input("Введіть class (наприклад 10A): ").strip()
                t0 = time.time()
                rows = self.model.complex_query_1(cls)
                t = (time.time() - t0) * 1000
                self.view.show_rows(rows)
                self.view.show_message(f"Час виконання: {t:.2f} ms")
            elif ch == "2":
                d1 = input("Дата з (YYYY-MM-DD): ").strip()
                d2 = input("Дата по (YYYY-MM-DD): ").strip()
                t0 = time.time()
                rows = self.model.complex_query_2(d1, d2)
                t = (time.time() - t0) * 1000
                self.view.show_rows(rows)
                self.view.show_message(f"Час виконання: {t:.2f} ms")
            elif ch == "3":
                subj = input("Назва предмета: ").strip()
                t0 = time.time()
                rows = self.model.complex_query_3(subj)
                t = (time.time() - t0) * 1000
                self.view.show_rows(rows)
                self.view.show_message(f"Час виконання: {t:.2f} ms")
            else:
                self.view.show_message("Невірний вибір.")
        except Exception as e:
            self.view.show_message(f"Помилка виконання запиту: {e}")

    # Допоміжні методи для валідного вводу
    def _read_nonempty(self, prompt):
        while True:
            v = input(prompt).strip()
            if v == "":
                print("Значення не може бути порожнім.")
                continue
            return v

    def _read_email(self, prompt):
        while True:
            v = input(prompt).strip()
            if v == "":
                print("Email не може бути порожнім.")
                continue
            if not EMAIL_RE.match(v):
                print("Невірний email.")
                continue
            return v

    def _read_phone(self, prompt):
        while True:
            v = input(prompt).strip()
            if v == "":
                print("Телефон не може бути порожнім.")
                continue
            if not PHONE_RE.match(v):
                print("Невірний формат телефону. Очікується +380XXXXXXXXX")
                continue
            return v

    def _read_date(self, prompt):
        while True:
            v = input(prompt).strip()
            if v == "":
                print("Дата не може бути порожньою.")
                continue
            try:
                datetime.strptime(v, "%Y-%m-%d")
            except:
                print("Невірний формат дати. Очікується YYYY-MM-DD.")
                continue
            return v

    def _read_class(self, prompt):
        while True:
            v = input(prompt).strip()
            if v == "":
                print("Class не може бути порожнім.")
                continue
            if not CLASS_RE.match(v):
                print("Невірний формат class (наприклад 10A).")
                continue
            return v
