import re
from datetime import datetime
import psycopg2
from psycopg2 import errors

from model import Model, ValidationError
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
                # Показати список таблиць
                self.view.show_rows([{"table": t} for t in self.model.get_tables()])
            elif cmd == "2":
                # Перегляд даних таблиці
                self.handle_list_table()
            elif cmd == "3":
                # Вставка
                self.handle_insert()
            elif cmd == "4":
                # Оновлення
                self.handle_update()
            elif cmd == "5":
                # Видалення
                self.handle_delete()
            elif cmd == "6":
                self.view.show_message("Вихід...")
                self.model.close()
                break
            else:
                self.view.show_message("Невідома команда.")

    # Перегляд даних таблиці
    def handle_list_table(self):
        table = self.view.choose_table(self.model.get_tables())
        if not table:
            return
        rows = self.model.list_table(table, limit=200)
        self.view.show_rows(rows)

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
                except errors.UniqueViolation:
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
                except errors.ForeignKeyViolation:
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
                except errors.ForeignKeyViolation:
                    self.view.show_message("Помилка: FK violation при вставці journal")
                except Exception as e:
                    self.view.show_message(f"Помилка при вставці: {e}")

        except Exception as e:
            self.view.show_message(f"Помилка при вставці: {e}")

    # Оновлення записів
    def handle_update(self):
        table = self.view.choose_table(self.model.get_tables())
        if not table:
            return
        pk = input("PK column name (наприклад student_id): ").strip()
        pkv = input("PK value: ").strip()
        if pkv.isdigit():
            pkv = int(pkv)
        updates = {}
        self.view.show_message("Вводьте col=val. Порожній рядок для завершення.")
        while True:
            s = input("col=val: ").strip()
            if not s:
                break
            if "=" not in s:
                print("Формат col=val")
                continue
            col, val = s.split("=", 1)
            col = col.strip()
            val = val.strip() or None
            updates[col] = val
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

        pkv_raw = input(f"PK value ({pk_col}): ").strip()
        if pk_type and any(x in pk_type for x in ("integer", "bigint", "smallint", "serial", "bigserial")):
            try:
                pkv = int(pkv_raw)
            except Exception:
                self.view.show_message("PK має бути цілим числом.")
                return
        else:
            pkv = pkv_raw

        try:
            row = self.model.delete_by_pk(table, pk_col, pkv)
            if row:
                self.view.show_message(f"Видалено рядок: {row}")
            else:
                self.view.show_message("Рядка з таким PK не знайдено.")
        except errors.ForeignKeyViolation:
            self.view.show_message("Неможливо видалити: існують пов'язані записи (порушення зовнішнього ключа).")
        except Exception as e:
            self.view.show_message(f"Помилка при видаленні: {e}")

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
            except Exception:
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
