import os

from sqlalchemy import (
    create_engine, Column, Integer, String, Date, ForeignKey
)
from sqlalchemy.orm import (
    declarative_base, relationship, sessionmaker
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.inspection import inspect

# Налаштування підключення до БД
DB = {
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "1111"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

ALLOWED_TABLES = ["parents", "student", "teacher", "subject", "journal"]
ATTENDANCE_STATUSES = ("present", "absent", "late")

Base = declarative_base()

# ORM-класи сутностей
class Parents(Base):
    __tablename__ = "parents"

    parents_id = Column(Integer, primary_key=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    phone = Column(String, nullable=False)
    email = Column(String, nullable=False)

    students = relationship("Student", back_populates="parent")


class Teacher(Base):
    __tablename__ = "teacher"

    teacher_id = Column(Integer, primary_key=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, nullable=False)

    journals = relationship("Journal", back_populates="teacher")


class Subject(Base):
    __tablename__ = "subject"

    subject_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    journals = relationship("Journal", back_populates="subject")


class Student(Base):
    __tablename__ = "student"

    student_id = Column(Integer, primary_key=True)
    parents_id = Column(Integer, ForeignKey("parents.parents_id"))
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    birth_date = Column(Date, nullable=False)
    # назва стовпця в БД "class", але в Python не можна мати змінну class
    class_ = Column("class", String, nullable=False)
    email = Column(String, nullable=False)

    parent = relationship("Parents", back_populates="students")
    journals = relationship("Journal", back_populates="student")


class Journal(Base):
    __tablename__ = "journal"

    journal_id = Column(Integer, primary_key=True)
    student_id = Column(Integer, ForeignKey("student.student_id"))
    teacher_id = Column(Integer, ForeignKey("teacher.teacher_id"))
    subject_id = Column(Integer, ForeignKey("subject.subject_id"))
    entry_date = Column(Date, nullable=False)
    grade = Column(Integer, nullable=True)
    attendance_status = Column(String, nullable=False)

    student = relationship("Student", back_populates="journals")
    teacher = relationship("Teacher", back_populates="journals")
    subject = relationship("Subject", back_populates="journals")


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

    ORM_CLASS_MAP = {
        "parents": Parents,
        "teacher": Teacher,
        "subject": Subject,
        "student": Student,
        "journal": Journal,
    }

    # Ініціалізація engine + session (ORM)
    def __init__(self):
        conn_str = (
            f"postgresql+psycopg2://{DB['user']}:{DB['password']}"
            f"@{DB['host']}:{DB['port']}/{DB['dbname']}"
        )
        self.engine = create_engine(conn_str, echo=False, future=True)
        SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)
        self.session = SessionLocal()
        Base.metadata.create_all(self.engine)

    # Закрити з'єднання
    def close(self):
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()

    # Допоміжні методи
    def _validate_table(self, table):
        if table not in ALLOWED_TABLES:
            raise ValueError("Невідома таблиця")

    def _obj_to_dict(self, obj):
        mapper = inspect(obj).mapper
        res = {}
        for attr in mapper.column_attrs:
            col_name = attr.expression.name  # ім'я колонки в БД
            res[col_name] = getattr(obj, attr.key)
        return res

    def _get_columns_list(self, table):
        self._validate_table(table)
        cls = self.ORM_CLASS_MAP[table]
        mapper = inspect(cls)
        return [col.name for col in mapper.columns]

    # Повернути список таблиць
    def get_tables(self):
        return ALLOWED_TABLES.copy()

    # Інформація про стовпці (для визначення типу PK у Controller)
    def get_columns(self, table):

        self._validate_table(table)
        cls = self.ORM_CLASS_MAP[table]
        mapper = inspect(cls)

        rows = []
        for col in mapper.columns:
            col_name = col.name
            col_type = col.type
            if isinstance(col_type, Integer):
                type_name = "integer"
            elif isinstance(col_type, String):
                type_name = "character varying"
            elif isinstance(col_type, Date):
                type_name = "date"
            else:
                type_name = type(col_type).__name__.lower()

            is_nullable = "YES" if col.nullable else "NO"

            rows.append(
                {
                    "column_name": col_name,
                    "data_type": type_name,
                    "is_nullable": is_nullable,
                }
            )
        return rows

    # Перегляд даних таблиці (через ORM)
    def list_table(self, table, limit=200):
        self._validate_table(table)
        cls = self.ORM_CLASS_MAP[table]
        pk_col = self.PK_MAP.get(table)

        query = self.session.query(cls)
        if pk_col is not None:
            query = query.order_by(getattr(cls, pk_col))

        objs = query.limit(limit).all()
        return [self._obj_to_dict(o) for o in objs]

    # Перевірка наявності рядка за PK (через ORM)
    def row_exists(self, table, pk_col, value):
        self._validate_table(table)
        cls = self.ORM_CLASS_MAP.get(table)
        if cls is None:
            raise ValueError("Невідомий ORM-клас для таблиці")
        obj = (
            self.session.query(cls)
            .filter(getattr(cls, pk_col) == value)
            .first()
        )
        return obj is not None

    # Insert-и через ORM
    def insert_parent(self, parents_id, first_name, last_name, phone, email):
        if parents_id is None:
            raise ValidationError(
                "parents_id обов'язковий для вставки (не можна автогенерувати)."
            )
        obj = Parents(
            parents_id=parents_id,
            first_name=first_name,
            last_name=last_name,
            phone=phone,
            email=email,
        )
        self.session.add(obj)
        try:
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            raise e.orig
        return obj.parents_id

    def insert_student(
        self,
        student_id,
        parents_id,
        first_name,
        last_name,
        birth_date,
        class_,
        email,
    ):
        if student_id is None:
            raise ValidationError(
                "student_id обов'язковий для вставки (не можна автогенерувати)."
            )
        if parents_id is not None and not self.row_exists(
            "parents", "parents_id", parents_id
        ):
            raise ValidationError("Parent with given parents_id not found.")

        obj = Student(
            student_id=student_id,
            parents_id=parents_id,
            first_name=first_name,
            last_name=last_name,
            birth_date=birth_date,
            class_=class_,
            email=email,
        )
        self.session.add(obj)
        try:
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            raise e.orig
        return obj.student_id

    def insert_teacher(self, teacher_id, first_name, last_name, email):
        if teacher_id is None:
            raise ValidationError(
                "teacher_id обов'язковий для вставки (не можна автогенерувати)."
            )
        obj = Teacher(
            teacher_id=teacher_id,
            first_name=first_name,
            last_name=last_name,
            email=email,
        )
        self.session.add(obj)
        try:
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            raise e.orig
        return obj.teacher_id

    def insert_subject(self, subject_id, name):
        if subject_id is None:
            raise ValidationError(
                "subject_id обов'язковий для вставки (не можна автогенерувати)."
            )
        obj = Subject(subject_id=subject_id, name=name)
        self.session.add(obj)
        try:
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            raise e.orig
        return obj.subject_id

    def insert_journal(
        self,
        journal_id,
        student_id,
        teacher_id,
        subject_id,
        entry_date,
        grade,
        attendance_status,
    ):
        if journal_id is None:
            raise ValidationError(
                "journal_id обов'язковий для вставки (не можна автогенерувати)."
            )
        if not self.row_exists("student", "student_id", student_id):
            raise ValidationError("Student not found.")
        if teacher_id is not None and not self.row_exists(
            "teacher", "teacher_id", teacher_id
        ):
            raise ValidationError("Teacher not found.")
        if subject_id is not None and not self.row_exists(
            "subject", "subject_id", subject_id
        ):
            raise ValidationError("Subject not found.")

        if attendance_status is not None and attendance_status not in ATTENDANCE_STATUSES:
            raise ValidationError("Invalid attendance_status.")

        if attendance_status == "absent":
            if grade is not None:
                raise ValidationError(
                    "If attendance is 'absent', grade must be NULL / not provided."
                )
        else:
            if grade is None:
                raise ValidationError(
                    "For present/late attendance grade must be provided (1..12)."
                )
            try:
                g = int(grade)
            except Exception:
                raise ValidationError("Grade must be integer.")
            if g < 1 or g > 12:
                raise ValidationError("Grade out of allowed range (1-12).")

        obj = Journal(
            journal_id=journal_id,
            student_id=student_id,
            teacher_id=teacher_id,
            subject_id=subject_id,
            entry_date=entry_date,
            grade=grade,
            attendance_status=attendance_status,
        )
        self.session.add(obj)
        try:
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            raise e.orig
        return obj.journal_id

    # Update через ORM
    def update_by_pk(self, table, pk_col, pk_val, updates: dict):
        self._validate_table(table)
        if not updates:
            return None

        cls = self.ORM_CLASS_MAP.get(table)
        if cls is None:
            raise ValueError("Невідомий ORM-клас для таблиці")

        obj = (
            self.session.query(cls)
            .filter(getattr(cls, pk_col) == pk_val)
            .one_or_none()
        )
        if obj is None:
            return None

        cols = self._get_columns_list(table)

        for col_name, value in updates.items():
            if col_name not in cols:
                raise ValueError(f"Невідомий стовпець: {col_name}")
            attr_name = col_name
            if table == "student" and col_name == "class":
                attr_name = "class_"

            if not hasattr(obj, attr_name):
                raise ValueError(f"Невідомий атрибут: {attr_name}")

            setattr(obj, attr_name, value)

        try:
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            raise e.orig

        return self._obj_to_dict(obj)

    # Delete через ORM
    def delete_by_pk(self, table, pk_col, pk_val):
        self._validate_table(table)
        cls = self.ORM_CLASS_MAP.get(table)
        if cls is None:
            raise ValueError("Невідомий ORM-клас для таблиці")

        obj = (
            self.session.query(cls)
            .filter(getattr(cls, pk_col) == pk_val)
            .one_or_none()
        )
        if obj is None:
            return None

        data = self._obj_to_dict(obj)
        self.session.delete(obj)
        try:
            self.session.commit()
        except IntegrityError as e:
            self.session.rollback()
            raise e.orig

        return data