РГР — «Електронний журнал» (на основі ЛР №1)

**Тема:** Проєктування та реалізація реляційної БД для електронного журналу  

## Короткий опис
Робота містить проєкт бази даних «Електронний журнал»: ER-модель, реляційну схему, SQL-скрипти для створення таблиць та звіт.

## Короткий опис таблиць (структура)
- `parents` — батьки (contacts): `parents_id` PK, `first_name`, `last_name`, `phone`, `email`.  
- `student` — учні: `student_id` PK, `parents_id` FK → `parents`, `first_name`, `last_name`, `birth_date`, `class`, `email`.  
- `teacher` — викладачі: `teacher_id` PK, `first_name`, `last_name`, `email`.  
- `subject` — предмети: `subject_id` PK, `name`.  
- `journal` — записи журналу: `journal_id` PK, `student_id` FK → `student`, `teacher_id` FK → `teacher`, `subject_id` FK → `subject`, `entry_date`, `grade`, `attendance_status`.
