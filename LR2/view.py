import decimal


class View:
    def show_menu(self):
        print("\n================= МЕНЮ =================")
        print("1. Показати список таблиць")
        print("2. Перегляд даних таблиці (limit)")
        print("3. Вставка даних в таблицю")
        print("4. Оновлення даних у таблиці")
        print("5. Видалення даних з таблиці (по PK)")
        print("6. Вихід")
        return input("Оберіть варіант: ").strip()

    def choose_table(self, tables):
        print("\nОберіть таблицю:")
        for i, t in enumerate(tables, 1):
            print(f"{i}. {t}")
        sel = input("Номер таблиці: ").strip()
        try:
            idx = int(sel) - 1
            return tables[idx]
        except Exception:
            print("Невірний вибір таблиці.")
            return None

    def input_prompt(self, prompt):
        return input(prompt)

    def _format_value(self, v):
        if v is None:
            return ""
        if isinstance(v, decimal.Decimal):
            if v == v.to_integral():
                return str(int(v))
            return f"{float(v):.2f}"
        if isinstance(v, float):
            if v.is_integer():
                return str(int(v))
            return f"{v:.2f}"
        return str(v)

    def show_rows(self, rows):
        if not rows:
            print("Немає результатів.")
            return
        first = rows[0]
        if isinstance(first, dict):
            keys = list(first.keys())
            col_widths = {}
            for k in keys:
                maxw = len(str(k))
                for r in rows:
                    v = r.get(k, "")
                    s = self._format_value(v)
                    if len(s) > maxw:
                        maxw = len(s)
                col_widths[k] = maxw
            header = " | ".join(k.ljust(col_widths[k]) for k in keys)
            sep = "-+-".join("-" * col_widths[k] for k in keys)
            print(header)
            print(sep)
            for r in rows:
                line = " | ".join(self._format_value(r.get(k, "")).ljust(col_widths[k]) for k in keys)
                print(line)
        else:
            for r in rows:
                if isinstance(r, (list, tuple)):
                    print(" | ".join(self._format_value(v) for v in r))
                else:
                    print(self._format_value(r))

    def show_message(self, msg):
        print(msg)
