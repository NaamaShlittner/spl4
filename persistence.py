import sqlite3
import atexit
from dbtools import Dao
 
# Data Transfer Objects:
class Employee(object):
    def __init__(self, id, name, salary, branche):
        self.id = id
        self.name = name
        self.salary = salary
        self.branche = branche

    def __str__(self):
        return f"({self.id}, '{self.name}', {self.salary}, {self.branche})"

 
class Supplier(object):
    def __init__(self, id, name, contact_information):
        self.id = id
        self.name = name
        self.contact_information = contact_information

    def __str__(self):
        return f"({self.id}, '{self.name}', '{self.contact_information}')"

class Product(object):
    def __init__(self, id, description, price, quantity):
        self.id = id
        self.description = description
        self.price = price
        self.quantity = quantity

    def __str__(self):
        return f"({self.id}, '{self.description}', {self.price}, {self.quantity})"

class Branche(object):
    def __init__(self, id, location, number_of_employees):
        self.id = id
        self.location = location
        self.number_of_employees = number_of_employees

    def __str__(self):
        return f"({self.id}, '{self.location}', {self.number_of_employees})"

class Activitie(object):
    def __init__(self, product_id, quantity, activator_id, date):
        self.product_id = product_id
        self.quantity = quantity
        self.activator_id = activator_id
        self.date = date

    def __str__(self):
        return f"({self.product_id}, {self.quantity}, {self.activator_id}, '{self.date}')"
 
 
#Repository
class Repository(object):
    def __init__(self):
        self._conn = sqlite3.connect('bgumart.db')
        self.employees = Dao(Employee, self._conn)
        self.suppliers = Dao(Supplier, self._conn)
        self.products = Dao(Product, self._conn)
        self.branches = Dao(Branche, self._conn)
        self.activities = Dao(Activitie, self._conn)

 
    def _close(self):
        self._conn.commit()
        self._conn.close()
 
    def create_tables(self):
        self._conn.executescript("""
            CREATE TABLE employees (
                id              INT         PRIMARY KEY,
                name            TEXT        NOT NULL,
                salary          REAL        NOT NULL,
                branche    INT REFERENCES branches(id)
            );
    
            CREATE TABLE suppliers (
                id                   INTEGER    PRIMARY KEY,
                name                 TEXT       NOT NULL,
                contact_information  TEXT
            );

            CREATE TABLE products (
                id          INTEGER PRIMARY KEY,
                description TEXT    NOT NULL,
                price       REAL NOT NULL,
                quantity    INTEGER NOT NULL
            );

            CREATE TABLE branches (
                id                  INTEGER     PRIMARY KEY,
                location            TEXT        NOT NULL,
                number_of_employees INTEGER
            );
    
            CREATE TABLE activities (
                product_id      INTEGER REFERENCES products(id),
                quantity        INTEGER NOT NULL,
                activator_id    INTEGER NOT NULL,
                date            TEXT    NOT NULL
            );
        """)

    def check_activity(self, act: Activitie):
        product = self.products.find(id=act.product_id)
        return product and product[0].quantity + act.quantity >= 0
    
    def add_activity(self, act: Activitie):
        self.activities.insert(act)
        product = self.products.find(id=act.product_id)
        if product:
            new_quantity = product[0].quantity + act.quantity
            self.execute_command(f"UPDATE products SET quantity = {new_quantity} WHERE id = {act.product_id}")

    def get_table(self, table_name: str):
        dao_mapping = {
            "suppliers": self.suppliers,
            "products": self.products,
            "branches": self.branches,
            "employees": self.employees,
            "activities": self.activities
        }
        if table_name in dao_mapping:
            if table_name == "activities":
                return dao_mapping.get(table_name).find_all(sort_columns=['date'])
            else:
                return dao_mapping.get(table_name).find_all(sort_columns=['id'])
        if table_name == "employees report":
            return self.get_employee_report() 
        elif table_name == "activities report":
            return self.get_activity_report()
    
    def get_activity_report(self):
        return self.activities._conn.execute("""
        SELECT 
            a.date as date,
            p.description,
            a.quantity,
            e.name,
            s.name 
        FROM activities AS a
        JOIN products AS p ON a.product_id = p.id
        LEFT JOIN employees e ON a.activator_id = e.id
        LEFT JOIN suppliers s ON a.activator_id  = s.id
        ORDER BY date;
        """).fetchall()
    
    def get_employee_report(self):
        q = self.activities._conn.execute("""
        SELECT
            e.name AS name,
            e.salary,
            b.location,
            SUM(a.quantity*p.price*-1)
        FROM employees AS e
        JOIN branches AS b on e.branche = b.id
        LEFT JOIN activities AS a ON e.id = a.activator_id
        LEFT JOIN products AS p ON a.product_id = p.id
        GROUP BY e.id , b.location
        ORDER BY name;
        """).fetchall()
        returnTable = []
        for row in q:
            total = row[3] if row[3] else 0
            returnTable.append((f"{row[0]} {row[1]} {row[2]} {total}"))
        return returnTable
    
    def execute_command(self, script: str) -> list:
        return self._conn.cursor().execute(script).fetchall()
 
# singleton
repo = Repository()
atexit.register(repo._close)