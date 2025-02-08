from persistence import *

import sys
import os

def add_branch(splittedline : list[str]):
    repo.execute_command(f"INSERT INTO branches (id, location, number_of_employees) VALUES ({splittedline[0]}, '{splittedline[1]}', {splittedline[2]})")
    pass

def add_supplier(splittedline : list[str]):
    repo.execute_command(f"INSERT INTO suppliers (id, name, contact_information) VALUES ({splittedline[0]}, '{splittedline[1]}', '{splittedline[2]}')")
    pass

def add_product(splittedline : list[str]):
    repo.execute_command(f"INSERT INTO products (id, description, price, quantity) VALUES ({splittedline[0]}, '{splittedline[1]}', {splittedline[2]}, {splittedline[3]})")
    pass

def add_employee(splittedline : list[str]):
    repo.execute_command(f"INSERT INTO employees (id, name, salary, branche) VALUES ({splittedline[0]}, '{splittedline[1]}', {splittedline[2]}, {splittedline[3]})")
    pass

adders = {  "B": add_branch,
            "S": add_supplier,
            "P": add_product,
            "E": add_employee}

def main(args : list[str]):
    inputfilename = args[1]
    # delete the database file if it exists
    repo._close()
    if os.path.isfile("bgumart.db"):
        os.remove("bgumart.db")
    repo.__init__()
    repo.create_tables()
    with open(inputfilename) as inputfile:
        for line in inputfile:
            splittedline : list[str] = line.strip().split(",")
            adders.get(splittedline[0])(splittedline[1:])
    

if __name__ == '__main__':
    main(sys.argv)