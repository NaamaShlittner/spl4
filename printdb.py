from persistence import *

def print_table(table_name: str):
    repo.__init__()
    print(table_name)
    table = repo.get_table(table_name.lower())
    for row in table:
        print(row)

def main():
    print_table("Activities")
    print_table("Branches")
    print_table("Employees")
    print_table("Products")
    print_table("Suppliers")
    print_table("Employees report")
    print_table("Activities report")

if __name__ == '__main__':
    main()