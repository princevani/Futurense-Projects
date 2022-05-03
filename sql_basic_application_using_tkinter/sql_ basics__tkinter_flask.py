import flask
import tkinter
from tkinter import *
import sqlalchemy
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String

meta = MetaData()

global root, welcome_label, login_label, user_label, user_name, password_label, user_password, login, connection,\
    cursor, step_label, text_label, mysql_btn, oracle_btn, back_btn, db_btn, ddl_btn, dml_btn, create_label, \
    drop_label, example_label


def create_com():
    example_label.config(text="""CREATE TABLE table_name 
    (
    column1 datatype,
    column2 datatype,
    column3 datatype,
    ....);""", bg="white", fg="black")


def drop_com():
    example_label.config(text="""DROP TABLE table_name;""", bg="white", fg="black")


def insert_com():
    example_label.config(text="""INSERT INTO table_name (column1, column2, column3, ...)"
                              "VALUES (value1, value2, value3, ...);""", bg="white", fg="black")


def update_com():
    example_label.config(text="""UPDATE table_name SET column1 = value1, column2 = value2, ...
    WHERE condition;""", bg="white", fg="black")


def dml_com():
    global step_label, text_label, ddl_btn, dml_btn, back_btn, create_label, drop_label,  example_label

    des = [step_label, text_label, ddl_btn, dml_btn, back_btn]
    for i in des:
        i.destroy()

    back_btn = Button(root, text="BACK", activebackground="green", command=db_command_back)
    back_btn.place(x=25, y=20)

    # Create label
    step_label = Label(root, text="Data Manipulation Language", fg="white", bg="black", pady=20)
    step_label.config(font=("Courier", 14))
    step_label.pack()

    text_label = Label(root, text="Click on the below commands for examples", fg="white", bg="black", pady=5)
    text_label.pack()

    # Create Sql Buttons
    create_label = Button(root, text="Insert", activebackground="green", command=insert_com)
    create_label.place(x=185, y=140, width=50, height=50)

    drop_label = Button(root, text="Update", activebackground="green", command=update_com)
    drop_label.place(x=285, y=140, width=50, height=50)

    example_label = Label(root, text="-- Example Will Be Displayed Here --", fg="white", bg="black", pady=5)
    example_label.place(x=185, y=210)


def ddl_com():
    global step_label, text_label, ddl_btn, dml_btn, back_btn, create_label, drop_label,  example_label

    des = [step_label, text_label, ddl_btn, dml_btn, back_btn]
    for i in des:
        i.destroy()

    back_btn = Button(root, text="BACK", activebackground="green", command=db_command_back)
    back_btn.place(x=25, y=20)

    # Create label
    step_label = Label(root, text="Data Definition Language", fg="white", bg="black", pady=20)
    step_label.config(font=("Courier", 14))
    step_label.pack()

    text_label = Label(root, text="Click on the below commands fro examples", fg="white", bg="black", pady=5)
    text_label.pack()

    # Create Sql Buttons
    create_label = Button(root, text="Create", activebackground="green", command=create_com)
    create_label.place(x=185, y=140, width=50, height=50)

    drop_label = Button(root, text="Drop", activebackground="green", command=drop_com)
    drop_label.place(x=285, y=140, width=50, height=50)

    example_label = Label(root, text="-- Example Will Be Displayed Here --", fg="white", bg="black", pady=5)
    example_label.place(x=180, y=210)


def mysql_operations():
    global step_label, text_label, ddl_btn, dml_btn, back_btn

    des = [step_label, text_label, mysql_btn, oracle_btn, back_btn]
    for i in des:
        i.destroy()

    back_btn = Button(root, text="BACK", activebackground="green", command=mysql_operations_back)
    back_btn.place(x=25, y=20)

    # Create label
    step_label = Label(root, text="MySql", fg="white", bg="black", pady=20)
    step_label.config(font=("Courier", 14))
    step_label.pack()

    text_label = Label(root, text="Click on the below buttons to continue", fg="white", bg="black", pady=5)
    text_label.pack()

    # Create Sql Buttons
    ddl_btn = Button(root, text="DDL", activebackground="green", command=ddl_com)
    ddl_btn.place(x=185, y=175, width=50, height=50)

    dml_btn = Button(root, text="DML", activebackground="green", command=dml_com)
    dml_btn.place(x=285, y=175, width=50, height=50)


def db_type():
    global step_label, text_label, mysql_btn, oracle_btn, back_btn
    des = [step_label, text_label, db_btn]
    for i in des:
        i.destroy()

    back_btn = Button(root, text="BACK", activebackground="green", command=back)
    back_btn.place(x=25, y=20)

    # Create label
    step_label = Label(root, text="Step > 2", fg="white", bg="black", pady=20)
    step_label.config(font=("Courier", 14))
    step_label.pack()

    text_label = Label(root, text="Please select DB Type", fg="white", bg="black", pady=5)
    text_label.pack()

    # Create Sql Buttons
    mysql_btn = Button(root, text="Mysql", activebackground="green", command=mysql_operations)
    mysql_btn.place(x=185, y=175, width=50, height=50)

    oracle_btn = Button(root, text="Oracle", activebackground="green", command=mysql_operations)
    oracle_btn.place(x=285, y=175, width=50, height=50)


def home_page():
    global step_label, text_label, db_btn

    # Create label
    step_label = Label(root, text="Step > 1", fg="white", bg="black", pady=20)
    step_label.config(font=("Courier", 14))
    step_label.pack()

    text_label = Label(root, text="Please select DB Operation", fg="white", bg="black", pady=5)
    text_label.pack()

    db_btn = Button(root, text="DB Operation", activebackground="green", command=db_type)
    db_btn.place(x=210, y=150, width=100)


def menu():
    # Creating Menubar
    menubar = Menu(root)

    # Adding File Menu and commands
    file = Menu(menubar, tearoff=0)
    menubar.add_cascade(label='File', menu=file)
    file.add_command(label='New File')
    file.add_command(label='Open...')
    file.add_command(label='Save')
    file.add_separator()
    file.add_command(label='Logout', command=logout)

    # Adding Edit Menu and commands
    edit = Menu(menubar, tearoff=0)
    menubar.add_cascade(label='Edit', menu=edit)
    edit.add_command(label='Cut')
    edit.add_command(label='Copy')
    edit.add_command(label='Paste')
    edit.add_command(label='Select All')
    edit.add_separator()
    edit.add_command(label='Find...')
    edit.add_command(label='Find again')

    # Adding Help Menu
    help_ = Menu(menubar, tearoff=0)
    menubar.add_cascade(label='Help', menu=help_)
    help_.add_command(label='Tk Help')
    help_.add_command(label='Demo')
    help_.add_separator()
    help_.add_command(label='About Tk')

    # display Menu
    root.config(menu=menubar)


def back():
    des = [step_label, text_label, mysql_btn, oracle_btn, back_btn]
    for element in des:
        element.destroy()

    home_page()


def mysql_operations_back():
    des = [step_label, text_label, ddl_btn, dml_btn, back_btn]
    for element in des:
        element.destroy()

    db_type()


def db_command_back():
    des = [step_label, text_label, mysql_btn, create_label, drop_label,  example_label]
    for element in des:
        element.destroy()

    mysql_operations()


def logout():
    token_label = tkinter.Label(root, text="Logout Success", bg="green")
    token_label.pack()

    root.destroy()
    window()


def login_activity():
    global connection, cursor
    token_label = tkinter.Label(root, text="", bg="red")
    token_label.pack()

    user_email = user_name.get()
    user_passwd = user_password.get()

    if user_email and user_passwd:
        # DEFINE THE DATABASE CREDENTIALS
        db_user, db_password, db_host, db_port = 'root', '9662', '127.0.0.1', 3306
        db_database, db_table_name = 'pdf_data', "user_base"

        # Creating the db engine using SQLAlchemy
        engine = create_engine(url=f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}")
        if sqlalchemy.inspect(engine).has_table(db_table_name):
            pass
        else:
            Table('user_base', meta,
                  Column('user_id', Integer),
                  Column('user_email', String(40)),
                  Column('password', String(40)),
                  )
            meta.create_all(engine)

        try:
            connection = mysql.connector.connect(host=db_host, database=db_database, user=db_user, password=db_password)
            u_data = f"select user_email, password from user_base where user_email='{user_email}' " \
                     f"and password='{user_password}';"
            cursor = connection.cursor()
            cursor.execute(u_data)
            records = cursor.fetchall()
            if len(records) == 0:
                cursor.execute("SELECT user_id FROM user_base ORDER BY user_id DESC LIMIT 1;")
                records = cursor.fetchall()
                if len(records) != 0:
                    user_id = records[0][0] + 1
                else:
                    user_id = 1
                engine.execute(f"INSERT INTO user_base VALUES ('{user_id}', '{user_email}', '{user_password}')")

                token_label.config(text="Registration Completed")

            else:
                token_label.config(text="Login Successfully :)")

        except mysql.connector.Error as e:
            print("Error reading data from MySQL table", e)
        finally:
            des = [welcome_label, login_label, user_label, user_name, password_label, user_password, login, token_label]
            for element in des:
                element.destroy()

            if connection.is_connected():
                connection.close()
                cursor.close()

            # Moving to next page
            menu()
            home_page()

    else:
        token_label.config(text="Please check your email and password")
        token_label.after(1500, lambda: token_label.destroy())


def login_page():
    global welcome_label, login_label, user_label, user_name, password_label, user_password, login

    # Create label
    welcome_label = Label(root, text="Welcome :)", fg="white", bg="black", pady=20)
    welcome_label.config(font=("Courier", 14))
    welcome_label.pack()

    login_label = Label(root, text="Please Login to continue", fg="white", bg="black", pady=5)
    login_label.pack()

    # Create Login field
    user_label = tkinter.Label(root, text="Username -")
    user_label.place(x=165, y=140)

    user_name = tkinter.Entry(root, width=50)
    user_name.focus_set()
    user_name.place(x=265, y=140, width=100)

    password_label = tkinter.Label(root, text="Password -")
    password_label.place(x=165, y=180, width=68)

    user_password = tkinter.Entry(root, width=50)
    user_password.place(x=265, y=180, width=100)

    # Create Login Button
    login = Button(root, text="Login", activebackground="green", command=login_activity)
    login.place(x=200, y=220, width=100)


def window():
    global root
    # create a tkinter window
    root = Tk()
    root.geometry('520x400')
    root.title("File Comparison")
    root.config(background="black")

    # Calling the login page
    login_page()

    # Make infinite loop for displaying app on screen
    root.mainloop()


# Program Starts
app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route("/", methods=['GET'])
def home():
    return window()


app.run()
