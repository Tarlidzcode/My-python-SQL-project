from faker import Faker
import pandas, numpy, pyodbc
from gspread import service_account_from_dict
from gspread_dataframe import set_with_dataframe


service_json = {
########################################
}


def create_records():
   emptylist = []
   for _ in range(15000):
       data = {
           "Id": Faker().uuid4().lower(), "Foreign_Id": Faker().uuid4().lower(),
           "Students": Faker().name() ,"Math": numpy.random.randint(0, 70),
           "Physics": numpy.random.randint(0, 82), "Language": numpy.random.randint(30, 100),
           "Academic_level": numpy.random.choice(["1st year", "2nd year", "3rd year", "4th year"]),
           "Gender": numpy.random.choice(["Male", "Female"])
           }
       emptylist.append(data)
   records = pandas.DataFrame(emptylist)
   return records
def create_geographic_records(records):
    existing_PK_Keys = records["Foreign_Id"]
    emptylist_area = []
    for fk_id in existing_PK_Keys:
        data = {
            "Id": fk_id, "City": Faker().city(),
            "Address": Faker().address()
            }
        emptylist_area.append(data)
    area = pandas.DataFrame(emptylist_area)
    return area

def create_date_records(records):
    emptylist_date = []
    existing_PK_Keys =  records["Foreign_Id"]
    for fk_id in existing_PK_Keys:
        year = numpy.random.choice(["2022", "2023", "2024"])
        semester = numpy.random.choice(["1st Semester", "2nd Semester"])
        if semester == "1st Semester":
            month = numpy.random.choice(["February", "March", "April", "May"])
        else:
            month = numpy.random.choice(["August", "September", "October", "November"])
       
        data = {
            "Id": fk_id,
            "Year": year,
            "Semester": semester,
            "Month": month
            }
        emptylist_date.append(data)
    dates = pandas.DataFrame(emptylist_date)
    return dates
# print(send_data_to_spread().head())
def send_data_to_spread(performance, service_json):
   googlesheet = service_account_from_dict(service_json)
   spreadsheet = googlesheet.open("SSIS_Python")
   worksheet = spreadsheet.worksheet("Performance")
   worksheet.clear()
   set_with_dataframe(worksheet, performance, include_index=False)
   
def send_data_to_spread_area(area, service_json):
   googlesheet = service_account_from_dict(service_json)
   spreadsheet = googlesheet.open("SSIS_Python")
   worksheet = spreadsheet.worksheet("Area")
   worksheet.clear()
   set_with_dataframe(worksheet, area, include_index=False)
   
def send_data_to_spread_dates(dates, service_json):
   googlesheet = service_account_from_dict(service_json)
   spreadsheet = googlesheet.open("SSIS_Python")
   worksheet = spreadsheet.worksheet("Dates")
   worksheet.clear()
   set_with_dataframe(worksheet, dates, include_index=False)
           
def get_data_from_spread(service_json):
   googlesheet = service_account_from_dict(service_json)
   spreadsheet = googlesheet.open("SSIS_Python")
   worksheet = spreadsheet.worksheet("Performance")
   records = worksheet.get_all_records()
   performance = pandas.DataFrame(records)
   return performance

def get_data_from_spread_area(service_json):
   googlesheet = service_account_from_dict(service_json)
   spreadsheet = googlesheet.open("SSIS_Python")
   worksheet = spreadsheet.worksheet("Area")
   records = worksheet.get_all_records()
   Area = pandas.DataFrame(records)
   return Area

def get_data_from_spread_dates(service_json):
   googlesheet = service_account_from_dict(service_json)
   spreadsheet = googlesheet.open("SSIS_Python")
   worksheet = spreadsheet.worksheet("Dates")
   records = worksheet.get_all_records()
   Dates = pandas.DataFrame(records)
   return Dates



conn = pyodbc.connect(
    r"DRIVER={ODBC Driver 18 for SQL Server};"
    r"SERVER=DESKTOP-ASLDL8R\MSSQLSERVER01;"
    r"DATABASE=Internship_projects;"
    r"Trusted_Connection=yes;"
    r"TrustServerCertificate=yes;"
)



def send_data_to_sql_server(conn, performance):
    conn.cursor().execute("drop table if exists area")
    conn.cursor().execute("drop table if exists dates")
    conn.cursor().execute("drop table if exists performance")
    conn.cursor().execute("create table performance (Id nvarchar(55) primary key, Foreign_Id nvarchar(55) not null unique, Students nvarchar(55), Math int, Physics int, Language int, Academic_level nvarchar(55), Gender nvarchar(55))")
    for index, row in performance.iterrows():
        conn.cursor().execute(
           "Insert into performance (Id, Foreign_Id, Students, Math, Physics, Language, Academic_level, Gender) values (?, ?, ?, ?, ?, ?, ?, ?)",
           (row["Id"], row["Foreign_Id"], row["Students"], row["Math"], row["Physics"], row["Language"], row["Academic_level"], row["Gender"])
       )
    conn.commit()
   
def send_data_to_sql_server_area(conn, area):
   # conn.cursor().execute("drop table if exists area")
   conn.cursor().execute("create table area (Id nvarchar(55) primary key, City nvarchar(55), Address nvarchar(255), constraint FK_area_performance foreign key (Id) references performance (Foreign_Id))")
   for index, row in area.iterrows():
       conn.cursor().execute(
           "Insert into area (Id, City, Address) values (?, ?, ?)",
           (row["Id"], row["City"], row["Address"])
       )
   conn.commit()
   
def send_data_to_sql_server_dates(conn, dates):
   # conn.cursor().execute("drop table if exists dates")
   conn.cursor().execute("create table dates (Id nvarchar(55) primary key, Year nvarchar(55), Semester nvarchar(55), Month nvarchar(55), constraint FK_dates_performance foreign key (Id) references performance (Foreign_Id))")
   for index, row in dates.iterrows():
       conn.cursor().execute(
           "Insert into dates (Id, Year, Semester, Month) values (?, ?, ?, ?)",
           (row["Id"], row["Year"], row["Semester"], row["Month"])
       )
   conn.commit()
if __name__=="__main__":
   performance = create_records() 
   area = create_geographic_records(performance)
   dates = create_date_records(performance)
   send_data_to_spread(performance, service_json)
   send_data_to_spread_area(area, service_json)
   send_data_to_spread_dates(dates, service_json)
   performance_table_got_from_gspread = get_data_from_spread(service_json)
   area_table_got_from_gspread = get_data_from_spread_area(service_json)
   dates_table_got_from_gspread = get_data_from_spread_dates(service_json)
   send_data_to_sql_server(conn, performance_table_got_from_gspread)
   send_data_to_sql_server_area(conn, area_table_got_from_gspread)
   send_data_to_sql_server_dates(conn, dates_table_got_from_gspread)