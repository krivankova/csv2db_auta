#!/usr/bin/python

import MySQLdb
import sys

g_csv_fn = "data/auta_data.csv"
g_table  = "cars_lenka"

def csv2data(_fn):
  """
  Read input CSV file and parse it
  """
  data = []
  with open(_fn, "r") as fd:
    print fd.readline()
    for line in fd:
      data.append(line.strip().split(";"))
  return data

def data2sql(_table, _data):
  """
  Insert each line of data to a MySQL db table 
  """
  keys = 'Veh_value, Exposure, Clm, Numclaims, Claimcst0, Veh_body, Veh_age, Gender, Area, Agecat'

  con = db_connect()
  with con:
    cur = con.cursor()
    i=1
    for row in _data:
      cmd = "INSERT INTO {} ({}) values {}".format(_table, keys, str(tuple(row)))
      res = cur.execute(cmd)
      #print cmd
      #print "{}: {}".format(i, res)
      #i += 1


def db_create_table(_table):
  """
  Create table
  """
  table_def="""(
    Id INT PRIMARY KEY AUTO_INCREMENT,
    Veh_value DECIMAL(6,4) NOT NULL,
    Exposure DECIMAL(11,10) NOT NULL,
    Clm BOOL NOT NULL,
    Numclaims TINYINT NOT NULL,
    Claimcst0 DECIMAL(13,8) NOT NULL,
    Veh_body VARCHAR(5) NOT NULL,
    Veh_age TINYINT NOT NULL,
    Gender VARCHAR(1) NOT NULL,
    Area VARCHAR(1) NOT NULL,
    Agecat TINYINT NOT NULL
    )
  """
  con = db_connect()
  with con:
    cur = con.cursor()
    cur.execute("create table {} {}".format(_table, table_def))

def db_connect():
  """
  Return connection handler to the 'testdb' database
  """
  return  MySQLdb.connect('localhost', 'testuser', 'test623', 'testdb');

def db_select(_table, _prn=True):
  """
  Run select command on a MySQL db table
  Return selected table (as list of rows)
  """
  keys = 'Veh_value, Exposure, Clm, Numclaims, Claimcst0, Veh_body, Veh_age, Gender, Area, Agecat'

  con = db_connect()
  with con:
    cur = con.cursor()
    #cmd = "SELECT count(id) as cislo FROM {} where numclaims > 0 ".format(_table, keys, )
    #cmd = "select veh_body, veh_age, gender, area, agecat, sum(numclaims) from {} where veh_body in ('SEDAN', 'STNWG', 'HBACK')  group by veh_body, veh_age, gender, area, agecat"
    #cmd = "select veh_body, veh_age, gender, area, agecat, count(*) from {} where veh_body in ('SEDAN', 'STNWG', 'HBACK')  group by veh_body, veh_age, gender, area, agecat"
    cmd = "select veh_body, veh_age, gender, area, agecat, sum(exposure) from cars_lenka where veh_body in ('SEDAN', 'STNWG', 'HBACK')  group by veh_body, veh_age, gender, area, agecat"
    res = cur.execute(cmd.format(_table))
    res = cur.fetchall()
    if _prn:
      print res
    return res

def write_tex(_data, _fn='out.tex'):
  """
  Write TeX code to file _fn
  """
  with open(_fn, 'w') as fd:
    for row in _data:
      line = ""
      for item in row:
        line += " & " + "{:5}".format(str(item))
      line += " \\\\\n"
      fd.write(line)


def write_tex_adhoc(_data, _fn='out.tex'):
  """
  Write TeX code to file _fn
  """
  line_of_agregated_numbers = "\n% "
  with open(_fn, 'w') as fd:
    for row in _data:
      line =  row[0] + " & "
      line += str(row[1]) + " & "
      line += row[2] + " & "
      line += row[3] + " & "
      line += str(row[4]) + " & "
      line += str(int(round(float(row[5]),0))) + ' \\\\\n'
      fd.write(line)
      line_of_agregated_numbers += str(int(round(float(row[5]),0))) + " "
    fd.write(line_of_agregated_numbers)

def db_select2():
  """
  Iterate over all combination of veh_age, area, gender, agecat
  Run select command on a MySQL db table and fetch 2 sums
  Return table of ratios of that sums
  """
  veh_body  = [ 'HBACK', 'SEDAN', 'STNWG' ]
  veh_age = range(1,5)
  area    = [ 'A', 'B', 'C', 'D', 'E', 'F' ]
  gender  = [ 'F', 'M' ]
  agecat  = range(1,7)

  out = []
  out_cl = []
  out_ex = []

  con = db_connect()
  with con:
    cur = con.cursor()
    for kar in veh_body:
      for i1 in veh_age:
        for i2 in area:
          row = []
          for i3 in gender:
            for i4 in agecat:
              cmd = "select sum(numclaims), sum(exposure) from cars_lenka where veh_body='{}' AND veh_age='{}' AND area='{}' AND gender='{}' AND agecat='{}' ".format(kar, i1, i2, i3, i4)
              res = cur.execute(cmd)
              res = cur.fetchall()
              if res[0][1]:
                ratio = round ( res[0][0] / res[0][1] * 100, 1 )
                out_cl.append(int(round(float(res[0][0]))))
                #out_ex.append(int(round(float(res[0][1]))))  # Exposure rounded to integer
                out_ex.append(float(res[0][1]))
              else:
                out_cl.append('N')
                out_ex.append('N')
                ratio = "N"
              row.append(ratio)
          #out.append(row)
          #print(row)
    print "sum(numclaims)"
    print out_cl
    print "sum(exposure)"
    print out_ex
  return out


def print_usage():
  print "Usage: {} [ create | load | select2 | tex ]".format(sys.argv[0])

if len(sys.argv) < 2:
  print_usage()
  sys.exit(1)

command = sys.argv[1]

if command == "create":
  db_create_table(g_table)
elif command == "load":
  data2sql(g_table, csv2data(g_csv_fn))
elif command == "select":
  db_select(g_table)
elif command == "select2":
  sel = db_select2()
#  write_tex(sel)
elif command == "tex":
  sel = db_select(g_table, False)
  write_tex_adhoc(sel)
else:
  print_usage()

