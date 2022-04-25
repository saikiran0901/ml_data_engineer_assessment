import sqlalchemy

# connect to the database
engine = sqlalchemy.create_engine("mysql://datatest:alligator@database/datatestdb")
connection = engine.connect()

metadata = sqlalchemy.schema.MetaData(engine)

# make an ORM object to refer to the table
Example = sqlalchemy.schema.Table('examples', metadata, autoload=True, autoload_with=engine)

# clean out table from previous runs
connection.execute(Example.delete())

names = ["Joe", "Mary", "Sue", "Bill"]

for name in names:
    connection.execute(Example.insert().values(name = name))

rows = connection.execute(sqlalchemy.sql.select([Example])).fetchall()
print("Found rows in database: ", len(rows))

assert len(rows) == len(names)

print("Test Successful")
