from db import DB

db = DB(username="glamp", hostname="localhost", dbname="yhat", dbtype="postgres")

print db.tables.people
print db.tables.people.id
print db.tables.things
print db.tables.things.person_id
print db.list_profiles()
