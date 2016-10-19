import bsddb3

filename = "berkeley.db"

db = bsddb3.db.DB()
db.open(filename, "example", dbtype=bsddb3.db.DB_BTREE, flags=bsddb3.db.DB_CREATE)

db.put(b"testkey", b"{'data': 'lol'}")
print(db.get(b"testkey"))
