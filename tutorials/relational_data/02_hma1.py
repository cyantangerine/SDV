import sys
import warnings
warnings.filterwarnings('ignore')
sys.path.append('/home/wbx/SDV/')
from sdv.relational import HMA1
from test_20_tables import save_tables, fetch_data_from_sqlite
import time
from sdv import Metadata
import os

print(os.getcwd())
times = []
last_time = time.time()



def addt():
    global times, last_time
    t = time.time()
    times.append(t-last_time)
    print(f"\n\n{t-last_time=}\n\n")
    last_time = t
    with open("./times.txt", "a") as f:
        f.write(str(times) + "\n")

metadata, tables = fetch_data_from_sqlite("./tutorials/relational_data/data_sqlite.db")
metadata = Metadata(metadata)
print(metadata)

for name, table in tables.items():
    print(name, table.shape)
save_tables(tables, "input.xlsx")

addt()

#with WriteableRedirector():
model = HMA1(metadata)
model.fit(tables)

addt()

model.save('my_model_10k.pkl')

addt()

new_data = model.sample()
save_tables(new_data)

addt()
print(times)
save_tables({"time": times}, "time.xlsx")
