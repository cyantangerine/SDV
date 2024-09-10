from sdv.relational import HMA1
import sys
import warnings
warnings.filterwarnings('ignore')
#from tutorials.newhma import NewModel as HMA1
#from tutorials.redirector import WriteableRedirector
import warnings
from sdv.relational import HMA1
from test_20_tables import save_tables
import time
from sdv import Metadata
import os

print(os.getcwd())
from test_20_tables import fetch_data_from_sqlite
times = []
last_time = time.time()
def addt():
    t = time.time()
    times.append(t-last_time)
    last_time = t

metadata, tables = fetch_data_from_sqlite()
metadata = Metadata(metadata)
for name, table in tables.items():
    print(name, table.shape)
from test_20_tables import save_tables
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