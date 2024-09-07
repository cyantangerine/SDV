from sdv.demo import sample_relational_demo

metadata, tables = sample_relational_demo(size=30)
from sdv.utils import display_tables

display_tables(tables)
for name, table in tables.items():
    print(name, table.shape)


from sdv.relational import HMA1

model = HMA1(metadata)
model.fit(tables)


new_data = model.sample()
display_tables(new_data)
