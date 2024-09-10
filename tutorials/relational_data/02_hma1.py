import warnings
from sdv.relational import HMA1
from tutorials.relational_data.test_20_tables import save_tables

warnings.filterwarnings('ignore')
loaded = HMA1.load('my_model_10k.pkl')
new_data = loaded.sample()
save_tables(new_data, "output_10k.xlsx")
