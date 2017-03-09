from . import config 
from peewee import TextField, SqliteDatabase, Model

database = SqliteDatabase(config.DATABASE_PATH, **{})


class MiniStorage(Model):
    class Meta:
        database = database


class customers(MiniStorage):
    """ Model for 'customers' table.
    """
    current_med_advantage = TextField(null=True)
    last_order_date = TextField(null=True)
    phone_no_ = TextField(null=True)
    segment = TextField(null=True)
    sell_to_customer_name = TextField(null=True)
    sell_to_customer_no_ = TextField(null=True, primary_key=True)
    ship_to_post_code = TextField(null=True)
    total_number_of_orders = TextField(null=True)
    usa_email = TextField(null=True)
    record_create_date = TextField(null=True)
    file_parse_date = TextField(null=True)
    
    class Meta:
        db_table = 'customers'

