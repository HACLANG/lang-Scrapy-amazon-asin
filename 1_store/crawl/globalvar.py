# def _init():
#     global php_get_store_id_table
#     # php_get_store_id_table = sys.argv[1]
#     php_get_store_id_table = "fr_shoes201801"


class GlobalVar:
    php_get_store_id_table = None
    php_get_site = None
def set_store_id_table(value):
    GlobalVar.php_get_store_id_table = value
def get_store_id_table():
    return GlobalVar.php_get_store_id_table
def set_site(value):
    GlobalVar.php_get_site = value
def get_site():
    return GlobalVar.php_get_site