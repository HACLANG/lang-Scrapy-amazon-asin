# def _init():
#     global php_get_store_id
#     # php_get_store_id = sys.argv[1]
#     php_get_store_id = "fr_shoes201801"


class GlobalVar:
    php_get_store_id = None
    php_get_site = None
def set_store_id(value):
    GlobalVar.php_get_store_id = value
def get_store_id():
    return GlobalVar.php_get_store_id
def set_site(value):
    GlobalVar.php_get_site = value
def get_site():
    return GlobalVar.php_get_site