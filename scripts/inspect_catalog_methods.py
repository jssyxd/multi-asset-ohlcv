import inspect
from nautilus_trader.persistence.catalog import ParquetDataCatalog

for method_name in [
    'get_file_list_from_data_cls',
    'filter_files',
    'get_intervals',
    'query_first_timestamp',
    'query_last_timestamp'
]:
    print(method_name, inspect.signature(getattr(ParquetDataCatalog, method_name)))
