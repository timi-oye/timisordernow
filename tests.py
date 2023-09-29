from dags.extraction import extract_customers_to_lake, extract_merchants_to_lake, extract_orders_to_lake

def test_orders_documentation_exists():
    assert type(extract_orders_to_lake.__doc__) == str
def test_merchants_documentation_exists():
    assert type(extract_merchants_to_lake.__doc__) == str
def test_customers_documentation_exists():
    assert type(extract_customers_to_lake.__doc__) == str



if __name__ =='__main__':
    test_orders_documentation_exists()
    test_merchants_documentation_exists()
    test_customers_documentation_exists()
    print('All tests passed!')