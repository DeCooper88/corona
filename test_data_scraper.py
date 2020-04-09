import pytest
import data_scraper


test_numbers = [('', 0), ('5(=)', 5), ('8(+300%)\n', 8), (' 3 ', 3),
                ('1,795(+21%)\n', 1795), ('23,403(+5.4%)\n', 23403)]


@pytest.mark.parametrize('test_input, expected', test_numbers)
def test_get_number(test_input, expected):
    assert data_scraper.get_number(test_input) == expected


test_dates = [('01-01-2020', '2020-01-01'), ('31-12-2020', '2020-12-31'),
              ('29-02-2020', '2020-02-29'), ('03-04-2020', '2020-04-03')]


@pytest.mark.parametrize('test_input, expected', test_dates)
def test_modify_date(test_input, expected):
    assert data_scraper.modify_date(test_input) == expected


test_rows = [
    (['2020-03-03', '\n\u200b\n\u200b\n\u200b\n\u200b\n\u200b\n', '13(+62%)\n', ''],
     ('2020-03-03', 13, 0)),
    (['2020-03-11', '\n\u200b\n\u200b\n\u200b\n\u200b\n', '314(+1%)\n', '3(+2%)'],
     ('2020-03-11', 314, 3)),
    (['2020-04-06', '\n\u200b\n\u200b\n\u200b\n', '20,814(+5.7%)\n', '1,632(+13%)'],
     ('2020-04-06', 20814, 1632))
]


@pytest.mark.parametrize('test_input, expected', test_rows)
def test_clean_data(test_input, expected):
    assert data_scraper.clean_data(test_input) == expected
