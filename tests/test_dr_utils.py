"""
    Tests for the dr_utils function
"""

# TO DO
# 1 - learn how to write tests, specially how to use fixtures
# 2 - improve existing tests
# 3 - improve test coverage

from utils.dr_utils import get_config_parameters


class TestGetConfigParameters:
    def test_get_config_parameters(self):
        assert get_config_parameters('config.yaml', 'companies_house') == {'url': 'http://download.companieshouse.gov.uk/'}

    def test_get_config_parameters_type(self):
        assert type(get_config_parameters('config.yaml', 'companies_house')) == dict
