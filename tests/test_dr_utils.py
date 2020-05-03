import pytest
from utils.dr_utils import get_config_parameters


class TestGetConfigParameters:
    def test_get_config_parameters(self):
        assert get_config_parameters('config.yaml', 'companies_house') == {'url': 'http://download.companieshouse.gov.uk/'}

    def test_get_config_parameters_type(self):
        assert type(get_config_parameters('config.yaml', 'companies_house')) == dict
