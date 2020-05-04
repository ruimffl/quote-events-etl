"""
    Tests for the dr_utils function
"""

# IMPROVEMENTS
# 1 - learn how to write tests properly, specially how to use fixtures
# 2 - improve existing tests
# 3 - improve test coverage including at least tests for functions extract_quote_fields() and extract_product_fields()
#
# Unit tests should also be complimented by data integrity tests that could either be implemented as parameterised
# queries or, if using _DBT, use its off-the-shelf test capability
#

from utils.dr_utils import get_config_parameters


class TestGetConfigParameters:
    def test_get_config_parameters(self):
        assert get_config_parameters('config.yaml', 'companies_house') == {'url': 'http://download.companieshouse.gov.uk/'}

    def test_get_config_parameters_type(self):
        assert type(get_config_parameters('config.yaml', 'companies_house')) == dict
