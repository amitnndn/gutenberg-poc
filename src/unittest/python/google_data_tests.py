import unittest
from google_data import get_least_year, get_year_from_date_string

class Test(unittest.TestCase):
	"""Test for `primes.py`"""
	def test_for_different_date_formats(self):
		"""Get the least year"""
		self.assertEqual(get_least_year(['2005','2016']), 2005)
		self.assertEqual(get_least_year(['2005','2016','1989-02-01']), 1989)
		self.assertEqual(get_least_year(['2543', '2345-03-12', '1989-02-01']), 1989)

	def test_for_get_year_from_date_string(self):

		self.assertEqual(get_year_from_date_string('2004'), 2004)
		self.assertEqual(get_year_from_date_string('1989-01-02'), 1989)
		self.assertEqual(get_year_from_date_string('asdf'), 9999)

