import unittest
import sys
import os

# Adjust path to import normalizer.py from the parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from normalizer import normalize_course_name, map_discipline, parse_hhmm_any, convert_odds_to_fractional_decimal

class TestNormalizerFunctions(unittest.TestCase):

    def test_normalize_course_name(self):
        """Tests that normalize_course_name handles various input formats correctly."""
        self.assertEqual(normalize_course_name("Woodbine (Thoroughbred)"), "woodbine")
        self.assertEqual(normalize_course_name("Cheltenham at Home"), "cheltenham")
        self.assertEqual(normalize_course_name("Flemington"), "flemington")
        self.assertEqual(normalize_course_name("  ROSEHILL GARDENS  "), "rosehill gardens")
        self.assertEqual(normalize_course_name("SANDOWN-LAKESIDE"), "sandown-lakeside")
        self.assertEqual(normalize_course_name("Kempton Park Racecourse"), "kempton")
        self.assertEqual(normalize_course_name("Ascot Raceway"), "ascot")
        self.assertEqual(normalize_course_name(""), "")
        # Test removal of common suffixes
        self.assertEqual(normalize_course_name("Churchill Downs Track"), "churchill downs")

    def test_map_discipline(self):
        """Tests that map_discipline correctly categorizes different race types."""
        self.assertEqual(map_discipline("greyhound"), "greyhound")
        self.assertEqual(map_discipline("dog racing"), "greyhound")
        self.assertEqual(map_discipline("thoroughbred"), "thoroughbred")
        self.assertEqual(map_discipline("flat racing"), "thoroughbred")
        self.assertEqual(map_discipline("Harness"), "harness")
        self.assertEqual(map_discipline("trotting"), "harness")
        self.assertEqual(map_discipline("standardbred"), "harness")
        self.assertEqual(map_discipline("steeplechase"), "jump")
        self.assertEqual(map_discipline("hurdles"), "jump")
        self.assertEqual(map_discipline("national hunt"), "jump")
        self.assertEqual(map_discipline("unknown"), "thoroughbred")
        self.assertEqual(map_discipline(""), "thoroughbred")

    def test_parse_hhmm_any(self):
        """Tests that parse_hhmm_any handles various time string formats correctly."""
        self.assertEqual(parse_hhmm_any("7:30 PM"), "19:30")
        self.assertEqual(parse_hhmm_any("7:30 pm"), "19:30")
        self.assertEqual(parse_hhmm_any("19.30"), "19:30")
        self.assertEqual(parse_hhmm_any("1:15 PM"), "13:15")
        self.assertEqual(parse_hhmm_any("08:00 AM"), "08:00")
        self.assertEqual(parse_hhmm_any("12:00 AM"), "00:00")  # Midnight
        self.assertEqual(parse_hhmm_any("12:00 PM"), "12:00")  # Noon
        self.assertEqual(parse_hhmm_any("8.00"), "08:00")
        self.assertEqual(parse_hhmm_any("23:59"), "23:59")
        # Invalid inputs
        self.assertIsNone(parse_hhmm_any("invalid time string"))
        self.assertIsNone(parse_hhmm_any(""))
        self.assertIsNone(parse_hhmm_any(None))

    def test_convert_odds_to_fractional_decimal(self):
        """Tests that convert_odds_to_fractional_decimal handles various odds formats."""
        # Fractional odds (numerator/denominator = decimal fraction)
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("5/2"), 2.5)  # 5÷2 = 2.5
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("1/1"), 1.0)  # 1÷1 = 1.0 (Evens)
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("10/1"), 10.0)  # 10÷1 = 10.0
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("7-2"), 3.5)  # 7÷2 = 3.5 (dash format)
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("3/1"), 3.0)
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("1/2"), 0.5)  # Odds-on
        
        # Special cases
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("EVS"), 1.0)   # Even money
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("EVENS"), 1.0) # Even money alternative
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("SP"), 999.0)  # Starting Price
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("NR"), 999.0)  # Non-runner
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("SCR"), 999.0) # Scratched
        
        # Decimal odds (convert from betting decimal to fractional decimal)
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("2.5"), 1.5)  # 2.5 - 1.0 = 1.5
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("3.0"), 2.0)  # 3.0 - 1.0 = 2.0
        
        # Invalid inputs
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("Invalid"), 999.0)
        self.assertAlmostEqual(convert_odds_to_fractional_decimal(""), 999.0)
        self.assertAlmostEqual(convert_odds_to_fractional_decimal("0/1"), 0.0)  # Edge case: 0 odds

    def test_normalize_course_name_edge_cases(self):
        """Additional edge case tests for course name normalization."""
        # Test multiple spaces and case variations
        self.assertEqual(normalize_course_name("  ROYAL   ASCOT  "), "royal ascot")
        # Test 'at' variations
        self.assertEqual(normalize_course_name("Woodbine at Mohawk"), "woodbine")
        self.assertEqual(normalize_course_name("Santa Anita at Los Alamitos"), "santa anita")

    def test_map_discipline_case_insensitive(self):
        """Test that discipline mapping is case insensitive."""
        self.assertEqual(map_discipline("GREYHOUND"), "greyhound")
        self.assertEqual(map_discipline("Thoroughbred"), "thoroughbred")
        self.assertEqual(map_discipline("HARNESS"), "harness")
        self.assertEqual(map_discipline("Jump"), "jump")

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
