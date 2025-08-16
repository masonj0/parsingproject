import unittest
from analysis import compute_signals
from normalizer import NormalizedRace, NormalizedRunner

class TestAnalysisEngine(unittest.TestCase):

    def test_market_consensus_signal(self):
        """
        Tests the calculation of the market_consensus signal based on runner odds.
        """
        # A race with a "perfect" book (100% implied probability) should result in a signal of 1.0
        runners_perfect_book = [
            NormalizedRunner(runner_id="1", name="Horse A", odds_decimal=2.0),
            NormalizedRunner(runner_id="2", name="Horse B", odds_decimal=4.0),
            NormalizedRunner(runner_id="3", name="Horse C", odds_decimal=4.0),
        ]
        race_perfect_book = NormalizedRace(
            track_key="test_track",
            race_key="test_track::r1",
            start_time_iso=None,
            runners=runners_perfect_book,
            schema_version="2.0"
        )
        signals = compute_signals(race_perfect_book)
        self.assertIn("market_consensus", signals)
        self.assertAlmostEqual(signals["market_consensus"], 1.0, places=5)

        # A race with a realistic overround (e.g., > 100%)
        runners_realistic_book = [
            NormalizedRunner(runner_id="1", name="Horse A", odds_decimal=2.0), # 50%
            NormalizedRunner(runner_id="2", name="Horse B", odds_decimal=3.0), # 33.33%
            NormalizedRunner(runner_id="3", name="Horse C", odds_decimal=6.0), # 16.67%
        ]
        race_realistic_book = NormalizedRace(
            track_key="test_track",
            race_key="test_track::r2",
            start_time_iso=None,
            runners=runners_realistic_book,
            schema_version="2.0"
        )
        # Overround = 1/2.0 + 1/3.0 + 1/6.0 = 0.5 + 0.33333... + 0.16666... = 1.0
        # Let's adjust for a more realistic overround > 1.0
        runners_realistic_book[2].odds_decimal = 5.0 # Changes overround to 1/2 + 1/3 + 1/5 = 0.5 + 0.333 + 0.2 = 1.0333
        overround = (1/2.0) + (1/3.0) + (1/5.0)
        expected_signal = 1 / overround
        signals = compute_signals(race_realistic_book)
        self.assertAlmostEqual(signals["market_consensus"], expected_signal, places=5)

    def test_market_consensus_edge_cases(self):
        """
        Tests edge cases for the market_consensus signal calculation.
        """
        # Race with no runners
        race_no_runners = NormalizedRace(track_key="test", race_key="test::r1", runners=[], schema_version="2.0", start_time_iso=None)
        signals = compute_signals(race_no_runners)
        self.assertEqual(signals.get("market_consensus"), 0.0)

        # Race with runners but no odds
        runners_no_odds = [
            NormalizedRunner(runner_id="1", name="Horse A", odds_decimal=None),
            NormalizedRunner(runner_id="2", name="Horse B", odds_decimal=None),
        ]
        race_no_odds = NormalizedRace(track_key="test", race_key="test::r2", runners=runners_no_odds, schema_version="2.0", start_time_iso=None)
        signals = compute_signals(race_no_odds)
        self.assertEqual(signals.get("market_consensus"), 0.0)

        # Race with some runners having no odds (should be ignored in calculation)
        runners_some_odds = [
            NormalizedRunner(runner_id="1", name="Horse A", odds_decimal=2.0),
            NormalizedRunner(runner_id="2", name="Horse B", odds_decimal=None),
            NormalizedRunner(runner_id="3", name="Horse C", odds_decimal=2.0),
        ]
        race_some_odds = NormalizedRace(track_key="test", race_key="test::r3", runners=runners_some_odds, schema_version="2.0", start_time_iso=None)
        overround = (1/2.0) + (1/2.0) # 1.0
        expected_signal = 1 / overround
        signals = compute_signals(race_some_odds)
        self.assertAlmostEqual(signals["market_consensus"], expected_signal, places=5)

if __name__ == '__main__':
    unittest.main()
