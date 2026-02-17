import unittest

from signal_backfill import evaluate_backfill_outcome


def c(ts, o, h, l, cl):
    return {"open_time": ts * 1000, "open": o, "high": h, "low": l, "close": cl}


class BackfillOutcomeTests(unittest.TestCase):
    def base_signal(self):
        return {
            "direction": "long",
            "entry_from": 99.0,
            "entry_to": 101.0,
            "sl": 95.0,
            "tp1": 105.0,
            "tp2": 110.0,
            "sent_at": 1000,
            "ttl_minutes": 10,
            "score": 10.0,
            "entry_mode": "wick",
            "be_trigger_pct": 2.0,
        }

    def test_no_entry_expired_no_entry(self):
        s = self.base_signal()
        candles = [c(1000, 110, 111, 109, 110), c(1300, 112, 113, 111, 112)]
        out = evaluate_backfill_outcome(s, candles, [], now_ts=2000, require_15m_confirm=False)
        self.assertEqual(out.final_status, "EXPIRED_NO_ENTRY")

    def test_entry_to_tp1(self):
        s = self.base_signal()
        s["be_trigger_pct"] = 10.0
        candles = [c(1000, 100, 102, 98, 100), c(1300, 101, 106, 100, 105)]
        out = evaluate_backfill_outcome(s, candles, [], now_ts=2000, require_15m_confirm=False)
        self.assertEqual(out.final_status, "TP1")

    def test_entry_to_sl(self):
        s = self.base_signal()
        candles = [c(1000, 100, 101, 99, 100), c(1300, 99, 100, 94, 95)]
        out = evaluate_backfill_outcome(s, candles, [], now_ts=2000, require_15m_confirm=False)
        self.assertEqual(out.final_status, "SL")

    def test_be_trigger_then_be(self):
        s = self.base_signal()
        candles = [
            c(1000, 100, 102, 99, 100),
            c(1300, 100, 103, 99.5, 102),
            c(1600, 102, 102.5, 99.8, 100.1),
        ]
        out = evaluate_backfill_outcome(s, candles, [], now_ts=2000, require_15m_confirm=False)
        self.assertEqual(out.final_status, "BE")

    def test_tp_and_sl_same_bar_deterministic(self):
        s = self.base_signal()
        s["tp1"] = 103.0
        s["sl"] = 97.0
        candles = [c(1000, 100, 101, 99, 100), c(1300, 100, 104, 96, 101)]
        out = evaluate_backfill_outcome(s, candles, [], now_ts=2000, require_15m_confirm=False)
        self.assertEqual(out.final_status, "SL")


if __name__ == "__main__":
    unittest.main()
