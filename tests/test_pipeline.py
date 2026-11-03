import unittest
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession

from src.pipeline import EVENT_SCHEMA, quality_rules


class QualityRulesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("pipeline-quality-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def classify(
        self,
        event_id="evt-1",
        event_type="purchase",
        user_id="u-1",
        quantity=1,
        amount=Decimal("2.50"),
    ):
        row = (
            event_id,
            event_type,
            user_id,
            "sku-1",
            quantity,
            amount,
            datetime(2026, 11, 3, 3, 0),
        )
        return quality_rules(self.spark.createDataFrame([row], EVENT_SCHEMA)).select(
            "is_valid", "quality_error"
        ).first()

    def test_valid_purchase_passes(self):
        result = self.classify()
        self.assertTrue(result.is_valid)
        self.assertIsNone(result.quality_error)

    def test_blank_event_id_is_quarantined(self):
        result = self.classify(event_id="  ")
        self.assertFalse(result.is_valid)
        self.assertEqual(result.quality_error, "missing_event_id")

    def test_unknown_event_type_is_quarantined(self):
        result = self.classify(event_type="login")
        self.assertFalse(result.is_valid)
        self.assertEqual(result.quality_error, "invalid_event_type")

    def test_missing_event_type_is_quarantined(self):
        result = self.classify(event_type=None)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.quality_error, "missing_event_type")

    def test_blank_user_id_is_quarantined(self):
        result = self.classify(user_id=" ")
        self.assertFalse(result.is_valid)
        self.assertEqual(result.quality_error, "missing_user_id")

    def test_nonpositive_quantity_is_quarantined(self):
        result = self.classify(quantity=0)
        self.assertFalse(result.is_valid)
        self.assertEqual(result.quality_error, "invalid_quantity")

    def test_negative_amount_is_quarantined(self):
        result = self.classify(amount=Decimal("-1.00"))
        self.assertFalse(result.is_valid)
        self.assertEqual(result.quality_error, "invalid_amount")


if __name__ == "__main__":
    unittest.main()
