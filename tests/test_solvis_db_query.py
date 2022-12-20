#!/usr/bin/env python
"""Tests for `solvis_store.model` package."""

import unittest

from moto import mock_dynamodb
from solvis_store import model
from solvis_store.solvis_db_query import get_rupture_ids


@mock_dynamodb
class PynamoTest(unittest.TestCase):
    def setUp(self):
        model.set_local_mode()
        model.SolutionLocationRadiusRuptureSet.create_table(wait=True)
        dataframe = model.SolutionLocationRadiusRuptureSet(
            solution_id='test_solution_id',
            location_radius='WLG:10000',
            radius=10000,
            location='WLG',
            ruptures=[1, 2, 3],
            rupture_count=3,
        )
        dataframe.save()

    def test_get_rupture_ids(self):
        ids = get_rupture_ids(solution_id='test_solution_id', locations=['WLG'], radius=10000)
        self.assertEqual(len(ids), 3)
        self.assertEqual(ids, set([1, 2, 3]))

    def tearDown(self):
        # with app.app_context():
        model.SolutionLocationRadiusRuptureSet.delete_table()
        return super(PynamoTest, self).tearDown()
