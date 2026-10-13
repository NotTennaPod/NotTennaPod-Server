#!/usr/bin/env python3
"""Executable protocol counterexamples (docs/PROTOCOL.md), not implementation or end-to-end tests.
Run: python3 spec/conformance/model_checks.py
"""
from fractions import Fraction
from itertools import permutations
import random
import unittest


def position_between(left, right, uid):
    assert 0 <= left < right <= 1 and 0 <= uid < 2**128
    width = right - left
    n = max(1, 130 + width.denominator.bit_length() - width.numerator.bit_length())
    while width * 2**n <= 2**130:
        n += 1
    while n > 1 and width * 2**(n-1) > 2**130:
        n -= 1
    lower = (left * 2**n).__floor__()
    residue, modulus = 2 * uid + 1, 2**129
    m = residue + ((lower - residue) // modulus + 1) * modulus
    assert m % 2 and left < Fraction(m, 2**n) < right
    return Fraction(m, 2**n), (n, m)


def causal_order(facts):
    pending = {f['id']: f for f in facts}
    done, ordered = set(), []
    while pending:
        ready = [f for f in pending.values() if set(f.get('deps', ())) <= done]
        if not ready:
            raise ValueError('missing dependency or cycle')
        f = min(ready, key=lambda f: (f['time'], f['id']))
        ordered.append(f)
        done.add(f['id'])
        del pending[f['id']]
    return ordered


def playback_projection(facts):
    owner, played, position, queued = None, False, 0, False
    for f in causal_order(facts):
        if f['kind'] == 'start':
            owner, position, queued = f['id'], f.get('position', 0), True
        elif f['kind'] == 'checkpoint' and owner == f['session']:
            position = f['position']
        elif f['kind'] == 'manual_played':
            played = True
        elif f['kind'] == 'reset':
            owner, played, position = f['id'], False, 0
        elif f['kind'] == 'complete' and owner == f['session']:
            played, position, queued = True, 0, False
    return played, position, queued


def residual(contribution, cuts):
    cut = max(cuts, default=(0, 0), key=lambda x: x[0])
    return max(0, contribution - cut[1])


def android_contribution(start, saves):
    contribution = 0
    for pos in saves:
        if pos > start:
            contribution = pos - start
    return contribution


def event(ident, kind, time, deps=(), **kwargs):
    return dict(id=ident, kind=kind, time=time, deps=deps, **kwargs)


def guid_repairs(old, new):
    present = {x['guid'] for x in new}
    result = {}
    keys = {(x.get('title'), x.get('date')) for x in old}
    for title, date in keys:
        if not title or date is None:
            continue
        before = [x for x in old if x.get('title') == title and x.get('date') == date
                  and x['guid'] not in present]
        after = [x for x in new if x.get('title') == title and x.get('date') == date]
        if len(before) == len(after) == 1:
            result[after[0]['guid']] = before[0]['guid']
    return result


def establishes_reused_guid(a, b):
    return (a['guid'] == b['guid'] and a['url'] != b['url']
            and a.get('date') is not None and b.get('date') is not None
            and abs(a['date'] - b['date']) > 7 * 86400)


class SpecCounterexamples(unittest.TestCase):
    def test_hosting_move_and_ambiguous_matching_vectors(self):
        old = dict(guid='G1', url='https://old/media', title='Episode', date=100)
        new = dict(guid='G2', url='https://new/media', title='Episode', date=100)
        self.assertEqual(guid_repairs([old], [new]), {'G2': 'G1'})
        self.assertEqual(guid_repairs([old], [new, dict(new, guid='G3')]), {})
        self.assertEqual(guid_repairs([old, dict(old, guid='G0')], [new]), {})
        self.assertEqual(guid_repairs([old], [dict(new, date=200)]), {})
        self.assertEqual(guid_repairs([dict(old, title=None)], [dict(new, title=None)]), {})
        self.assertEqual(guid_repairs([dict(old, date=None)], [dict(new, date=None)]), {})

    def test_dynamic_ads_are_not_guid_reuse(self):
        a = dict(guid='G', url='https://cdn/audio?token=1', date=100, duration=20)
        b = dict(a, url='https://cdn/audio?token=2', duration=23)
        self.assertFalse(establishes_reused_guid(a, b))
        self.assertFalse(establishes_reused_guid(a, dict(b, date=None)))
        self.assertFalse(establishes_reused_guid(dict(a, date=None), b))
        self.assertTrue(establishes_reused_guid(a, dict(b, date=100 + 8 * 86400)))

    def test_distinct_concurrent_keys_and_nested_gaps(self):
        rng = random.Random(20261004)
        keys, used = [Fraction(0), Fraction(1)], set()
        for _ in range(1500):
            i = rng.randrange(len(keys)-1)
            uid = rng.getrandbits(128)
            while uid in used:
                uid = rng.getrandbits(128)
            used.add(uid)
            key, (n, m) = position_between(keys[i], keys[i+1], uid)
            self.assertNotIn(key, keys)
            self.assertEqual(key, Fraction(m, 2**n))
            keys.insert(i+1, key)
        left, right = Fraction(1, 3), Fraction(1, 3) + Fraction(1, 2**300)
        batch = [position_between(left, right, i)[0] for i in range(1000)]
        self.assertEqual(len(set(batch)), 1000)

    def test_smallest_gap_repeatedly_remains_insertable(self):
        left, right = Fraction(0), Fraction(1)
        for uid in range(80):
            key, _ = position_between(left, right, uid)
            right = key
        self.assertGreater(right, left)

    def test_causal_dependencies_override_wrong_clock(self):
        fs = [event('earlier', 'manual_played', 100),
              event('later', 'reset', 1, ('earlier',))]
        for order in permutations(fs):
            self.assertEqual(playback_projection(order), (False, 0, False))

    def test_legitimate_completion_then_replay_stays_played(self):
        fs = [event('s1', 'start', 1),
              event('c1', 'complete', 2, ('s1',), session='s1'),
              event('s2', 'start', 3, ('c1',)),
              event('p2', 'checkpoint', 4, ('s2',), session='s2', position=45)]
        for order in permutations(fs):
            self.assertEqual(playback_projection(order), (True, 45, True))

    def test_stale_completion_has_no_effect_in_every_arrival_order(self):
        fs = [event('s1', 'start', 1),
              event('s2', 'start', 2, ('s1',)),
              event('c1', 'complete', 3, ('s1',), session='s1'),
              event('p2', 'checkpoint', 4, ('s2',), session='s2', position=45)]
        for order in permutations(fs):
            self.assertEqual(playback_projection(order), (False, 45, True))

    def test_reset_prevents_old_lazy_completion(self):
        fs = [event('s1', 'start', 1), event('reset', 'reset', 2, ('s1',)),
              event('c1', 'complete', 3, ('s1',), session='s1')]
        for order in permutations(fs):
            self.assertEqual(playback_projection(order), (False, 0, True))

    def test_recovery_retains_hidden_manual_mark(self):
        fs = [event('mark', 'manual_played', 1), event('s1', 'start', 2, ('mark',)),
              event('s2', 'start', 3, ('s1',)),
              event('c1', 'complete', 4, ('s1',), session='s1')]
        self.assertTrue(playback_projection(fs)[0])
        # A latest-value-only replica would lack mark and lose this state.
        reduced = [dict(f, deps=tuple(d for d in f['deps'] if d != 'mark'))
                   for f in fs if f['id'] != 'mark']
        self.assertFalse(playback_projection(reduced)[0])

    def test_reset_ordinal_not_numeric_maximum(self):
        self.assertEqual(residual(15, [(2, 20), (3, 10)]), 5)
        for cuts in permutations([(2, 20), (3, 10)]):
            self.assertEqual(residual(15, cuts), 5)
        self.assertEqual(residual(20, [(2, 20)]), 0)
        self.assertEqual(residual(7, []), 7)

    def test_restart_and_below_baseline_rewind(self):
        self.assertEqual(android_contribution(10, [20, 5]), 10)
        first = android_contribution(0, [10])
        second = android_contribution(10, [5])
        self.assertEqual(first + second, 10)

    def test_missing_dependency_never_silently_reorders(self):
        with self.assertRaises(ValueError):
            causal_order([event('b', 'reset', 2, ('missing',))])


if __name__ == '__main__':
    unittest.main(verbosity=2)
