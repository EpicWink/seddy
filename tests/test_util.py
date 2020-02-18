"""Test ``seddy._util``."""

from seddy import _util as seddy_util


def test_list_paginated():
    # Build input
    def fn(foo, bar=42, nextPageToken=None):
        spams = {None: [0], "spam": [1, 2, 3], "eggs": [4, 7, 9], "ham": [10, 42, 99]}
        tokens = {None: "spam", "spam": "eggs", "eggs": "ham"}
        resp = {"foo": foo * bar, "spam": spams[nextPageToken]}
        if nextPageToken in tokens:
            resp["nextPageToken"] = tokens[nextPageToken]
        return resp

    kwargs = {"foo": "ab", "bar": 7}

    # Run function
    assert seddy_util.list_paginated(fn, "spam", kwargs) == {
        "foo": "ababababababab", "spam": [0, 1, 2, 3, 4, 7, 9, 10, 42, 99]
    }
