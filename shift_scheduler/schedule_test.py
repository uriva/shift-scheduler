import itertools

import toolz
from toolz.curried import operator

import gamla

from . import schedule


@gamla.curry
def _no_one_is_especially_unavailable(shift, person):
    return 0


def test_john_is_lazy():
    availabilty = gamla.curry(lambda shift, time, person: person != "john")
    scheduling = gamla.reduce(
        schedule.assign_shift(
            ["john", "jeff", "jona"],
            availabilty,
            schedule.comparator(
                gamla.just(1.0),
                _no_one_is_especially_unavailable,
                lambda time, shift: 1,
            ),
        ),
        {},
        itertools.product(["oncall"], schedule.get_all_days_in_month(2020, 1)),
    )
    assert "john" not in scheduling
    assert "jeff" in scheduling
    assert "jona" in scheduling


def test_using_table():
    working_weekends = ["a", "b"]
    not_working_weekends = ["no1", "no2"]

    def is_weekend(time):
        # Friday or Saturday.
        return time.weekday() in [4, 5]

    @gamla.curry
    def availabilty(shift, time, person):
        if is_weekend(time):
            return person in working_weekends
        return True

    gamla.pipe(
        gamla.reduce(
            schedule.assign_shift(
                working_weekends + not_working_weekends,
                availabilty,
                schedule.comparator(
                    gamla.just(1.0),
                    _no_one_is_especially_unavailable,
                    lambda time, shift: 2 if is_weekend(time) else 1,
                ),
            ),
            {},
            itertools.product(
                ["oncall"],
                toolz.concat(
                    [
                        schedule.get_all_days_in_month(2020, 1),
                        schedule.get_all_days_in_month(2020, 2),
                        schedule.get_all_days_in_month(2020, 3),
                        schedule.get_all_days_in_month(2020, 4),
                    ]
                ),
            ),
        ),
        dict.values,
        gamla.map(
            gamla.compose_left(
                dict.keys,
                gamla.map(gamla.ternary(is_weekend, gamla.just(2), gamla.just(1))),
                sum,
            )
        ),
        lambda efforts: itertools.combinations(efforts, 2),
        gamla.map(gamla.star(lambda x, y: abs(x - y))),
        max,
        gamla.check(operator.gt(2), AssertionError),
    )
