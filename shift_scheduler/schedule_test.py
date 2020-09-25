import itertools
import toolz
from toolz.curried import operator

import gamla

from . import schedule, time_utils


def test_john_is_lazy():
    people_with_shifts = gamla.pipe(
        gamla.reduce(
            schedule.assign_shift(
                ["john", "jeff", "jona"],
                gamla.curry(lambda scheduling, shift, person: person != "john"),
                lambda scheduling, shift: schedule.compare_by(
                    [
                        gamla.compose_left(
                            schedule.shifts_manned_by_person(scheduling),
                            toolz.count,
                        )
                    ]
                ),
            ),
            {},
            time_utils.get_all_days_in_month(2020, 1),
        ),
        gamla.map(toolz.first),
        frozenset,
    )
    assert "john" not in people_with_shifts
    assert "jeff" in people_with_shifts
    assert "jona" in people_with_shifts


def test_groups_with_weekend_preferences():
    working_weekends = ["a", "b"]
    not_working_weekends = ["no1", "no2"]

    shift_to_money = gamla.ternary(
        gamla.anyjuxt(time_utils.is_friday, time_utils.is_saturday),
        gamla.just(2),
        gamla.just(1),
    )

    gamla.pipe(
        gamla.reduce(
            schedule.assign_shift(
                working_weekends + not_working_weekends,
                gamla.curry(
                    lambda scheduling, shift, person: (
                        not gamla.anyjuxt(time_utils.is_friday, time_utils.is_saturday)(
                            shift
                        )
                        or person in working_weekends,
                    )
                ),
                lambda scheduling, shift: schedule.compare_by(
                    [
                        gamla.compose_left(
                            schedule.shifts_manned_by_person(scheduling),
                            gamla.map(shift_to_money),
                            sum,
                        )
                    ]
                ),
            ),
            {},
            toolz.concat(
                [
                    time_utils.get_all_days_in_month(2020, 1),
                    time_utils.get_all_days_in_month(2020, 2),
                    time_utils.get_all_days_in_month(2020, 3),
                    time_utils.get_all_days_in_month(2020, 4),
                ]
            ),
        ),
        gamla.edges_to_graph,
        gamla.valmap(
            gamla.compose_left(
                gamla.map(shift_to_money),
                sum,
            )
        ),
        dict.values,
        lambda efforts: itertools.combinations(efforts, 2),
        gamla.map(gamla.star(lambda x, y: abs(x - y))),
        max,
        gamla.check(operator.gt(2), AssertionError),
    )
