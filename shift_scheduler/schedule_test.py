import itertools

import gamla

from . import schedule


def test_john_is_lazy():
    availabilty = gamla.curry(lambda shift, time, person: person != "john")
    scheduling = gamla.reduce(
        schedule.assign_shift(
            ["john", "jeff", "jona"],
            availabilty,
            schedule.comparator(
                gamla.just(1.0),
                lambda shift: lambda person: 0,
                lambda time, shift: 1,
            ),
        ),
        {},
        itertools.product(["oncall"], schedule.get_all_days_in_month(2020, 1)),
    )
    assert "john" not in scheduling
    assert "jeff" in scheduling
    assert "jona" in scheduling
