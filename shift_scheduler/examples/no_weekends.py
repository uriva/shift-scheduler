import datetime
import itertools

import gamla
import toolz
from toolz import curried

from shift_scheduler import schedule, time_utils


@gamla.curry
def _availabilty(working_weekends, scheduling, shift, person):
    return (
        not gamla.anymap(
            gamla.anyjuxt(time_utils.is_friday, time_utils.is_saturday),
            shift,
        )
        or person in working_weekends
    )


def alternating_weekdays_and_weekends(start_date):
    buffer = ()
    current = start_date
    while True:
        if time_utils.is_friday(current) or time_utils.is_sunday(current):
            yield buffer
            buffer = ()
        buffer = (*buffer, current)
        current += datetime.timedelta(days=1)


def _run(working_weekends, not_working_weekends):
    return gamla.reduce(
        schedule.assign_shift(
            working_weekends + not_working_weekends,
            _availabilty(not_working_weekends),
            lambda scheduling, shift: schedule.compare_by(
                [
                    gamla.compose_left(
                        schedule.shifts_manned_by_person(scheduling),
                        toolz.concat,
                        gamla.map(
                            gamla.ternary(
                                gamla.anyjuxt(
                                    time_utils.is_friday,
                                    time_utils.is_saturday,
                                ),
                                gamla.just(2),
                                gamla.just(1),
                            ),
                        ),
                        sum,
                    ),
                ],
            ),
        ),
        {},
        gamla.pipe(
            alternating_weekdays_and_weekends(datetime.date(2020, 10, 1)),
            curried.take(30),
        ),
    )


_scheduling_to_text = gamla.compose_left(
    curried.mapcat(
        gamla.compose_left(
            gamla.star(
                lambda person, shift: (
                    (person,),
                    shift,
                ),
            ),
            gamla.star(itertools.product),
        ),
    ),
    gamla.map(
        gamla.compose_left(
            gamla.star(lambda person, date: (date.strftime("%Y-%m-%d %A"), person)),
            "\t".join,
            lambda s: s.expandtabs(25),
        ),
    ),
    curried.sorted,
    "\n".join,
)


def _write():
    gamla.pipe(
        _run(
            [
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
            ],
            [
                "i",
                "j",
            ],
        ),
        _scheduling_to_text,
        open("./oncall_rotation.txt", "w").writelines,
    )


_write()
