import datetime
import itertools
import logging
import random

import gamla

from shift_scheduler import schedule, time_utils

_is_weekend = gamla.anyjuxt(
    time_utils.is_friday,
    time_utils.is_saturday,
)


@gamla.curry
def _availabilty(working_weekends, scheduling, shift, person):
    return not gamla.anymap(_is_weekend, shift) or person in working_weekends


def _weekday_shifts(start_date):
    buffer = ()
    current = start_date
    while True:
        if time_utils.is_sunday(current):
            buffer = ()
        if time_utils.is_friday(current):
            yield buffer
            buffer = ()
        buffer = (*buffer, current)
        current += datetime.timedelta(days=1)


def _weekend_shifts(start_date):
    buffer = ()
    current = start_date
    while True:
        if time_utils.is_friday(current):
            buffer = ()
        if time_utils.is_sunday(current):
            yield buffer
            buffer = ()
        buffer = (*buffer, current)
        current += datetime.timedelta(days=1)


_days_to_weight = gamla.compose_left(
    gamla.map(
        gamla.ternary(
            _is_weekend,
            gamla.just(2),
            gamla.just(1),
        ),
    ),
    sum,
)


def _weighted_shifts(scheduling):
    return gamla.compose_left(
        schedule.shifts_manned_by_person(scheduling),
        gamla.concat,
        _days_to_weight,
    )


def _run(working_weekends, not_working_weekends):
    everyone = working_weekends + not_working_weekends
    # Shuffle to avoid situations where people order consistently
    # makes some people get more shifts.
    random.shuffle(everyone)
    return gamla.reduce(
        schedule.assign_shift(
            everyone,
            _availabilty(working_weekends),
            lambda scheduling, shift: schedule.compare_by(
                [
                    _weighted_shifts(scheduling),
                ],
            ),
        ),
        {},
        gamla.pipe(
            datetime.date(2020, 7, 26),
            # In small numbers it is better to allocate first the weekend shifts,
            # otherwise we might over-allocate people who do all shift kinds.
            gamla.juxt(_weekend_shifts, _weekday_shifts),
            gamla.mapcat(gamla.take(22)),
        ),
    )


_flatten_days = gamla.mapcat(
    gamla.compose_left(
        gamla.star(
            lambda person, shift: (
                (person,),
                shift,
            ),
        ),
        gamla.star(itertools.product),
    ),
)

_scheduling_to_text = gamla.compose_left(
    _flatten_days,
    gamla.map(
        gamla.compose_left(
            gamla.star(
                lambda person, date: (date.strftime("%Y-%m-%d %A"), person, "\n"),
            ),
            "\t".join,
            lambda s: s.expandtabs(25),
        ),
    ),
    gamla.sort,
)


_print_justice = gamla.compose_left(
    _flatten_days,
    gamla.edges_to_graph,
    gamla.valmap(
        gamla.juxt(
            gamla.compose_left(
                _days_to_weight,
                gamla.wrap_str("weight: {}"),
            ),
            gamla.compose_left(gamla.count, gamla.wrap_str("total: {}")),
            gamla.compose_left(
                gamla.filter(_is_weekend),
                gamla.count,
                gamla.wrap_str("weekend: {}"),
            ),
        ),
    ),
    logging.info,
)


def _write():
    text = gamla.pipe(
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
        gamla.side_effect(_print_justice),
        _scheduling_to_text,
    )
    open("./oncall_rotation.txt", "w").writelines(text)


_write()
