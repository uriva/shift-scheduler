import calendar
import datetime
import functools
from typing import Callable, Dict, FrozenSet, Iterable, Text, Tuple

import gamla
import toolz
from toolz import curried
from toolz.curried import operator

Person = Text
Shift = Text
Time = datetime.date
DateToShift = Dict[Time, Shift]
Scheduling = Dict[Person, DateToShift]


class NoPersonAvailable(Exception):
    pass


def _compute_money(
    money: Callable[[Shift, Time], float],
    date_to_shift: DateToShift,
):
    return gamla.pipe(date_to_shift, dict.items, gamla.map(gamla.star(money)), sum)


def _compute_shift_amount(shift: Shift, date_to_shift: DateToShift):
    return gamla.pipe(
        date_to_shift, dict.values, curried.filter(operator.eq(shift)), toolz.count
    )


@gamla.curry
def _compare_by_heuristic(heuristics, x, y):
    for heuristic in heuristics:
        delta = heuristic(x) - heuristic(y)
        if delta:
            return delta
    return 0


@gamla.curry
def assign_shift(
    people: FrozenSet[Person],
    # Possible dates where a person can be assigned a shift.
    is_available: Callable[[Shift, Time], Callable[[Person], bool]],
    # Given the intermediatry scheduling and shift, returns a intenger for signaling which person should be preferred.
    comparator: Callable[[Scheduling, Shift], Callable[[Person, Person], int]],
    scheduling: Scheduling,
    shift_and_time: Tuple[Shift, Time],
):
    shift, time = shift_and_time
    return gamla.pipe(
        people,
        curried.filter(
            gamla.alljuxt(
                toolz.complement(
                    lambda person: toolz.get_in([person, time], scheduling)
                ),
                is_available(shift, time),
            )
        ),
        curried.sorted(
            key=functools.cmp_to_key(comparator(scheduling, shift)),
        ),
        gamla.translate_exception(toolz.first, StopIteration, NoPersonAvailable),
        lambda person: gamla.assoc_in(scheduling, [person, time], shift),
    )


@gamla.curry
def comparator(
    # If an individual is only filling a 50% position, their value would be 0.5.
    person_to_sow: Dict[Person, float],
    availabilty: Callable[[Shift], Callable[[Person], int]],
    money_for_time_and_shift: Callable[[Time, Shift], float],
    scheduling: Scheduling,
    shift: Shift,
):
    return _compare_by_heuristic(
        [
            lambda person: (
                _compute_money(money_for_time_and_shift, scheduling.get(person, {}))
                / person_to_sow(person)
            ),
            lambda person: (
                _compute_shift_amount(
                    shift,
                    scheduling.get(person, {}),
                )
                / person_to_sow(person)
            ),
            availabilty(shift),
        ]
    )


def get_all_days_in_month(year: int, month: int) -> Iterable[Time]:
    return (
        datetime.date(year, month, day)
        for day in range(1, calendar.monthrange(year, month)[1] + 1)
    )


def is_friday_or_saturday(time: Time):
    return time.weekday() in [4, 5]


@gamla.curry
def no_one_is_especially_unavailable(shift: Shift, person: Person) -> int:
    return 0


scheduling_to_text = gamla.compose_left(
    dict.items,
    curried.mapcat(
        gamla.star(
            lambda person, time_to_shift: gamla.pipe(
                time_to_shift,
                dict.items,
                curried.map(
                    gamla.compose_left(
                        gamla.star(
                            lambda time, shift: (
                                time.strftime("%Y-%m-%d %A"),
                                person,
                            )
                        ),
                        "\t".join,
                        lambda s: s.expandtabs(25),
                    ),
                ),
            )
        )
    ),
    curried.sorted,
    "\n".join,
)
