import functools
from typing import Any, Callable, Collection, FrozenSet, Iterable, Text, Tuple

import gamla
import toolz
from toolz import curried
from toolz.curried import operator

Person = Text
Shift = Any
Scheduling = FrozenSet[Tuple[Person, Shift]]


class NoPersonAvailable(Exception):
    pass


@gamla.curry
def compare_by(
    heuristics: Iterable[Callable[[Person], float]],
    x: Person,
    y: Person,
) -> float:
    for heuristic in heuristics:
        delta = heuristic(x) - heuristic(y)
        if delta:
            return delta
    return 0


@gamla.curry
def assign_shift(
    people: Collection[Person],
    is_available: Callable[[Scheduling, Shift], Callable[[Person], bool]],
    # Given the intermediatry scheduling and shift, returns a intenger for signaling which person should be preferred.
    comparator: Callable[[Scheduling, Shift], Callable[[Person, Person], int]],
    scheduling: Scheduling,
    shift: Shift,
) -> Scheduling:
    return gamla.pipe(
        people,
        curried.filter(
            is_available(scheduling, shift),
        ),
        curried.sorted(
            key=functools.cmp_to_key(comparator(scheduling, shift)),
        ),
        gamla.translate_exception(toolz.first, StopIteration, NoPersonAvailable),
        lambda person: {*scheduling, (person, shift)},
    )


@gamla.curry
def shifts_manned_by_person(scheduling: Scheduling, person: Person) -> Iterable[Shift]:
    return gamla.pipe(
        scheduling,
        curried.filter(
            gamla.compose_left(toolz.first, operator.eq(person)),
        ),
        curried.map(toolz.second),
    )
