import gamla

from shift_scheduler import schedule

import toolz
import itertools


@gamla.curry
def _availabilty(working_weekends, _, time, person):
    if schedule.is_friday_or_saturday(time):
        return person in working_weekends
    return True


def _run(working_weekends, not_working_weekends):
    return gamla.reduce(
        schedule.assign_shift(
            working_weekends + not_working_weekends,
            _availabilty(not_working_weekends),
            schedule.comparator(
                gamla.just(1.0),
                schedule.no_one_is_especially_unavailable,
                lambda time, shift: 2 if schedule.is_friday_or_saturday(time) else 1,
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
    )


def _write():
    gamla.pipe(
        _run(["a", "b", "c"], ["e", "f"]),
        schedule.scheduling_to_text,
        lambda text: open("./oncall_rotation.txt", "w").writelines(text),
    )


_write()