"""A process of the ORM cache benchmark: ``python -m benchmarks.orm_worker <role> <json args>``.

The parent sets the contender, the databases and the clock offset in the environment. The worker prints the role's
result as JSON on its last line of output.
"""

import json
import os
import sys
import time


def main() -> None:
    role, raw_args = sys.argv[1], sys.argv[2]
    offset = float(os.environ.get("BENCH_ORM_CLOCK_OFFSET", "0"))
    if offset:
        # Shifted before cachalot binds time.time at import, as on a host whose clock is off.
        real_time, real_time_ns, offset_ns = time.time, time.time_ns, round(offset * 1e9)
        time.time = lambda: real_time() + offset
        time.time_ns = lambda: real_time_ns() + offset_ns
    os.environ["DJANGO_SETTINGS_MODULE"] = "benchmarks.orm_settings"

    import django

    django.setup()

    from benchmarks.ormbench import scorecard, workloads

    roles = {**workloads.ROLES, "scorecard": scorecard.run}
    result = roles[role](json.loads(raw_args))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
