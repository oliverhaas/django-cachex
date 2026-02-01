# Derived from django-redis (https://github.com/jazzband/django-redis)
# Copyright (c) 2011-2016 Andrey Antukh <niwi@niwi.nz>
# Copyright (c) 2011 Sean Bleier
# Licensed under BSD-3-Clause
#
# django-redis was used as inspiration for this project. The code similarity
# is high for this file, but also somewhat coincidental given the minimal
# nature of this utility function.


def default_reverse_key(key: str) -> str:
    return key.split(":", 2)[2]
