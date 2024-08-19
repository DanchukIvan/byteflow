# Implementations

---

## Core classes

::: byteflows.scheduling.limits
    options:
      show_source: true
      group_by_category: false
      members:
        - CountLimit
        - MemoryLimit
        - TimeLimit
        - UnableBufferize

::: byteflows.scheduling.timeinterval
    options:
      show_source: true
      group_by_category: false
      members:
        - AlwaysRun
        - DailyInterval
        - TimeCondition
        - WeekdayInterval

## Service classes and utilities

::: byteflows.scheduling.limits
    options:
      show_source: true
      group_by_category: false
      members:
        - get_allowed_limits
        - limit
        - setup_limit

## Types, constants and enums

::: byteflows.scheduling.timeinterval
    options:
      show_source: true
      group_by_category: false
      members:
        - AllowedWeekDays
        - WeekDaysType
