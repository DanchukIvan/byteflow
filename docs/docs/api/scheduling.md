# Implementations

---

## Core classes

::: yass.scheduling.limits
    options:
      show_source: true
      group_by_category: false
      members:
        - CountLimit
        - MemoryLimit
        - TimeLimit
        - UnableBufferize

::: yass.scheduling.timeinterval
    options:
      show_source: true
      group_by_category: false
      members:
        - AlwaysRun
        - DailyInterval
        - TimeCondition
        - WeekdayInterval

## Service classes and utilities

::: yass.scheduling.limits
    options:
      show_source: true
      group_by_category: false
      members:
        - get_allowed_limits
        - limit
        - setup_limit

## Types, constants and enums

::: yass.scheduling.timeinterval
    options:
      show_source: true
      group_by_category: false
      members:
        - AllowedWeekDays
        - WeekDaysType
