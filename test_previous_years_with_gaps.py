from titanpublic import date_logic

print(date_logic.previous_years_with_gaps(
    20220101,
    3,
    "ncaam",
    [20200301],
).ranges)
