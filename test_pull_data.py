import os
import titanpublic

sport = "ncaam"
titan_env = "dev"
features = ("away_conference",)

date_range = titanpublic.date_logic.previous_years_with_gaps(
    20221011,
    3,
    "ncaam",
    [20211011],
)

result, _ = titanpublic.pull_data_multi_range(
    titanpublic.pod_helpers.database_resolver(sport, titan_env),
    features,
    date_range,
    titanpublic.shared_logic.get_secrets(os.path.dirname(os.path.abspath(__file__))),
)

print(result)
