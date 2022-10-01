# titan-public
A nice API for accessing titan data

## Please ignore.  Personal project.

# Instructions

1. One time, install mysql locally.  https://stackoverflow.com/a/7475296
2. Pip install this library.  For example, include this in your requirements:

```
titanpublic @ git+https://github.com/gaffney2010/titan-public.git#egg=titanpublic-1.14.1
```

3. Copy `secrets.yaml` into your directory.  pip install pyyaml and frozendict.

Now you can pull data like this:

```
import os

from frozendict import frozendict
import titanpublic
import yaml

with open(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "secrets.yaml"), "r"
) as f:
    secrets = yaml.load(f, Loader=yaml.Loader)
# Need this for caching reasons.
secrets = frozendict(secrets)

df, _ = titanpublic.pull_data("ncaam", ("feature_1", "feature_2"), 20201201, 20201231, secrets)
```
