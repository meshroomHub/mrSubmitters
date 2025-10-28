# Meshroom Submitters

## Dependencies

This repository is based on :
- `simpleFarm` : private wrapper around tractor
- `tractor`:  https://rmanwiki-26.pixar.com/space/TRA/22184334/

## Setup

To setup here are the required variables to set :

```sh
def commands():
    env.PYTHONPATH.append("{root}/")
    env.PYTHONPATH.append("{root}/meshroom")
    env.PYTHONPATH.append("{root}/python")
    # Command line nodes
    env.MESHROOM_SUBMITTERS_PATH.append('{root}/meshroom')
    # Set default submitter
    env.MESHROOM_DEFAULT_SUBMITTER.set('Tractor')
    # Config folder
    env.MR_SUBMITTERS_CONFIGS.set("{root}/config")
    # Script folder (tractor wrapper, ...)
    env.MR_SUBMITTERS_SCRITPS.set("{root}/script")
```
