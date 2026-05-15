# Meshroom Submitters

## Dependencies

This repository is based on :
- `tractor`:  https://rmanwiki-26.pixar.com/space/TRA/22184334/

Optionally you can use :
- `tractorLoginManager`: A private tractor wrapper to handle login. If you don't have it
it will display an interface for you to input your credentials.

## Setup

To setup here are the required variables to set :

```sh
ROOT={root}  # Replace by the package root
export PYTHONPATH=$PYTHONPATH:$ROOT/
export PYTHONPATH=$PYTHONPATH:$ROOT/meshroom
export PYTHONPATH=$PYTHONPATH:$ROOT/python
# Variables required by Meshroom Submitters
export MR_SUBMITTERS_CONFIGS=$ROOT/config
export MR_SUBMITTERS_SCRITPS=$ROOT/script
# Declare Meshroom plugin
export MESHROOM_SUBMITTERS_PATH=$MESHROOM_SUBMITTERS_PATH:$ROOT/meshroom
# Set default submitter
export MESHROOM_DEFAULT_SUBMITTER=Tractor
```

## Build & Release

## With [Rez](https://github.com/AcademySoftwareFoundation/rez)

```sh
# Build
rez build $rez_build_args --install --prefix {build_root} --clean

# Release
export MRSUBMITTERS_RELEASE_PATH="{install_root}"
rez release
```
