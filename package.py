name = "mrSubmitters"

version = "1.2.0"

plugin_for = ["meshroom"]

requires = [
    "tractorLoginManager",
    "tractor"
]

private_build_requires = ["cmake-3"]

with scope("config") as config:
    # Specify the path where the package will be install with the command rez release
    config.release_packages_path = "/s/apps/packages/mikrosVfx/multiview"

def commands():
    env.PYTHONPATH.append("{root}")
    env.PYTHONPATH.append("{root}/meshroom")
    env.PYTHONPATH.append("{root}/python")

    # Fallback rez version
    # If no REZ_VERSION is set we will set the job REZ_VERSION as this version
    env.FARM_REZ_VERSION = "2.104.9"

    # Command line nodes
    env.MESHROOM_SUBMITTERS_PATH.append("{root}/meshroom")
    # Set default submitter
    env.MESHROOM_DEFAULT_SUBMITTER.set("Tractor")

    # Service key expr used for jobs
    env.DEFAULT_TRACTOR_SERVICE.set("")
    
    env.MR_SUBMITTERS_CONFIGS.set("{root}/config")
    env.MR_SUBMITTERS_SCRITPS.set("{root}/script")

    # Tractor task wrapper
    alias("tractorExpander", "python {root}/script/tractorExpander.py")
