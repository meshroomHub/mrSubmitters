#!/usr/bin/env python

"""
# Tractor config

The config file is used to drive machines where jobs are going to be submitted. The config is used to setup **Service Key Expressions**, which is a field that is setup on the tractor job or task. Tasks inherit the job servicekey expr.

Some definitions :
- *Blade :* a physical machine that is see by tractor and provide Service key  
- *Slot :* define the number of task that a single physical blade can run at once. Most of the blades who provide mikrosRender tag are actually single slot (with the exception of the frarnd31XX who are dual slot, others like mikrosScript are sliced into more, 6 for now, but should in the future have 1 slot per physical cpu, or 1slot per 2 cpu threads) 
- *Tag :* short name for Service Key Expr

## Main tags

We can use one of :
- `mikrosRender` : Common multithread render blade (most of the farm) 
- `mikrosScript` : Design blade who are sliced into many small slot, allowing to many mono-threaded task that’s not to heavy in ram usage (there is between 1.5Go-2Go per slot)
- `gpu` : design reserved ogs and playblast compliant blades 
- `cp` : tag reserved for review stations, always paired with the hostname of the review machine for local copy on their local SSD drive (commonly 4 slot per blade)

## CPU tag

We can use `@.nCPUs` (ex `mikrosRender,@nCPUs=40`) to select how much CPU cores we want per Blade

## Ram tags

We can use `@.mem` to select how much ram is available per blade.

Warning : this will apply on ALL blade resources, not the slot capacity real capacity, so, if a task require at least 100G of ram, with the @.mem you can end on some `frarnd31XX` who have 192Gb physical ram installed, but have 2slots, so each job will only have a real 96Gb of availible ram.

Instead, we can use ramXXX : These are keys that provide the **maximum** ram *PER SLOT* on a blade, when the native and usual `@.mem>XX` only point to the availible ram on the full blade. As we want to slice in slot some blades, the `@.mem` aproch become obsolete and not accurate enough. 
- `ram32` 
- `ram64` 
- `ram96` 
- `ram128` 
- `ram192` 
- `ram256` 
- `ram512` 

You can 

## VRam tags

These are keys who inform about the total Vram and in some case the graphic card generation (manely used for deeplearning, meshroom)  
side note: only workstations have graphic card, the dedicated “render blades” never provide those tags. 
- `cuda8G` 
- `cuda12G` 
- `cuda16G` 
- `cuda48G` 
- `cudaC`  (Ampere or higher generation or architecture) 

## Other tags

Keys rnd/wkst, to help excluding or including workstation only, or dedicated render blades only 
- `rnd`
- `wkst`

## Service Key Expr syntax

https://rmanwiki-26.pixar.com/space/TRA/22184254/Job+Scripting

- Logical AND: `&&` or the `,` (preferably use the coma for a more compact and readable format) 
- Logical OR: `||`
- Logical NOT: `!`
- We can also use `<`,`>`,`<=`,`>=`,`=`,`!=` (e.g. `@.nCPUs>=16`)

Hostname/profile with wildcard (to avoid in possible, profile name will sligthly changes in a near future, hostname are more consistant, but can still evolve too) :  
- `Frarnd1105` (no need of double quote) 
- `“frarnd*”`  (double quote mandatory because of the `*` wildcard)  

Expressions can also be encapsulated into `()` if needed.  
"""

from enum import IntEnum


class Level(IntEnum):
    NONE = 0
    NORMAL = 1
    INTENSIVE = 2
    EXTREME = 3
    SCRIPT=-1


SCRIPT_CONFIGS = "mikrosScript"
CPU_CONFIGS = {
    "LEVELS": {
        "NONE": "mikrosRender",
        "NORMAL": "mikrosRender",
        "INTENSIVE": "mikrosRender,rnd",
        "EXTREME": "mikrosRender,rnd,@.nCPU>200"
    },
    "RAM": {
        "NONE": "",
        "NORMAL": "",  # ram64 is the minimum for all machines
        "INTENSIVE": "ram128",
        "EXTREME": "ram256"
    }
}
GPU_CONFIGS = {
    "LEVELS": {
        "NONE": "mikrosRender",
        "NORMAL": "mikrosRender,cuda8G",
        "INTENSIVE": "mikrosRender,cuda16G",
        "EXTREME": "mikrosRender,cuda16G,cudaC"
    },
    "RAM": {
        "NONE": "",
        "NORMAL": "",  # ram64 is the minimum for all machines
        "INTENSIVE": "ram128",
        "EXTREME": "ram128"
    }
}

def get_config(cpu:int, ram:int, gpu:int, excludeHosts:list[str]=None):
    """ Tries to fetch the adequate config that matches requirements """
    if cpu == Level.SCRIPT and gpu<=0:
        return SCRIPT_CONFIGS
    if gpu>0:
        configType = GPU_CONFIGS
        configLevel = gpu
    else:
        configType = CPU_CONFIGS
        configLevel = cpu
    config = configType["LEVELS"][Level(configLevel).name.upper()]
    ramconfig = configType["RAM"][Level(ram).name.upper()]
    if ramconfig:
        config += "," + ramconfig
    if excludeHosts:
        config += "," + ",".join([f"!{host}"for host in excludeHosts])
    return config


def __test__():
    from itertools import product
    for gpu, cpu, ram in product(range(4), range(4), range(4)):
        service = get_config(cpu, ram, gpu)
        print(f"GPU={Level(gpu).name:<10} CPU={Level(cpu).name:<10} RAM={Level(ram).name:<10}  -> {service}")
