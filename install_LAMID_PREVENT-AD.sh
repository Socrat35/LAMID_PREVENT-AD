#!/bin/bash

function installpip {
    pythonversion=`python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))'`
    if [[ "${pythonversion}" == "2.7" ]]; then
        versionname='python-pip'   
    elif [[ "${pythonversion:0:1}" == "3" ]]; then
        versionname='python3-pip'   
    fi
    echo 'installing python3-pip'
    apt-get install ${versionname}
}

function installpackage {
    pip=`which pip`
    if [[ -z "${pip}" ]]; then
        installpip
    fi
    pip install $1
}

# Checking if python is installed
python=`which python`
if [[ -z "${python}" ]]; then
    echo 'python is not installed. Aborting.'
fi

#checking if all packages are available
declare -a packages=("sys" "getopt" "os" "errno" "getpass" "json" "requests")
for package in "${packages[@]}"
do
    if ! python -c "import pkgutil; exit(not pkgutil.find_loader(\"${package}\"))"; then
        echo "package ${package} not found."
        installpackage ${package}
    fi
done

