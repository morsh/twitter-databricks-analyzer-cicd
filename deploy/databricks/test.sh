#!/bin/bash

# Access granted under MIT Open Source License: https://en.wikipedia.org/wiki/MIT_License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation 
# the rights to use, copy, modify, merge, publish, distribute, sublicense, # and/or sell copies of the Software, 
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions 
# of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED 
# TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#
#
# Description: Deploy Databricks cluster
#
# Usage: 
#
# Requirments:  
#

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

# Set path
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"

# Constants
RED='\033[0;31m'
ORANGE='\033[0;33m'
NC='\033[0m'

wait_for_run () {
    # See here: https://docs.azuredatabricks.net/api/latest/jobs.html#jobsrunresultstate
    declare mount_run_id=$1
    declare wait_for_state=${2:-""}^^
    while : ; do
        life_cycle_status=$(databricks runs get --run-id $mount_run_id | jq -r ".state.life_cycle_state")
        result_state=$(databricks runs get --run-id $mount_run_id | jq -r ".state.result_state")
        if [[ $result_state == "SUCCESS" || $result_state == "SKIPPED" ]]; then
            break;
        elif [[ $life_cycle_status == "INTERNAL_ERROR" || $result_state == "FAILED" ]]; then
            echo -e "${RED} error running job, details ahead"
            run_status=$(databricks runs get --run-id $mount_run_id)
            err_msg=$(echo $run_status | jq -r ".state.state_message")
            echo -e "${RED}Error while running ${mount_run_id}: ${err_msg} ${NC}"
            exit 1
        elif [[ $wait_for_state && $life_cycle_status == $wait_for_state ]]; then
            echo "The job ${mount_run_id} has reached status ${wait_for_state}"
            break;
        else 
            echo "Waiting for run ${mount_run_id} to finish..."
            sleep 30s
        fi
    done
}

wait_for_start () {
    # See here: https://docs.azuredatabricks.net/api/latest/jobs.html#jobsrunresultstate
    declare cluster_id=$1
    while : ; do
        result_state=$(databricks clusters get --cluster-id $cluster_id | jq -r ".state") 
        if [[ $result_state == "RUNNING" ]]; then
            break;
        elif [[ $result_state == "TERMINATED" ]]; then
            echo -e "${RED}Error while starting ${cluster_id} ${NC}"
            exit 1
        else 
            echo "Waiting for cluster ${cluster_id} to start..."
            sleep 2m
        fi
    done
}

cluster_exists () {
    declare cluster_name="$1"
    declare cluster=$(databricks clusters list | tr -s " " | cut -d" " -f2 | grep ^${cluster_name}$)
    if [[ -n $cluster ]]; then
        return 0; # cluster exists
    else
        return 1; # cluster does not exists
    fi
}

yes_or_no () {
    while true; do
        read -p "$(echo -e ${ORANGE}"$* [y/n]: "${NC})" yn
        case $yn in
            [Yy]*) return 0  ;;
            [Nn]*) echo -e "${RED}Aborted${NC}" ; return  1 ;;
        esac
    done
}

_main() {
    echo -e "${ORANGE}"
    echo -e "!! -- WARNING --!!"
    echo -e "If this is the second time you are running this, this will re-upload and overwrite existing notebooks with the same names in the 'notebooks' folder. "
    echo -e "This will also drop and reload data in rating and recommendation Tables."
    echo -e "${NC}"
    yes_or_no "Are you sure you want to continue (Y/N)?" || { exit 1; }

    # Upload notebooks and dashboards
    echo "Uploading notebooks..."
    databricks workspace import_dir "../../notebooks" "/social" --overwrite

    # # For each job in the config folder, stop any existing running jobs and start the new job
    # echo "Searching for job runs to stop..."
    # # TODO: check if assured order if not add sort
    # for f in ./config/run.02.enrichment.config.json; do
    #     for jn in "$(cat "$f" | jq -r ".run_name")"; do

    #         # Search for active running jobs and stop them
    #         declare runids=$(databricks runs list --active-only --output JSON | jq -c ".runs // []" | jq -c "[.[] | select(.run_name == \"$jn\")]" | jq .[].run_id)
    #         for id in $runids; do
    #             echo "Stopping job id $id..."
    #             databricks runs cancel --run-id $id
    #         done

    #         # Descern if the next execution should be continuous or a one time execution and execute accordingly
    #         if [[ $jn == 'Continuous:'* ]]; then
    #             echo "Running job $jn and waiting for status to be <Running>..."
    #             wait_for_run $(databricks runs submit --json-file "$f" | jq -r ".run_id" ) "RUNNING"
    #         else 
    #             echo "Running job $jn and waiting for completion..."
    #             wait_for_run $(databricks runs submit --json-file "$f" | jq -r ".run_id" )
    #         fi
    #     done 
    # done
    
    # , mount storage and setup up tables
    # echo "Mounting blob storage. This may take a while as cluster spins up..."
    # wait_for_run $(databricks runs submit --json-file "./config/run.mountstorage.config.json" | jq -r ".run_id" )
    # echo "Starting injest tweets. This may take a while as cluster spins up..."
    # wait_for_run $(databricks runs submit --json-file "./config/run.injesttweets.config.json" | jq -r ".run_id" ) "RUNNING"
    # echo "Starting windowed timeline. This may take a while as cluster spins up..."
    # wait_for_run $(databricks runs submit --json-file "./config/run.injesttweets.config.json" | jq -r ".run_id" ) "RUNNING"

    # Schedule and run jobs
    # databricks jobs run-now --job-id $(databricks jobs create --json-file "./config/job.ingestdata.config.json" | jq ".job_id")
}

_main

# Use a new name and the token you created manually: 
# databricks configure --token