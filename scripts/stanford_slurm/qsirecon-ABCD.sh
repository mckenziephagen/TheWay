## NOTE ##
# This workflow is derived from the Datalad Handbook

## Ensure the environment is ready to bootstrap the analysis workspace
# Check that we have conda installed
#conda activate
#if [ $? -gt 0 ]; then
#    echo "Error initializing conda. Exiting"
#    exit $?
#fi

DATALAD_VERSION=$(datalad --version)

if [ $? -gt 0 ]; then
    echo "No datalad available in your conda environment."
    echo "Try pip install datalad"
    # exit 1
fi

echo USING DATALAD VERSION ${DATALAD_VERSION}

set -e -u

recon_json=$3
recon_spec=$(basename ${recon_json} .json) 

## Set up the directory that will contain the necessary directories
PROJECTROOT=${PWD}/outputs/${recon_spec}-qsirecon-ABCD

if [[ -d ${PROJECTROOT} ]]
then
    echo ${PROJECTROOT} already exists
    # exit 1
fi

if [[ ! -w $(dirname ${PROJECTROOT}) ]]
then
    echo Unable to write to ${PROJECTROOT}\'s parent. Change permissions and retry
    # exit 1
fi

## Check the BIDS input
BIDSINPUT=$1
if [[ -z ${BIDSINPUT} ]]
then
    echo "Required argument is an identifier of the BIDS source"
    # exit 1
fi

## Start making things
mkdir -p ${PROJECTROOT}
cp $recon_json $PROJECTROOT

cd ${PROJECTROOT}

mkdir timing
# Jobs are set up to not require a shared filesystem (except for the lockfile)
# ------------------------------------------------------------------------------
# RIA-URL to a different RIA store from which the dataset will be cloned from.
# Both RIA stores will be created
input_store="ria+file://${PROJECTROOT}/input_ria"
output_store="ria+file://${PROJECTROOT}/output_ria"

# Create a source dataset with all analysis components as an analysis access
# point.
datalad create -c yoda analysis

mkdir analysis/code/jsons

cp $(basename ${recon_json})  analysis/code/jsons

cd analysis

# create dedicated input and output locations. Results will be pushed into the
# output sibling and the analysis will start with a clone from the input sibling.
datalad create-sibling-ria -s output "${output_store}" --new-store-ok
pushremote=$(git remote get-url --push output)
datalad create-sibling-ria -s input  --storage-sibling=off "${input_store}" --new-store-ok

# register the input dataset
echo "Cloning input dataset into analysis dataset"
datalad clone -d . ${BIDSINPUT} inputs/data
# amend the previous commit with a nicer commit message
git commit --amend -m 'Register input data dataset as a subdataset'

##ABCD updated
data_files=(inputs/data/sub-*)

for i in ${data_files[@]}
    do temp_subs+=( $( echo $i | cut -d '/' -f 3 | cut -d '_' -f 1 | sort) )
 done
SUBJECTS=($(printf "%s\n" "${temp_subs[@]}" | sort -u))
if [ -z "${SUBJECTS}" ]
then
    echo "No subjects found in input data"
    # exit 1
fi

# Clone the containers dataset. If specified on the command, use that path
CONTAINERDS=$2
datalad install -d . --source ${CONTAINERDS} containers

## the actual compute job specification
cat > code/participant_job.sh << "EOT"
#!/bin/bash

#SBATCH --partition=normal,jyeatman
#SBATCH --time=24:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=64G

# Set up the correct conda environment
echo I\'m in $PWD using `which python`

# fail whenever something is fishy, use -x to get verbose logfiles
set -e -u -x
datalad_start=$SECONDS
# Set up the remotes and get the subject id from the call
dssource="$1"
pushgitremote="$2"
recon_json="$3"
recon_spec=$(basename ${recon_json} .json)

subid="$(sed -n ${SLURM_ARRAY_TASK_ID}p code/session_list | cut -f 1 -d ' ')"
sesid="ses-baselineYear1Arm1"


# change into user SCRATCH space (change to L_SCRATCH for node-specific l-scratch)
cd ${SCRATCH}


# Used for the branch names and the temp dir
BRANCH="job-${SLURM_JOB_ID}-${subid}"
mkdir ${BRANCH}
cd ${BRANCH}

# get the analysis dataset, which includes the inputs as well
# importantly, we do not clone from the lcoation that we want to push the
# results to, in order to avoid too many jobs blocking access to
# the same location and creating a throughput bottleneck
datalad clone "${dssource}" ds

# all following actions are performed in the context of the superdataset
cd ds

# in order to avoid accumulation temporary git-annex availability information
# and to avoid a syncronization bottleneck by having to consolidate the
# git-annex branch across jobs, we will only push the main tracking branch
# back to the output store (plus the actual file content). Final availability
# information can be establish via an eventual `git-annex fsck -f joc-storage`.
# this remote is never fetched, it accumulates a larger number of branches
# and we want to avoid progressive slowdown. Instead we only ever push
# a unique branch per each job (subject AND process specific name)
git remote add outputstore "$pushgitremote"

# all results of this job will be put into a dedicated branch
git checkout -b "${BRANCH}"

# we pull down the input subject manually in order to discover relevant
# files. We do this outside the recorded call, because on a potential
# re-run we want to be able to do fine-grained recomputing of individual
# outputs. The recorded calls will have specific paths that will enable
# recomputation outside the scope of the original setup

datalad get -n "inputs/data/${subid}_qsiprep-v0.16.1.zip"
echo datalad got
# Remove all subjects we're not working on
(cd inputs/data && rm -rf `find . -type d -name 'sub*' | grep -v $subid`)
echo unnecessary subjects removed

timing_json="${SCRATCH}/datalad_processing/outputs/${recon_spec}-qsirecon-ABCD/timing/${BRANCH}_timing.json"

qsiprep_start=$SECONDS
echo "{'datalad_setup': $(( $qsiprep_start - $datalad_start))" >> $timing_json
# ------------------------------------------------------------------------------
datalad run \
    -i code/qsirecon_zip.sh \
    -i inputs/data/${subid}_qsiprep-v0.16.1.zip \
    -i containers/.datalad/environments/qsiprep-unstable/image \
    --expand inputs \
    --explicit \
    -o ${subid}_${sesid}_qsirecon-0.17.0.zip \
    -m "qsirecon:0.17.0 ${subid} ${sesid}" \
    "bash ./code/qsirecon_zip.sh ${subid} ${sesid} ${recon_json}"

qsiprep_end=$SECONDS

echo "'qsirecon': $(( $qsiprep_end - $qsiprep_start))" >> $timing_json
 
# file content first -- does not need a lock, no interaction with Git
datalad push --to output-storage
# and the output branch
flock $DSLOCKFILE git push outputstore

echo TMPDIR TO DELETE
echo ${BRANCH}

datalad drop -r --nocheck inputs/data
datalad drop -r . --nocheck
git annex dead here
cd ../..
rm -rf $BRANCH

datalad_end=$SECONDS
echo "'datalad_cleanup': $(( $datalad_end - $qsiprep_end))" >> $timing_json
echo "'total_time': $(( $datalad_end - $datalad_start))}" >> $timing_json


echo SUCCESS
# job handler should clean up workspace
EOT

chmod +x code/participant_job.sh

cat > code/qsirecon_zip.sh << "EOT"
#!/bin/bash

set -e -u -x

subid="$1"
sesid="$2"
recon_json="$3"

7z x "inputs/data/${subid}_qsiprep-v0.16.1.zip" -oderivatives
cp derivatives/qsiprep/dataset_description.json . 

sesfromfiles="$(ls -1 derivatives/qsiprep/${subid} | grep ses- | sed 's/\/$//')"
nsessions="$(ls -1 derivatives/qsiprep/${subid} | grep ses- | wc -l)"

if [[ "${sesfromfiles}" != "${sesid}" ]]; then
  printf '%s\n' "Session name in zip file does not equal ${sesid}" >&2  # write error message to stderr
  exit 1
fi

if (("${nsessions}" != "1")); then
  printf '%s\n' "Number of sessions is not equal to 1" >&2  # write error message to stderr
  exit 1
fi

mkdir -p ${PWD}/.git/tmp/wdir

singularity run --cleanenv -B ${PWD} \
    containers/.datalad/environments/qsiprep-unstable/image \
    --skip-bids-validation \
    derivatives/qsiprep  derivatives participant \
    --recon_input derivatives/qsiprep \
    --recon_spec code/${recon_json} \
    --fs-license-file /home/groups/jyeatman/software/freesurfer_license.txt \
    --skip-odf-reports \
    --output_resolution 1.7 \
    --recon-only 

#zip results
7z a ${subid}_${sesid}_qsirecon-0.17.0.zip derivatives

#clean up 
rm -rf derivatives
rm dataset_description.json

EOT

chmod +x code/qsirecon_zip.sh

mkdir logs
echo .slurm_datalad_lock >> .gitignore
echo logs >> .gitignore

datalad save -m "Participant compute job implementation"

cat > code/merge_outputs.sh << "EOT"
#!/bin/bash
set -e -u -x
EOT

# Add a script for merging outputs
MERGE_POSTSCRIPT=https://raw.githubusercontent.com/PennLINC/TheWay/main/scripts/cubic/merge_outputs_postscript.sh
echo "outputsource=${output_store}#$(datalad -f '{infos[dataset][id]}' wtf -S dataset)" \
    >> code/merge_outputs.sh
echo "cd ${PROJECTROOT}" >> code/merge_outputs.sh
wget -qO- ${MERGE_POSTSCRIPT} >> code/merge_outputs.sh

################################################################################
# SLURM SETUP START - remove or adjust to your needs
################################################################################
env_flags="--export=DSLOCKFILE=${PWD}/.slurm_datalad_lock"
echo '#!/bin/bash' > code/sbatch_calls.sh
dssource="${input_store}#$(datalad -f '{infos[dataset][id]}' wtf -S dataset)"
pushgitremote=$(git remote get-url --push output)
eo_args="-e ${PWD}/logs/%A_%a.e -o ${PWD}/logs/%A_%a.o"
touch code/session_list
for subject in ${SUBJECTS[@]}; do
  SESSIONS=$(find inputs/data/${subject}* | cut -d '_' -f 2 | cut -d '-' -f 2 )
  for session in ${SESSIONS}; do
    echo ${subject} ${session} >> code/session_list
  done
done

tot_jobs=$(cat code/session_list | wc -l)
job_limit=500
job_arrays=()
for i in $(seq 1 $job_limit $tot_jobs); 
    do job_arrays+=( $i )
done
job_arrays+=( $tot_jobs )
num_arrays=${#job_arrays[@]}
for index in $(seq 0 $(( $num_arrays - 2))); do     
    echo "sbatch ${env_flags} --array \
        ${job_arrays[$index]}-$(( ${job_arrays[$((index + 1))]} - 1 ))\
        --job-name batch${index} ${eo_args} \
        ${PWD}/code/participant_job.sh \
        ${dssource} ${pushgitremote} ${3}" >> code/sbatch_calls.sh; 
done 
datalad save -m "SLURM submission setup" code/ .gitignore

################################################################################
# SLURM SETUP END
################################################################################

# cleanup - we have generated the job definitions, we do not need to keep a
# massive input dataset around. Having it around wastes resources and makes many
# git operations needlessly slow

#if you're debugging inputs/data, this makes it hard to check what's being downloaded
#but it does *really* slow down your datalad clone operations
datalad uninstall -r --nocheck inputs/data


# make sure the fully configured output dataset is available from the designated
# store for initial cloning and pushing the results.
datalad push --to input
datalad push --to output

# Add an alias to the data in the RIA store
RIA_DIR=$(find $PROJECTROOT/output_ria/???/ -maxdepth 1 -type d | sort | tail -n 1)
mkdir -p ${PROJECTROOT}/output_ria/alias
ln -s ${RIA_DIR} ${PROJECTROOT}/output_ria/alias/data

# if we get here, we are happy
echo SUCCESS
