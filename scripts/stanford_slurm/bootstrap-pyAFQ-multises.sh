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


## Set up the directory that will contain the necessary directories
PROJECTROOT=${PWD}/pyAFQ-multises
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
cd ${PROJECTROOT}

# Jobs are set up to not require a shared filesystem (except for the lockfile)
# ------------------------------------------------------------------------------
# RIA-URL to a different RIA store from which the dataset will be cloned from.
# Both RIA stores will be created
input_store="ria+file://${PROJECTROOT}/input_ria"
output_store="ria+file://${PROJECTROOT}/output_ria"

# Create a source dataset with all analysis components as an analysis access
# point.
datalad create -c yoda analysis
cd analysis

# create dedicated input and output locations. Results will be pushed into the
# output sibling and the analysis will start with a clone from the input sibling.
datalad create-sibling-ria -s output "${output_store}"
pushremote=$(git remote get-url --push output)
datalad create-sibling-ria -s input  --storage-sibling off "${input_store}"

# register the input dataset
echo "Cloning input dataset into analysis dataset"
datalad clone -d . ${BIDSINPUT} inputs/data
# amend the previous commit with a nicer commit message
git commit --amend -m 'Register input data dataset as a subdataset'

##NKI updated
SUBJECTS=$(find inputs/data -name 'sub-*' | cut -d '/' -f 3 | cut -d '_' -f 1 | sort)
if [ -z "${SUBJECTS}" ]
then
    echo "No subjects found in input data"
    # exit 1
fi

echo $SUBJECTS

# Clone the containers dataset. If specified on the command, use that path
CONTAINERDS=$2
datalad install -d . --source ${CONTAINERDS} containers

## the actual compute job specification
cat > code/participant_job.sh << "EOT"
#!/bin/bash

#SBATCH --partition=normal,jyeatman
#SBATCH --time=24:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=6

# Set up the correct conda environment
echo I\'m in $PWD using `which python`

# fail whenever something is fishy, use -x to get verbose logfiles
set -e -u -x

# Set up the remotes and get the subject id from the call
dssource="$1"
pushgitremote="$2"
subid="$3"
sesid="$4"

# change into the cluster-assigned temp directory.
cd ${L_SCRATCH}


# Used for the branch names and the temp dir
BRANCH="job-${SLURM_JOB_ID}-${subid}-${sesid}"
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

datalad get -n "inputs/data/${subid}_ses-${sesid}_qsiprep-0.14.2.zip"

# Reomve all subjects we're not working on
(cd inputs/data && rm -rf `find . -type d -name 'sub*' | grep -v $subid`)

# Unzip the subject subdir
# TODO: change to unip into derivates 
7z x ${subid}_ses-${sesid}_qsiprep-0.14.2.zip -oderivatives

# ------------------------------------------------------------------------------
# Do the run!

datalad run \
    -i code/pyafq_zip.sh \
    -i inputs/data/${subid}/${sesid} \
    -i "inputs/data/*json" \
    -i containers/images/bids/bids-pyafq--0.12.sing \
    --expand inputs \
    --explicit \
    -o ${subid}_${sesid}_pyafq-0.12.zip \
    -m "pyafq:0.12 ${subid} ${sesid}" \
    "bash ./code/pyafq_zip.sh ${subid} ${sesid}"

# file content first -- does not need a lock, no interaction with Git
datalad push --to output-storage
# and the output branch
flock $DSLOCKFILE git push outputstore

echo TMPDIR TO DELETE
echo ${BRANCH}

datalad uninstall -r --nocheck --if-dirty ignore inputs/data
datalad drop -r . --nocheck
git annex dead here
cd ../..
rm -rf $BRANCH

echo SUCCESS
# job handler should clean up workspace
EOT

chmod +x code/participant_job.sh

cat > code/pyafq_zip.sh << "EOT"
#!/bin/bash

set -e -u -x

subid="$1"
sesid="$2"

# Create a pyAFQ TOML configuration file
tomlfile=${PWD}/${sesid}_pyafq_config.toml
touch ${tomlfile}

echo "bids_path = \".\"" >> ${tomlfile}
echo "preproc_pipeline" = \"qsiprep\"" >> ${tomlfile}
echo "bids_filters" = \"{'desc': 'preproc'}\"" >> ${tomlfile}

echo "mapping_definition = \"ItkMap(warp_suffix='xfm', warp_filters={'from': 'MNI152NLin2009cAsym', 'to': 'T1w', 'scope': 'qsiprep'})\"" >> ${tomlfile}
echo "brain_mask_definition = \"ImageFile(suffix='mask', filters={'desc': 'brain', 'space': 'T1w', 'scope': 'qsiprep'})\"" >> ${tomlfile}
echo "scalars = \"['dki_fa', 'dki_md', 'dki_awf', 'dki_mk']\"" >> ${tomlfile}
echo "bundle_info = \"['ATR_L', 'ATR_R', 'CGC_L', 'CGC_R', 'CST_L', 'CST_R', 'IFO_L', 'IFO_R', 'ILF_L', 'ILF_R', 'SLF_L', 'SLF_R', 'ARC_L', 'ARC_R', 'UNC_L', 'UNC_R', 'AntFrontal', 'Motor', 'Occipital', 'Orbital', 'PostParietal', 'SupFrontal', 'SupParietal', 'Temporal']\"" >> ${tomlfile}
echo "seed_mask = \"ScalarImage('dki_fa')\"" >> ${tomlfile}
echo "stop_mask = \"ScalarImage('dki_fa')\"" >> ${tomlfile}
echo "odf_model = \"CSD\"" >> ${tomlfile}
echo "directions = \"prob\"" >> ${tomlfile}


echo "parallel_segmentation = \"{'n_jobs': ${SLURM_NPROCS}, 'engine': 'joblib', 'backend': 'loky'}\"" >> ${tomlfile}

mkdir -p ${PWD}/.git/tmp/wdir

singularity run --cleanenv -B ${PWD} \
    containers/images/bids/bids-pyafq--0.12.sif \
    ${tomlfile}
   
cd prep
#zip results

7z a ../${subid}_ses-${sesid}_pyafq-0.12.zip afq
rm -rf prep .git/tmp/wdir
rm ${tomlfile}

EOT

chmod +x code/pyafq_zip.sh

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
eo_args="-e ${PWD}/logs/%j.e -o ${PWD}/logs/%j.o"
for subject in ${SUBJECTS}; do
  SESSIONS=$(find inputs/data/${subject}* | cut -d '_' -f 2 | cut -d '-' -f 2 )
  for session in ${SESSIONS}; do
    echo "sbatch ${env_flags} --job-name  qp${subject}_${session} ${eo_args} \
    ${PWD}/code/participant_job.sh \
    ${dssource} ${pushgitremote} ${subject} ${session}" >> code/sbatch_calls.sh
  done
done
datalad save -m "SLURM submission setup" code/ .gitignore

################################################################################
# SLURM SETUP END
################################################################################

# cleanup - we have generated the job definitions, we do not need to keep a
# massive input dataset around. Having it around wastes resources and makes many
# git operations needlessly slow

#if you're debugging inputs/data, this is problematic
#datalad uninstall -r --nocheck inputs/data


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
