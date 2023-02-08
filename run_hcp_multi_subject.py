import os
import sys
import time
import resource
import argparse
from itertools import repeat
from multiprocessing import Pool

# +++++++++++++++++++++   DEFAULT PATH/SCRIPT VARIABLES   ++++++++++++++++++++++
TIME_DATE = time.strftime("%Y%m%d_%H%M%S")
PIPELINE_ROOT = r'/rfanfs/pnl-zorro/projects/ampscz_mri/new_version/objPipe'
MULTI_SUBJECT_ROOT = r'/rfanfs/pnl-zorro/projects/ampscz_mri/data/single_test2'
CONFIG_LOC = '/rfanfs/pnl-zorro/projects/ampscz_mri/new_version/test_config.ini'
BIDS_STUDY_ROOT = '/rfanfs/pnl-zorro/projects/ampscz_mri/data/test_bids'
NIFTI_PATH_FROM_SUBJECT_ROOT = 'unprocessed/Diffusion'
CUDA_COUNT = len(os.popen('nvidia-smi -L').readlines())
CUDA_DEVICE = 0
PROCESSES = os.cpu_count()
MAX_TASK_PER_CHILD = 1
OUTPUT_FILE_LOCATION = os.path.join(MULTI_SUBJECT_ROOT, 'output')
OUTPUT_FILE_NAME = 'run_hcp_multi_subject_' + TIME_DATE + '.out'
OUTPUT_ERROR_NAME = 'run_hcp_multi_subject_' + TIME_DATE + '.err'

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++ OVERWRITE THE DEFAULT VARIABLES IF PASSED AS ARGUMENTS +++++++++++
parser = argparse.ArgumentParser()
parser.add_argument('-pr', '--pipeline_root',
                    help='The location of the pipeline root',
                    default=PIPELINE_ROOT)
parser.add_argument('-mr', '--multi_subject_root',
                    help='The location of the multi subject root',
                    default=MULTI_SUBJECT_ROOT)
parser.add_argument('-cl', '--config_location',
                    help='The location of the config file',
                    default=CONFIG_LOC)
parser.add_argument('-br', '--bids_study_root',
                    help='The location of the bids study root',
                    default=BIDS_STUDY_ROOT)
parser.add_argument('-nr', '--nifti_path_from_subject_root',
                    help='The path from the subject root to the nifti file',
                    default=NIFTI_PATH_FROM_SUBJECT_ROOT)
parser.add_argument('-cc', '--cuda_devices',
                    help='The number of cuda devices. '
                         'Default is the number of cuda devices on the system.'
                         ' Only set if you want to not use all GPUs on the system',
                    default=CUDA_COUNT)
parser.add_argument('-cd', '--use_cuda_device',
                    help='The cuda device to use. '
                         'Default is 0. '
                         'Only set if you want to cycle through the GPUs on the system',
                    default=CUDA_DEVICE)
parser.add_argument('-p', '--processes',
                    help='The number of processes to run in parallel. '
                         'Default is the number of cuda devices on the system. '
                         'This maximum should be set based on the total available '
                         'memory divided by the total memory used by a process. ',
                    default=PROCESSES)
parser.add_argument('-t', '--max_tasks_per_child',
                    help='The number of tasks per child, default is 1. '
                         'Keep low to avoid memory issues',
                    default=MAX_TASK_PER_CHILD)
parser.add_argument('-o', '--output_file_location',
                    help='The location of the output file',
                    default=OUTPUT_FILE_LOCATION)
parser.add_argument('-f', '--output_file_name',
                    help='The name of the output file',
                    default=OUTPUT_FILE_NAME)
parser.add_argument('-e', '--output_error_name',
                    help='The name of the error file',
                    default=OUTPUT_ERROR_NAME)
parser.add_argument('-r', '--redirect_output',
                    help='Flag to redirect the output to a file',
                    default=False,
                    action='store_true')
args = parser.parse_args()
PIPELINE_ROOT = args.pipeline_root
MULTI_SUBJECT_ROOT = args.multi_subject_root
CONFIG_LOC = args.config_location
BIDS_STUDY_ROOT = args.bids_study_root
CUDA_COUNT = int(args.cuda_devices)
CUDA_DEVICE = int(args.use_cuda_device)
PROCESSES = int(args.processes)
MAX_TASK_PER_CHILD = int(args.max_tasks_per_child)
OUTPUT_FILE_LOCATION = args.output_file_location
OUTPUT_FILE_NAME = args.output_file_name
OUTPUT_ERROR_NAME = args.output_error_name
REDIRECT_OUTPUT = args.redirect_output

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++   print the variables    +++++++++++++++++++++++++++++
print('PIPELINE_ROOT: ', PIPELINE_ROOT)
print('MULTI_SUBJECT_ROOT: ', MULTI_SUBJECT_ROOT)
print('CONFIG_LOC: ', CONFIG_LOC)
print('BIDS_STUDY_ROOT: ', BIDS_STUDY_ROOT)
print('CUDA_DEVICE_COUNT: ', CUDA_COUNT)
print('CUDA_DEVICE: ', CUDA_DEVICE)
print('PROCESSES: ', PROCESSES)
print('MAX_TASK_PER_CHILD: ', MAX_TASK_PER_CHILD)
print('REDIRECT_OUTPUT: ', REDIRECT_OUTPUT)
if REDIRECT_OUTPUT:
    print('OUTPUT_FILE_LOCATION: ', OUTPUT_FILE_LOCATION)
    print('OUTPUT_FILE_NAME: ', OUTPUT_FILE_NAME)
    print('OUTPUT_ERROR_NAME: ', OUTPUT_ERROR_NAME)

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++ Redirecting the output if set to +++++++++++++++++++++++++
if REDIRECT_OUTPUT:
    print("redirecting output to files located at ", OUTPUT_FILE_LOCATION)
    if not os.path.exists(OUTPUT_FILE_LOCATION):
        os.makedirs(OUTPUT_FILE_LOCATION)
    sys.stdout = open(os.path.join(OUTPUT_FILE_LOCATION, OUTPUT_FILE_NAME), 'w')
    sys.stderr = open(os.path.join(OUTPUT_FILE_LOCATION, OUTPUT_ERROR_NAME), 'w')

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++ Removing any old pipeline environment variables ++++++++++++++++++
for i, item in enumerate(sys.path):
    if 'objPipe' in item:
        print('remove', item)
        sys.path.pop(i)
sys.path.append(PIPELINE_ROOT)
# import the objPipe module and HcpSubject class to now use in pipeline
import objPipe
from objPipe.subject import HcpSubject
from objPipe.utils.utils import re, pd, Path, print_df, shutil


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++   UTILITY FUNCTIONS   ++++++++++++++++++++++++++++++
def extract_subject_data():
    """ extracts the subject data from the multi subject root directory for use
    in the HcpSubject class instances

    Returns
    -------
    nifti locations : list
        the list of nifti locations
    subject names : list
        the list of subject names
    session names : list
        the list of session names
    """
    multi_subject_root = Path(MULTI_SUBJECT_ROOT)
    hcp_sub_nifti_location_list = []

    for sub in multi_subject_root.glob('*'):
        if sub.is_dir():
            nifti_file_location = sub / NIFTI_PATH_FROM_SUBJECT_ROOT
            if nifti_file_location.exists() and len(list(nifti_file_location.glob('*nii*'))) > 0:
                hcp_sub_nifti_location_list.append(nifti_file_location)

    # extract the subject name and session name from the hcp_sub_nifti_location
    subject_name_list = []
    session_name_list = []
    for hcp_sub_nifti_location in hcp_sub_nifti_location_list:
        subject_name_list.append(hcp_sub_nifti_location.parts[-3].split('_')[0])
        session_name_list.append(re.sub(r'^0*', '', re.sub(r'\D', '', hcp_sub_nifti_location.parts[-3].split('_')[1])))

    subject_data = list(zip(subject_name_list, session_name_list, hcp_sub_nifti_location_list))
    subject_data.sort(key=lambda x: x[0])

    print('{:^130}'.format('Running HCP Pipeline on the following subjects'))
    print_df(pd.DataFrame(subject_data, columns=['Subject', 'Session', 'Nifti Location']))

    time.sleep(5)

    return hcp_sub_nifti_location_list, subject_name_list, session_name_list


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def run_hcp_subject(hcp_sub_nifti_location,
                    subject_name,
                    session_name,
                    bids_study_root,
                    config_location,
                    cuda_device):
    """ run the hcp_subject for each subject in parallel based on cpu cores

    Parameters
    ----------
    hcp_sub_nifti_location : str
        the location of the hcp subject nifti files
    subject_name : str
        the name of the subject
    session_name : str
        the name of the session
    bids_study_root : str
        the location of the bids study root
    config_location : str
        the location of the config file
    cuda_device : int
        the cuda device to use

    Returns
    -------
    None

    """
    t0 = time.time()
    os.environ['CUDA_VISIBLE_DEVICES'] = str(CUDA_DEVICE % CUDA_COUNT)

    hcp_subject = HcpSubject(
        hcp_sub_nifti_location,
        session_name=session_name,
        subject_name=subject_name,
        bids_study_root=bids_study_root,
        config_loc=config_location)

    hcp_subject.set_bsub_attributes()  # loc: subject.py:64
    hcp_subject.check_bids_dirs()  # loc: bids.py:240
    force = False
    hcp_subject.write_series_desc_csv(force)  # loc: nifti.py:127
    diff_file_df = hcp_subject.get_diff_file_df()  # loc: nifti.py:172
    print_df(diff_file_df)
    hcp_subject.register_diff_raw_file(diff_file_df)  # loc: nifti.py:250
    hcp_subject.set_dwi_unring_attributes()  # loc: subject.py:102
    hcp_subject.set_dwi_eddy_attributes()  # loc: subject.py:128
    hcp_subject.set_dti_scalar_attributes()  # loc: subject.py:152
    hcp_subject.set_summary_attributes()  # loc: subject.py:164
    hcp_subject.check_files()  # loc: subject.py:73
    hcp_subject.run_gibbs_unring_auto(force)  # loc: noise.py:39q
    hcp_subject.save_b0_from_raw_dwis(force)  # loc: dwi.py:138
    hcp_subject.topup_preparation_for_rev_encod_dwi(force)

    if not hcp_subject.diff_mask.is_file():
        hcp_subject.run_otsu_masking(
            hcp_subject.topup_hifi,
            hcp_subject.diff_mask,
            vol_idx=[0, 1],
            force=force
        )

    hcp_subject.eddy_preparation_for_rev_encode_dwi(force)  # loc: eddy.py:145
    hcp_subject.merge_dwis_for_eddy(force)  # loc: eddy.py:188
    hcp_subject.get_slspec(force)  # loc: eddy.py:418
    print(' !!++  ******** running eddy_for_rev_encod_dwi **********  ++!! ')
    hcp_subject.eddy_for_rev_encod_dwi()  # loc: eddy.py:223

    # calculate end time
    t1 = time.time()
    print('time to run subjeect: ', (t1 - t0), ' seconds')


def run_hcp_multi_subject():
    """  run the multiple hcp subjects in parallel.
    """

    t0 = time.perf_counter()

    print("testing run_hcp_multi_subject")

    print(objPipe.__file__)

    # extract the nifti file location for each subject
    hcp_sub_nifti_location_list, subject_name_list, session_name_list = extract_subject_data()

    # check that the number of subjects is equal to the number of sessions and nifti locations
    assert len(hcp_sub_nifti_location_list) == len(subject_name_list) == len(session_name_list)

    with Pool(processes=PROCESSES, maxtasksperchild=MAX_TASK_PER_CHILD) as pool:
        pool.starmap(run_hcp_subject,
                     zip(hcp_sub_nifti_location_list, subject_name_list, session_name_list, repeat(BIDS_STUDY_ROOT),
                         repeat(CONFIG_LOC), range(CUDA_DEVICE, CUDA_DEVICE + len(hcp_sub_nifti_location_list))))

    # close the pool and wait for the work to finish
    pool.close()

    # calculate end time
    time_elapsed = (time.perf_counter() - t0)
    memMb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0 / 1024.0
    print("%5.1f secs %5.1f MByte" % (time_elapsed, memMb))


if __name__ == '__main__':
    run_hcp_multi_subject()
