import os
import sys
import time
import argparse
from itertools import repeat
from multiprocessing import Pool

# +++++++++++++++++++++++++   DEFAULT PATH VARIABLES   +++++++++++++++++++++++++
PIPELINE_ROOT = r'/rfanfs/pnl-zorro/projects/ampscz_mri/new_version/objPipe'
MULTI_SUBJECT_ROOT = r'/rfanfs/pnl-zorro/projects/ampscz_mri/data2/multi'
CONFIG_LOC = '/rfanfs/pnl-zorro/projects/ampscz_mri/new_version/test_config.ini'
BIDS_STUDY_ROOT = '/rfanfs/pnl-zorro/projects/ampscz_mri/data2/test_bids'
CUDA_DEVICE_COUNT = len(os.popen('nvidia-smi -L').readlines())
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# ++++++++++ OVERWRITE THE DEFAULT PATH VARIABLES IF PASSED AS ARGUMENTS +++++++
parser = argparse.ArgumentParser()
parser.add_argument('-p', '--pipeline_root', help='the location of the pipeline root', default=PIPELINE_ROOT)
parser.add_argument('-m', '--multi_subject_root', help='the location of the multi subject root', default=MULTI_SUBJECT_ROOT)
parser.add_argument('-c', '--config_location', help='the location of the config file', default=CONFIG_LOC)
parser.add_argument('-b', '--bids_study_root', help='the location of the bids study root', default=BIDS_STUDY_ROOT)
parser.add_argument('-d', '--cuda_devices', help='the number of cuda devices', default=CUDA_DEVICE_COUNT)
args = parser.parse_args()
PIPELINE_ROOT = args.pipeline_root
MULTI_SUBJECT_ROOT = args.multi_subject_root
CONFIG_LOC = args.config_location
BIDS_STUDY_ROOT = args.bids_study_root
CUDA_DEVICE_COUNT = int(args.cuda_devices)
# print all the variables
print('PIPELINE_ROOT: ', PIPELINE_ROOT)
print('MULTI_SUBJECT_ROOT: ', MULTI_SUBJECT_ROOT)
print('CONFIG_LOC: ', CONFIG_LOC)
print('BIDS_STUDY_ROOT: ', BIDS_STUDY_ROOT)
print('CUDA_DEVICE_COUNT: ', CUDA_DEVICE_COUNT)
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Removing any old pipeline environment variables that may have been set prior
for i, item in enumerate(sys.path):
    if 'objPipe' in item:
        print('remove', item)
        sys.path.pop(i)
sys.path.append(PIPELINE_ROOT)

# import the objPipe module and HcpSubject class
import objPipe
from objPipe.subject import HcpSubject
from objPipe.utils.utils import re, pd, Path, print_df, shutil


# +++++++++++++++++++++++++   UTILITY FUNCTIONS   ++++++++++++++++++++++++++++++
def extract_subject_data():
    """ extract the subject data from the bids study root

    Returns
    -------
    nifti locations : list
        the list of nifti locations
    subject names : list
        the list of subject names
    session names : list
        the list of session names
    """
    print("testing extract_nifti_file_location")
    multi_subject_root = Path(MULTI_SUBJECT_ROOT)
    hcp_sub_nifti_location_list = []
    for sub in multi_subject_root.glob('*'):
        if sub.is_dir():
            hcp_sub_nifti_location_list.append(sub / 'unprocessed' / 'Diffusion')

    # extract the subject name and session name from the hcp_sub_nifti_location
    subject_name_list = []
    session_name_list = []
    for hcp_sub_nifti_location in hcp_sub_nifti_location_list:
        subject_name_list.append(hcp_sub_nifti_location.parts[-3].split('_')[0])
        session_name_list.append(
            re.findall(r'\d+', str(hcp_sub_nifti_location.parts[-3].split('_')[0]))[0])
    return hcp_sub_nifti_location_list, subject_name_list, session_name_list
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


def run_hcp_subject(hcp_sub_nifti_location,
                    subject_name,
                    session_name,
                    bids_study_root,
                    config_location,
                    CUDA_DEVICE):
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
    CUDA_DEVICE : int
        the cuda device to use

    Returns
    -------
    None

    """
    t0 = time.time()


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
    hcp_subject.run_gibbs_unring_auto(force)  # loc: noise.py:39
    hcp_subject.save_b0_from_raw_dwis(force)  # loc: dwi.py:138
    hcp_subject.topup_preparation_for_rev_encod_dwi(force)

    os.environ['CUDA_VISIBLE_DEVICES'] = str(CUDA_DEVICE % CUDA_DEVICE_COUNT)

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
    print(' ++  ******** about to run eddy_for_rev_encod_dwi **********  ++ ')
    hcp_subject.eddy_for_rev_encod_dwi()  # loc: eddy.py:223
    # calculate end time
    t1 = time.time()
    print('time to run subject: ', (t1 - t0), ' seconds')


def run_hcp_multi_subject():
    """  run the multiple hcp subjects in parallel.
    """
    t0 = time.time()
    print("testing run_hcp_multi_subject")
    CUDA_DEVICE = 0

    print(objPipe.__file__)

    # extract the nifti file location for each subject
    hcp_sub_nifti_location_list, subject_name_list, session_name_list = extract_subject_data()

    with Pool(processes=len(subject_name_list)) as pool:
        pool.starmap(run_hcp_subject,
                     zip(hcp_sub_nifti_location_list, subject_name_list, session_name_list, repeat(BIDS_STUDY_ROOT),
                         repeat(CONFIG_LOC), range(CUDA_DEVICE, CUDA_DEVICE + len(hcp_sub_nifti_location_list))))

    # calculate end time
    t1 = time.time()
    print('time to run all subjects: ', (t1 - t0), ' seconds')


if __name__ == '__main__':
    run_hcp_multi_subject()
