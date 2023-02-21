import resource
import sys
import time

for i, item in enumerate(sys.path):
    if 'objPipe' in item:
        print('remove', item)
        sys.path.pop(i)
sys.path.append('/rfanfs/pnl-zorro/projects/ampscz_mri/new_version/objPipe')

import objPipe
from objPipe.subject import HcpSubject
from objPipe.utils.utils import re, pd, Path, print_df, shutil


def test_run_hcp_subject():
    t0 = time.perf_counter()
    print(objPipe.__file__)
    hcp_sub_nifti_location = r'/rfanfs/pnl-zorro/projects/ampscz_mri/data/single_test/HCA6002236_V1_MR/unprocessed/Diffusion'
    hcpSubject = HcpSubject(
        hcp_sub_nifti_location,
        session_name='1',
        subject_name='HCA6002236',
        bids_study_root='/rfanfs/pnl-zorro/projects/ampscz_mri/data2/test_bids',
        config_loc='/rfanfs/pnl-zorro/projects/ampscz_mri/new_version/test_config.ini')

    hcpSubject.run_pipeline()

    # hcpSubject.set_bsub_attributes()  # loc: subject.py:64
    #
    # # ryan
    # hcpSubject.check_bids_dirs()  # loc: bids.py:240
    # force = False
    # hcpSubject.write_series_desc_csv(force)  # loc: nifti.py:127
    # diff_file_df = hcpSubject.get_diff_file_df()  # loc: nifti.py:172
    # print_df(diff_file_df)
    # hcpSubject.register_diff_raw_file(diff_file_df)  # loc: nifti.py:250
    # hcpSubject.set_dwi_unring_attributes()  # loc: subject.py:102
    # hcpSubject.set_dwi_eddy_attributes()  # loc: subject.py:128
    # hcpSubject.set_dti_scalar_attributes()  # loc: subject.py:152
    # hcpSubject.set_summary_attributes()  # loc: subject.py:164
    # hcpSubject.check_files()  # loc: subject.py:73
    # hcpSubject.run_gibbs_unring_auto(force)  # loc: noise.py:39
    # hcpSubject.save_b0_from_raw_dwis(force)  # loc: dwi.py:138
    # hcpSubject.topup_preparation_for_rev_encod_dwi(force)  # loc: topup.py:302
    # if not hcpSubject.diff_mask.is_file():  # loc: subject.py:130
    #     hcpSubject.run_otsu_masking(  # loc: masking.py:69
    #         hcpSubject.topup_hifi,  # loc: subject.py:212
    #         hcpSubject.diff_mask,  # loc: subject.py:130
    #         vol_idx=[0, 1],
    #         force=force
    #     )
    # hcpSubject.eddy_preparation_for_rev_encode_dwi(force)  # loc: eddy.py:145
    # hcpSubject.merge_dwis_for_eddy(force)  # loc: eddy.py:188
    # hcpSubject.get_slspec(force)  # loc: eddy.py:418
    # print(' ++  ******** about to run eddy_for_rev_encod_dwi **********  ++ ')
    # hcpSubject.eddy_for_rev_encod_dwi()  # loc: eddy.py:223
    #
    # # TODO: need to re-create the final mask
    # hcpSubject.check_files()  # loc: subject.py:73
    #
    # # calculate end time and memory usage
    # time_elapsed = (time.perf_counter() - t0)
    # memMb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0 / 1024.0
    # print("%5.1f secs %5.1f MByte" % (time_elapsed, memMb))


if __name__ == '__main__':
    test_run_hcp_subject()
