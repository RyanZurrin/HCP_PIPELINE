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


if __name__ == '__main__':
    test_run_hcp_subject()
