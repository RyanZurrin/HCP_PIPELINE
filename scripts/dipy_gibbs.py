from dipy.denoise.gibbs import gibbs_removal


def gibbs_removal_wrapper(dwi, mask, num_threads=1):
    """Wrapper for dipy.denoise.gibbs.gibbs_removal.

    Parameters
    ----------
    dwi : 4D array
        DWI data.
    mask : 3D array
        Mask of the DWI data.
    num_threads : int, optional
        Number of threads to use. Default: 1.

    Returns
    -------
    gibbs_corrected : 4D array
        Gibbs corrected DWI data.
    """
    gibbs_corrected = gibbs_removal(dwi, mask, num_threads=num_threads)
    return gibbs_corrected


