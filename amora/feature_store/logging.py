from amora.feature_store import settings


def patch_tqdm() -> None:
    """
    Monkey patch tqdm logging to use only ASCII characters, instead of the default UNICODE format
    """
    from feast import feature_store

    def patched_tqdm(*args, **kwargs):
        from tqdm import tqdm

        return tqdm(
            *args,
            **kwargs,
            # If true, use ASCII characters ' 123456789#' instead of unicode (smooth blocks) to fill the meter.
            ascii=settings.TQDM_ASCII_LOGGING,
            # Whether to disable the entire progressbar wrapper. If set to None, disable on non-TTY.
            disable=settings.TQDM_DISABLE,
        )

    feature_store.tqdm = patched_tqdm
