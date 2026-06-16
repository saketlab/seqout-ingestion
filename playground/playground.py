import h5py


def _count_h5_cells(path):
    with h5py.File(path, "r") as f:
        n_cells = None

        def find_barcodes(name, obj):
            nonlocal n_cells
            if n_cells is not None:
                return
            if isinstance(obj, h5py.Dataset) and name.endswith("/barcodes"):
                n_cells = len(obj)

        f.visititems(find_barcodes)

        # if n_cells is None and "obs/_index" in f:
        #     n_cells = len(f["obs/_index"])

        # if n_cells is None and "X" in f:
        #     n_cells = f["X"].shape[0]

    return n_cells


print(_count_h5_cells("./dump/GSM2833286_Pentamer_WT_Rep1.h5"))
