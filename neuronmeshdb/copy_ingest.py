import cloudvolume


def ingest_from_precomputed(cv_path, table_id):
    cv = cloudvolume.CloudVolume(cv_path)

    mesh_dir = f"{cv.cloudpath}/{cv.info['mesh']}"

    cv_stor = cloudvolume.Storage(mesh_dir)