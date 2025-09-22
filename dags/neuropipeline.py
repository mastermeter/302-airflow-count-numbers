from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator


WORKDIR = Path("/tmp/neuro_workdir")
DOCKER_ARGS = {
    "image": "oesteban/afni:latest",
    "auto_remove": "force",
    "docker_url": "unix://var/run/docker.sock",
    #"user": "1000:1001"
}

DATASET_REPO_URL = "https://github.com/OpenNeuroDatasets/ds000005"
DATASET_DIR = WORKDIR / "data" / "ds000005"
OUTPUT_DIR = WORKDIR / "outputs"
RAW_PATH = DATASET_DIR / "sub-01" / "anat" / "sub-01_T1w.nii.gz"
STRIP_PATH = OUTPUT_DIR / "sub-01_desc-brain_T1w.nii.gz"
SEGMENTS_DIR = OUTPUT_DIR / "segments"
NIFTI_PATH = OUTPUT_DIR / "sub-01_seg-tissues_dseg.nii.gz"
AFNI_PATH = SEGMENTS_DIR / "Classes+orig"


with DAG(dag_id="neuro-pipeline", schedule_interval=None) as dag:
    BashOperator(
        task_id="checks",
        bash_command="docker container ls",
        run_as_user="default"
    )
    cleanup_workdir = BashOperator(
        task_id="cleanup_workdir",
        bash_command=f"rm -rf {WORKDIR}"
    )
    ensure_dataset = DockerOperator(
        task_id="ensure_dataset",
        **DOCKER_ARGS,
        command=f"datalad clone {DATASET_REPO_URL} {DATASET_DIR} && datalad get -d {DATASET_DIR} {RAW_PATH}"
    )
    skullstrip = DockerOperator(
        task_id="skullstrip",
        **DOCKER_ARGS,
        command=f"3dSkullStrip -input {RAW_PATH} -prefix {STRIP_PATH}"
    )
    segment = DockerOperator(
        task_id="segment",
        **DOCKER_ARGS,
        command=f"3dSeg -anat {STRIP_PATH} -mask AUTO -prefix {SEGMENTS_DIR}"
    )
    convert = DockerOperator(
        task_id="convert",
        **DOCKER_ARGS,
        command=f"3dAFNItoNIFTI -prefix {NIFTI_PATH} {AFNI_PATH}"
    )

    cleanup_workdir >> ensure_dataset >> skullstrip >> segment >> convert
