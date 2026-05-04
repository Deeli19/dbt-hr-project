from prefect import flow, task
import subprocess


@task
def dbt_seed():
    subprocess.run(
        ["dbt", "seed"],
        check=True
    )


@task
def dbt_snapshot():
    subprocess.run(
        ["dbt", "snapshot"],
        check=True
    )


@task
def dbt_build():
    subprocess.run(
        ["dbt", "build"],
        check=True
    )


@task
def dbt_docs():
    subprocess.run(
        ["dbt", "docs", "generate"],
        check=True
    )


@flow(name="hr-analytics-pipeline")
def hr_pipeline():

    dbt_seed()

    dbt_snapshot()

    dbt_build()

    dbt_docs()


if __name__ == "__main__":
    hr_pipeline()