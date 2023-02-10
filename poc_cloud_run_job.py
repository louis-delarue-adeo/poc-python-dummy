# https://github.com/googleapis/python-run/tree/main/samples/generated_samples
from google.cloud import run_v2 as cloud_run

PARENT_VALUE = "projects/dfdp-dev-20102657-deployment/locations/europe-west1"


class CloudRunJobManager:
    def __init__(self, project: str, location: str):
        self.services_client = cloud_run.ServicesClient()
        self.jobs_client = cloud_run.JobsClient()
        self.parent = f"projects/{project}/locations/{location}"

    def list_services(self):
        client = self.services_client

        # Initialize request argument(s)
        request = cloud_run.ListServicesRequest(
            parent=PARENT_VALUE,
        )

        # Make the request
        page_result = client.list_services(request=request)

        # Handle the response
        for response in page_result:
            print(response)

    def list_jobs(self):
        client = self.jobs_client

        # Initialize request argument(s)
        request = cloud_run.ListJobsRequest(
            parent=PARENT_VALUE,
        )

        # Make the request
        page_result = client.list_jobs(request=request)

        # Handle the response
        for response in page_result:
            print(response)

    def run_job(self, job_name: str):
        client = self.jobs_client

        # Initialize request argument(s)
        request = cloud_run.RunJobRequest(
            name=PARENT_VALUE + f"/jobs/{job_name}",
        )

        # Make the request
        operation = client.run_job(request=request)

        print("Waiting for operation to complete...")

        response = operation.result()

        # Handle the response
        print(response)

    def create_job(self, job_name: str, container):
        client = self.jobs_client

        # Initialize request argument(s)
        job = cloud_run.Job()
        job.template.template.max_retries = 1
        job.template.template.containers = [container]
        job.launch_stage = "BETA"

        request = cloud_run.CreateJobRequest(
            parent=PARENT_VALUE,
            job=job,
            job_id=job_name,
        )

        # Make the request
        operation = client.create_job(request=request)

        print("Waiting for operation to complete...")

        response = operation.result()

        # Handle the response
        print(response)


# Idée
# Modifier le job conf en fonction du tag du container si le process existe déjà
# Sinon, créer un nouveau job conf

if __name__ == "__main__":
    job_name = "test-job-3"
    cjl = CloudRunJobManager("dfdp-dev-20102657-deployment", "europe-west1")
    cjl.create_job(
        job_name,
        container=cloud_run.Container(
            name="job",
            image="us-docker.pkg.dev/cloudrun/container/job:latest",
        ),
    )
    cjl.run_job(job_name)
