try:
    from prefect import serve
except ImportError:
    # Fallback for older versions or if serve is not available
    from prefect.deployments import serve

from .flow import ingest_cycle
from anomaly_detection.flow import anomaly_detection_cycle

if __name__ == "__main__":
    # In Prefect 3.x, we use the serve function to serve multiple deployments
    # Using interval-based scheduling for reliable every-5-minute execution

    # Concurrency Strategy:
    # - Each deployment is limited to 1 concurrent job at a time
    # - Task-level concurrency tags ensure only ONE step runs per script at a time
    import pendulum

    serve(
        ingest_cycle.to_deployment(
            name="DPS-ingest_cycle-deployment",
            interval=pendulum.duration(minutes=5),  # Every 5 minutes
            concurrency_limit=1,
        ),
        anomaly_detection_cycle.to_deployment(
            name="DPS-anomaly_detection_cycle-deployment",
            interval=pendulum.duration(minutes=15),
            concurrency_limit=1,
        ),
    )
