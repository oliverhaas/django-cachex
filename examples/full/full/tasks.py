"""
Sample Celery tasks for the full example project.

These tasks demonstrate how Celery queue data appears in the django-cachex
admin. Tasks sleep briefly so they can be observed in the queue before
being processed by a worker.
"""

# T201: print statements are intentional for visibility

import time

from full.celery import app


@app.task
def add(x: int, y: int) -> int:
    """Simple addition -- finishes quickly."""
    return x + y


@app.task
def send_email(to: str, subject: str, body: str) -> dict:
    """Simulated email send -- takes a few seconds."""
    time.sleep(3)
    return {"to": to, "subject": subject, "body": body, "status": "sent"}


@app.task
def generate_report(report_type: str) -> dict:
    """Simulated report generation -- takes longer."""
    time.sleep(5)
    return {"report_type": report_type, "status": "complete", "pages": 42}
