"""
Pipeline Notification Utilities

This module provides notification callbacks for Airflow DAGs to send status updates
via email and/or Slack webhooks. Notifications are sent on DAG/task success and failure.

Configuration is via environment variables:
- NOTIFICATION_ENABLED: Enable notifications (true/false)
- NOTIFICATION_CHANNELS: Comma-separated list of channels (email,slack)
- SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD: Email settings
- NOTIFICATION_EMAIL_TO: Recipient email address(es), comma-separated
- NOTIFICATION_EMAIL_FROM: Sender email address
- SLACK_WEBHOOK_URL: Slack incoming webhook URL
"""

import os
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any, Callable
from datetime import datetime, timezone
import urllib.request
import urllib.error
import functools
import traceback

logger = logging.getLogger(__name__)

# Global storage for exception info (used by on_failure_callback)
# This is a workaround for Airflow 3.0 where exceptions are not passed to callbacks
_last_exception_info: Dict[str, str] = {}


def store_exception(task_id: str, exception: Exception) -> None:
    """Store exception info for later retrieval by on_failure_callback."""
    global _last_exception_info
    exc_type = type(exception).__name__
    exc_value = str(exception)
    _last_exception_info[task_id] = f"{exc_type}: {exc_value}"
    logger.debug(f"Stored exception for task {task_id}: {_last_exception_info[task_id]}")


def get_stored_exception(task_id: str) -> Optional[str]:
    """Retrieve stored exception info for a task."""
    return _last_exception_info.get(task_id)


def clear_stored_exception(task_id: str) -> None:
    """Clear stored exception info for a task."""
    global _last_exception_info
    if task_id in _last_exception_info:
        del _last_exception_info[task_id]


def capture_exceptions(func: Callable) -> Callable:
    """
    Decorator to capture exceptions and store them for on_failure_callback.

    Use this decorator on task functions to ensure exceptions are available
    in failure notifications. Airflow 3.0 does not pass exceptions to callbacks.

    Usage:
        @task
        @capture_exceptions
        def my_task(**context):
            # task code
            pass
    """
    @functools.wraps(func)
    def wrapper(*args, **context):
        task_id = 'unknown'
        try:
            # Try to get task_id from context
            ti = context.get('ti') or context.get('task_instance')
            if ti:
                task_id = getattr(ti, 'task_id', 'unknown')
        except:
            pass

        try:
            return func(*args, **context)
        except Exception as e:
            # Store the exception before re-raising
            store_exception(task_id, e)
            raise

    return wrapper


def is_notifications_enabled() -> bool:
    """Check if notifications are enabled."""
    return os.environ.get('NOTIFICATION_ENABLED', 'false').lower() == 'true'


def get_notification_channels() -> List[str]:
    """Get list of enabled notification channels."""
    channels = os.environ.get('NOTIFICATION_CHANNELS', '')
    return [c.strip().lower() for c in channels.split(',') if c.strip()]


def send_slack_notification(
    message: str,
    title: str,
    color: str = '#36a64f',  # green for success
    fields: Optional[List[Dict[str, str]]] = None
) -> bool:
    """
    Send a notification to Slack via webhook.

    Args:
        message: Main message text
        title: Attachment title
        color: Sidebar color (hex)
        fields: Optional list of field dicts with 'title' and 'value' keys

    Returns:
        True if successful, False otherwise
    """
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL', '')

    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not configured, skipping Slack notification")
        return False

    attachment = {
        'color': color,
        'title': title,
        'text': message,
        'ts': int(datetime.now().timestamp()),
    }

    if fields:
        attachment['fields'] = [
            {'title': f['title'], 'value': f['value'], 'short': f.get('short', True)}
            for f in fields
        ]

    payload = {
        'attachments': [attachment]
    }

    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                logger.info("Slack notification sent successfully")
                return True
            else:
                logger.warning(f"Slack notification failed with status {response.status}")
                return False
    except urllib.error.URLError as e:
        logger.error(f"Failed to send Slack notification: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending Slack notification: {e}")
        return False


def send_email_notification(
    subject: str,
    body: str,
    html_body: Optional[str] = None
) -> bool:
    """
    Send an email notification via SMTP.

    Args:
        subject: Email subject
        body: Plain text body
        html_body: Optional HTML body

    Returns:
        True if successful, False otherwise
    """
    smtp_host = os.environ.get('SMTP_HOST', '')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_password = os.environ.get('SMTP_PASSWORD', '')
    email_from = os.environ.get('NOTIFICATION_EMAIL_FROM', smtp_user)
    email_to = os.environ.get('NOTIFICATION_EMAIL_TO', '')

    if not all([smtp_host, email_to]):
        logger.warning("Email notification not configured (missing SMTP_HOST or NOTIFICATION_EMAIL_TO)")
        return False

    recipients = [e.strip() for e in email_to.split(',') if e.strip()]

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = email_from
        msg['To'] = ', '.join(recipients)

        msg.attach(MIMEText(body, 'plain'))
        if html_body:
            msg.attach(MIMEText(html_body, 'html'))

        # Determine if we should use TLS
        use_tls = smtp_port in (587, 465)

        if smtp_port == 465:
            # SSL from the start
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=10) as server:
                if smtp_user and smtp_password:
                    server.login(smtp_user, smtp_password)
                server.sendmail(email_from, recipients, msg.as_string())
        else:
            # Standard SMTP with optional STARTTLS
            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                if use_tls:
                    server.starttls()
                if smtp_user and smtp_password:
                    server.login(smtp_user, smtp_password)
                server.sendmail(email_from, recipients, msg.as_string())

        logger.info(f"Email notification sent to {recipients}")
        return True

    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email notification: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending email notification: {e}")
        return False


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def format_number(num: int) -> str:
    """Format number with commas."""
    return f"{num:,}"


def get_migration_stats(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract migration statistics from XCom values (legacy, uses context).
    """
    dag_run = context.get('dag_run')
    return get_migration_stats_from_dag_run(dag_run)


def get_migration_stats_from_dag_run(dag_run) -> Dict[str, Any]:
    """
    Extract migration statistics from XCom values using a DagRun object.

    Returns dict with tables_migrated, total_rows, validation_status, etc.
    """
    stats = {
        'tables_migrated': 0,
        'total_rows': 0,
        'tables_list': [],
        'validation_passed': None,
        'rows_per_second': 0,
    }

    try:
        if not dag_run:
            logger.warning("No dag_run provided")
            return stats

        logger.info(f"Getting migration stats from DagRun: {dag_run}")

        def _get_ti(dg, task_id: str):
            ti_obj = None
            try:
                ti_obj = dg.get_task_instance(task_id)
            except Exception as ex:
                logger.warning(f"Could not get task instance {task_id} from provided DagRun: {ex}")
            if ti_obj:
                return ti_obj
            # Fallback: reload DagRun from DB (Airflow callbacks sometimes pass a lightweight object)
            reloaded = get_dag_run_from_db(dg.dag_id, dg.run_id)
            if reloaded:
                try:
                    return reloaded.get_task_instance(task_id)
                except Exception as ex:
                    logger.warning(f"Fallback DagRun lookup failed for {task_id}: {ex}")
            return None

        # Pull extracted schema stats
        ti_extract = _get_ti(dag_run, 'extract_source_schema')
        if ti_extract:
            logger.info(f"Found task instance: {ti_extract}")
            extracted_tables = ti_extract.xcom_pull(key='extracted_tables')
            logger.info(f"extracted_tables XCom: {extracted_tables}")
            if extracted_tables:
                stats['tables_list'] = extracted_tables
                stats['tables_migrated'] = len(extracted_tables)

            total_rows = ti_extract.xcom_pull(key='total_row_count')
            logger.info(f"total_row_count XCom: {total_rows}")
            if total_rows:
                stats['total_rows'] = total_rows
        else:
            logger.warning("Could not get task instance for extract_source_schema")

        # Fallback: aggregate from collect_all_results return value if schema XComs are missing
        if stats['tables_migrated'] == 0 or stats['total_rows'] == 0:
            ti_collect = _get_ti(dag_run, 'collect_all_results')
            if ti_collect:
                results = ti_collect.xcom_pull(task_ids='collect_all_results', key='return_value')
                logger.info(f"collect_all_results XCom: {results}")
                if results and isinstance(results, list):
                    stats['tables_migrated'] = len(results)
                    stats['tables_list'] = [r.get('table_name', 'unknown') for r in results]
                    stats['total_rows'] = sum(r.get('rows_transferred', 0) for r in results)

        # Calculate throughput if we have duration and rows
        if dag_run.start_date and dag_run.end_date and stats['total_rows']:
            duration = (dag_run.end_date - dag_run.start_date).total_seconds()
            if duration > 0:
                stats['rows_per_second'] = int(stats['total_rows'] / duration)

        logger.info(f"Migration stats: {stats}")

    except Exception as e:
        logger.warning(f"Could not retrieve migration stats: {e}")
        import traceback
        logger.warning(traceback.format_exc())

    return stats


def get_dag_run_from_db(dag_id: str, run_id: str):
    """
    Fetch full DagRun object from database.
    In Airflow 3.0, DAG callbacks receive a lightweight DTO without DB access.
    """
    try:
        from airflow.models import DagRun
        dag_runs = DagRun.find(dag_id=dag_id, run_id=run_id)
        if dag_runs:
            return dag_runs[0]
    except Exception as e:
        logger.warning(f"Could not fetch DagRun from DB: {e}")
    return None


def format_dag_context(context: Dict[str, Any]) -> Dict[str, str]:
    """Extract useful information from Airflow context for notifications."""
    # Debug: log available context keys
    logger.info(f"Notification callback context keys: {list(context.keys())}")

    dag_run_dto = context.get('dag_run')

    # Debug: log dag_run details
    if dag_run_dto:
        logger.info(f"dag_run type: {type(dag_run_dto)}, dag_id: {getattr(dag_run_dto, 'dag_id', 'N/A')}")
    else:
        logger.warning("dag_run not found in context")

    # Get DAG ID and run_id from the DTO
    dag_id = getattr(dag_run_dto, 'dag_id', None) if dag_run_dto else None
    run_id = getattr(dag_run_dto, 'run_id', None) if dag_run_dto else None

    # Fetch full DagRun from database for complete information
    dag_run = None
    if dag_id and run_id:
        dag_run = get_dag_run_from_db(dag_id, run_id)
        logger.info(f"Fetched DagRun from DB: {dag_run}")

    # Fall back to DTO if DB fetch failed
    if not dag_run:
        dag_run = dag_run_dto

    # Get DAG ID
    if not dag_id:
        if 'dag' in context:
            dag = context['dag']
            dag_id = getattr(dag, 'dag_id', 'Unknown')
        else:
            dag_id = 'Unknown'

    # Get run_id
    if not run_id:
        run_id = 'Unknown'

    # Get start_date - handle timezone-aware datetime
    start_date = 'Unknown'
    if dag_run and dag_run.start_date:
        try:
            start_date = dag_run.start_date.strftime('%Y-%m-%d %H:%M:%S UTC')
        except:
            start_date = str(dag_run.start_date)

    # Get end_date
    end_date = None
    if dag_run and dag_run.end_date:
        try:
            end_date = dag_run.end_date.strftime('%Y-%m-%d %H:%M:%S UTC')
        except:
            end_date = str(dag_run.end_date)

    # Calculate duration
    duration = ''
    if dag_run and dag_run.start_date and dag_run.end_date:
        try:
            delta = dag_run.end_date - dag_run.start_date
            duration = format_duration(delta.total_seconds())
        except:
            pass

    info = {
        'dag_id': dag_id,
        'run_id': run_id,
        'start_date': start_date,
        'end_date': end_date or 'N/A',
        'duration': duration or 'N/A',
        '_dag_run': dag_run,  # Store for use in get_migration_stats
    }

    logger.info(f"Extracted context info: dag_id={dag_id}, run_id={run_id}, start={start_date}, end={end_date}, duration={duration}")

    # Add task info if available
    task_instance = context.get('task_instance') or context.get('ti')
    if task_instance:
        info['task_id'] = getattr(task_instance, 'task_id', 'Unknown')
        info['state'] = str(getattr(task_instance, 'state', 'Unknown'))

    return info


def on_dag_success(context: Dict[str, Any]) -> None:
    """
    Callback for DAG success. Sends notifications to configured channels.

    NOTE: In Airflow 3.0, DAG-level callbacks may not be executed reliably.
    Use send_success_notification() from a final task instead.

    Usage in DAG:
        @dag(on_success_callback=on_dag_success, ...)
    """
    if not is_notifications_enabled():
        logger.debug("Notifications disabled, skipping success callback")
        return

    channels = get_notification_channels()
    if not channels:
        logger.debug("No notification channels configured")
        return

    info = format_dag_context(context)

    # Pass the full dag_run to get_migration_stats
    dag_run = info.pop('_dag_run', None)
    stats = get_migration_stats_from_dag_run(dag_run)

    title = f"✅ DAG Success: {info['dag_id']}"

    # Build summary message
    summary_parts = ["Migration pipeline completed successfully."]
    if stats['tables_migrated'] > 0:
        summary_parts.append(f"Migrated {stats['tables_migrated']} tables with {format_number(stats['total_rows'])} total rows.")
    if stats['rows_per_second'] > 0:
        summary_parts.append(f"Throughput: {format_number(stats['rows_per_second'])} rows/sec.")

    message = " ".join(summary_parts)

    # Build fields for Slack
    fields = [
        {'title': 'DAG', 'value': info['dag_id']},
        {'title': 'Run ID', 'value': info['run_id']},
        {'title': 'Started', 'value': info['start_date']},
        {'title': 'Duration', 'value': info['duration']},
    ]

    if stats['tables_migrated'] > 0:
        fields.append({'title': 'Tables', 'value': str(stats['tables_migrated'])})
        fields.append({'title': 'Total Rows', 'value': format_number(stats['total_rows'])})

    if stats['rows_per_second'] > 0:
        fields.append({'title': 'Throughput', 'value': f"{format_number(stats['rows_per_second'])} rows/sec"})

    if 'slack' in channels:
        send_slack_notification(
            message=message,
            title=title,
            color='#36a64f',  # green
            fields=fields
        )

    if 'email' in channels:
        subject = title

        # Build tables list for email
        tables_section = ""
        if stats['tables_list']:
            tables_section = "\nTables Migrated:\n" + "\n".join(f"  • {t}" for t in stats['tables_list'])

        body = f"""{title}

{message}

Run Details:
  • DAG: {info['dag_id']}
  • Run ID: {info['run_id']}
  • Started: {info['start_date']}
  • Completed: {info['end_date']}
  • Duration: {info['duration']}

Migration Statistics:
  • Tables Migrated: {stats['tables_migrated']}
  • Total Rows: {format_number(stats['total_rows'])}
  • Throughput: {format_number(stats['rows_per_second'])} rows/sec
{tables_section}

This is an automated notification from the MSSQL to PostgreSQL Migration Pipeline.
"""

        # Build HTML email
        tables_html = ""
        if stats['tables_list']:
            tables_html = "<h3>Tables Migrated</h3><ul>" + "".join(f"<li>{t}</li>" for t in stats['tables_list']) + "</ul>"

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
<h2 style="color: #28a745; border-bottom: 2px solid #28a745; padding-bottom: 10px;">{title}</h2>
<p style="font-size: 16px;">{message}</p>

<h3>Run Details</h3>
<table style="border-collapse: collapse; width: 100%;">
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold; width: 150px;">DAG:</td><td style="padding: 8px;">{info['dag_id']}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Run ID:</td><td style="padding: 8px; font-family: monospace; font-size: 12px;">{info['run_id']}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Started:</td><td style="padding: 8px;">{info['start_date']}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Completed:</td><td style="padding: 8px;">{info['end_date']}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Duration:</td><td style="padding: 8px; font-weight: bold; color: #28a745;">{info['duration']}</td></tr>
</table>

<h3>Migration Statistics</h3>
<table style="border-collapse: collapse; width: 100%;">
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold; width: 150px;">Tables Migrated:</td><td style="padding: 8px;">{stats['tables_migrated']}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Total Rows:</td><td style="padding: 8px; font-weight: bold;">{format_number(stats['total_rows'])}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Throughput:</td><td style="padding: 8px;">{format_number(stats['rows_per_second'])} rows/sec</td></tr>
</table>

{tables_html}

<p style="color: #666; font-size: 12px; margin-top: 20px; border-top: 1px solid #ddd; padding-top: 10px;">
This is an automated notification from the MSSQL to PostgreSQL Migration Pipeline.
</p>
</body>
</html>
"""
        send_email_notification(subject, body, html_body)


def on_dag_failure(context: Dict[str, Any]) -> None:
    """
    Callback for DAG failure. Sends notifications to configured channels.

    Usage in DAG:
        @dag(on_failure_callback=on_dag_failure, ...)
    """
    if not is_notifications_enabled():
        logger.debug("Notifications disabled, skipping failure callback")
        return

    channels = get_notification_channels()
    if not channels:
        logger.debug("No notification channels configured")
        return

    info = format_dag_context(context)
    exception = context.get('exception')

    title = f"❌ DAG Failed: {info['dag_id']}"
    error_msg = str(exception) if exception else 'Unknown error'
    message = f"Migration pipeline failed.\n\nError: {error_msg}"

    fields = [
        {'title': 'DAG', 'value': info['dag_id']},
        {'title': 'Run ID', 'value': info['run_id']},
        {'title': 'Started', 'value': info['start_date']},
        {'title': 'Duration', 'value': info['duration']},
        {'title': 'Error', 'value': error_msg[:100], 'short': False},
    ]

    if 'slack' in channels:
        send_slack_notification(
            message=message,
            title=title,
            color='#dc3545',  # red
            fields=fields
        )

    if 'email' in channels:
        subject = title
        body = f"""{title}

Migration pipeline failed.

Run Details:
  • DAG: {info['dag_id']}
  • Run ID: {info['run_id']}
  • Started: {info['start_date']}
  • Duration: {info['duration']}

Error:
{error_msg}

Please check the Airflow UI for more details.

This is an automated notification from the MSSQL to PostgreSQL Migration Pipeline.
"""
        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
<h2 style="color: #dc3545; border-bottom: 2px solid #dc3545; padding-bottom: 10px;">{title}</h2>
<p>Migration pipeline failed.</p>

<h3>Run Details</h3>
<table style="border-collapse: collapse; width: 100%;">
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold; width: 150px;">DAG:</td><td style="padding: 8px;">{info['dag_id']}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Run ID:</td><td style="padding: 8px; font-family: monospace; font-size: 12px;">{info['run_id']}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Started:</td><td style="padding: 8px;">{info['start_date']}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Duration:</td><td style="padding: 8px;">{info['duration']}</td></tr>
</table>

<h3 style="color: #dc3545;">Error</h3>
<pre style="background: #fff3f3; padding: 15px; border-radius: 5px; border-left: 4px solid #dc3545; overflow-x: auto;">{error_msg}</pre>

<p>Please check the Airflow UI for more details.</p>

<p style="color: #666; font-size: 12px; margin-top: 20px; border-top: 1px solid #ddd; padding-top: 10px;">
This is an automated notification from the MSSQL to PostgreSQL Migration Pipeline.
</p>
</body>
</html>
"""
        send_email_notification(subject, body, html_body)


def on_task_failure(context: Dict[str, Any]) -> None:
    """
    Callback for task failure. Sends notifications when all retries are exhausted.

    In Airflow 3.0, this callback is only invoked when the task has failed
    and no more retries remain (final failure).

    Usage in DAG default_args:
        default_args={'on_failure_callback': on_task_failure, ...}
    """
    if not is_notifications_enabled():
        return

    channels = get_notification_channels()
    if not channels:
        return

    # Get task instance info
    ti = context.get('task_instance') or context.get('ti')
    task_id = getattr(ti, 'task_id', 'unknown') if ti else 'unknown'

    # Try multiple ways to get the error message (Airflow 3.0 compatibility)
    error_msg = None

    # Method 0: Check our stored exception (captured by @capture_exceptions decorator)
    stored_exception = get_stored_exception(task_id)
    if stored_exception:
        error_msg = stored_exception
        # Clear it after retrieval to avoid stale data
        clear_stored_exception(task_id)
        logger.info(f"Retrieved stored exception for {task_id}: {error_msg[:100]}")

    # Method 1: Direct exception in context
    if not error_msg:
        exception = context.get('exception')
        if exception:
            error_msg = f"{type(exception).__name__}: {str(exception)}"

    # Method 2: Try to get from task instance
    if not error_msg and ti:
        ti_exception = getattr(ti, 'exception', None)
        if ti_exception:
            error_msg = f"{type(ti_exception).__name__}: {str(ti_exception)}"

    # Method 3: Try reason field
    if not error_msg:
        reason = context.get('reason')
        if reason:
            error_msg = str(reason)

    # Method 4: Try to extract from sys.exc_info()
    if not error_msg:
        import sys
        exc_info = sys.exc_info()
        if exc_info[1]:
            error_msg = f"{type(exc_info[1]).__name__}: {str(exc_info[1])}"

    # Method 5: Try traceback module for current exception
    if not error_msg:
        current_exc = traceback.format_exc()
        if current_exc and current_exc != 'NoneType: None\n':
            error_lines = current_exc.strip().split('\n')
            error_msg = error_lines[-1] if error_lines else None

    if not error_msg:
        error_msg = 'Error details not available - check Airflow UI for full logs'

    logger.info(f"Captured error message: {error_msg[:200] if error_msg else 'None'}")

    # Log retry info for debugging
    if ti:
        try_number = getattr(ti, 'try_number', 1)
        max_tries = getattr(ti, 'max_tries', 0)
        logger.info(f"Task failure: try_number={try_number}, max_tries={max_tries}, is_final=True")

    # Get context info
    dag_run = context.get('dag_run')
    dag_id = dag_run.dag_id if dag_run else 'Unknown'
    run_id = dag_run.run_id if dag_run else 'Unknown'

    # Calculate duration
    duration_seconds = 0
    start_date = None
    if dag_run and dag_run.start_date:
        start_date = dag_run.start_date
        duration_seconds = (datetime.now(timezone.utc) - dag_run.start_date).total_seconds()

    # Send failure notification
    send_failure_notification(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        start_date=start_date,
        duration_seconds=duration_seconds,
        error_message=error_msg,
    )


def send_custom_notification(
    title: str,
    message: str,
    status: str = 'info',
    fields: Optional[List[Dict[str, str]]] = None
) -> None:
    """
    Send a custom notification to all configured channels.

    Args:
        title: Notification title
        message: Main message
        status: One of 'success', 'error', 'warning', 'info'
        fields: Optional list of {'title': ..., 'value': ...} dicts

    Usage:
        from include.mssql_pg_migration.notifications import send_custom_notification
        send_custom_notification(
            title="Migration Progress",
            message="Transferred 10M rows",
            status="info",
            fields=[{'title': 'Table', 'value': 'votes'}]
        )
    """
    if not is_notifications_enabled():
        return

    channels = get_notification_channels()
    if not channels:
        return

    color_map = {
        'success': '#36a64f',  # green
        'error': '#dc3545',    # red
        'warning': '#ffc107',  # amber
        'info': '#17a2b8',     # blue
    }
    color = color_map.get(status, '#17a2b8')

    if 'slack' in channels:
        send_slack_notification(message, title, color, fields)

    if 'email' in channels:
        subject = title
        body = f"{title}\n\n{message}"
        if fields:
            body += "\n\nDetails:\n"
            for f in fields:
                body += f"  • {f['title']}: {f['value']}\n"
        send_email_notification(subject, body)


def send_success_notification(
    dag_id: str,
    run_id: str,
    start_date,
    duration_seconds: float,
    stats: Dict[str, Any],
) -> None:
    """
    Send a success notification with migration statistics.
    This is designed to be called from a task, not a callback.

    Args:
        dag_id: The DAG ID
        run_id: The run ID
        start_date: Start datetime of the DAG run
        duration_seconds: Duration in seconds
        stats: Dictionary with tables_migrated, total_rows, tables_list, rows_per_second
    """
    if not is_notifications_enabled():
        logger.info("Notifications disabled, skipping success notification")
        return

    channels = get_notification_channels()
    if not channels:
        logger.info("No notification channels configured")
        return

    logger.info(f"Sending success notification via channels: {channels}")

    # Format timestamps
    start_str = 'Unknown'
    end_str = 'Unknown'
    if start_date:
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S UTC')
        from datetime import datetime, timezone
        end_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

    duration_str = format_duration(duration_seconds) if duration_seconds > 0 else 'N/A'

    title = f"✅ DAG Success: {dag_id}"

    # Build summary message
    summary_parts = ["Migration pipeline completed successfully."]
    if stats.get('tables_migrated', 0) > 0:
        summary_parts.append(f"Migrated {stats['tables_migrated']} tables with {format_number(stats.get('total_rows', 0))} total rows.")
    if stats.get('rows_per_second', 0) > 0:
        summary_parts.append(f"Throughput: {format_number(stats['rows_per_second'])} rows/sec.")

    message = " ".join(summary_parts)

    # Build fields for Slack
    fields = [
        {'title': 'DAG', 'value': dag_id},
        {'title': 'Run ID', 'value': run_id},
        {'title': 'Started', 'value': start_str},
        {'title': 'Duration', 'value': duration_str},
    ]

    if stats.get('tables_migrated', 0) > 0:
        fields.append({'title': 'Tables', 'value': str(stats['tables_migrated'])})
        fields.append({'title': 'Total Rows', 'value': format_number(stats.get('total_rows', 0))})

    if stats.get('rows_per_second', 0) > 0:
        fields.append({'title': 'Throughput', 'value': f"{format_number(stats['rows_per_second'])} rows/sec"})

    if 'slack' in channels:
        send_slack_notification(
            message=message,
            title=title,
            color='#36a64f',  # green
            fields=fields
        )

    if 'email' in channels:
        subject = title

        # Build tables list for email
        tables_section = ""
        tables_list = stats.get('tables_list', [])
        if tables_list:
            tables_section = "\nTables Migrated:\n" + "\n".join(f"  • {t}" for t in tables_list)

        body = f"""{title}

{message}

Run Details:
  • DAG: {dag_id}
  • Run ID: {run_id}
  • Started: {start_str}
  • Completed: {end_str}
  • Duration: {duration_str}

Migration Statistics:
  • Tables Migrated: {stats.get('tables_migrated', 0)}
  • Total Rows: {format_number(stats.get('total_rows', 0))}
  • Throughput: {format_number(stats.get('rows_per_second', 0))} rows/sec
{tables_section}

This is an automated notification from the MSSQL to PostgreSQL Migration Pipeline.
"""

        # Build HTML email
        tables_html = ""
        if tables_list:
            tables_html = "<h3>Tables Migrated</h3><ul>" + "".join(f"<li>{t}</li>" for t in tables_list) + "</ul>"

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
<h2 style="color: #28a745; border-bottom: 2px solid #28a745; padding-bottom: 10px;">{title}</h2>
<p style="font-size: 16px;">{message}</p>

<h3>Run Details</h3>
<table style="border-collapse: collapse; width: 100%;">
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold; width: 150px;">DAG:</td><td style="padding: 8px;">{dag_id}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Run ID:</td><td style="padding: 8px; font-family: monospace; font-size: 12px;">{run_id}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Started:</td><td style="padding: 8px;">{start_str}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Completed:</td><td style="padding: 8px;">{end_str}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Duration:</td><td style="padding: 8px; font-weight: bold; color: #28a745;">{duration_str}</td></tr>
</table>

<h3>Migration Statistics</h3>
<table style="border-collapse: collapse; width: 100%;">
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold; width: 150px;">Tables Migrated:</td><td style="padding: 8px;">{stats.get('tables_migrated', 0)}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Total Rows:</td><td style="padding: 8px; font-weight: bold;">{format_number(stats.get('total_rows', 0))}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Throughput:</td><td style="padding: 8px;">{format_number(stats.get('rows_per_second', 0))} rows/sec</td></tr>
</table>

{tables_html}

<p style="color: #666; font-size: 12px; margin-top: 20px; border-top: 1px solid #ddd; padding-top: 10px;">
This is an automated notification from the MSSQL to PostgreSQL Migration Pipeline.
</p>
</body>
</html>
"""
        result = send_email_notification(subject, body, html_body)
        logger.info(f"Email notification sent: {result}")


def send_failure_notification(
    dag_id: str,
    run_id: str,
    task_id: str,
    start_date,
    duration_seconds: float,
    error_message: str,
) -> None:
    """
    Send a failure notification with error details.
    This is designed to be called from an on_failure_callback at the task level.

    Args:
        dag_id: The DAG ID
        run_id: The run ID
        task_id: The failed task ID
        start_date: Start datetime of the DAG run
        duration_seconds: Duration in seconds before failure
        error_message: The error message
    """
    if not is_notifications_enabled():
        logger.info("Notifications disabled, skipping failure notification")
        return

    channels = get_notification_channels()
    if not channels:
        logger.info("No notification channels configured")
        return

    logger.info(f"Sending failure notification via channels: {channels}")

    # Format timestamps
    start_str = 'Unknown'
    end_str = 'Unknown'
    if start_date:
        start_str = start_date.strftime('%Y-%m-%d %H:%M:%S UTC')
        from datetime import datetime, timezone
        end_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

    duration_str = format_duration(duration_seconds) if duration_seconds > 0 else 'N/A'

    title = f"❌ DAG Failed: {dag_id}"
    message = f"Migration pipeline failed at task '{task_id}'."

    # Build fields for Slack
    fields = [
        {'title': 'DAG', 'value': dag_id},
        {'title': 'Failed Task', 'value': task_id},
        {'title': 'Run ID', 'value': run_id},
        {'title': 'Started', 'value': start_str},
        {'title': 'Duration', 'value': duration_str},
        {'title': 'Error', 'value': error_message[:200] if error_message else 'Unknown', 'short': False},
    ]

    if 'slack' in channels:
        send_slack_notification(
            message=message,
            title=title,
            color='#dc3545',  # red
            fields=fields
        )

    if 'email' in channels:
        subject = title

        body = f"""{title}

{message}

Run Details:
  • DAG: {dag_id}
  • Failed Task: {task_id}
  • Run ID: {run_id}
  • Started: {start_str}
  • Failed At: {end_str}
  • Duration: {duration_str}

Error:
{error_message or 'Unknown error'}

Please check the Airflow UI for more details.

This is an automated notification from the MSSQL to PostgreSQL Migration Pipeline.
"""

        html_body = f"""
<html>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
<h2 style="color: #dc3545; border-bottom: 2px solid #dc3545; padding-bottom: 10px;">{title}</h2>
<p style="font-size: 16px;">{message}</p>

<h3>Run Details</h3>
<table style="border-collapse: collapse; width: 100%;">
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold; width: 150px;">DAG:</td><td style="padding: 8px;">{dag_id}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Failed Task:</td><td style="padding: 8px; color: #dc3545; font-weight: bold;">{task_id}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Run ID:</td><td style="padding: 8px; font-family: monospace; font-size: 12px;">{run_id}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Started:</td><td style="padding: 8px;">{start_str}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Failed At:</td><td style="padding: 8px;">{end_str}</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Duration:</td><td style="padding: 8px;">{duration_str}</td></tr>
</table>

<h3 style="color: #dc3545;">Error</h3>
<pre style="background: #fff3f3; padding: 15px; border-radius: 5px; border-left: 4px solid #dc3545; overflow-x: auto; white-space: pre-wrap;">{error_message or 'Unknown error'}</pre>

<p>Please check the Airflow UI for more details.</p>

<p style="color: #666; font-size: 12px; margin-top: 20px; border-top: 1px solid #ddd; padding-top: 10px;">
This is an automated notification from the MSSQL to PostgreSQL Migration Pipeline.
</p>
</body>
</html>
"""
        result = send_email_notification(subject, body, html_body)
        logger.info(f"Failure email notification sent: {result}")
