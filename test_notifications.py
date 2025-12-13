#!/usr/bin/env python3
"""
Test script for notification system.

This script tests both email and Slack notifications to verify
the notification system is working correctly with the configured credentials.
"""

import sys
import os
import logging
from datetime import datetime, timezone

# Setup path to include our modules
sys.path.insert(0, 'include')

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv()

from mssql_pg_migration.notifications import (
    send_slack_notification,
    send_email_notification,
    send_success_notification,
    send_failure_notification,
    send_custom_notification,
    is_notifications_enabled,
    get_notification_channels,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_configuration():
    """Test that notification configuration is loaded correctly."""
    logger.info("=" * 80)
    logger.info("TEST 1: Configuration Check")
    logger.info("=" * 80)

    enabled = is_notifications_enabled()
    channels = get_notification_channels()

    logger.info(f"Notifications enabled: {enabled}")
    logger.info(f"Configured channels: {channels}")

    # Check environment variables
    logger.info(f"SMTP_HOST: {os.getenv('SMTP_HOST', 'NOT SET')}")
    logger.info(f"SMTP_PORT: {os.getenv('SMTP_PORT', 'NOT SET')}")
    logger.info(f"SMTP_USER: {os.getenv('SMTP_USER', 'NOT SET')}")
    logger.info(f"NOTIFICATION_EMAIL_TO: {os.getenv('NOTIFICATION_EMAIL_TO', 'NOT SET')}")
    logger.info(f"SLACK_WEBHOOK_URL: {'SET' if os.getenv('SLACK_WEBHOOK_URL') else 'NOT SET'}")

    if not enabled:
        logger.error("❌ Notifications are DISABLED - set NOTIFICATION_ENABLED=true in .env")
        return False

    if not channels:
        logger.error("❌ No notification channels configured - set NOTIFICATION_CHANNELS in .env")
        return False

    logger.info("✅ Configuration check PASSED\n")
    return True


def test_slack_notification():
    """Test Slack notification."""
    logger.info("=" * 80)
    logger.info("TEST 2: Slack Notification")
    logger.info("=" * 80)

    channels = get_notification_channels()
    if 'slack' not in channels:
        logger.warning("⚠️  Slack not in configured channels, skipping")
        return True

    webhook_url = os.getenv('SLACK_WEBHOOK_URL', '')
    if not webhook_url:
        logger.error("❌ SLACK_WEBHOOK_URL not configured")
        return False

    logger.info(f"Webhook URL: {webhook_url[:50]}...")

    # Send test message
    success = send_slack_notification(
        message="This is a test message from the MSSQL to PostgreSQL Migration Pipeline.",
        title="🧪 Test Notification - Slack",
        color='#17a2b8',  # blue for info
        fields=[
            {'title': 'Test Type', 'value': 'Slack Integration'},
            {'title': 'Timestamp', 'value': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')},
            {'title': 'Status', 'value': 'Testing notification system'},
        ]
    )

    if success:
        logger.info("✅ Slack notification sent successfully!")
        logger.info("   Check your Slack channel for the message")
    else:
        logger.error("❌ Failed to send Slack notification")

    logger.info("")
    return success


def test_email_notification():
    """Test Email notification."""
    logger.info("=" * 80)
    logger.info("TEST 3: Email Notification")
    logger.info("=" * 80)

    channels = get_notification_channels()
    if 'email' not in channels:
        logger.warning("⚠️  Email not in configured channels, skipping")
        return True

    smtp_host = os.getenv('SMTP_HOST', '')
    smtp_user = os.getenv('SMTP_USER', '')
    email_to = os.getenv('NOTIFICATION_EMAIL_TO', '')

    if not all([smtp_host, smtp_user, email_to]):
        logger.error("❌ Email configuration incomplete")
        return False

    logger.info(f"SMTP Host: {smtp_host}")
    logger.info(f"From: {smtp_user}")
    logger.info(f"To: {email_to}")

    # Plain text body
    body = """Test Notification - Email

This is a test email from the MSSQL to PostgreSQL Migration Pipeline notification system.

Test Details:
  • Test Type: Email Integration
  • Timestamp: {timestamp}
  • Status: Testing notification system

If you received this email, the notification system is working correctly!

This is an automated test message.
""".format(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'))

    # HTML body
    html_body = """
<html>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
<h2 style="color: #17a2b8; border-bottom: 2px solid #17a2b8; padding-bottom: 10px;">🧪 Test Notification - Email</h2>
<p>This is a test email from the MSSQL to PostgreSQL Migration Pipeline notification system.</p>

<h3>Test Details</h3>
<table style="border-collapse: collapse; width: 100%;">
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold; width: 150px;">Test Type:</td><td style="padding: 8px;">Email Integration</td></tr>
<tr><td style="padding: 8px; font-weight: bold;">Timestamp:</td><td style="padding: 8px;">{timestamp}</td></tr>
<tr style="background: #f8f9fa;"><td style="padding: 8px; font-weight: bold;">Status:</td><td style="padding: 8px;">Testing notification system</td></tr>
</table>

<p style="margin-top: 20px;">If you received this email, the notification system is working correctly! ✅</p>

<p style="color: #666; font-size: 12px; margin-top: 20px; border-top: 1px solid #ddd; padding-top: 10px;">
This is an automated test message from the MSSQL to PostgreSQL Migration Pipeline.
</p>
</body>
</html>
""".format(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'))

    # Send test email
    success = send_email_notification(
        subject="🧪 Test Notification - Email",
        body=body,
        html_body=html_body
    )

    if success:
        logger.info("✅ Email notification sent successfully!")
        logger.info(f"   Check {email_to} for the message")
    else:
        logger.error("❌ Failed to send email notification")

    logger.info("")
    return success


def test_success_notification():
    """Test success notification with migration stats."""
    logger.info("=" * 80)
    logger.info("TEST 4: Success Notification (with stats)")
    logger.info("=" * 80)

    # Mock migration statistics
    stats = {
        'tables_migrated': 9,
        'total_rows': 94_600_000,
        'rows_per_second': 450_000,
        'tables_list': ['Votes', 'Comments', 'Posts', 'Badges', 'Users', 'PostLinks', 'VoteTypes', 'PostTypes', 'LinkTypes'],
    }

    start_date = datetime.now(timezone.utc)
    duration_seconds = 210  # 3.5 minutes

    send_success_notification(
        dag_id='test_mssql_to_postgres_migration',
        run_id='manual__2025-12-12T00:00:00.000000+00:00',
        start_date=start_date,
        duration_seconds=duration_seconds,
        stats=stats
    )

    logger.info("✅ Success notification sent")
    logger.info("   Check your channels for the detailed success message")
    logger.info("")
    return True


def test_failure_notification():
    """Test failure notification with error details."""
    logger.info("=" * 80)
    logger.info("TEST 5: Failure Notification (with error)")
    logger.info("=" * 80)

    start_date = datetime.now(timezone.utc)
    duration_seconds = 45

    send_failure_notification(
        dag_id='test_mssql_to_postgres_migration',
        run_id='manual__2025-12-12T00:00:00.000000+00:00',
        task_id='transfer_table_data',
        start_date=start_date,
        duration_seconds=duration_seconds,
        error_message='ConnectionError: Unable to connect to SQL Server - timeout after 30 seconds'
    )

    logger.info("✅ Failure notification sent")
    logger.info("   Check your channels for the detailed failure message")
    logger.info("")
    return True


def test_custom_notification():
    """Test custom notification."""
    logger.info("=" * 80)
    logger.info("TEST 6: Custom Notification")
    logger.info("=" * 80)

    send_custom_notification(
        title="🎉 Custom Test Notification",
        message="This is a custom notification to demonstrate the flexibility of the notification system.",
        status='info',
        fields=[
            {'title': 'Feature', 'value': 'Custom Notifications'},
            {'title': 'Use Case', 'value': 'Progress updates, milestones, alerts'},
        ]
    )

    logger.info("✅ Custom notification sent")
    logger.info("")
    return True


def main():
    """Run all notification tests."""
    logger.info("\n" + "=" * 80)
    logger.info("NOTIFICATION SYSTEM TEST SUITE")
    logger.info("=" * 80 + "\n")

    results = []

    # Test 1: Configuration
    try:
        result = test_configuration()
        results.append(("Configuration", result))
        if not result:
            logger.error("\n❌ Configuration test failed - cannot continue")
            return 1
    except Exception as e:
        logger.error(f"❌ Configuration test FAILED: {e}", exc_info=True)
        return 1

    # Test 2: Slack
    try:
        result = test_slack_notification()
        results.append(("Slack Notification", result))
    except Exception as e:
        logger.error(f"❌ Slack test FAILED: {e}", exc_info=True)
        results.append(("Slack Notification", False))

    # Test 3: Email
    try:
        result = test_email_notification()
        results.append(("Email Notification", result))
    except Exception as e:
        logger.error(f"❌ Email test FAILED: {e}", exc_info=True)
        results.append(("Email Notification", False))

    # Test 4: Success notification
    try:
        result = test_success_notification()
        results.append(("Success Notification", result))
    except Exception as e:
        logger.error(f"❌ Success notification FAILED: {e}", exc_info=True)
        results.append(("Success Notification", False))

    # Test 5: Failure notification
    try:
        result = test_failure_notification()
        results.append(("Failure Notification", result))
    except Exception as e:
        logger.error(f"❌ Failure notification FAILED: {e}", exc_info=True)
        results.append(("Failure Notification", False))

    # Test 6: Custom notification
    try:
        result = test_custom_notification()
        results.append(("Custom Notification", result))
    except Exception as e:
        logger.error(f"❌ Custom notification FAILED: {e}", exc_info=True)
        results.append(("Custom Notification", False))

    # Summary
    logger.info("=" * 80)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("=" * 80)

    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{test_name}: {status}")

    passed_count = sum(1 for _, passed in results if passed)
    total_count = len(results)

    logger.info("=" * 80)
    logger.info(f"TOTAL: {passed_count}/{total_count} tests passed")

    if passed_count == total_count:
        logger.info("🎉 ALL TESTS PASSED!")
        logger.info("\nNotification system is fully operational.")
        logger.info("Check your Slack channel and email inbox for test messages.")
        return 0
    else:
        logger.error(f"⚠️  {total_count - passed_count} TEST(S) FAILED")
        return 1


if __name__ == '__main__':
    sys.exit(main())
