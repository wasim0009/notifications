"""fcm.py"""

import boto3
import google.auth
from google.oauth2 import service_account
from boto3.dynamodb.conditions import Key
import google.auth.transport.requests
import requests

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
patient_device_table = dynamodb.Table("patient_device_table")  # Update with your table name
supervisor_device_table = dynamodb.Table("nurse_supervisor_device_table")  # Update with your table name

# FCM API URL
FCM_API_URL = "https://fcm.googleapis.com/v1/projects/senseai-mobile/messages:send"

def _get_access_token():
    """Retrieve a valid access token that can be used to authorize requests.

    :return: Access token.
    """
    credentials = service_account.Credentials.from_service_account_file(
        'senseai-mobile-firebase-adminsdk-ndv1n-1842a7c341.json', scopes=["https://www.googleapis.com/auth/firebase.messaging"]
    )
    request = google.auth.transport.requests.Request()
    credentials.refresh(request)
    return credentials.token

def get_device_tokens(patient_id, supervisor_id):
    """
    Retrieve device tokens for a given patient and supervisor.

    :param patient_id: ID of the patient
    :param supervisor_id: ID of the supervisor
    :return: List of device tokens
    """
    device_tokens = []

    # Fetch patient device tokens
    response = patient_device_table.query(
        KeyConditionExpression=Key('patient_id').eq(patient_id)
    )
    for item in response.get('Items', []):
        device_tokens.append(item['device_id'])

    # Fetch supervisor device tokens
    response = supervisor_device_table.query(
        KeyConditionExpression=Key('supervisor_id').eq(supervisor_id)
    )
    for item in response.get('Items', []):
        device_tokens.append(item['device_id'])

    return device_tokens

def send_fcm_notification(notification_type, message_text, patient_id, supervisor_id, additional_data=None):
    """
    Send FCM notification to devices of a patient and their supervisor.

    :param notification_type: Type of the notification (e.g., "alert", "daily-notification", "hardware_alarm")
    :param message_text: Text of the notification message
    :param patient_id: ID of the patient
    :param supervisor_id: ID of the supervisor
    :param additional_data: Dictionary of additional data to include in the notification payload
    """
    # Get device tokens
    device_tokens = get_device_tokens(patient_id, supervisor_id)

    # Construct headers with access token
    headers = {
        'Authorization': 'Bearer ' + _get_access_token(),
        'Content-Type': 'application/json; UTF-8',
    }

    # Construct notification payload
    notification_payload = {
        "message": {
            "notification": {
                "title": "Notification",
                "body": message_text,
                "sound": "default"
            },
            "data": {
                "notification_type": notification_type,
                "message": message_text
            }
        }
    }

    # Add additional data if provided
    if additional_data:
        notification_payload["message"]["data"].update(additional_data)

    # Send notification to each device
    for token in device_tokens:
        notification_payload["message"]["token"] = token
        try:
            response = requests.post(FCM_API_URL, headers=headers, json=notification_payload)
            response.raise_for_status()
            print(f'Successfully sent message to {token}: {response.json()}')
        except requests.exceptions.RequestException as e:
            print(f'Error sending message to {token}: {e}')

# Example usage (for testing)
if __name__ == "__main__":
    # Example input for testing
    notification_type = "alert"
    message_text = "Important information!"
    patient_id = "2ae00d86-f298-4625-a164-3e2fc4253bac"  # Replace with actual patient ID
    supervisor_id = "a6f30147-f26c-43af-b019-47c9c002949e"  # Replace with actual supervisor ID
    additional_data = {
        "alert_type": "warning",
        "extra_info": "Additional information here"
    }

    send_fcm_notification(notification_type, message_text, patient_id, supervisor_id, additional_data)
