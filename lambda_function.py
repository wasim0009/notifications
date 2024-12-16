import json
import boto3
import re
from decimal import Decimal
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
import traceback
import concurrent.futures

# Initialize AWS clients and resources.
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
lambda_client = boto3.client('lambda')

# DynamoDB tables
device_location_table = dynamodb.Table('patient-device-location')
threshold_table = dynamodb.Table('PatientThreshold')
smart_notification_table = dynamodb.Table('smart-notifcations')
supervisor_device_table = dynamodb.Table('nurse_supervisor_device_table')
nurse_patient_table = dynamodb.Table('Nurse-Patient-Relationship')
nurse_supervisor_table = dynamodb.Table('NurseSupervisor')
facility_table = dynamodb.Table('Facilities')
patient_facility_table = dynamodb.Table('Patient-Facility-Relationship')
facility_threshold_table = dynamodb.Table('FacilityThreshold')
global_threshold_table = dynamodb.Table('GlobalPatientThreshold')
patients_table = dynamodb.Table('Patients')

# Name of the FCM notification Lambda function
FCM_LAMBDA_FUNCTION_NAME = 'FCM-Generic-Code'

# Configuration for cooldown period in minutes
NOTIFICATION_COOLDOWN_PERIOD = 900  # default is 15 minutes

def json_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Object of type {} is not JSON serializable".format(type(obj)))

def get_supervisor_id(patient_id):
    try:
        # Query nurse_patient_table to get supervisor_ids associated with the patient_id
        response = nurse_patient_table.query(
            IndexName='patient_id-index',
            KeyConditionExpression=Key('patient_id').eq(patient_id)
        )

        # Extract supervisor_ids from the response
        supervisor_ids = [item['supervisor_id'] for item in response['Items']]

        # Retrieve nurse supervisors' details using supervisor_ids
        supervisors = []
        for supervisor_id in supervisor_ids:
            response = nurse_supervisor_table.get_item(
                Key={'supervisor_id': supervisor_id}
            )
            if 'Item' in response:
                supervisor_details = response['Item']
                supervisor_name = supervisor_details.get('nurse_supervisors_name', '')
                supervisors.append({'supervisor_id': supervisor_id, 'supervisor_name': supervisor_name})

        # If supervisors found, return the first supervisor_id (assuming one supervisor per patient)
        if supervisors:
            return supervisor_ids[0]
        else:
            raise ValueError(f"No supervisor found for patient_id: {patient_id}")

    except ValueError as ve:
        raise ve  # Re-raise the ValueError to be handled by the caller (lambda_handler)

    except Exception as e:
        print('Error getting nurse supervisors by patient ID:', e)
        raise RuntimeError('Error getting nurse supervisors by patient ID')

def invoke_fcm_lambda(notification_type, message_text, patient_id, supervisor_id, additional_data):
    payload = {
        "notification_type": notification_type,
        "message_text": message_text,
        "patient_id": patient_id,
        "supervisor_id": supervisor_id,
        "additional_data": additional_data
    }

    print("Sending payload to FCM Lambda:", json.dumps(payload, default=json_default))  # Debug print

    response = lambda_client.invoke(
        FunctionName=FCM_LAMBDA_FUNCTION_NAME,
        InvocationType='Event',
        Payload=json.dumps(payload)
    )

    print("FCM Lambda invoked with response:", response)  # Debug print

# def get_smart_notifications_by_category(patient_id, category):
#     try:
#         # Use the query method to retrieve notifications based on patient_id and category
#         response = smart_notification_table.query(
#             IndexName='patient_id-category-index',
#             KeyConditionExpression=Key('patient_id').eq(patient_id) & Key('category').eq(category),
#             ScanIndexForward=True,
#             Limit=20
#         )

#         # Extract the items from the response
#         notifications = response['Items']

#         # Convert timestamps to human-readable format
#         notifications = [convert_timestamp(item) for item in notifications]

#         return notifications

#     except Exception as e:
#         print('Error getting smart notifications by category:', e)
#         print("stack trace", traceback.format_exc())
#         raise RuntimeError('Error getting smart notifications by category')

def get_smart_notifications(device_id=None, patient_id=None, supervisor_id=None, facility_id=None):
    try:
        device_ids = []

        # If no identifiers are provided, scan the entire table
        if device_id is None and patient_id is None and supervisor_id is None and facility_id is None:
            response = smart_notification_table.scan(
                FilterExpression=Attr('resolved').eq(False),
                Limit=100
            )
            notifications = response['Items']
        else:
            # Retrieve device_ids based on provided identifiers (facility_id, patient_id, etc.)
            if facility_id:
                # Get patient_ids associated with the facility_id
                response = patient_facility_table.query(
                    IndexName='facility_id-index',
                    KeyConditionExpression=Key('facility_id').eq(facility_id)
                )
                print(f'Facility response: {response}')  # Debug print
                patient_ids = [item['patient_id'] for item in response['Items']]

                # Get device_ids associated with the patient_ids
                for patient_id in patient_ids:
                    response = device_location_table.query(
                        IndexName='patient_id-index',
                        KeyConditionExpression=Key('patient_id').eq(patient_id)
                    )
                    print(f'Device location response for patient_id {patient_id}: {response}')  # Debug print
                    device_ids.extend([item['device_id'] for item in response['Items']])
            elif patient_id:
                # Get device_ids associated with the patient_id
                response = device_location_table.query(
                    IndexName='patient_id-index',
                    KeyConditionExpression=Key('patient_id').eq(patient_id)
                )
                print(f'Device location response for patient_id {patient_id}: {response}')  # Debug print
                device_ids = [item['device_id'] for item in response['Items']]
            elif supervisor_id:
                # Get device_ids associated with the patient_id
                response = nurse_patient_table.query(
                    KeyConditionExpression=Key('supervisor_id').eq(supervisor_id)
                )
                print(f'Nurse-patient response for supervisor_id {supervisor_id}: {response}')  # Debug print
                patient_ids = [item['patient_id'] for item in response['Items']]

                # Get device_ids associated with the patient_ids
                for patient_id in patient_ids:
                    response = device_location_table.query(
                        IndexName='patient_id-index',
                        KeyConditionExpression=Key('patient_id').eq(patient_id)
                    )
                    print(f'Device location response for patient_id {patient_id}: {response}')  # Debug print
                    device_ids.extend([item['device_id'] for item in response['Items']])
            elif device_id:
                device_ids = [device_id]

            notifications = []

            # Use concurrent futures to query notifications in parallel for each device_id
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit queries for each device_id to the executor
                future_to_device = {executor.submit(query_device_notifications, device_id): device_id for device_id in device_ids}

                # Collect all the notifications returned by the queries
                for future in concurrent.futures.as_completed(future_to_device):
                    device_notifications = future.result()
                    print(f'Notifications for device_id {future_to_device[future]}: {device_notifications}')  # Debug print
                    notifications.extend(device_notifications)

        # Sort all notifications by the timestamp (descending)
        notifications = sorted(notifications, key=lambda x: x['timestamp'], reverse=False)

        # Convert timestamps to human-readable format
        notifications = [convert_timestamp(item) for item in notifications]

        return notifications[:20]  # Return up to 20 notifications

    except Exception as e:
        print('Error getting smart notifications:', e)
        print("stack trace", traceback.format_exc())
        raise RuntimeError('Error getting smart notifications')

def query_device_notifications(device_id):
    """Helper function to query notifications for a given device_id."""
    last_evaluated_key = None
    device_notifications = []

    while True:
        query_params = {
            "IndexName": 'device_id-timestamp-index',
            "KeyConditionExpression": Key('device_id').eq(device_id),
            "FilterExpression": Attr('resolved').eq(False),
            "ScanIndexForward": True,
            "Limit": 20
        }

        if last_evaluated_key is not None:
            query_params["ExclusiveStartKey"] = last_evaluated_key

        response = smart_notification_table.query(**query_params)
        device_notifications.extend(response['Items'])

        if len(device_notifications) >= 20 or 'LastEvaluatedKey' not in response:
            break

        last_evaluated_key = response.get('LastEvaluatedKey')

    return device_notifications

def convert_timestamp(item):
    timestamp_str = item['timestamp']
    timestamp_dt = datetime.fromisoformat(timestamp_str)
    human_readable_timestamp = timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')
    item['timestamp'] = human_readable_timestamp
    return item

def update_notification_status(notification_id, device_id, resolved, resolved_comments):
    try:
        # Convert the boolean value to a string for DynamoDB
        resolved_str = str(resolved).lower()

        # Update the notification in the smart_notification_table
        response = smart_notification_table.update_item(
            Key={
                'notification_id': notification_id,
                'device_id': device_id
            },
            UpdateExpression="set resolved = :r, resolved_comments = :c",
            ExpressionAttributeValues={
                ':r': resolved_str,
                ':c': resolved_comments
            },
            ReturnValues="ALL_NEW"
        )

        # Return a success message
        return {'message': 'Notification resolved successfully'}

    except Exception as e:
        print('Error updating notification status:', e)
        raise RuntimeError('Error updating notification status')

def is_within_cooldown(device_id, timestamp):
    try:

        current_timestamp = datetime.fromisoformat(timestamp)

        response = smart_notification_table.query(
            IndexName='device_id-timestamp-index',
            KeyConditionExpression=Key('device_id').eq(device_id) & Key('timestamp').lt(current_timestamp.isoformat()),
            Limit=1,
            ScanIndexForward=False  # Get the latest notification first
        )
        print(response)
        if response['Items']:
            last_notification = response['Items'][0]
            last_timestamp = datetime.fromisoformat(last_notification['timestamp'])  # Convert to datetime object
            # timestamp = datetime.fromisoformat(timestamp)  # Convert the timestamp string to a datetime object
            cooldown_period = NOTIFICATION_COOLDOWN_PERIOD  # convert to seconds
            # print(last_timestamp)
            # print(current_timestamp)
            # # Check if the time difference is less than the cooldown period
            # print((timestamp - last_timestamp).total_seconds())
            # print(cooldown_period)
            time_difference = (current_timestamp - last_timestamp).total_seconds()

            print(f"Last Notification Timestamp: {last_timestamp}")
            print(f"Current Notification Timestamp: {current_timestamp}")
            print(f"Cooldown Period (seconds): {cooldown_period}")
            print(f"Time Difference (seconds): {time_difference}")

            if time_difference < cooldown_period:
                return True  # Cooldown is still active

        return False  # Cooldown has passed

    except Exception as e:
        print('Error checking cooldown:', e)
        return False

def handle_sqs_event(event):
    for record in event['Records']:
        try:
            # Extract message body
            message_body = json.loads(record['body'], parse_float=Decimal)
            print("Message body:", json.dumps(message_body, default=json_default))  # Debug print

            # Retrieve device_id from the message
            device_id = message_body.get('device_id')
            print("Device ID:", device_id)  # Debug print

            # Retrieve timestamp from the message
            timestamp = message_body.get('timestamp')
            print("Timestamp:", timestamp)  # Debug print

            # Retrieve patient_id from patient-device-location table using device_id as partition key
            device_response = device_location_table.query(
                KeyConditionExpression=Key('device_id').eq(device_id)
            )
            print("Device response:", json.dumps(device_response, default=json_default))  # Debug print
            if not device_response['Items']:
                raise ValueError("No patient found for the given device_id")
            patient_id = device_response['Items'][0]['patient_id']
            location = device_response['Items'][0]['location']
            print("Patient ID:", patient_id)  # Debug print

            # Retrieve patient_name from patients table using patient_id as partition key
            patient_response = patients_table.get_item(
                Key={'patient_id': patient_id}
            )
            print("Patient response:", json.dumps(patient_response, default=json_default))  # Debug print
            if 'Item' not in patient_response:
                raise ValueError("No patient found for the given patient_id")
            patient_name = patient_response['Item']['patient_name']
            print("Patient Name:", patient_name)  # Debug print

            # Retrieve patient table using patient_id as partition key
            threshold_response = threshold_table.get_item(
                Key={'patient_id': patient_id}
            )
            print("Threshold response:", json.dumps(threshold_response, default=json_default))  # Debug print

            if 'Item' in threshold_response:
                threshold_data = threshold_response['Item']
            else:
                # If no patient-specific threshold is found, try to get facility-specific threshold
                facility_response = patient_facility_table.get_item(
                    Key={'patient_id': patient_id}
                )
                print("Facility response:", json.dumps(facility_response, default=json_default))  # Debug print

                if 'Item' in facility_response:
                    facility_id = facility_response['Item']['facility_id']
                    facility_threshold_response = facility_threshold_table.get_item(
                        Key={'facility_id': facility_id}
                    )
                    print("Facility Threshold response:", json.dumps(facility_threshold_response, default=json_default))  # Debug print

                    if 'Item' in facility_threshold_response:
                        threshold_data = facility_threshold_response['Item']
                    else:
                        # If no facility-specific threshold is found, use the global threshold
                        global_threshold_response = global_threshold_table.get_item(
                            Key={'threshold_id': '1'}
                        )
                        print("Global Threshold response:", json.dumps(global_threshold_response, default=json_default))  # Debug print

                        if 'Item' in global_threshold_response:
                            threshold_data = global_threshold_response['Item']
                        else:
                            raise ValueError("No threshold data found for the given patient_id, facility_id, or global threshold")
                else:
                    # If no facility data is found, use the global threshold
                    global_threshold_response = global_threshold_table.get_item(
                        Key={'threshold_id': '1'}
                    )
                    print("Global Threshold response:", json.dumps(global_threshold_response, default=json_default))  # Debug print

                    if 'Item' in global_threshold_response:
                        threshold_data = global_threshold_response['Item']
                    else:
                        raise ValueError("No threshold data found for the given patient_id, facility_id, or global threshold")

            # Extract relevant sensor data from message
            light_value = float(message_body.get('light'))
            sound_value = float(message_body.get('sound'))
            temp_value = float(message_body.get('temp'))
            print("Light value:", light_value)  # Debug print
            print("Sound value:", sound_value)  # Debug print
            print("Temperature value:", temp_value)  # Debug print

            # Extract relevant threshold values
            ambient_light_min = float(threshold_data.get('ambient_light_min'))
            ambient_light_max = float(threshold_data.get('ambient_light_max'))
            ambient_sound_min = float(threshold_data.get('ambient_sound_min'))
            ambient_sound_max = float(threshold_data.get('ambient_sound_max'))
            ambient_temperature_min = float(threshold_data.get('ambient_temperature_min'))
            ambient_temperature_max = float(threshold_data.get('ambient_temperature_max'))
            print("Ambient light min:", ambient_light_min)  # Debug print
            print("Ambient light max:", ambient_light_max)  # Debug print
            print("Ambient sound min:", ambient_sound_min)  # Debug print
            print("Ambient sound max:", ambient_sound_max)  # Debug print
            print("Ambient temperature min:", ambient_temperature_min)  # Debug print
            print("Ambient temperature max:", ambient_temperature_max)  # Debug print

            timestamp_dt = datetime.fromisoformat(timestamp)

            formatted_timestamp = timestamp_dt.strftime("%-d-%b-%Y %H:%M")

            # List to hold notifications
            notifications = []

            # Check if light value is within or outside threshold range
            if light_value < ambient_light_min:
                notifications.append({'message': f'Light value ({light_value:.2f} lux) is below ambient light minimum value ({ambient_light_min:.2f} lux) in {location} at {formatted_timestamp}', 'category': 'ambient_light'})
            elif light_value > ambient_light_max:
                notifications.append({'message': f'Light value ({light_value:.2f} lux) is above ambient light maximum value ({ambient_light_max:.2f} lux) in {location} at {formatted_timestamp}', 'category': 'ambient_light'})

            # Check if sound value is within or outside threshold range
            if sound_value < ambient_sound_min:
                notifications.append({'message': f'Sound value ({sound_value:.2f} dB) is below ambient sound minimum value ({ambient_sound_min:.2f} dB) in {location} at {formatted_timestamp}', 'category': 'ambient_sound'})
            elif sound_value > ambient_sound_max:
                notifications.append({'message': f'Sound value ({sound_value:.2f} dB) is above ambient sound maximum value ({ambient_sound_max:.2f} dB) in {location} at {formatted_timestamp}', 'category': 'ambient_sound'})

            # Check if temperature value is within or outside threshold range
            if temp_value < ambient_temperature_min:
                notifications.append({'message': f'Temperature value ({temp_value:.2f} deg) is below ambient temperature minimum value ({ambient_temperature_min:.2f} deg) in {location} at {formatted_timestamp}', 'category': 'ambient_temperature'})
            elif temp_value > ambient_temperature_max:
                notifications.append({'message': f'Temperature value ({temp_value:.2f} deg) is above ambient temperature maximum value ({ambient_temperature_max:.2f} deg) in {location} at {formatted_timestamp}', 'category': 'ambient_temperature'})

            if not notifications:
                # No notifications to insert, continue to next record
                print("All values are within the ambient range, no notification required")
                continue

            # Insert notifications into SmartNotificationTable and invoke FCM notification Lambda
            for notification in notifications:
                notification_message = notification['message']
                notification_category = notification['category']

                # Generate unique notification ID using device ID, timestamp, and a unique identifier for each notification
                unique_suffix = datetime.now().strftime("%Y%m%d%H%M%S%f")
                notification_id = f"{device_id}_{timestamp}_{unique_suffix}"
                print("Notification ID:", notification_id)  # Debug print

                # Remove special characters and hyphens from notification ID
                notification_id = re.sub(r'[^a-zA-Z0-9]', '', notification_id)
                print("Cleaned notification ID:", notification_id)  # Debug print

                # Check cooldown period
                if is_within_cooldown(device_id, timestamp):
                    print(f"Notification for category {notification_category} is within the cooldown period, skipping notification.")
                    continue
                #TODO
                # Insert the notification into the SmartNotificationTable
                smart_notification_table.put_item(
                    Item={
                        'notification_id': notification_id,
                        'device_id': device_id,
                        'message': notification_message,
                        'category': notification_category,
                        'timestamp': timestamp,
                        'resolved': str(False),  # Set the default value to False as a string
                        'resolved_comments': '',
                        'patient': patient_name,
                        'patient_id': patient_id
                    }
                )
                print("Notification inserted successfully")  # Debug print

                # Send FCM notification by invoking the FCM Lambda function
                supervisor_id = get_supervisor_id(patient_id)
                print(f"Supervisor ID for patient {patient_id}: {supervisor_id}")  # Debug print

                invoke_fcm_lambda(
                    notification_type="alert",
                    message_text=notification_message,
                    patient_id=patient_id,
                    supervisor_id=supervisor_id,
                    additional_data={"timestamp": timestamp, "device_id": device_id}
                )
                print("FCM Lambda invoked successfully")  # Debug print

        except Exception as e:
            print('Error processing SQS event:', e)
            print("Stack trace:", traceback.format_exc())
            raise RuntimeError('Error processing SQS event')

def lambda_handler(event, context):
    try:
        if 'httpMethod' in event:
            # Handle API Gateway trigger
            if event['httpMethod'] == 'GET':
                params = event.get('queryStringParameters') or {}
                device_id = params.get('device_id')
                patient_id = params.get('patient_id')
                facility_id = params.get('facility_id')
                supervisor_id = params.get('supervisor_id')
                # category = params.get('category')

                # if category:
                #     notifications = get_smart_notifications_by_category(patient_id, category)
                # else:
                notifications = get_smart_notifications(device_id, patient_id, supervisor_id, facility_id)

                notifications = sorted(notifications, key=lambda x: x['timestamp'], reverse=False)

                return {
                    'statusCode': 200,
                    'body': json.dumps(notifications, default=json_default)
                }
            elif event['httpMethod'] == 'PUT':
                body = json.loads(event['body'])
                notification_id = body.get('notification_id')
                device_id = body.get('device_id')
                resolved = body.get('resolved')
                resolved_comments = body.get('resolved_comments')

                result = update_notification_status(notification_id, device_id, resolved, resolved_comments)

                return {
                    'statusCode': 200,
                    'body': json.dumps(result)
                }
            else:
                return {
                    'statusCode': 405,
                    'body': json.dumps({'error': 'Method Not Allowed'})
                }
        elif 'Records' in event:
            # Handle SQS trigger
            handle_sqs_event(event)
            return {
                'statusCode': 200,
                'body': json.dumps('Notifications processed successfully')
            }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid event source'})
            }

    except KeyError as ke:
        print('Missing key in event:', ke)  # Debug print
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Missing key in event'})
        }

    except ValueError as ve:
        print('Value error:', ve)  # Debug print
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(ve)})
        }

    except Exception as e:
        print('Error:', str(e))  # Debug print
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
