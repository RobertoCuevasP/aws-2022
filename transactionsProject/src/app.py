import json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')
def transaction(event, context):
    body = json.loads(event["body"])
    sender_name = body["sender"] # user_1
    receiver_name = body["receiver"] #user_2
    amount = int(body["amount"]) #10000$
    anomaly = "0"
    table = dynamodb.Table('bankTransactions')
    response = table.get_item(
        Key={
            'pk': 'user',
            'sk': sender_name
        }
    )
    sender = response["Item"]
    #print(response)
    #print(sender)
    
    response2 = table.get_item(
        Key={
            'pk': 'user',
            'sk': receiver_name
        }
    )
    receiver = response2["Item"]
    #Insertar transaccion en la tabla
    time = datetime.datetime.now()
    day = str(datetime.date.today())
    transaction_name = 'transaction_' + sender_name
    
    #Verificacion de anomalias
    #Datos para anomalia 2
    #transactionsSender = table.query(
    #    KeyConditionExpression='pk = :pk AND begins_with ( sk , :sk )',
    #    ExpressionAttributeValues={
    #        ':pk': {'S': f'transaction_{sender_name}'},
    #        ':sk': {'S': {day} }
    #    }
    #)
    
    transactionsSender = table.query(
        KeyConditionExpression = Key('pk').eq(transaction_name) & Key('sk').begins_with(day)
    )
    
    print(transactionsSender['Items'])
    print(len(transactionsSender['Items']))
    
    #Datos para anomalia 3
    result_amount_sender = int(sender['saldo_total']) - amount
    
    message = "Transaction successful"
    if result_amount_sender < 0:
        anomaly = "4"
        message = "No tiene saldo suficiente"
    #Anomalia 1
    elif amount >= 20000 and int(receiver['ganancias_mes']) <= 2000:
        anomaly = "1"
        #Escribir el mensaje de la anomalia
        message = "La persona que recibe (envia) gana menos de $2000"
    #Anomalia 2
    elif len(transactionsSender['Items']) >= 5:
        anomaly = "2"
        #Escribir mensaje
        message = "El user ya realizo mas de 5 transacciones en el dia"
    #Anomalia 3
    elif amount >= 10000 and result_amount_sender <= 100:
        anomaly = "3"
        #Escribir mensaje
        message = "A su saldo le quedaria menos de 100 dolares"

    table.put_item(
       Item={
            'pk': f'transaction_{sender_name}',
            'sk': f'{time}#{receiver_name}', 
            'amount': str(amount),
            'anomaly': anomaly
        }
    )
    
    #Si no hay anomalia, modificar campos de sender y receiver
    if anomaly == "0":
        #Actualizar sender y receiver
        result_amount_receiver = int(receiver['saldo_total']) + amount
        final_amount_receiver = str(result_amount_receiver)
        final_amount_sender = str(result_amount_sender)
        #Sender
        table.update_item(
            Key={
                'pk': 'user',
                'sk': sender_name
            },
            UpdateExpression='SET saldo_total = :sal',
            ExpressionAttributeValues={
                ':sal': final_amount_sender
            }
        )
        #Receiver
        table.update_item(
            Key={
                'pk': 'user',
                'sk': receiver_name
            },
            UpdateExpression='SET saldo_total = :sal',
            ExpressionAttributeValues={
                ':sal': final_amount_receiver
            }
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(message)
        }
    else: 
        return {
            'statusCode': 401,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(message)
        }


def get_users(event, context):
    table = dynamodb.Table('bankTransactions')
    response = table.query(
        KeyConditionExpression=Key('pk').eq("user")
    )
    #print(response)
    return {
        'statusCode': 200,
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
        'body': json.dumps(response["Items"])
    }
    
def get_transactions(event, context):
    table = dynamodb.Table('bankTransactions')
    response = table.scan(
        FilterExpression=Attr('pk').begins_with("transaction")
    )
    
    return {
        'statusCode': 200,
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
        'body': json.dumps(response["Items"])
    }

def get_transactions_from_user(event, context):
    
    user = event["pathParameters"]["user"]
    table = dynamodb.Table('bankTransactions')
    pk = "transaction_" + user
    response = table.query(
        KeyConditionExpression=Key('pk').eq(pk)
    )
    return {
        'statusCode': 200,
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
        'body': json.dumps(response["Items"])
    }