import json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')
def transaction(event, context):
    body = json.loads(event["body"])
    #Datos de la transaccion
    sender_name = body["sender"] 
    receiver_name = body["receiver"]
    amount = int(body["amount"]) 
    anomaly = "0"
    
    #Obtencion de los usuarios
    table = dynamodb.Table('bankTransactions')
    response = table.get_item(
        Key={
            'pk': 'user',
            'sk': sender_name
        }
    )
    sender = response["Item"]
    
    response2 = table.get_item(
        Key={
            'pk': 'user',
            'sk': receiver_name
        }
    )
    receiver = response2["Item"]
    time = datetime.datetime.now()
    day = str(datetime.date.today())
    
    #Obtencion de transacciones realizadas por un usuario en la fecha (anomalia 2)
    transaction_name = 'transaction_' + sender_name
    
    transactionsSender = table.query(
        KeyConditionExpression = Key('pk').eq(transaction_name) & Key('sk').begins_with(day)
    )
    
    #Calculo previo para verificar anomalia 3
    result_amount_sender = int(sender['saldo_total']) - amount
    
    #Verificacion de anomalias
    message = "Transaction successful"
    #Anomalia base
    if result_amount_sender < 0:
        anomaly = "4"
        message = "No tiene saldo suficiente"
    #Anomalia 1
    elif amount >= 20000 and int(receiver['ganancias_mes']) <= 2000:
        anomaly = "1"
        message = "La persona que recibe (envia) gana menos de $2000"
    #Anomalia 2
    elif len(transactionsSender['Items']) >= 5:
        anomaly = "2"
        message = "El user ya realizo mas de 5 transacciones en el dia"
    #Anomalia 3
    elif amount >= 10000 and result_amount_sender <= 100:
        anomaly = "3"
        message = "A su saldo le quedaria menos de 100 dolares"
    
    #Insertar transaccion en la tabla
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