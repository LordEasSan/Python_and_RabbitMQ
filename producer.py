#!/usr/bin/python3

import pika

print('Collegamento a RabbitMQ...')

# in questo modo noi stiamo chiedendo di creare un oggetto che rappresenta
# i parametri di connessione a una istanza di Rabbit mq che gira sulla stessa
# macchina sulla quale poi verrà eseguito il nostro progetto.

params = pika.ConnectionParameters(host='localhost')

# ora dobbiamo creare un oggetto che rappresenta la connessione

connection = pika.BlockingConnection(params)
# blocking Connection una connessione che blocca per così dire il producer in aggancio sulla Exchange

# a questo punto possiamo chiedere il canale

channel = connection.channel()


channel.queue_declare(queue='worker_queue')

print('...eseguito')

i = 0
while True:
    message = str(i)
    i += 1
# ora dobbiamo consegnare questo messaggio a RabbitMQ su un Exchange, il primo parametro è qual'è
# l'exchange che vogliamo usare, il secondo parametro è la definizione del Binding, siccome non ci
# sono regole di instradamento particolari noi chiediamo solo al nostro exchange di consegnare tutto
# quello che arriva sull'exchange ad una coda, quindi possiamo fornire semplicemente il nome della
# coda e infine dobbiamo inserire il corpo del messaggio.
    channel.basic_publish(exchange='', routing_key='worker_queue', body=message)

    print('inviato messaggio %s', message)
    
    if i > 100_000:
        break

connection.close()
