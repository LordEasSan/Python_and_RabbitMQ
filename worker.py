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

def callback(ch, method, properties, body):
    print('Ricevuto %s' % body)

# il punto in cui noi però andiamo a chiedere a Rabbit di ricevere dalla coda ogni singolo messaggio
# viene fatto in questo punto chiedendo all'oggetto Channel di eseguire quello che si chiama basic
# consume, questo metodo richiede di specificare qual'è la callback ovvero la funzione che dovrà
# essere chiamata automaticamente ogni volta che arriva un messaggio dalla coda, poi dobbiamo 
# definire qual'è la coda dalla quale noi vogliamo ricevere il messaggio ed infine dobbiamo 
# specificare che non garantiremo a Rabbit di aver ricevuto il messaggio ( acknowledge )


channel.basic_consume(queue='worker_queue', on_message_callback = callback, auto_ack=False)

channel.start_consuming()
