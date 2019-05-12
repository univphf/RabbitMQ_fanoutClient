package com.ht.dev.fanoutc;

import com.rabbitmq.client.*;
import java.io.IOException;

public class ReceiveLogs {
  private static final String EXCHANGE_NAME = "logs";

  public static void main(String[] argv) throws Exception
  {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    //definir un exchange de type FANOUT
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

    //Declarer une file d'attente dans rabbitMQ et recuperer son nom
    String queueName = channel.queueDeclare().getQueue();

    //on va binder cette file sur l'exchange sans prise en compte
    //d'un routage via une routing key
    channel.queueBind(queueName, EXCHANGE_NAME, "");

    System.out.println(" [*] Attente de messages.CTRL+C pour quitter");

    Consumer consumer = new DefaultConsumer(channel)
    {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,AMQP.BasicProperties properties, byte[] body) throws IOException
      {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Reception de : '" + message + "'");
      }
    };

    //mettre en place le listener et la fct callback de traitement
    channel.basicConsume(queueName, true, consumer);
  }

}

