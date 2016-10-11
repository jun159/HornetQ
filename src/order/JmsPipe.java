package order;

/**
 * Matric 1: A0126258A
 * Name   1: Luah Bao Jun
 * 
 * Matric 2: A0117057J
 * Name   2: Varunica
 *
 * This file implements a pipe that transfer messages using JMS.
 */

import java.util.Properties;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JmsPipe implements IPipe {
    
    // Sender and receiver
    private QueueSender sender;
    private QueueReceiver receiver;
    
    // Helper classes
    private QueueConnectionFactory connectionFactory;
    private QueueConnection connection;
    private QueueSession session;
   
    // Misc
    private Queue queue;
    private TextMessage senderMessage;

    public JmsPipe(String factoryName, String queueName) 
            throws NamingException, JMSException {
        
        InitialContext initialContext = getInitialContext();
        connectionFactory = (QueueConnectionFactory) initialContext.lookup(factoryName);
        connection = connectionFactory.createQueueConnection();      
        connection.start();
        session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = (Queue) initialContext.lookup(queueName);
        senderMessage = session.createTextMessage();
    }

    @Override
    public void write(Order s) {
        try {
            if(sender == null) {
                sender = session.createSender(queue);
            }
            senderMessage.setText(s.toString());
            sender.send(senderMessage);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Order read() {
        Order order = null;
        
        try {
            if(receiver == null) {
                receiver = session.createReceiver(queue);
            }
            TextMessage receiverMessage = (TextMessage) receiver.receive();
            return Order.fromString(receiverMessage.getText());
        } catch (JMSException e) {
            e.printStackTrace();
        }
        
        return order;
    }

    @Override
    public void close() {
        try {
            session.close();
            connection.close();
            sender.close();
            receiver.close();     
        }
        catch (JMSException e) {
            e.printStackTrace();
        }
    }
    
    private static InitialContext getInitialContext()
            throws NamingException {
        Properties props = new Properties();
        props.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
        props.put(Context.PROVIDER_URL, "jnp://localhost:1099");
        props.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
        return new InitialContext(props);
    }
}
