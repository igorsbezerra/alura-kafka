package dev.igor.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var myProducer = new KafkaMyProducer()) {
            for (var i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var value = key + ",43214321,190,90";
                myProducer.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are processing your order!";
                myProducer.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
