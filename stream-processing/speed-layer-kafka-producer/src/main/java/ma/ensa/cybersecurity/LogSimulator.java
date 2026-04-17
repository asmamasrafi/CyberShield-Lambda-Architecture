package ma.ensa.cybersecurity;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class LogSimulator {

    public static void main(String[] args) {
        // 1. Configuration de la connexion à Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094"); // L'adresse de ton Docker Kafka
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2. Création du Producteur
        Producer<String, String> producer = new KafkaProducer<>(props);
        String topicName = "cybersecurity-logs";
        String csvFilePath = "cybersecurity_threat_detection_logs.csv";

        System.out.println("🚀 Démarrage de la simulation réseau vers Kafka...");

        // 3. Lecture du fichier CSV et envoi en continu
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            boolean isFirstLine = true;

            while ((line = br.readLine()) != null) {
                // On saute la première ligne si ce sont les en-têtes (noms des colonnes)
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                // Envoi de la ligne de log à Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, line);
                producer.send(record);

                System.out.println("✅ Log envoyé : " + line);

                // Simulation du temps réel : pause de 0.5 seconde
                Thread.sleep(500); 
            }
        } catch (Exception e) {
            System.err.println("❌ Erreur : " + e.getMessage());
            System.err.println("Vérifie que le fichier CSV est bien à la racine de ton projet !");
        } finally {
            producer.close();
            System.out.println("🛑 Fin de la transmission.");
        }
    }
}