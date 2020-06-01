(ns kafka-test.core
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clj-log4j2.core :as log])
  (:import [java.util Properties]
           [org.apache.kafka.clients.producer ProducerRecord ProducerConfig KafkaProducer]
           [org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer ConsumerRecords ConsumerRecord]
           [java.util Arrays]
           [java.time Duration]))

(defn listen-producer
  []
  (let [newProps (Properties.)]
    (.put newProps ProducerConfig/BOOTSTRAP_SERVERS_CONFIG "localhost:9092")
    (.put newProps "acks", "all")
    (.put newProps "retries" (int 0))
    (.put newProps "batch.size" (int 16384))
    (.put newProps "buffer.memory" 33554432)
    (.put newProps "linger.ms" 1)
    (.put newProps "key.serializer" "org.apache.kafka.common.serialization.StringSerializer")
    (.put newProps "value.serializer" "org.apache.kafka.common.serialization.StringSerializer")
    (KafkaProducer. newProps)))

(defn consumer-prop
  []
  (let [consumer-prop (Properties.)]
    (.put consumer-prop "bootstrap.servers" "localhost:9092")
    (.put consumer-prop "group.id" "text-reading-group")
    (.put consumer-prop "enable.auto.commit" true)
    (.put consumer-prop "auto.commit.interval.ms" (int 1000))
    (.put consumer-prop ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest")
    (.put consumer-prop "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    (.put consumer-prop "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer")
    consumer-prop))

(def topics (atom {}))

(defn consume
  [records stopwords]
  (mapv
   (fn
     [record]
     (mapv
      (fn [word]
        (let [word (str/lower-case word)]
          (when (not (contains? stopwords word))
            (swap! topics update word #(-> % (or 0) inc) ))))
      (str/split (.value record) #"\s")))
   (iterator-seq (.iterator records)))
  (spit "count-words.txt" @topics))

(defn produce
  [resource-file]
  (with-open [rdr (io/reader (io/resource resource-file))]
    (let [listen-producer (listen-producer)]
      (doseq [line (line-seq rdr)]
        (let [[key plot] (str/split line #"\t")]
          (log/info  key)
          (.send listen-producer (ProducerRecord. "voice" key plot ))
          ))
      (.close listen-producer))))

(defn consume-records
  "I don't do a whole lot ... yet."
  []
  (let [listen-consumer (KafkaConsumer. (consumer-prop))
        stopwords (with-open [rdr (io/reader (io/resource "stopword.txt"))]
                    (set (mapv #(-> % str/lower-case str/trim) (line-seq rdr))))]
    (.subscribe listen-consumer '("voice"))
    (while true
      (consume (.poll listen-consumer (Duration/ofMillis (* 100 60))) stopwords))))

(defn -main
  [command & {:strs [--topic --file]}]
  (cond
    (= command "produce") (produce --file)
    (= command "consume") (consume-records)))
